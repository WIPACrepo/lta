# component.py
"""Module to implement an abstract base Component for the Long Term Archive."""

import asyncio
from datetime import datetime
from logging import Logger
import os
from pathlib import Path
from typing import Any, Dict, Optional
from uuid import uuid4

from rest_tools.client import RestClient  # type: ignore
from urllib.parse import urljoin

from .lta_const import drain_semaphore_filename

COMMON_CONFIG: Dict[str, Optional[str]] = {
    "COMPONENT_NAME": None,
    "HEARTBEAT_PATCH_RETRIES": None,
    "HEARTBEAT_PATCH_TIMEOUT_SECONDS": None,
    "HEARTBEAT_SLEEP_DURATION_SECONDS": None,
    "LTA_REST_TOKEN": None,
    "LTA_REST_URL": None,
    "SOURCE_SITE": None,
    "WORK_SLEEP_DURATION_SECONDS": None,
}

def unique_id() -> str:
    """Return a unique ID for a module instance."""
    return str(uuid4())

class Component:
    """
    Component is a Long Term Archive component.

    This is an abstract base class for all Long Term Archive components,
    providing the boilerplate component behavior. Subclasses implement the
    particular work cycle behaviors.
    """

    def __init__(self,
                 component_type: str,
                 config: Dict[str, str],
                 logger: Logger) -> None:
        """
        Create an LTA component.

        component_type - The type of the Component; picker, bundler, etc.
        component_name - The name of the Component; node16-picker, node23-bundler, etc.
        config - A dictionary of configuration values.
        logger - The object the Component should use for logging.
        """
        # validate the provided configuration
        self.validate_config(config)
        # assimilate provided arguments
        self.type = component_type
        self.name = config["COMPONENT_NAME"]
        self.instance_uuid = unique_id()
        self.config = config
        self.logger = logger
        # validate and assimilate the configuration
        self.heartbeat_patch_retries = int(config["HEARTBEAT_PATCH_RETRIES"])
        self.heartbeat_patch_timeout_seconds = float(config["HEARTBEAT_PATCH_TIMEOUT_SECONDS"])
        self.heartbeat_sleep_duration_seconds = float(config["HEARTBEAT_SLEEP_DURATION_SECONDS"])
        self.lta_rest_token = config["LTA_REST_TOKEN"]
        self.lta_rest_url = config["LTA_REST_URL"]
        self.source_site = config["SOURCE_SITE"]
        self.work_sleep_duration_seconds = float(config["WORK_SLEEP_DURATION_SECONDS"])
        # record some default state
        timestamp = datetime.utcnow().isoformat()
        self.last_work_begin_timestamp = timestamp
        self.last_work_end_timestamp = timestamp
        # log the way this component has been configured
        self.logger.info(f"{self.type} '{self.name}' is configured:")
        for name in config:
            self.logger.info(f"{name} = {config[name]}")

    async def run(self) -> None:
        """Perform the Component's work cycle."""
        self.logger.info(f"Starting {self.type} work cycle")
        # start the work cycle stopwatch
        self.last_work_begin_timestamp = datetime.utcnow().isoformat()
        # perform the work
        try:
            await self._do_work()
        except Exception as e:
            # ut oh, something went wrong; log about it
            self.logger.error(f"Error occurred during the {self.type} work cycle")
            self.logger.error(f"Error was: '{e}'", exc_info=True)
        # stop the work cycle stopwatch
        self.last_work_end_timestamp = datetime.utcnow().isoformat()
        self.logger.info(f"Ending {self.type} work cycle")

    def validate_config(self, config: Dict[str, str]) -> None:
        """Validate the configuration provided to the component."""
        # these are the configuration variables required of all components
        for name in COMMON_CONFIG:
            if name not in config:
                raise ValueError(f"Missing expected configuration parameter: '{name}'")
            if not config[name]:
                raise ValueError(f"Missing expected configuration parameter: '{name}'")
        # these are the configuration variables required by the subclass
        EXPECTED_CONFIG = self._expected_config()
        for name in EXPECTED_CONFIG:
            if name not in config:
                raise ValueError(f"Missing expected configuration parameter: '{name}'")
            if not config[name]:
                raise ValueError(f"Missing expected configuration parameter: '{name}'")

    def _do_status(self) -> Dict[str, Any]:
        """Override this to provide status updates."""
        raise NotImplementedError()

    def _expected_config(self) -> Dict[str, Optional[str]]:
        """Override this to return expected configuration."""
        raise NotImplementedError()

    async def _do_work(self) -> None:
        """Override this to provide work cycle behavior."""
        raise NotImplementedError()


def check_drain_semaphore(component: Component) -> bool:
    """Check if a drain semaphore exists in the current working directory."""
    cwd = os.getcwd()
    semaphore_name = drain_semaphore_filename(component.type)
    semaphore_path = os.path.join(cwd, semaphore_name)
    return Path(semaphore_path).exists()


async def patch_status_heartbeat(component: Component) -> bool:
    """PATCH /status/{component} to update LTA with a status heartbeat."""
    component.logger.info("Sending status heartbeat")
    # determine which resource to PATCH
    status_route = f"/status/{component.type}"
    status_url = urljoin(component.lta_rest_url, status_route)
    # determine the body to PATCH with
    status_body = {
        component.name: {
            "timestamp": datetime.utcnow().isoformat(),
            "last_work_begin_timestamp": component.last_work_begin_timestamp,
            "last_work_end_timestamp": component.last_work_end_timestamp,
        }
    }
    # ask the base class to annotate the status body
    status_update = component._do_status()
    status_body.update(status_update)
    # attempt to PATCH the status resource
    component.logger.info(f"PATCH {status_url} - {status_body}")
    try:
        rc = RestClient(component.lta_rest_url,
                        token=component.lta_rest_token,
                        timeout=component.heartbeat_patch_timeout_seconds,
                        retries=component.heartbeat_patch_retries)
        # Use the RestClient to PATCH our heartbeat to the LTA DB
        await rc.request("PATCH", status_route, status_body)
    except Exception as e:
        # if there was a problem, yo I'll solve it
        component.logger.error(f"Error trying to PATCH {status_route} with heartbeat")
        component.logger.error(f"Error was: '{e}'", exc_info=True)
        return False
    # indicate to the caller that the heartbeat was successful
    return True


async def status_loop(component: Component) -> None:
    """Run status heartbeat updates as an infinite loop."""
    component.logger.info("Starting status loop")
    while not check_drain_semaphore(component):
        # PATCH /status/{component}
        await patch_status_heartbeat(component)
        # sleep until we PATCH the next heartbeat
        await asyncio.sleep(component.heartbeat_sleep_duration_seconds)
    component.logger.info("Ending status heartbeats; drain semaphore detected.")


async def work_loop(component: Component) -> None:
    """Run component work cycles as an infinite loop."""
    component.logger.info("Starting work loop")
    while not check_drain_semaphore(component):
        # Do the work of the component
        await component.run()
        # sleep until we need to work again
        await asyncio.sleep(component.work_sleep_duration_seconds)
    component.logger.info("Component drained; shutting down.")
