# component.py
"""Module to implement an abstract base Component for the Long Term Archive."""

import asyncio
from datetime import datetime
from logging import Logger
import os
from pathlib import Path
import sys
from typing import Any, Dict, Optional
from uuid import uuid4

from rest_tools.client import ClientCredentialsAuth, RestClient
import wipac_telemetry.tracing_tools as wtt

from .lta_const import drain_semaphore_filename
from .rest_server import boolify

COMMON_CONFIG: Dict[str, Optional[str]] = {
    "CLIENT_ID": None,
    "CLIENT_SECRET": None,
    "COMPONENT_NAME": None,
    "DEST_SITE": None,
    "INPUT_STATUS": None,
    "LOG_LEVEL": "NOTSET",
    "LTA_AUTH_OPENID_URL": None,
    "LTA_REST_URL": None,
    "OUTPUT_STATUS": None,
    "PROMETHEUS_METRICS_PORT": "8080",
    "RUN_ONCE_AND_DIE": "False",
    "RUN_UNTIL_NO_WORK": "False",
    "SOURCE_SITE": None,
    "WORK_RETRIES": "3",
    "WORK_SLEEP_DURATION_SECONDS": "60",
    "WORK_TIMEOUT_SECONDS": "30",
}

LOGGING_DENY_LIST = ["CLIENT_SECRET", "FILE_CATALOG_CLIENT_SECRET"]


def now() -> str:
    """Return string timestamp for current time, to the second."""
    return datetime.utcnow().isoformat(timespec='seconds')


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
        self.client_id = config["CLIENT_ID"]
        self.client_secret = config["CLIENT_SECRET"]
        self.dest_site = config["DEST_SITE"]
        self.input_status = config["INPUT_STATUS"]
        self.lta_auth_openid_url = config["LTA_AUTH_OPENID_URL"]
        self.lta_rest_url = config["LTA_REST_URL"]
        self.output_status = config["OUTPUT_STATUS"]
        self.run_once_and_die = boolify(config["RUN_ONCE_AND_DIE"])
        self.run_until_no_work = boolify(config["RUN_UNTIL_NO_WORK"])
        self.source_site = config["SOURCE_SITE"]
        self.work_retries = int(config["WORK_RETRIES"])
        self.work_sleep_duration_seconds = float(config["WORK_SLEEP_DURATION_SECONDS"])
        self.work_timeout_seconds = float(config["WORK_TIMEOUT_SECONDS"])
        # record some default state
        timestamp = datetime.utcnow().isoformat()
        self.last_work_begin_timestamp = timestamp
        self.last_work_end_timestamp = timestamp
        # log the way this component has been configured
        self.logger.info(f"{self.type} '{self.name}' is configured:")
        for name in config:
            if name in LOGGING_DENY_LIST:
                self.logger.info(f"{name} = [秘密]")
            else:
                self.logger.info(f"{name} = {config[name]}")

    @wtt.spanned()
    async def run(self) -> None:
        """Perform the Component's work cycle."""
        self.logger.info(f"Starting {self.type} work cycle")
        # obtain a RestClient to talk to the LTA REST service (LTA DB)
        lta_rc = ClientCredentialsAuth(address=self.lta_rest_url,
                                       token_url=self.lta_auth_openid_url,
                                       client_id=self.client_id,
                                       client_secret=self.client_secret,
                                       timeout=self.work_timeout_seconds,
                                       retries=self.work_retries)
        # start the work cycle stopwatch
        self.last_work_begin_timestamp = datetime.utcnow().isoformat()
        # perform the work
        try:
            await self._do_work(lta_rc)
        except Exception as e:
            # ut oh, something went wrong; log about it
            self.logger.error(f"Error occurred during the {self.type} work cycle")
            self.logger.error(f"Error was: '{e}'", exc_info=True)
        # stop the work cycle stopwatch
        self.last_work_end_timestamp = datetime.utcnow().isoformat()
        self.logger.info(f"Ending {self.type} work cycle")
        # if we are configured to run until no work, then die
        if self.run_until_no_work:
            sys.exit()

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

    async def _do_work(self, lta_rc: RestClient) -> None:
        """Override this to provide work cycle behavior."""
        raise NotImplementedError()


def check_drain_semaphore(component: Component) -> bool:
    """Check if a drain semaphore exists in the current working directory."""
    cwd = os.getcwd()
    semaphore_name = drain_semaphore_filename(component.type)
    semaphore_path = os.path.join(cwd, semaphore_name)
    return Path(semaphore_path).exists()


async def work_loop(component: Component) -> None:
    """Run component work cycles as an infinite loop."""
    component.logger.info("Starting work loop")
    while not check_drain_semaphore(component):
        # Do the work of the component
        await component.run()
        # sleep until we need to work again
        await asyncio.sleep(component.work_sleep_duration_seconds)
    component.logger.info("Component drained; shutting down.")
