# component.py
"""Module to implement an abstract base Component for the Long Term Archive."""

# fmt:off

import asyncio
import itertools
import time
from logging import Logger
import os
from pathlib import Path
import sys
from typing import Any, Dict, Optional
from uuid import uuid4

from prometheus_client import Counter, Histogram
from rest_tools.client import ClientCredentialsAuth, RestClient
from wipac_dev_tools import strtobool
from wipac_dev_tools.prometheus_tools import AsyncPromWrapper, GlobalLabels, HistogramBuckets

from .lta_const import drain_semaphore_filename

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


def unique_id() -> str:
    """Return a unique ID for a module instance."""
    return str(uuid4())


# fmt:on


class PrometheusResultTracker:
    """Class to track the results of a work cycle."""

    def __init__(
        self,
        success_counter: Counter,
        failure_counter: Counter,
        histogram: Histogram,
        start_ts: float,
    ) -> None:
        self._success_counter = success_counter
        self._failure_counter = failure_counter
        self._histogram = histogram
        self._start_ts = start_ts
        self._done = False

    def record_success(self):
        """Record a successful work -- this should only be called at the end."""
        if self._done:
            raise RuntimeError("Cannot record result twice.")
        self._done = True
        self._success_counter.inc()
        self._histogram.observe(time.monotonic() - self._start_ts)

    def record_failure(self):
        """Record a failed work -- this should only be called at the end."""
        if self._done:
            raise RuntimeError("Cannot record result twice.")
        self._done = True


# fmt:off


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
        self.type = component_type  # common name of the component
        self.name = config["COMPONENT_NAME"]  # unique name of the component instance
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
        self.run_once_and_die = strtobool(config["RUN_ONCE_AND_DIE"])
        self.run_until_no_work = strtobool(config["RUN_UNTIL_NO_WORK"])
        self.source_site = config["SOURCE_SITE"]
        self.work_retries = int(config["WORK_RETRIES"])
        self.work_sleep_duration_seconds = float(config["WORK_SLEEP_DURATION_SECONDS"])
        self.work_timeout_seconds = float(config["WORK_TIMEOUT_SECONDS"])
        # log the way this component has been configured
        self.logger.info(f"{self.type} '{self.name}' is configured:")
        for name in config:
            if name in LOGGING_DENY_LIST:
                self.logger.info(f"{name} = [秘密]")
            else:
                self.logger.info(f"{name} = {config[name]}")
        # set up Prometheus metrics
        self.prometheus = GlobalLabels({
            # define everything identifiable to the component variety, but not the process
            #   IOW, we don't want new procs (k8s pods) to produce unique histograms
            'source_site': str(self.source_site),
            'dest_site': str(self.dest_site),
            'type': str(self.type),
            'input_status': str(self.input_status),
            'output_status': str(self.output_status),
        })

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
        # perform the work
        try:
            await self._do_work(lta_rc)
        except Exception as e:
            # ut oh, something went wrong; log about it
            self.logger.error(f"Error occurred during the {self.type} work cycle")
            self.logger.error(f"Error was: '{e}'")
            self.logger.exception(e)  # logs the stack trace
        self.logger.info(f"Ending {self.type} work cycle")
        # if we are configured to run until no work, then die
        if self.run_until_no_work:
            self.logger.warning("Run until no work configured -- exiting.")
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

    @AsyncPromWrapper(lambda self: self.prometheus.counter(
        "work_counts",
        "finished work counts by success/failure",
        labels=["work"],
        finalize=False,
    ))
    async def _do_work(self, prom_counter: Counter, lta_rc: RestClient) -> None:
        """Perform a work cycle for this component."""
        prometheus_histogram = self.prometheus.histogram(
            "single_work_latency_seconds",
            "time taken to process a single work item (only successes are recorded)",
            buckets=HistogramBuckets.TENMINUTE,  # TODO: do we want to make this configurable?
        )

        for i in itertools.count():
            # process a single work item
            self.logger.info(f"Requesting work on #{i} (0-indexed)...")
            prom_tracker = PrometheusResultTracker(
                prom_counter.labels({"work": "success"}),
                prom_counter.labels({"work": "failure"}),
                prometheus_histogram,
                time.monotonic(),  # instantiate now for accurate timestamp
            )
            ret = await self._do_work_claim(lta_rc, prom_tracker)

            # now, decide whether to continue or pause the work cycle
            if self.run_once_and_die:
                self.logger.warning("Run once and die configured -- exiting.")
                sys.exit()
            elif ret:
                self.logger.info("Continuing work cycle.")
                continue
            else:
                self.logger.info("Pausing work cycle.")
                break

    async def _do_work_claim(
        self,
        lta_rc: RestClient,
        prom_tracker: PrometheusResultTracker,
    ) -> bool:
        """Claim a [insert component's LTA object here] and perform work on it.

        This function is only called by '_do_work()', and the return values control
        the work cycle and whether the component should continue or pause.

        Returns:
            True  - continue the work cycle
            False - pause the work cycle

        Raises:
            Any Exception - stops the work cycle, pauses the component,
                            then resumes after some time (work_sleep_duration_seconds).
        """
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
