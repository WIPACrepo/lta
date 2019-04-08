# replicator.py
"""Module to implement the Replicator component of the Long Term Archive."""

import asyncio
import json
from logging import Logger
import logging
import sys
from typing import Any, Dict, Optional

from rest_tools.client import RestClient  # type: ignore

from .component import COMMON_CONFIG, Component, status_loop, work_loop
from .config import from_environment
from .log_format import StructuredFormatter

EXPECTED_CONFIG = COMMON_CONFIG.copy()
EXPECTED_CONFIG.update({
    "LTA_SITE_CONFIG": "etc/site.json",
    "WORK_RETRIES": "3",
    "WORK_TIMEOUT_SECONDS": "30",
})


class Replicator(Component):
    """
    Replicator is a Long Term Archive component.

    A Replicator is responsible for registering completed archive bundles
    with the Rucio transfer service. Rucio will then replicate the bundle
    from the source (i.e.: WIPAC Data Warehouse) to the destination(s),
    (i.e.: DESY, NERSC DTN).

    It uses the LTA DB to find completed bundles that need to be registered.
    It registers the bundles with Rucio. It updates the Bundle and the
    corresponding TransferRequest in the LTA DB with a 'transferring' status.
    """

    def __init__(self, config: Dict[str, str], logger: Logger) -> None:
        """
        Create a Replicator component.

        config - A dictionary of required configuration values.
        logger - The object the bundler should use for logging.
        """
        super(Replicator, self).__init__("replicator", config, logger)
        self.work_retries = int(config["WORK_RETRIES"])
        self.work_timeout_seconds = float(config["WORK_TIMEOUT_SECONDS"])
        with open(config["LTA_SITE_CONFIG"]) as site_data:
            self.lta_site_config = json.load(site_data)
        self.sites = self.lta_site_config["sites"]

    def _do_status(self) -> Dict[str, Any]:
        """Replicator has no additional status to contribute."""
        return {}

    def _expected_config(self) -> Dict[str, Optional[str]]:
        """Replicator provides our expected configuration dictionary."""
        return EXPECTED_CONFIG

    async def _do_work(self) -> None:
        """Perform a work cycle for this component."""
        await self._consume_bundles_to_replicate()

    async def _consume_bundles_to_replicate(self) -> None:
        """Consume bundles from the LTA DB and register them with Rucio."""
        # TODO: Perform the work against rucio
        # 1. Pop bundles from the LTA DB
        # 2. for each bundle
        # 2.1. upload and regsiter the bundle to rucio
        #     rucio upload --rse $RSE --scope SCOPE --register-after-upload --pfn PFN --name NAME /PATH/TO/BUNDLE
        # 2.2. for each destination site
        # 2.2.1. add the BUNDLE_DID from 2.1 to the replica
        #     rucio attach DEST_CONTAINER_DID BUNDLE_DID
        # 2.3. update the Bundle in the LTA DB; registration information
        # 2.4. update the TransferRequest in the LTA DB; state: [None] -> [Transferring]
        lta_rc = RestClient(self.lta_rest_url,
                            token=self.lta_rest_token,
                            timeout=self.work_timeout_seconds,
                            retries=self.work_retries)
        await lta_rc.request("GET", "/Bundles")
        # inform the log that we've finished out work cycle
        self.logger.info(f"Replicator work cycle complete. Going on vacation.")


def runner() -> None:
    """Configure a Replicator component from the environment and set it running."""
    # obtain our configuration from the environment
    config = from_environment(EXPECTED_CONFIG)
    # configure structured logging for the application
    structured_formatter = StructuredFormatter(
        component_type='Replicator',
        component_name=config["COMPONENT_NAME"],
        ndjson=True)
    stream_handler = logging.StreamHandler(sys.stdout)
    stream_handler.setFormatter(structured_formatter)
    root_logger = logging.getLogger(None)
    root_logger.setLevel(logging.NOTSET)
    root_logger.addHandler(stream_handler)
    logger = logging.getLogger("lta.bundler")
    # create our Replicator service
    bundler = Replicator(config, logger)
    # let's get to work
    bundler.logger.info("Adding tasks to asyncio loop")
    loop = asyncio.get_event_loop()
    loop.create_task(status_loop(bundler))
    loop.create_task(work_loop(bundler))


def main() -> None:
    """Configure a Replicator component from the environment and set it running."""
    runner()
    asyncio.get_event_loop().run_forever()


if __name__ == "__main__":
    main()
