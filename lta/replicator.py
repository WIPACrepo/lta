# replicator.py
"""Module to implement the Replicator component of the Long Term Archive."""

import asyncio
import json
from logging import Logger
import logging
import sys
from typing import Any, Dict, Optional

from rest_tools.client import RestClient  # type: ignore

from .component import COMMON_CONFIG, Component, now, status_loop, work_loop
from .config import from_environment
from .log_format import StructuredFormatter
from .lta_types import BundleType
from .transfer.service import instantiate


EXPECTED_CONFIG = COMMON_CONFIG.copy()
EXPECTED_CONFIG.update({
    "RUCIO_PASSWORD": None,
    "TRANSFER_CONFIG_PATH": "etc/rucio.json",
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
        logger - The object the replicator should use for logging.
        """
        super(Replicator, self).__init__("replicator", config, logger)
        with open(config["TRANSFER_CONFIG_PATH"]) as config_data:
            self.transfer_config = json.load(config_data)
        self.transfer_config["password"] = config["RUCIO_PASSWORD"]
        self.work_retries = int(config["WORK_RETRIES"])
        self.work_timeout_seconds = float(config["WORK_TIMEOUT_SECONDS"])

    def _do_status(self) -> Dict[str, Any]:
        """Replicator has no additional status to contribute."""
        return {}

    def _expected_config(self) -> Dict[str, Optional[str]]:
        """Replicator provides our expected configuration dictionary."""
        return EXPECTED_CONFIG

    async def _do_work(self) -> None:
        """Perform a work cycle for this component."""
        self.logger.info("Starting work on Bundles.")
        work_claimed = True
        while work_claimed:
            work_claimed = await self._do_work_claim()
            work_claimed &= not self.run_once_and_die
        self.logger.info("Ending work on Bundles.")

    async def _do_work_claim(self) -> bool:
        """Claim a bundle and perform work on it."""
        # 1. Ask the LTA DB for the next Bundle to be transferred
        # configure a RestClient to talk to the LTA DB
        lta_rc = RestClient(self.lta_rest_url,
                            token=self.lta_rest_token,
                            timeout=self.work_timeout_seconds,
                            retries=self.work_retries)
        self.logger.info("Asking the LTA DB for a Bundle to transfer.")
        source = self.source_site
        pop_body = {
            "claimant": f"{self.name}-{self.instance_uuid}"
        }
        response = await lta_rc.request('POST', f'/Bundles/actions/pop?source={source}&status=created', pop_body)
        self.logger.info(f"LTA DB responded with: {response}")
        bundle = response["bundle"]
        if not bundle:
            self.logger.info("LTA DB did not provide a Bundle to transfer. Going on vacation.")
            return False
        # process the Bundle that we were given
        try:
            await self._replicate_bundle_to_destination_site(lta_rc, bundle)
        except Exception as e:
            await self._quarantine_bundle(lta_rc, bundle, f"{e}")
            return False
        # if we were successful at processing work, let the caller know
        return True

    async def _quarantine_bundle(self,
                                 lta_rc: RestClient,
                                 bundle: BundleType,
                                 reason: str) -> None:
        """Quarantine the supplied bundle using the supplied reason."""
        self.logger.error(f'Sending Bundle {bundle["uuid"]} to quarantine: {reason}.')
        patch_body = {
            "status": "quarantined",
            "reason": reason,
        }
        try:
            await lta_rc.request('PATCH', f'/Bundles/{bundle["uuid"]}', patch_body)
        except Exception as e:
            self.logger.error(f'Unable to quarantine Bundle {bundle["uuid"]}: {e}.')

    async def _replicate_bundle_to_destination_site(self, lta_rc: RestClient, bundle: BundleType) -> None:
        """Replicate the supplied bundle using the configured transfer service."""
        bundle_id = bundle["uuid"]
        # instantiate a TransferService to replicate the bundle
        xfer_service = instantiate(self.transfer_config, self.logger)
        # ask the transfer service to start the transfer
        xfer_ref = await xfer_service.start(bundle)
        # update the Bundle in the LTA DB
        patch_body = {
            "status": "transferring",
            "update_timestamp": now(),
            "claimed": False,
            "transfer_reference": xfer_ref,
        }
        self.logger.info(f"PATCH /Bundles/{bundle_id} - '{patch_body}'")
        await lta_rc.request('PATCH', f'/Bundles/{bundle_id}', patch_body)

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
    logger = logging.getLogger("lta.replicator")
    # create our Replicator service
    replicator = Replicator(config, logger)
    # let's get to work
    replicator.logger.info("Adding tasks to asyncio loop")
    loop = asyncio.get_event_loop()
    loop.create_task(status_loop(replicator))
    loop.create_task(work_loop(replicator))

def main() -> None:
    """Configure a Replicator component from the environment and set it running."""
    runner()
    asyncio.get_event_loop().run_forever()

if __name__ == "__main__":
    main()
