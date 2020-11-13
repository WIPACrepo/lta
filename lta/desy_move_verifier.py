# desy_move_verifier.py
"""Module to implement the DesyMoveVerifier component of the Long Term Archive."""

import asyncio
import json
from logging import Logger
import logging
import os
import sys
from typing import Any, Dict, Optional
from urllib.parse import urlparse

from rest_tools.client import RestClient  # type: ignore
from rest_tools.server import from_environment  # type: ignore

from .component import COMMON_CONFIG, Component, now, status_loop, work_loop
from .log_format import StructuredFormatter
from .lta_types import BundleType
from .transfer.service import instantiate

EXPECTED_CONFIG = COMMON_CONFIG.copy()
EXPECTED_CONFIG.update({
    "DEST_SITE": None,
    "NEXT_STATUS": None,
    "TRANSFER_CONFIG_PATH": "etc/rucio.json",
    "WORK_RETRIES": "3",
    "WORK_TIMEOUT_SECONDS": "30",
})

OLD_MTIME_EPOCH_SEC = 30 * 60  # 30 MINUTES * 60 SEC_PER_MIN


class DesyMoveVerifier(Component):
    """
    DesyMoveVerifier is a Long Term Archive component.

    A DesyMoveVerifier is responsible for verifying that a transfer to a
    destination site has completed successfully. The transfer service is
    queried as to the status of its work.
    """

    def __init__(self, config: Dict[str, str], logger: Logger) -> None:
        """
        Create a DesyMoveVerifier component.

        config - A dictionary of required configuration values.
        logger - The object the desy_move_verifier should use for logging.
        """
        super(DesyMoveVerifier, self).__init__("desy_move_verifier", config, logger)
        self.dest_site = config["DEST_SITE"]
        self.next_status = config["NEXT_STATUS"]
        with open(config["TRANSFER_CONFIG_PATH"]) as config_data:
            self.transfer_config = json.load(config_data)
        self.work_retries = int(config["WORK_RETRIES"])
        self.work_timeout_seconds = float(config["WORK_TIMEOUT_SECONDS"])
        pass

    def _do_status(self) -> Dict[str, Any]:
        """DesyMoveVerifier has no additional status to contribute."""
        return {}

    def _expected_config(self) -> Dict[str, Optional[str]]:
        """Provide expected configuration dictionary."""
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
        # 1. Ask the LTA DB for the next Bundle to be verified
        # configure a RestClient to talk to the LTA DB
        lta_rc = RestClient(self.lta_rest_url,
                            token=self.lta_rest_token,
                            timeout=self.work_timeout_seconds,
                            retries=self.work_retries)
        self.logger.info("Asking the LTA DB for a Bundle to verify.")
        pop_body = {
            "claimant": f"{self.name}-{self.instance_uuid}"
        }
        response = await lta_rc.request('POST', f'/Bundles/actions/pop?dest={self.dest_site}&status=transferring', pop_body)
        self.logger.info(f"LTA DB responded with: {response}")
        bundle = response["bundle"]
        if not bundle:
            self.logger.info("LTA DB did not provide a Bundle to verify. Going on vacation.")
            return False
        # process the Bundle that we were given
        try:
            await self._verify_bundle(lta_rc, bundle)
        except Exception as e:
            await self._quarantine_bundle(lta_rc, bundle, f"{e}")
            raise e
        # if we were successful at processing work, let the caller know
        return True

    async def _quarantine_bundle(self,
                                 lta_rc: RestClient,
                                 bundle: BundleType,
                                 reason: str) -> None:
        """Quarantine the supplied bundle using the supplied reason."""
        self.logger.error(f'Sending Bundle {bundle["uuid"]} to quarantine: {reason}.')
        right_now = now()
        patch_body = {
            "status": "quarantined",
            "reason": f"BY:{self.name}-{self.instance_uuid} REASON:{reason}",
            "work_priority_timestamp": right_now,
        }
        try:
            await lta_rc.request('PATCH', f'/Bundles/{bundle["uuid"]}', patch_body)
        except Exception as e:
            self.logger.error(f'Unable to quarantine Bundle {bundle["uuid"]}: {e}.')

    async def _verify_bundle(self, lta_rc: RestClient, bundle: BundleType) -> bool:
        """Verify the provided Bundle with the transfer service and update the LTA DB."""
        bundle_id = bundle["uuid"]
        # we're going to ask Rucio, "Are you done yet?"
        dest = bundle["dest"]
        pfn_prefix = self.transfer_config["sites"][dest]["pfn"]  # UGLY: hard-coded RucioTransferService dependency
        parsed_url = urlparse(pfn_prefix)
        rucio_path = parsed_url.path
        bundle_name = os.path.basename(bundle["bundle_path"])
        bundle_path = os.path.join(rucio_path, bundle_name)
        # we'll check to see what Rucio thinks about the file
        self.logger.info(f"Querying Rucio about Bundle file {bundle_path}")
        # instantiate a TransferService to query about the bundle
        xfer_service = instantiate(self.transfer_config)
        # ask the transfer service about the status of the transfer
        xfer_status = await xfer_service.status(bundle["transfer_reference"])
        self.logger.info(f"Bundle transfer status is: {xfer_status}")
        # if it's not completed, we're still waiting to verify it
        if not xfer_status["completed"]:
            self.logger.info(f"Transfer of Bundle {bundle_id} is incomplete and will not be verified at this time.")
            await self._unclaim_bundle(lta_rc, bundle)
            return False
        # update the Bundle in the LTA DB
        self.logger.info(f"Rucio says Bundle file {bundle_path} has finished transferring.")
        patch_body = {
            "status": self.next_status,
            "reason": "",
            "update_timestamp": now(),
            "claimed": False,
        }
        self.logger.info(f"PATCH /Bundles/{bundle_id} - '{patch_body}'")
        await lta_rc.request('PATCH', f'/Bundles/{bundle_id}', patch_body)
        return True

    async def _unclaim_bundle(self, lta_rc: RestClient, bundle: BundleType) -> bool:
        """Run the myquota command to determine disk usage at the site."""
        self.logger.info("Bundle is not ready to be verified; will unclaim it.")
        bundle_id = bundle["uuid"]
        right_now = now()
        patch_body: Dict[str, Any] = {
            "update_timestamp": right_now,
            "claimed": False,
            "work_priority_timestamp": right_now,
        }
        self.logger.info(f"PATCH /Bundles/{bundle_id} - '{patch_body}'")
        await lta_rc.request('PATCH', f'/Bundles/{bundle_id}', patch_body)
        return True


def runner() -> None:
    """Configure a DesyMoveVerifier component from the environment and set it running."""
    # obtain our configuration from the environment
    config = from_environment(EXPECTED_CONFIG)
    # configure structured logging for the application
    structured_formatter = StructuredFormatter(
        component_type='DesyMoveVerifier',
        component_name=config["COMPONENT_NAME"],
        ndjson=True)
    stream_handler = logging.StreamHandler(sys.stdout)
    stream_handler.setFormatter(structured_formatter)
    root_logger = logging.getLogger(None)
    root_logger.setLevel(logging.NOTSET)
    root_logger.addHandler(stream_handler)
    logger = logging.getLogger("lta.desy_move_verifier")
    # create our DesyMoveVerifier service
    desy_move_verifier = DesyMoveVerifier(config, logger)
    # let's get to work
    desy_move_verifier.logger.info("Adding tasks to asyncio loop")
    loop = asyncio.get_event_loop()
    loop.create_task(status_loop(desy_move_verifier))
    loop.create_task(work_loop(desy_move_verifier))


def main() -> None:
    """Configure a DesyMoveVerifier component from the environment and set it running."""
    runner()
    asyncio.get_event_loop().run_forever()


if __name__ == "__main__":
    main()
