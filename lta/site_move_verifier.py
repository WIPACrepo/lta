# site_move_verifier.py
"""Module to implement the SiteMoveVerifier component of the Long Term Archive."""

import asyncio
import json
from logging import Logger
import logging
import os
import sys
from typing import Any, Dict, Optional
from urllib.parse import urlparse

from rest_tools.client import RestClient  # type: ignore

from .component import COMMON_CONFIG, Component, now, status_loop, work_loop
from .config import from_environment
from .crypto import sha512sum
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


class SiteMoveVerifier(Component):
    """
    SiteMoveVerifier is a Long Term Archive component.

    A SiteMoveVerifier is responsible for verifying that a transfer to a
    destination site has completed successfully. The transfer service is
    queried as to the status of its work. The SiteMoveVerifier then
    calculates the checksum of the file to verify that the contents have
    been copied faithfully.
    """

    def __init__(self, config: Dict[str, str], logger: Logger) -> None:
        """
        Create a SiteMoveVerifier component.

        config - A dictionary of required configuration values.
        logger - The object the site_move_verifier should use for logging.
        """
        super(SiteMoveVerifier, self).__init__("site_move_verifier", config, logger)
        self.dest_site = config["DEST_SITE"]
        self.next_status = config["NEXT_STATUS"]
        with open(config["TRANSFER_CONFIG_PATH"]) as config_data:
            self.transfer_config = json.load(config_data)
        self.work_retries = int(config["WORK_RETRIES"])
        self.work_timeout_seconds = float(config["WORK_TIMEOUT_SECONDS"])
        pass

    def _do_status(self) -> Dict[str, Any]:
        """Provide additional status for the SiteMoveVerifier."""
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
        await self._verify_bundle(lta_rc, bundle)
        return True

    async def _verify_bundle(self, lta_rc: RestClient, bundle: BundleType) -> bool:
        """Verify the provided Bundle with the transfer service and update the LTA DB."""
        bundle_id = bundle["uuid"]
        # instantiate a TransferService to query about the bundle
        xfer_service = instantiate(self.transfer_config)
        # ask the transfer service about the status of the transfer
        xfer_status = await xfer_service.status(bundle["transfer_reference"])
        self.logger.info(f"Bundle transfer status is: {xfer_status}")
        # if it's not completed, we're still waiting to verify it
        if not xfer_status["completed"]:
            self.logger.info(f"Transfer of Bundle {bundle_id} is incomplete and will not be verified at this time.")
            return False
        # when we verify bundles, we do so at the destination site
        dest = bundle["dest"]
        pfn_prefix = self.transfer_config["sites"][dest]["pfn"]  # UGLY: hard-coded RucioTransferService dependency
        parsed_url = urlparse(pfn_prefix)
        rucio_path = parsed_url.path
        bundle_name = os.path.basename(bundle["bundle_path"])
        bundle_path = os.path.join(rucio_path, bundle_name)
        # we'll compute the bundle's checksum
        self.logger.info(f"Computing SHA512 checksum for bundle: '{bundle_path}'")
        checksum_sha512 = sha512sum(bundle_path)
        self.logger.info(f"Bundle '{bundle_path}' has SHA512 checksum '{checksum_sha512}'")
        # now we'll compare the bundle's checksum
        if bundle["checksum"]["sha512"] != checksum_sha512:
            self.logger.info(f"SHA512 checksum at the time of bundle creation: {bundle['checksum']['sha512']}")
            self.logger.info(f"SHA512 checksum of the file at the destination: {checksum_sha512}")
            self.logger.info(f"These checksums do NOT match, and the Bundle will NOT be verified.")
            bundle["status"] = "quarantined"
            bundle["reason"] = f"Checksum mismatch between creation and destination: {checksum_sha512}"
            self.logger.info(f"PATCH /Bundles/{bundle_id} - '{bundle}'")
            await lta_rc.request('PATCH', f'/Bundles/{bundle_id}', bundle)
            return False
        # update the Bundle in the LTA DB
        self.logger.info(f"Destination checksum matches bundle creation checksum; the bundle is now verified.")
        bundle["status"] = self.next_status
        bundle["update_timestamp"] = now()
        bundle["claimed"] = False
        self.logger.info(f"PATCH /Bundles/{bundle_id} - '{bundle}'")
        await lta_rc.request('PATCH', f'/Bundles/{bundle_id}', bundle)
        return True


def runner() -> None:
    """Configure a SiteMoveVerifier component from the environment and set it running."""
    # obtain our configuration from the environment
    config = from_environment(EXPECTED_CONFIG)
    # configure structured logging for the application
    structured_formatter = StructuredFormatter(
        component_type='SiteMoveVerifier',
        component_name=config["COMPONENT_NAME"],
        ndjson=True)
    stream_handler = logging.StreamHandler(sys.stdout)
    stream_handler.setFormatter(structured_formatter)
    root_logger = logging.getLogger(None)
    root_logger.setLevel(logging.NOTSET)
    root_logger.addHandler(stream_handler)
    logger = logging.getLogger("lta.site_move_verifier")
    # create our SiteMoveVerifier service
    site_move_verifier = SiteMoveVerifier(config, logger)
    # let's get to work
    site_move_verifier.logger.info("Adding tasks to asyncio loop")
    loop = asyncio.get_event_loop()
    loop.create_task(status_loop(site_move_verifier))
    loop.create_task(work_loop(site_move_verifier))


def main() -> None:
    """Configure a SiteMoveVerifier component from the environment and set it running."""
    runner()
    asyncio.get_event_loop().run_forever()


if __name__ == "__main__":
    main()
