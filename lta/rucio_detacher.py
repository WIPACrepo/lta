# rucio_detacher.py
"""Module to implement the RucioDetacher component of the Long Term Archive."""

import asyncio
import json
from logging import Logger
import logging
import os
import sys
from typing import Any, Dict, Optional

from rest_tools.client import RestClient  # type: ignore
from rest_tools.server import from_environment  # type: ignore

from .component import COMMON_CONFIG, Component, now, status_loop, work_loop
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


class RucioDetacher(Component):
    """
    RucioDetacher is a Long Term Archive component.

    A RucioDetacher is responsible for prompting Rucio to delete intermediate
    copies of archive bundles that have finished processing at their
    destination site(s). The archive bundles are marked for deletion using
    the Rucio transfer service. Rucio will then remove the intermediate bundle
    files from the source and destination site(s).

    It uses the LTA DB to find verified bundles that need to be deleted. It
    deattaches the bundles from the datasets within Rucio. It updates the Bundle
    and the corresponding TransferRequest in the LTA DB with a 'deleted' status.
    """

    def __init__(self, config: Dict[str, str], logger: Logger) -> None:
        """
        Create a RucioDetacher component.

        config - A dictionary of required configuration values.
        logger - The object the rucio_detacher should use for logging.
        """
        super(RucioDetacher, self).__init__("rucio_detacher", config, logger)
        with open(config["TRANSFER_CONFIG_PATH"]) as config_data:
            self.transfer_config = json.load(config_data)
        self.transfer_config["password"] = config["RUCIO_PASSWORD"]
        self.work_retries = int(config["WORK_RETRIES"])
        self.work_timeout_seconds = float(config["WORK_TIMEOUT_SECONDS"])

    def _do_status(self) -> Dict[str, Any]:
        """Provide additional status for the RucioDetacher."""
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
        # 1. Ask the LTA DB for the next Bundle to be deleted
        # configure a RestClient to talk to the LTA DB
        lta_rc = RestClient(self.lta_rest_url,
                            token=self.lta_rest_token,
                            timeout=self.work_timeout_seconds,
                            retries=self.work_retries)
        self.logger.info("Asking the LTA DB for a Bundle to delete.")
        source = self.source_site
        pop_body = {
            "claimant": f"{self.name}-{self.instance_uuid}"
        }
        response = await lta_rc.request('POST', f'/Bundles/actions/pop?source={source}&status=completed', pop_body)
        self.logger.info(f"LTA DB responded with: {response}")
        bundle = response["bundle"]
        if not bundle:
            self.logger.info("LTA DB did not provide a Bundle to delete. Going on vacation.")
            return False
        # process the Bundle that we were given
        await self._detach_bundle(lta_rc, bundle)
        return True

    def _calculate_xfer_reference(self, site_name: str, file_name: str) -> str:
        """Compute the transfer reference to pass to RucioTransferService."""
        # "transfer_reference": "NERSC-LTA|nersc-dataset|d85fa59e420811ea8c90c6259865d176.zip",
        site = self.transfer_config["sites"][site_name]
        return f'{site["rse"]}|{site["dataset"]}|{file_name}'

    async def _detach_bundle(self, lta_rc: RestClient, bundle: BundleType) -> None:
        """Detach the provided Bundle with the transfer service and update the LTA DB."""
        bundle_id = bundle["uuid"]
        # instantiate a TransferService to delete the bundle
        xfer_service = instantiate(self.transfer_config)
        # ask the transfer service to cancel (i.e.: delete) the transfer
        basename = os.path.basename(bundle["bundle_path"])
        dest_xfer_ref = self._calculate_xfer_reference(bundle["dest"], basename)
        await xfer_service.cancel(dest_xfer_ref)
        source_xfer_ref = self._calculate_xfer_reference(bundle["source"], basename)
        await xfer_service.cancel(source_xfer_ref)
        # update the Bundle in the LTA DB
        patch_body = {
            "status": "detached",
            "update_timestamp": now(),
            "claimed": False,
        }
        self.logger.info(f"PATCH /Bundles/{bundle_id} - '{patch_body}'")
        await lta_rc.request('PATCH', f'/Bundles/{bundle_id}', patch_body)


def runner() -> None:
    """Configure a RucioDetacher component from the environment and set it running."""
    # obtain our configuration from the environment
    config = from_environment(EXPECTED_CONFIG)
    # configure structured logging for the application
    structured_formatter = StructuredFormatter(
        component_type='RucioDetacher',
        component_name=config["COMPONENT_NAME"],
        ndjson=True)
    stream_handler = logging.StreamHandler(sys.stdout)
    stream_handler.setFormatter(structured_formatter)
    root_logger = logging.getLogger(None)
    root_logger.setLevel(logging.NOTSET)
    root_logger.addHandler(stream_handler)
    logger = logging.getLogger("lta.rucio_detacher")
    # create our RucioDetacher service
    rucio_detacher = RucioDetacher(config, logger)
    # let's get to work
    rucio_detacher.logger.info("Adding tasks to asyncio loop")
    loop = asyncio.get_event_loop()
    loop.create_task(status_loop(rucio_detacher))
    loop.create_task(work_loop(rucio_detacher))


def main() -> None:
    """Configure a RucioDetacher component from the environment and set it running."""
    runner()
    asyncio.get_event_loop().run_forever()


if __name__ == "__main__":
    main()
