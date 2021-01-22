# gridftp_replicator.py
"""Module to implement the GridFTPReplicator component of the Long Term Archive."""

import asyncio
from logging import Logger
import logging
import sys
from typing import Any, Dict, Optional

from rest_tools.client import RestClient  # type: ignore
from rest_tools.server import from_environment  # type: ignore

from .component import COMMON_CONFIG, Component, now, status_loop, work_loop
from .log_format import StructuredFormatter
from .lta_types import BundleType
from .transfer.globus import SiteGlobusProxy
from .transfer.gridftp import GridFTP

EMPTY_STRING_SENTINEL_VALUE = "48be4069-8423-45b1-b7db-57e0ee8761a9"

EXPECTED_CONFIG = COMMON_CONFIG.copy()
EXPECTED_CONFIG.update({
    "GLOBUS_PROXY_DURATION": "72",
    "GLOBUS_PROXY_PASSPHRASE": EMPTY_STRING_SENTINEL_VALUE,
    "GLOBUS_PROXY_VOMS_ROLE": EMPTY_STRING_SENTINEL_VALUE,
    "GLOBUS_PROXY_VOMS_VO": EMPTY_STRING_SENTINEL_VALUE,
    "GLOBUS_PROXY_OUTPUT": EMPTY_STRING_SENTINEL_VALUE,
    "GRIDFTP_DEST_URL": None,
    "GRIDFTP_TIMEOUT": "1200",
    "WORK_RETRIES": "3",
    "WORK_TIMEOUT_SECONDS": "30",
})


class GridFTPReplicator(Component):
    """
    GridFTPReplicator is a Long Term Archive component.

    A GridFTPReplicator is responsible for copying the completed archive
    bundles from a GridFTP location to a GridFTP destination. GridFTP will
    then replicate the bundle from the source (i.e.: WIPAC Data Warehouse)
    to the destination(s), (i.e.: DESY, NERSC DTN).

    It uses the LTA DB to find completed bundles that need to be replicated.
    It issues a globus-url-copy command. It updates the Bundle and the
    corresponding TransferRequest in the LTA DB with a 'transferring' status.
    """

    def __init__(self, config: Dict[str, str], logger: Logger) -> None:
        """
        Create a GridFTPReplicator component.

        config - A dictionary of required configuration values.
        logger - The object the replicator should use for logging.
        """
        super(GridFTPReplicator, self).__init__("replicator", config, logger)
        self.gridftp_dest_url = config["GRIDFTP_DEST_URL"]
        self.gridftp_timeout = int(config["GRIDFTP_TIMEOUT"])
        self.work_retries = int(config["WORK_RETRIES"])
        self.work_timeout_seconds = float(config["WORK_TIMEOUT_SECONDS"])

    def _do_status(self) -> Dict[str, Any]:
        """GridFTPReplicator has no additional status to contribute."""
        return {}

    def _expected_config(self) -> Dict[str, Optional[str]]:
        """GridFTPReplicator provides our expected configuration dictionary."""
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
        response = await lta_rc.request('POST', f'/Bundles/actions/pop?source={source}&status=staged', pop_body)
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

    async def _replicate_bundle_to_destination_site(self, lta_rc: RestClient, bundle: BundleType) -> None:
        """Replicate the supplied bundle using the configured transfer service."""
        # get our ducks in a row
        bundle_id = bundle["uuid"]
        bundle_path = bundle["bundle_path"]
        # make sure our proxy credentials are all in order
        self.logger.info('Updating proxy credentials')
        sgp = SiteGlobusProxy()
        sgp.update_proxy()
        # tell GridFTP to 'put' our file to the destination
        self.logger.info(f'Sending {bundle_path} to {self.gridftp_dest_url}')
        GridFTP.put(self.gridftp_dest_url,
                    filename=bundle_path,
                    request_timeout=self.gridftp_timeout)
        # update the Bundle in the LTA DB
        patch_body = {
            "status": "transferring",
            "reason": "",
            "update_timestamp": now(),
            "claimed": False,
            "transfer_reference": "globus-url-copy",
        }
        self.logger.info(f"PATCH /Bundles/{bundle_id} - '{patch_body}'")
        await lta_rc.request('PATCH', f'/Bundles/{bundle_id}', patch_body)

def runner() -> None:
    """Configure a GridFTPReplicator component from the environment and set it running."""
    # obtain our configuration from the environment
    config = from_environment(EXPECTED_CONFIG)
    # remove anything optional that wasn't specified
    for key in config.keys():
        if config[key] is EMPTY_STRING_SENTINEL_VALUE:
            del config[key]
    # configure structured logging for the application
    structured_formatter = StructuredFormatter(
        component_type='GridFTPReplicator',
        component_name=config["COMPONENT_NAME"],
        ndjson=True)
    stream_handler = logging.StreamHandler(sys.stdout)
    stream_handler.setFormatter(structured_formatter)
    root_logger = logging.getLogger(None)
    root_logger.setLevel(logging.NOTSET)
    root_logger.addHandler(stream_handler)
    logger = logging.getLogger("lta.replicator")
    # create our GridFTPReplicator service
    replicator = GridFTPReplicator(config, logger)
    # let's get to work
    replicator.logger.info("Adding tasks to asyncio loop")
    loop = asyncio.get_event_loop()
    loop.create_task(status_loop(replicator))
    loop.create_task(work_loop(replicator))

def main() -> None:
    """Configure a GridFTPReplicator component from the environment and set it running."""
    runner()
    asyncio.get_event_loop().run_forever()

if __name__ == "__main__":
    main()
