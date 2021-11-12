# gridftp_replicator.py
"""Module to implement the GridFTPReplicator component of the Long Term Archive."""

import asyncio
import logging
import os
import sys
from typing import Any, Dict, Optional

from rest_tools.client import RestClient
from rest_tools.server import from_environment
import wipac_telemetry.tracing_tools as wtt

from .component import COMMON_CONFIG, Component, now, status_loop, work_loop
from .joiner import join_smart_url
from .log_format import StructuredFormatter
from .lta_types import BundleType
from .rest_server import boolify
from .transfer.globus import SiteGlobusProxy
from .transfer.gridftp import GridFTP

Logger = logging.Logger

EXPECTED_CONFIG = COMMON_CONFIG.copy()
EXPECTED_CONFIG.update({
    # "GLOBUS_PROXY_DURATION": "72",
    # "GLOBUS_PROXY_OUTPUT": None,
    # "GLOBUS_PROXY_PASSPHRASE": None,
    # "GLOBUS_PROXY_VOMS_ROLE": None,
    # "GLOBUS_PROXY_VOMS_VO": None,
    "GRIDFTP_DEST_URL": None,
    "GRIDFTP_TIMEOUT": "1200",
    "USE_FULL_BUNDLE_PATH": "FALSE",
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
        self.use_full_bundle_path = boolify(config["USE_FULL_BUNDLE_PATH"])
        self.work_retries = int(config["WORK_RETRIES"])
        self.work_timeout_seconds = float(config["WORK_TIMEOUT_SECONDS"])

    def _do_status(self) -> Dict[str, Any]:
        """GridFTPReplicator has no additional status to contribute."""
        return {}

    def _expected_config(self) -> Dict[str, Optional[str]]:
        """GridFTPReplicator provides our expected configuration dictionary."""
        return EXPECTED_CONFIG

    @wtt.spanned()
    async def _do_work(self) -> None:
        """Perform a work cycle for this component."""
        self.logger.info("Starting work on Bundles.")
        work_claimed = True
        while work_claimed:
            work_claimed = await self._do_work_claim()
            work_claimed &= not self.run_once_and_die
        self.logger.info("Ending work on Bundles.")

    @wtt.spanned()
    async def _do_work_claim(self) -> bool:
        """Claim a bundle and perform work on it."""
        # 1. Ask the LTA DB for the next Bundle to be transferred
        # configure a RestClient to talk to the LTA DB
        lta_rc = RestClient(self.lta_rest_url,
                            token=self.lta_rest_token,
                            timeout=self.work_timeout_seconds,
                            retries=self.work_retries)
        self.logger.info("Asking the LTA DB for a Bundle to transfer.")
        pop_body = {
            "claimant": f"{self.name}-{self.instance_uuid}"
        }
        response = await lta_rc.request('POST', f'/Bundles/actions/pop?source={self.source_site}&dest={self.dest_site}&status={self.input_status}', pop_body)
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

    @wtt.spanned()
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

    @wtt.spanned()
    async def _replicate_bundle_to_destination_site(self, lta_rc: RestClient, bundle: BundleType) -> None:
        """Replicate the supplied bundle using the configured transfer service."""
        # get our ducks in a row
        bundle_id = bundle["uuid"]
        bundle_path = bundle["bundle_path"]  # /mnt/lfss/jade-lta/bundler_out/fdd3c3865d1011eb97bb6224ddddaab7.zip
        # make sure our proxy credentials are all in order
        self.logger.info('Updating proxy credentials')
        sgp = SiteGlobusProxy()
        sgp.update_proxy()
        # tell GridFTP to 'put' our file to the destination
        basename = os.path.basename(bundle_path)
        if self.use_full_bundle_path:
            dest_path = bundle["path"]  # /data/exp/IceCube/2015/filtered/level2/0320
            dest_url = join_smart_url([self.gridftp_dest_url, dest_path, basename])
        else:
            dest_url = join_smart_url([self.gridftp_dest_url, basename])
        self.logger.info(f'Sending {bundle_path} to {dest_url}')
        try:
            GridFTP.put(dest_url,
                        filename=bundle_path,
                        request_timeout=self.gridftp_timeout)
        except Exception as e:
            self.logger.error(f'GridFTP threw an error: {e}')
        # update the Bundle in the LTA DB
        patch_body = {
            "status": self.output_status,
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
    # configure structured logging for the application
    structured_formatter = StructuredFormatter(
        component_type='GridFTPReplicator',
        component_name=config["COMPONENT_NAME"],  # type: ignore[arg-type]
        ndjson=True)
    stream_handler = logging.StreamHandler(sys.stdout)
    stream_handler.setFormatter(structured_formatter)
    root_logger = logging.getLogger(None)
    root_logger.setLevel(logging.NOTSET)
    root_logger.addHandler(stream_handler)
    logger = logging.getLogger("lta.replicator")
    # create our GridFTPReplicator service
    replicator = GridFTPReplicator(config, logger)  # type: ignore[arg-type]
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
