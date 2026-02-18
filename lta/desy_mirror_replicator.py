# desy_mirror_replicator.py
"""Module to implement the DesyMirrorReplicator component of the Long Term Archive."""

# fmt:off

import asyncio
import logging
import os
import sys
from typing import Any, Optional

from prometheus_client import start_http_server
from rest_tools.client import RestClient
from wipac_dev_tools import strtobool

from .component import COMMON_CONFIG, Component, DoWorkClaimResult, work_loop
from .utils import now
from .lta_tools import from_environment
from .lta_types import BundleType
from .transfer.sync import Sync


EXPECTED_CONFIG = COMMON_CONFIG.copy()
EXPECTED_CONFIG.update({
    # is this component instantiated for testing in a CI system?
    "CI_TEST": "False",
    # url of the WebDAV host; 'https://globe-door.ifh.de:2880'
    "DEST_URL": None,
    # path of the archival root at DESY; '/pnfs/ifh.de/acs/icecube/archive'
    "DEST_BASE_PATH": None,
    # local source directory for the bundles; '/data/user/jadelta/ltatemp/bundler_todesy'
    "INPUT_PATH": None,
    # number of parallel operations to allow on the WebDAV server
    "MAX_PARALLEL": "100",
})

# logging
Logger = logging.Logger
LOG = logging.getLogger(__name__)


class DesyMirrorReplicator(Component):
    """
    DesyMirrorReplicator is a Long Term Archive component.

    A DesyMirrorReplicator is responsible for copying the completed archive
    bundles from the staging directory of a rate limiter to the archival
    destination at DESY.

    It uses the LTA DB to find completed bundles that need to be replicated.
    It issues a put_path() call to use WebDAV to copy the bundle to DESY. It
    updates the Bundle and the corresponding TransferRequest in the LTA DB
    with a 'transferring' status.
    """
    def __init__(self, config: dict[str, str], logger: Logger) -> None:
        """
        Create a DesyMirrorReplicator component.

        config - A dictionary of required configuration values.
        logger - The object the replicator should use for logging.
        """
        super(DesyMirrorReplicator, self).__init__("desy_mirror_replicator", config, logger)
        self.ci_test = strtobool(config["CI_TEST"])
        self.dest_base_path = config["DEST_BASE_PATH"]
        self.dest_url = config["DEST_URL"]
        self.input_path = config["INPUT_PATH"]

    def _do_status(self) -> dict[str, Any]:
        """DesyMirrorReplicator has no additional status to contribute."""
        return {}

    def _expected_config(self) -> dict[str, Optional[str]]:
        """DesyMirrorReplicator provides our expected configuration dictionary."""
        return EXPECTED_CONFIG

    async def _do_work_claim(self, lta_rc: RestClient) -> DoWorkClaimResult.ReturnType:
        """Claim a bundle and perform work on it -- see super for return value meanings."""
        # 1. Ask the LTA DB for the next Bundle to be transferred
        self.logger.info("Asking the LTA DB for a Bundle to transfer.")
        pop_body = {
            "claimant": f"{self.name}-{self.instance_uuid}"
        }
        response = await lta_rc.request('POST', f'/Bundles/actions/pop?source={self.source_site}&dest={self.dest_site}&status={self.input_status}', pop_body)
        self.logger.info(f"LTA DB responded with: {response}")
        bundle = response["bundle"]
        if not bundle:
            self.logger.info("LTA DB did not provide a Bundle to transfer. Going on vacation.")
            return DoWorkClaimResult.NothingClaimed("PAUSE")

        # process the Bundle that we were given
        try:
            await self._replicate_bundle_to_destination_site(lta_rc, bundle)
            return DoWorkClaimResult.Successful("CONTINUE")
        except Exception as e:
            return DoWorkClaimResult.QuarantineNow("PAUSE", bundle, "BUNDLE", e)

    async def _replicate_bundle_to_destination_site(self, lta_rc: RestClient, bundle: BundleType) -> None:
        """Replicate the supplied bundle using the configured transfer service."""
        # get our ducks in a row
        bundle_id = bundle["uuid"]
        bundle_path = bundle["bundle_path"]  # /data/user/jadelta/ltatemp/bundler_todesy/fdd3c3865d1011eb97bb6224ddddaab7.zip
        # calculate the destination path of the bundle
        data_warehouse_path = bundle["path"]  # /data/exp/IceCube/2015/unbiased/PFRaw/0320
        basename = os.path.basename(bundle["bundle_path"])
        stupid_python_path = os.path.sep.join([data_warehouse_path, basename])
        dest_path = os.path.normpath(stupid_python_path)
        # create Sync to transfer to DESY
        sync = Sync(self.config)
        try:
            LOG.info(f"Replicating {bundle_path} -> {dest_path}")
            await sync.put_path(bundle_path, dest_path, int(self.work_timeout_seconds))
        except Exception as e:
            self.logger.error(f'DESY Sync raised an Exception: {e}')
            raise e
        # update the Bundle in the LTA DB
        patch_body = {
            "status": self.output_status,
            "reason": "",
            "transfer_dest_path": dest_path,
            "final_dest_location": {
                "path": dest_path,
            },
            "update_timestamp": now(),
            "claimed": False,
            "transfer_reference": "desy-mirror-replicator",
        }
        self.logger.info(f"PATCH /Bundles/{bundle_id} - '{patch_body}'")
        await lta_rc.request('PATCH', f'/Bundles/{bundle_id}', patch_body)


async def main(desy_mirror_replicator: DesyMirrorReplicator) -> None:
    """Execute the work loop of the DesyMirrorReplicator component."""
    LOG.info("Starting asynchronous code")
    await work_loop(desy_mirror_replicator)
    LOG.info("Ending asynchronous code")


def main_sync() -> None:
    """Configure a DesyMirrorReplicator component from the environment and set it running."""
    # obtain our configuration from the environment
    config = from_environment(EXPECTED_CONFIG)
    # configure logging for the application
    log_level = getattr(logging, config["LOG_LEVEL"].upper())
    logging.basicConfig(
        format="{asctime} [{threadName}] {levelname:5} ({filename}:{lineno}) - {message}",
        level=log_level,
        stream=sys.stdout,
        style="{",
    )
    # create our DesyMirrorReplicator service
    LOG.info("Starting synchronous code")
    desy_mirror_replicator = DesyMirrorReplicator(config, LOG)
    # let's get to work
    metrics_port = int(config["PROMETHEUS_METRICS_PORT"])
    start_http_server(metrics_port)
    asyncio.run(main(desy_mirror_replicator))
    LOG.info("Ending synchronous code")


if __name__ == "__main__":
    main_sync()
