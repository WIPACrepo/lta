# desy_move_verifier.py
"""Module to implement the DesyMoveVerifier component of the Long Term Archive."""

# fmt:off

import asyncio
import logging
import os
import sys
from typing import Any, Dict, Optional

from prometheus_client import Counter, Gauge, start_http_server
from rest_tools.client import RestClient
import wipac_telemetry.tracing_tools as wtt

from .component import COMMON_CONFIG, Component, now, work_loop
from .crypto import sha512sum
from .joiner import join_smart, join_smart_url
from .lta_tools import from_environment
from .lta_types import BundleType
from .transfer.globus import SiteGlobusProxy
from .transfer.gridftp import GridFTP

Logger = logging.Logger

LOG = logging.getLogger(__name__)

EXPECTED_CONFIG = COMMON_CONFIG.copy()
EXPECTED_CONFIG.update({
    "GRIDFTP_DEST_URL": None,
    "GRIDFTP_TIMEOUT": "1200",
    "WORK_RETRIES": "3",
    "WORK_TIMEOUT_SECONDS": "30",
    "WORKBOX_PATH": None,
})

# prometheus metrics
failure_counter = Counter('lta_failures', 'lta processing failures', ['component', 'level', 'type'])
load_gauge = Gauge('lta_load_level', 'lta work processed', ['component', 'level', 'type'])
success_counter = Counter('lta_successes', 'lta processing successes', ['component', 'level', 'type'])


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
        self.gridftp_dest_url = config["GRIDFTP_DEST_URL"]
        self.gridftp_timeout = int(config["GRIDFTP_TIMEOUT"])
        self.work_retries = int(config["WORK_RETRIES"])
        self.work_timeout_seconds = float(config["WORK_TIMEOUT_SECONDS"])
        self.workbox_path = config["WORKBOX_PATH"]

    def _do_status(self) -> Dict[str, Any]:
        """DesyMoveVerifier has no additional status to contribute."""
        return {}

    def _expected_config(self) -> Dict[str, Optional[str]]:
        """Provide expected configuration dictionary."""
        return EXPECTED_CONFIG

    @wtt.spanned()
    async def _do_work(self, lta_rc: RestClient) -> None:
        """Perform a work cycle for this component."""
        self.logger.info("Starting work on Bundles.")
        load_level = -1
        work_claimed = True
        while work_claimed:
            load_level += 1
            work_claimed = await self._do_work_claim(lta_rc)
            # if we are configured to run once and die, then die
            if self.run_once_and_die:
                sys.exit()
        load_gauge.labels(component='desy_move_verifier', level='bundle', type='work').set(load_level)
        self.logger.info("Ending work on Bundles.")

    @wtt.spanned()
    async def _do_work_claim(self, lta_rc: RestClient) -> bool:
        """Claim a bundle and perform work on it."""
        # 1. Ask the LTA DB for the next Bundle to be verified
        self.logger.info("Asking the LTA DB for a Bundle to verify.")
        pop_body = {
            "claimant": f"{self.name}-{self.instance_uuid}"
        }
        response = await lta_rc.request('POST', f'/Bundles/actions/pop?source={self.source_site}&dest={self.dest_site}&status={self.input_status}', pop_body)
        self.logger.info(f"LTA DB responded with: {response}")
        bundle = response["bundle"]
        if not bundle:
            self.logger.info("LTA DB did not provide a Bundle to verify. Going on vacation.")
            return False
        # process the Bundle that we were given
        try:
            await self._verify_bundle(lta_rc, bundle)
            success_counter.labels(component='desy_move_verifier', level='bundle', type='work').inc()
        except Exception as e:
            failure_counter.labels(component='desy_move_verifier', level='bundle', type='exception').inc()
            await self._quarantine_bundle(lta_rc, bundle, f"{e}")
            raise e
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
            "original_status": bundle["status"],
            "status": "quarantined",
            "reason": f"BY:{self.name}-{self.instance_uuid} REASON:{reason}",
            "work_priority_timestamp": right_now,
        }
        try:
            await lta_rc.request('PATCH', f'/Bundles/{bundle["uuid"]}', patch_body)
        except Exception as e:
            self.logger.error(f'Unable to quarantine Bundle {bundle["uuid"]}: {e}.')

    @wtt.spanned()
    async def _verify_bundle(self, lta_rc: RestClient, bundle: BundleType) -> bool:
        """Verify the provided Bundle with the transfer service and update the LTA DB."""
        # get our ducks in a row
        bundle_id = bundle["uuid"]
        bundle_path = bundle["bundle_path"]  # /mnt/lfss/jade-lta/bundler_out/fdd3c3865d1011eb97bb6224ddddaab7.zip
        # make sure our proxy credentials are all in order
        self.logger.info('Updating proxy credentials')
        sgp = SiteGlobusProxy()
        sgp.update_proxy()
        # tell GridFTP to 'get' our file back from the destination
        basename = os.path.basename(bundle_path)
        dest_path = bundle["path"]  # /data/exp/IceCube/2015/filtered/level2/0320
        dest_url = join_smart_url([self.gridftp_dest_url, dest_path, basename])
        work_path = join_smart([self.workbox_path, basename])
        self.logger.info(f'Copying {dest_url} to {work_path}')
        try:
            GridFTP.get(dest_url,
                        filename=work_path,
                        request_timeout=self.gridftp_timeout)
        except Exception as e:
            self.logger.error(f'GridFTP threw an error: {e}')
        # we'll compute the bundle's checksum
        self.logger.info(f"Computing SHA512 checksum for bundle: '{work_path}'")
        checksum_sha512 = sha512sum(work_path)
        self.logger.info(f"Checksum complete, removing file: '{work_path}'")
        os.remove(work_path)
        self.logger.info(f"Bundle '{work_path}' has SHA512 checksum '{checksum_sha512}'")
        # now we'll compare the bundle's checksum
        if bundle["checksum"]["sha512"] != checksum_sha512:
            self.logger.info(f"SHA512 checksum at the time of bundle creation: {bundle['checksum']['sha512']}")
            self.logger.info(f"SHA512 checksum of the file at the destination: {checksum_sha512}")
            self.logger.info("These checksums do NOT match, and the Bundle will NOT be verified.")
            right_now = now()
            patch_body: Dict[str, Any] = {
                "status": "quarantined",
                "reason": f"BY:{self.name}-{self.instance_uuid} REASON:Checksum mismatch between creation and destination: {checksum_sha512}",
                "work_priority_timestamp": right_now,
            }
            self.logger.info(f"PATCH /Bundles/{bundle_id} - '{patch_body}'")
            await lta_rc.request('PATCH', f'/Bundles/{bundle_id}', patch_body)
            return False
        # update the Bundle in the LTA DB
        self.logger.info("Destination checksum matches bundle creation checksum; the bundle is now verified.")
        patch_body = {
            "status": self.output_status,
            "reason": "",
            "update_timestamp": now(),
            "claimed": False,
        }
        self.logger.info(f"PATCH /Bundles/{bundle_id} - '{patch_body}'")
        await lta_rc.request('PATCH', f'/Bundles/{bundle_id}', patch_body)
        return True


async def main(desy_move_verifier: DesyMoveVerifier) -> None:
    """Execute the work loop of the DesyMoveVerifier component."""
    LOG.info("Starting asynchronous code")
    await work_loop(desy_move_verifier)
    LOG.info("Ending asynchronous code")


def main_sync() -> None:
    """Configure a DesyMoveVerifier component from the environment and set it running."""
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
    # create our DesyMoveVerifier service
    LOG.info("Starting synchronous code")
    desy_move_verifier = DesyMoveVerifier(config, LOG)
    # let's get to work
    metrics_port = int(config["PROMETHEUS_METRICS_PORT"])
    start_http_server(metrics_port)
    asyncio.run(main(desy_move_verifier))
    LOG.info("Ending synchronous code")


if __name__ == "__main__":
    main_sync()