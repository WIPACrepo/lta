# rate_limiter.py
"""Module to implement the RateLimiter component of the Long Term Archive."""

import asyncio
import logging
import os
import shutil
import sys
from typing import Any, Dict, List, Optional, Tuple

from prometheus_client import start_http_server
from rest_tools.client import ClientCredentialsAuth, RestClient
import wipac_telemetry.tracing_tools as wtt

from .component import COMMON_CONFIG, Component, now, work_loop
from .lta_tools import from_environment
from .lta_types import BundleType

Logger = logging.Logger

LOG = logging.getLogger(__name__)

EXPECTED_CONFIG = COMMON_CONFIG.copy()
EXPECTED_CONFIG.update({
    "INPUT_PATH": None,
    "OUTPUT_PATH": None,
    "OUTPUT_QUOTA": None,
    "WORK_RETRIES": "3",
    "WORK_TIMEOUT_SECONDS": "30",
})


class RateLimiter(Component):
    """
    RateLimiter is a Long Term Archive component.

    A RateLimiter moves bundle archives from an input directory to an output
    directory, according to a configured output quota. If the total size of
    the files in the output directory exceed the configured output quota, the
    bundle archive will not be moved. This component limits the rate of bundles
    that are "in-flight" to the destination site at any given time.
    """

    def __init__(self, config: Dict[str, str], logger: Logger) -> None:
        """
        Create a RateLimiter component.

        config - A dictionary of required configuration values.
        logger - The object the rate_limiter should use for logging.
        """
        super(RateLimiter, self).__init__("rate_limiter", config, logger)
        self.input_path = config["INPUT_PATH"]
        self.output_path = config["OUTPUT_PATH"]
        self.output_quota = int(config["OUTPUT_QUOTA"])
        self.work_retries = int(config["WORK_RETRIES"])
        self.work_timeout_seconds = float(config["WORK_TIMEOUT_SECONDS"])

    def _do_status(self) -> Dict[str, Any]:
        """Contribute no additional status."""
        return {}

    def _enumerate_path(self, path: str) -> List[str]:
        """Recursively walk the file system to enumerate files at provided path."""
        self.logger.info(f"Enumerating all files in {path}")
        # enumerate all of the files on disk to be checked
        disk_files = []
        for root, dirs, files in os.walk(path):
            disk_files.extend([os.path.join(root, file) for file in files])
        self.logger.info(f"Found {len(disk_files)} entries in {path}")
        return disk_files

    def _expected_config(self) -> Dict[str, Optional[str]]:
        """Provide expected configuration dictionary."""
        return EXPECTED_CONFIG

    def _get_files_and_size(self, path: str) -> Tuple[List[str], int]:
        """Recursively walk and add the files of files in the file system."""
        # enumerate all of the files on disk to be checked
        disk_files = self._enumerate_path(path)
        # for all of the files we want to check
        self.logger.info(f"Determining total size of files in {path}")
        size = 0
        for disk_file in disk_files:
            try:
                # determine the size of the file
                size += os.path.getsize(disk_file)
            except Exception as e:
                # whoops, looks like somebody downstream moved it
                self.logger.error(f"Skipped getsize() on missing file: {disk_file}", exc_info=e)
                continue
            self.logger.debug(f"Size so far: {size} bytes")
        self.logger.info(f"Found {len(disk_files)} entries ({size} bytes) in {path}")
        return (disk_files, size)

    @wtt.spanned()
    async def _do_work(self) -> None:
        """Perform a work cycle for this component."""
        self.logger.info("Starting work on Bundles.")
        work_claimed = True
        while work_claimed:
            work_claimed = await self._do_work_claim()
            # if we are configured to run once and die, then die
            if self.run_once_and_die:
                sys.exit()
        self.logger.info("Ending work on Bundles.")

    @wtt.spanned()
    async def _do_work_claim(self) -> bool:
        """Claim a bundle and perform work on it."""
        # 1. Ask the LTA DB for the next Bundle to be staged
        # configure a RestClient to talk to the LTA DB
        lta_rc = ClientCredentialsAuth(address=self.lta_rest_url,
                                       token_url=self.lta_auth_openid_url,
                                       client_id=self.client_id,
                                       client_secret=self.client_secret,
                                       timeout=self.work_timeout_seconds,
                                       retries=self.work_retries)
        self.logger.info("Asking the LTA DB for a Bundle to stage.")
        pop_body = {
            "claimant": f"{self.name}-{self.instance_uuid}"
        }
        response = await lta_rc.request('POST', f'/Bundles/actions/pop?source={self.source_site}&dest={self.dest_site}&status={self.input_status}', pop_body)
        self.logger.info(f"LTA DB responded with: {response}")
        bundle = response["bundle"]
        if not bundle:
            self.logger.info("LTA DB did not provide a Bundle to stage. Going on vacation.")
            return False
        # process the Bundle that we were given
        try:
            await self._stage_bundle(lta_rc, bundle)
        except Exception as e:
            await self._quarantine_bundle(lta_rc, bundle, f"{e}")
            raise e
        # even if we were successful, take a break between bundles
        return False

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
    async def _stage_bundle(self, lta_rc: RestClient, bundle: BundleType) -> bool:
        """Stage the Bundle to the output directory for transfer."""
        bundle_id = bundle["uuid"]
        # measure output directory size, our bundle's size, and the quota
        output_size = self._get_files_and_size(self.output_path)[1]
        bundle_size = bundle["size"]
        total_size = output_size + bundle_size
        # if we would exceed our destination quota
        self.logger.debug(f'output_size: {output_size}')
        self.logger.debug(f'bundle_size: {bundle_size}')
        self.logger.debug(f'total_size: {total_size}')
        self.logger.debug(f'output_quota: {self.output_quota}')
        if total_size > self.output_quota:
            self.logger.info(f"Bundle {bundle_id} has size {bundle_size} bytes.")
            self.logger.info(f"Output directory currently holds {output_size} bytes.")
            self.logger.info(f"Staging Bundle to output directory would exceed the configured quota of {self.output_quota}.")
            self.logger.info("Bundle will be unclaimed and staged at a later time.")
            await self._unclaim_bundle(lta_rc, bundle)
            return False
        # this bundle is ready to be staged
        bundle_name = os.path.basename(bundle["bundle_path"])
        src_path = os.path.join(self.input_path, bundle_name)
        dst_path = os.path.join(self.output_path, bundle_name)
        self.logger.info(f"Moving Bundle {src_path} -> {dst_path}")
        shutil.move(src_path, dst_path)
        # update the Bundle in the LTA DB
        self.logger.info("Bundle has been staged to the output directory.")
        patch_body = {
            "bundle_path": dst_path,
            "claimed": False,
            "status": self.output_status,
            "reason": "",
            "update_timestamp": now(),
        }
        self.logger.info(f"PATCH /Bundles/{bundle_id} - '{patch_body}'")
        await lta_rc.request('PATCH', f'/Bundles/{bundle_id}', patch_body)
        return True

    @wtt.spanned()
    async def _unclaim_bundle(self, lta_rc: RestClient, bundle: BundleType) -> bool:
        """Return the Bundle to the LTA DB, unclaim it for processing at a later date."""
        self.logger.info("Bundle is not ready to be staged; will unclaim it.")
        bundle_id = bundle["uuid"]
        right_now = now()
        patch_body: Dict[str, Any] = {
            "claimed": False,
            "update_timestamp": right_now,
            "work_priority_timestamp": right_now,
        }
        self.logger.info(f"PATCH /Bundles/{bundle_id} - '{patch_body}'")
        await lta_rc.request('PATCH', f'/Bundles/{bundle_id}', patch_body)
        return True


async def main(rate_limiter: RateLimiter) -> None:
    """Execute the work loop of the RateLimiter component."""
    LOG.info("Starting asynchronous code")
    await work_loop(rate_limiter)
    LOG.info("Ending asynchronous code")


def main_sync() -> None:
    """Configure a RateLimiter component from the environment and set it running."""
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
    # create our RateLimiter service
    LOG.info("Starting synchronous code")
    rate_limiter = RateLimiter(config, LOG)
    # let's get to work
    metrics_port = int(config["PROMETHEUS_METRICS_PORT"])
    start_http_server(metrics_port)
    asyncio.run(main(rate_limiter))
    LOG.info("Ending synchronous code")


if __name__ == "__main__":
    main_sync()
