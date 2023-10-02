# site_move_verifier.py
"""Module to implement the SiteMoveVerifier component of the Long Term Archive."""

import asyncio
import logging
import os
from subprocess import PIPE, run
import sys
from typing import Any, Dict, List, Optional

from prometheus_client import Counter, Gauge, start_http_server
from rest_tools.client import ClientCredentialsAuth, RestClient
import wipac_telemetry.tracing_tools as wtt

from .component import COMMON_CONFIG, Component, now, work_loop
from .crypto import sha512sum
from .joiner import join_smart
from .lta_tools import from_environment
from .lta_types import BundleType
from .rest_server import boolify

Logger = logging.Logger

LOG = logging.getLogger(__name__)

EXPECTED_CONFIG = COMMON_CONFIG.copy()
EXPECTED_CONFIG.update({
    "DEST_ROOT_PATH": None,
    "USE_FULL_BUNDLE_PATH": "FALSE",
    "WORK_RETRIES": "3",
    "WORK_TIMEOUT_SECONDS": "30",
})

MYQUOTA_ARGS = ["/usr/bin/myquota", "-G"]

OLD_MTIME_EPOCH_SEC = 30 * 60  # 30 MINUTES * 60 SEC_PER_MIN

# prometheus metrics
failure_counter = Counter('lta_failures', 'lta processing failures', ['component', 'level', 'type'])
load_gauge = Gauge('lta_load_level', 'lta work processed', ['component', 'level', 'type'])
success_counter = Counter('lta_successes', 'lta processing successes', ['component', 'level', 'type'])


def as_nonempty_columns(s: str) -> List[str]:
    """Split the provided string into columns and return the non-empty ones."""
    cols = s.split(" ")
    nonempty = list(filter(discard_empty, cols))
    return nonempty


def discard_empty(s: str) -> bool:
    """Return true if the provided string is non-empty."""
    if s:
        return True
    return False


def parse_myquota(s: str) -> List[Dict[str, str]]:
    """Split the provided string into columns and return the non-empty ones."""
    results = []
    lines = s.split("\n")
    keys = as_nonempty_columns(lines[0])
    for i in range(1, len(lines)):
        if lines[i]:
            values = as_nonempty_columns(lines[i])
            quota_dict = {}
            for j in range(0, len(keys)):
                quota_dict[keys[j]] = values[j]
            results.append(quota_dict)
    return results


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
        self.dest_root_path = config["DEST_ROOT_PATH"]
        self.use_full_bundle_path = boolify(config["USE_FULL_BUNDLE_PATH"])
        self.work_retries = int(config["WORK_RETRIES"])
        self.work_timeout_seconds = float(config["WORK_TIMEOUT_SECONDS"])
        pass

    def _do_status(self) -> Dict[str, Any]:
        """Provide additional status for the SiteMoveVerifier."""
        quota = []
        stdout = self._execute_myquota()
        if stdout:
            quota = parse_myquota(stdout)
        return {"quota": quota}

    def _expected_config(self) -> Dict[str, Optional[str]]:
        """Provide expected configuration dictionary."""
        return EXPECTED_CONFIG

    @wtt.spanned()
    async def _do_work(self) -> None:
        """Perform a work cycle for this component."""
        self.logger.info("Starting work on Bundles.")
        load_level = -1
        work_claimed = True
        while work_claimed:
            load_level += 1
            work_claimed = await self._do_work_claim()
            # if we are configured to run once and die, then die
            if self.run_once_and_die:
                sys.exit()
        load_gauge.labels(component='site_move_verifier', level='bundle', type='work').set(load_level)
        self.logger.info("Ending work on Bundles.")

    @wtt.spanned()
    async def _do_work_claim(self) -> bool:
        """Claim a bundle and perform work on it."""
        # 1. Ask the LTA DB for the next Bundle to be verified
        # configure a RestClient to talk to the LTA DB
        lta_rc = ClientCredentialsAuth(address=self.lta_rest_url,
                                       token_url=self.lta_auth_openid_url,
                                       client_id=self.client_id,
                                       client_secret=self.client_secret,
                                       timeout=self.work_timeout_seconds,
                                       retries=self.work_retries)
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
            success_counter.labels(component='site_move_verifier', level='bundle', type='work').inc()
        except Exception as e:
            failure_counter.labels(component='site_move_verifier', level='bundle', type='exception').inc()
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
        if self.use_full_bundle_path:
            bundle_name = join_smart([bundle["path"], os.path.basename(bundle["bundle_path"])])
        else:
            bundle_name = os.path.basename(bundle["bundle_path"])
        bundle_path = join_smart([self.dest_root_path, bundle_name])
        # we'll compute the bundle's checksum
        self.logger.info(f"Computing SHA512 checksum for bundle: '{bundle_path}'")
        checksum_sha512 = sha512sum(bundle_path)
        self.logger.info(f"Bundle '{bundle_path}' has SHA512 checksum '{checksum_sha512}'")
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

    @wtt.spanned()
    def _execute_myquota(self) -> Optional[str]:
        """Run the myquota command to determine disk usage at the site."""
        completed_process = run(MYQUOTA_ARGS, stdout=PIPE, stderr=PIPE)
        # if our command failed
        if completed_process.returncode != 0:
            self.logger.info(f"Command to check quota failed: {completed_process.args}")
            self.logger.info(f"returncode: {completed_process.returncode}")
            self.logger.info(f"stdout: {str(completed_process.stdout)}")
            self.logger.info(f"stderr: {str(completed_process.stderr)}")
            return None
        # otherwise, we succeeded
        return completed_process.stdout.decode("utf-8")


async def main(site_move_verifier: SiteMoveVerifier) -> None:
    """Execute the work loop of the SiteMoveVerifier component."""
    LOG.info("Starting asynchronous code")
    await work_loop(site_move_verifier)
    LOG.info("Ending asynchronous code")


def main_sync() -> None:
    """Configure a SiteMoveVerifier component from the environment and set it running."""
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
    # create our SiteMoveVerifier service
    LOG.info("Starting synchronous code")
    site_move_verifier = SiteMoveVerifier(config, LOG)
    # let's get to work
    metrics_port = int(config["PROMETHEUS_METRICS_PORT"])
    start_http_server(metrics_port)
    asyncio.run(main(site_move_verifier))
    LOG.info("Ending synchronous code")


if __name__ == "__main__":
    main_sync()
