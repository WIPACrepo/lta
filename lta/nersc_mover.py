# nersc_mover.py
"""Module to implement the NerscMover component of the Long Term Archive."""

import asyncio
import logging
import os
from subprocess import PIPE, run
import sys
from typing import Any, Dict, List, Optional

from prometheus_client import Counter, Gauge, start_http_server
from rest_tools.client import RestClient
import wipac_telemetry.tracing_tools as wtt

from .component import COMMON_CONFIG, Component, now, work_loop
from .lta_tools import from_environment
from .lta_types import BundleType

Logger = logging.Logger

LOG = logging.getLogger(__name__)

EXPECTED_CONFIG = COMMON_CONFIG.copy()
EXPECTED_CONFIG.update({
    "HPSS_AVAIL_PATH": "/usr/bin/hpss_avail.py",
    "MAX_COUNT": None,
    "RSE_BASE_PATH": None,
    "TAPE_BASE_PATH": None,
    "WORK_RETRIES": "3",
    "WORK_TIMEOUT_SECONDS": "30",
})

# prometheus metrics
failure_counter = Counter('lta_failures', 'lta processing failures', ['component', 'level', 'type'])
load_gauge = Gauge('lta_load_level', 'lta work processed', ['component', 'level', 'type'])
success_counter = Counter('lta_successes', 'lta processing successes', ['component', 'level', 'type'])


class NerscMover(Component):
    """
    NerscMover is a Long Term Archive component.

    A NerscMover runs at the NERSC site and is responsible for issuing the
    command necessary to move an archive ZIP from disk into the High
    Performance Storage System (HPSS) tape system.

    See: https://docs.nersc.gov/filesystems/archive/

    It uses the LTA DB to find bundles that have a 'taping' status. After
    issuing the HPSS command, the Bundle is updated in the LTA DB to have a
    'verifying' status.

    The HSI commands used to interact with the HPSS tape system are documented
    online.

    See: http://www.mgleicher.us/index.html/hsi/hsi_reference_manual_2/hsi_commands/
    """

    def __init__(self, config: Dict[str, str], logger: Logger) -> None:
        """
        Create a NerscMover component.

        config - A dictionary of required configuration values.
        logger - The object the nersc_mover should use for logging.
        """
        super(NerscMover, self).__init__("nersc_mover", config, logger)
        self.hpss_avail_path = config["HPSS_AVAIL_PATH"]
        self.max_count = int(config["MAX_COUNT"])
        self.rse_base_path = config["RSE_BASE_PATH"]
        self.tape_base_path = config["TAPE_BASE_PATH"]
        self.work_retries = int(config["WORK_RETRIES"])
        self.work_timeout_seconds = float(config["WORK_TIMEOUT_SECONDS"])

    def _do_status(self) -> Dict[str, Any]:
        """NerscMover has no additional status to contribute."""
        return {}

    def _expected_config(self) -> Dict[str, Optional[str]]:
        """NerscMover provides our expected configuration dictionary."""
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
        load_gauge.labels(component='nersc_mover', level='bundle', type='work').set(load_level)
        self.logger.info("Ending work on Bundles.")

    @wtt.spanned()
    async def _do_work_claim(self, lta_rc: RestClient) -> bool:
        """Claim a bundle and perform work on it."""
        # 0. Do some pre-flight checks to ensure that we can do work
        # if the HPSS system is not available
        args = [self.hpss_avail_path, "archive"]
        completed_process = run(args, stdout=PIPE, stderr=PIPE)
        if completed_process.returncode != 0:
            # prevent this instance from claiming any work
            self.logger.error(f"Unable to do work; HPSS system not available (returncode: {completed_process.returncode})")
            return False
        # 1. Ask the LTA DB for the next Bundle to be taped
        self.logger.info("Asking the LTA DB for a Bundle to tape at NERSC with HPSS.")
        pop_body = {
            "claimant": f"{self.name}-{self.instance_uuid}"
        }
        response = await lta_rc.request('POST', f'/Bundles/actions/pop?source={self.source_site}&dest={self.dest_site}&status={self.input_status}', pop_body)
        self.logger.info(f"LTA DB responded with: {response}")
        bundle = response["bundle"]
        if not bundle:
            self.logger.info("LTA DB did not provide a Bundle to tape at NERSC with HPSS. Going on vacation.")
            return False
        # process the Bundle that we were given
        try:
            await self._write_bundle_to_hpss(lta_rc, bundle)
            success_counter.labels(component='nersc_mover', level='bundle', type='work').inc()
            return True
        except Exception as e:
            failure_counter.labels(component='nersc_mover', level='bundle', type='exception').inc()
            bundle_id = bundle["uuid"]
            right_now = now()
            patch_body = {
                "original_status": bundle["status"],
                "status": "quarantined",
                "reason": f"BY:{self.name}-{self.instance_uuid} REASON:Exception during execution: {e}",
                "work_priority_timestamp": right_now,
            }
            self.logger.info(f"PATCH /Bundles/{bundle_id} - '{patch_body}'")
            await lta_rc.request('PATCH', f'/Bundles/{bundle_id}', patch_body)
        return False

    @wtt.spanned()
    async def _write_bundle_to_hpss(self, lta_rc: RestClient, bundle: BundleType) -> bool:
        """Replicate the supplied bundle using the configured transfer service."""
        bundle_id = bundle["uuid"]
        # determine the name and path of the bundle
        basename = os.path.basename(bundle["bundle_path"])
        data_warehouse_path = bundle["path"]
        # determine the input path that contains the bundle
        stupid_python_path = os.path.sep.join([self.rse_base_path, basename])
        input_path = os.path.normpath(stupid_python_path)
        # determine the output path where it should be stored on hpss
        stupid_python_path = os.path.sep.join([self.tape_base_path, data_warehouse_path, basename])
        hpss_path = os.path.normpath(stupid_python_path)
        # run an hsi command to create the destination directory
        #     mkdir     -> create a directory to store the bundle on tape
        #     -p        -> create any intermediate (parent) directories as necessary
        hpss_base = os.path.dirname(hpss_path)
        args = ["/usr/bin/hsi", "mkdir", "-p", hpss_base]
        if not await self._execute_hsi_command(lta_rc, bundle, args):
            return False
        # run an hsi command to put the file on tape
        #     put       -> write the source path to the hpss system at the dest path
        #     -c on     -> turn on the calculation of checksums by the hpss system
        #     -H sha512 -> specify that the SHA512 algorithm be used to calculate the checksum
        #     :         -> HPSS ... ¯\_(ツ)_/¯
        args = ["/usr/bin/hsi", "put", "-c", "on", "-H", "sha512", input_path, ":", hpss_path]
        if not await self._execute_hsi_command(lta_rc, bundle, args):
            return False
        # otherwise, update the Bundle in the LTA DB
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
    async def _execute_hsi_command(self, lta_rc: RestClient, bundle: BundleType, args: List[str]) -> bool:
        completed_process = run(args, stdout=PIPE, stderr=PIPE)
        # if our command failed
        if completed_process.returncode != 0:
            self.logger.info(f"Command to tape bundle to HPSS failed: {completed_process.args}")
            self.logger.info(f"returncode: {completed_process.returncode}")
            self.logger.info(f"stdout: {str(completed_process.stdout)}")
            self.logger.info(f"stderr: {str(completed_process.stderr)}")
            bundle_id = bundle["uuid"]
            right_now = now()
            patch_body = {
                "status": "quarantined",
                "reason": f"BY:{self.name}-{self.instance_uuid} REASON:hsi Command Failed - {completed_process.args} - {completed_process.returncode} - {str(completed_process.stdout)} - {str(completed_process.stderr)}",
                "work_priority_timestamp": right_now,
            }
            self.logger.info(f"PATCH /Bundles/{bundle_id} - '{patch_body}'")
            await lta_rc.request('PATCH', f'/Bundles/{bundle_id}', patch_body)
            return False
        # otherwise, we succeeded
        return True


async def main(nersc_mover: NerscMover) -> None:
    """Execute the work loop of the NerscMover component."""
    LOG.info("Starting asynchronous code")
    await work_loop(nersc_mover)
    LOG.info("Ending asynchronous code")


def main_sync() -> None:
    """Configure a NerscMover component from the environment and set it running."""
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
    # create our NerscMover service
    LOG.info("Starting synchronous code")
    nersc_mover = NerscMover(config, LOG)
    # let's get to work
    metrics_port = int(config["PROMETHEUS_METRICS_PORT"])
    start_http_server(metrics_port)
    asyncio.run(main(nersc_mover))
    LOG.info("Ending synchronous code")


if __name__ == "__main__":
    main_sync()
