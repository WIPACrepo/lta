# nersc_retriever.py
"""Module to implement the NerscRetriever component of the Long Term Archive."""

import asyncio
from logging import Logger
import logging
import os
from subprocess import PIPE, run
import sys
from typing import Any, Dict, List, Optional

from rest_tools.client import RestClient  # type: ignore
from rest_tools.server import from_environment  # type: ignore
import wipac_telemetry.tracing_tools as wtt

from .component import COMMON_CONFIG, Component, now, status_loop, work_loop
from .log_format import StructuredFormatter
from .lta_types import BundleType


EXPECTED_CONFIG = COMMON_CONFIG.copy()
EXPECTED_CONFIG.update({
    "RSE_BASE_PATH": None,
    "TAPE_BASE_PATH": None,
    "WORK_RETRIES": "3",
    "WORK_TIMEOUT_SECONDS": "30",
})


class NerscRetriever(Component):
    """
    NerscRetriever is a Long Term Archive component.

    A NerscRetriever runs at the NERSC site and is responsible for issuing the
    command necessary to copy an archive ZIP from the High Performance Storage
    System (HPSS) tape system to scratch disk.

    See: https://docs.nersc.gov/filesystems/archive/

    It uses the LTA DB to find bundles that have a 'specified' status. After
    issuing the HPSS command, the Bundle is updated in the LTA DB to have a
    'staged' status.

    The HSI commands used to interact with the HPSS tape system are documented
    online.

    See: http://www.hpss-collaboration.org/documentation.shtml
    """

    def __init__(self, config: Dict[str, str], logger: Logger) -> None:
        """
        Create a NerscRetriever component.

        config - A dictionary of required configuration values.
        logger - The object the nersc_retriever should use for logging.
        """
        super(NerscRetriever, self).__init__("nersc_retriever", config, logger)
        self.rse_base_path = config["RSE_BASE_PATH"]
        self.tape_base_path = config["TAPE_BASE_PATH"]
        self.work_retries = int(config["WORK_RETRIES"])
        self.work_timeout_seconds = float(config["WORK_TIMEOUT_SECONDS"])

    def _do_status(self) -> Dict[str, Any]:
        """NerscRetriever has no additional status to contribute."""
        return {}

    def _expected_config(self) -> Dict[str, Optional[str]]:
        """NerscRetriever provides our expected configuration dictionary."""
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
        # 0. Do some pre-flight checks to ensure that we can do work
        # if the HPSS system is not available
        args = ["/usr/common/software/bin/hpss_avail", "archive"]
        completed_process = run(args, stdout=PIPE, stderr=PIPE)
        if completed_process.returncode != 0:
            # prevent this instance from claiming any work
            self.logger.error(f"Unable to do work; HPSS system not available (returncode: {completed_process.returncode})")
            return False
        # 1. Ask the LTA DB for the next Bundle to be taped
        self.logger.info("Asking the LTA DB for a Bundle copy from tape at NERSC with HPSS.")
        # configure a RestClient to talk to the LTA DB
        lta_rc = RestClient(self.lta_rest_url,
                            token=self.lta_rest_token,
                            timeout=self.work_timeout_seconds,
                            retries=self.work_retries)
        pop_body = {
            "claimant": f"{self.name}-{self.instance_uuid}"
        }
        response = await lta_rc.request('POST', f'/Bundles/actions/pop?source={self.source_site}&dest={self.dest_site}&status={self.input_status}', pop_body)
        self.logger.info(f"LTA DB responded with: {response}")
        bundle = response["bundle"]
        if not bundle:
            self.logger.info("LTA DB did not provide a Bundle to copy from tape at NERSC with HPSS. Going on vacation.")
            return False
        # process the Bundle that we were given
        try:
            await self._read_bundle_from_hpss(lta_rc, bundle)
            return True
        except Exception as e:
            bundle_id = bundle["uuid"]
            right_now = now()
            patch_body = {
                "status": "quarantined",
                "reason": f"BY:{self.name}-{self.instance_uuid} REASON:Exception during execution: {e}",
                "work_priority_timestamp": right_now,
            }
            self.logger.info(f"PATCH /Bundles/{bundle_id} - '{patch_body}'")
            await lta_rc.request('PATCH', f'/Bundles/{bundle_id}', patch_body)
        return False

    @wtt.spanned()
    async def _read_bundle_from_hpss(self, lta_rc: RestClient, bundle: BundleType) -> bool:
        """Send a command to HPSS to retrieve the supplied bundle from tape."""
        bundle_id = bundle["uuid"]
        # determine the name and path of the bundle
        basename = os.path.basename(bundle["bundle_path"])
        data_warehouse_path = bundle["path"]
        # determine the input path where it should be stored on hpss
        stupid_python_path = os.path.sep.join([self.tape_base_path, data_warehouse_path, basename])
        hpss_path = os.path.normpath(stupid_python_path)
        # determine the output path where we stage the bundle for transfer
        stupid_python_path = os.path.sep.join([self.rse_base_path, basename])
        output_path = os.path.normpath(stupid_python_path)
        # run an hsi command to get the file from tape
        #     get       -> read the source path from the hpss system to the dest path
        #     -c on     -> turn on the verification of checksums by the hpss system
        #     :         -> HPSS ... ¯\_(ツ)_/¯
        args = ["/usr/bin/hsi", "get", "-c", "on", output_path, ":", hpss_path]
        if not await self._execute_hsi_command(lta_rc, bundle, args):
            return False
        # update the Bundle in the LTA DB
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
            self.logger.info(f"Command to read bundle from HPSS failed: {completed_process.args}")
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

def runner() -> None:
    """Configure a NerscRetriever component from the environment and set it running."""
    # obtain our configuration from the environment
    config = from_environment(EXPECTED_CONFIG)
    # configure structured logging for the application
    structured_formatter = StructuredFormatter(
        component_type='NerscRetriever',
        component_name=config["COMPONENT_NAME"],
        ndjson=True)
    stream_handler = logging.StreamHandler(sys.stdout)
    stream_handler.setFormatter(structured_formatter)
    root_logger = logging.getLogger(None)
    root_logger.setLevel(logging.NOTSET)
    root_logger.addHandler(stream_handler)
    logger = logging.getLogger("lta.nersc_retriever")
    # create our NerscRetriever service
    nersc_retriever = NerscRetriever(config, logger)
    # let's get to work
    nersc_retriever.logger.info("Adding tasks to asyncio loop")
    loop = asyncio.get_event_loop()
    loop.create_task(status_loop(nersc_retriever))
    loop.create_task(work_loop(nersc_retriever))

def main() -> None:
    """Configure a NerscRetriever component from the environment and set it running."""
    runner()
    asyncio.get_event_loop().run_forever()

if __name__ == "__main__":
    main()
