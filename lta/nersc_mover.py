# nersc_mover.py
"""Module to implement the NerscMover component of the Long Term Archive."""

import asyncio
from logging import Logger
import logging
import os
from subprocess import run
import sys
from typing import Any, Dict, List, Optional

from rest_tools.client import RestClient  # type: ignore

from .component import COMMON_CONFIG, Component, now, status_loop, work_loop
from .config import from_environment
from .log_format import StructuredFormatter
from .lta_types import BundleType


EXPECTED_CONFIG = COMMON_CONFIG.copy()
EXPECTED_CONFIG.update({
    "RSE_BASE_PATH": None,
    "TAPE_BASE_PATH": None,
    "WORK_RETRIES": "3",
    "WORK_TIMEOUT_SECONDS": "30",
})


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
        self.rse_bath_path = config["RSE_BASE_PATH"]
        self.tape_bath_path = config["TAPE_BASE_PATH"]
        self.work_retries = int(config["WORK_RETRIES"])
        self.work_timeout_seconds = float(config["WORK_TIMEOUT_SECONDS"])

    def _do_status(self) -> Dict[str, Any]:
        """NerscMover has no additional status to contribute."""
        return {}

    def _expected_config(self) -> Dict[str, Optional[str]]:
        """NerscMover provides our expected configuration dictionary."""
        return EXPECTED_CONFIG

    async def _do_work(self) -> None:
        """Perform a work cycle for this component."""
        self.logger.info("Starting work on Bundles.")
        work_claimed = True
        while work_claimed:
            work_claimed = await self._do_work_claim()
        self.logger.info("Ending work on Bundles.")

    async def _do_work_claim(self) -> bool:
        """Claim a bundle and perform work on it."""
        # 1. Ask the LTA DB for the next Bundle to be taped
        # configure a RestClient to talk to the LTA DB
        lta_rc = RestClient(self.lta_rest_url,
                            token=self.lta_rest_token,
                            timeout=self.work_timeout_seconds,
                            retries=self.work_retries)
        self.logger.info("Asking the LTA DB for a Bundle to tape at NERSC with HPSS.")
        pop_body = {
            "claimant": f"{self.name}-{self.instance_uuid}"
        }
        response = await lta_rc.request('POST', '/Bundles/actions/pop?dest=NERSC&status=taping', pop_body)
        self.logger.info(f"LTA DB responded with: {response}")
        bundle = response["bundle"]
        if not bundle:
            self.logger.info("LTA DB did not provide a Bundle to tape at NERSC with HPSS. Going on vacation.")
            return False
        # process the Bundle that we were given
        await self._write_bundle_to_hpss(lta_rc, bundle)
        return True

    async def _write_bundle_to_hpss(self, lta_rc: RestClient, bundle: BundleType) -> bool:
        """Replicate the supplied bundle using the configured transfer service."""
        bundle_id = bundle["uuid"]
        # determine the path where rucio copied the bundle
        basename = os.path.basename(bundle["bundle_path"])
        rucio_path = os.path.join(self.rse_bath_path, basename)
        # determine the path where it should be stored on hpss
        data_warehouse_path = bundle["path"]
        stupid_python_path = os.path.sep.join([self.tape_bath_path, data_warehouse_path, basename])
        hpss_path = os.path.normpath(stupid_python_path)
        # run an hsi command to create the destination directory
        #     mkdir     -> create a directory to store the bundle on tape
        #     -p        -> create any intermediate (parent) directories as necessary
        hpss_base = os.path.dirname(hpss_path)
        args = ["hsi", "mkdir", "-p", hpss_base]
        if not await self._execute_hsi_command(lta_rc, bundle, args):
            return False
        # run an hsi command to put the file on tape
        #     put       -> write the source path to the hpss system at the dest path
        #     -c on     -> turn on the calculation of checksums by the hpss system
        #     -H sha512 -> specify that the SHA512 algorithm be used to calculate the checksum
        #     :         -> HPSS ... ¯\_(ツ)_/¯
        args = ["hsi", "put", "-c", "on", "-H", "sha512", rucio_path, ":", hpss_path]
        if not await self._execute_hsi_command(lta_rc, bundle, args):
            return False
        # otherwise, update the Bundle in the LTA DB
        bundle["status"] = "verifying"
        bundle["update_timestamp"] = now()
        bundle["claimed"] = False
        self.logger.info(f"PATCH /Bundles/{bundle_id} - '{bundle}'")
        await lta_rc.request('PATCH', f'/Bundles/{bundle_id}', bundle)
        return True

    async def _execute_hsi_command(self, lta_rc: RestClient, bundle: BundleType, args: List[str]) -> bool:
        completed_process = run(args)
        # if our command failed
        if completed_process.returncode != 0:
            self.logger.info(f"Command to tape bundle to HPSS failed: {completed_process.args}")
            self.logger.info(f"returncode: {completed_process.returncode}")
            self.logger.info(f"stdout: {completed_process.stdout}")
            self.logger.info(f"stderr: {completed_process.stderr}")
            bundle_id = bundle["uuid"]
            bundle["status"] = "quarantined"
            bundle["reason"] = f"hsi Command Failed"
            self.logger.info(f"PATCH /Bundles/{bundle_id} - '{bundle}'")
            await lta_rc.request('PATCH', f'/Bundles/{bundle_id}', bundle)
            return False
        # otherwise, we succeeded
        return True

def runner() -> None:
    """Configure a NerscMover component from the environment and set it running."""
    # obtain our configuration from the environment
    config = from_environment(EXPECTED_CONFIG)
    # configure structured logging for the application
    structured_formatter = StructuredFormatter(
        component_type='NerscMover',
        component_name=config["COMPONENT_NAME"],
        ndjson=True)
    stream_handler = logging.StreamHandler(sys.stdout)
    stream_handler.setFormatter(structured_formatter)
    root_logger = logging.getLogger(None)
    root_logger.setLevel(logging.NOTSET)
    root_logger.addHandler(stream_handler)
    logger = logging.getLogger("lta.nersc_mover")
    # create our NerscMover service
    nersc_mover = NerscMover(config, logger)
    # let's get to work
    nersc_mover.logger.info("Adding tasks to asyncio loop")
    loop = asyncio.get_event_loop()
    loop.create_task(status_loop(nersc_mover))
    loop.create_task(work_loop(nersc_mover))

def main() -> None:
    """Configure a NerscMover component from the environment and set it running."""
    runner()
    asyncio.get_event_loop().run_forever()

if __name__ == "__main__":
    main()
