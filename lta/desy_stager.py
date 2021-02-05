# desy_stager.py
"""Module to implement the DesyStager component of the Long Term Archive."""

import asyncio
from logging import Logger
import logging
import os
import shutil
import sys
from typing import Any, Dict, List, Optional, Tuple

from rest_tools.client import RestClient  # type: ignore
from rest_tools.server import from_environment  # type: ignore

from .component import COMMON_CONFIG, Component, now, status_loop, work_loop
from .log_format import StructuredFormatter
from .lta_types import BundleType

EXPECTED_CONFIG = COMMON_CONFIG.copy()
EXPECTED_CONFIG.update({
    "BUNDLE_DEST_PATH": None,
    "BUNDLE_SOURCE_PATH": None,
    "DEST_SITE": None,
    "WORK_RETRIES": "3",
    "WORK_TIMEOUT_SECONDS": "30",
})


def _enumerate_path(path: str) -> List[str]:
    """Recursively walk the file system to enumerate files at provided path."""
    # enumerate all of the files on disk to be checked
    disk_files = []
    for root, dirs, files in os.walk(path):
        disk_files.extend([os.path.join(root, file) for file in files])
    return disk_files

def _get_files_and_size(path: str) -> Tuple[List[str], int]:
    """Recursively walk and add the files of files in the file system."""
    # enumerate all of the files on disk to be checked
    disk_files = _enumerate_path(path)
    # for all of the files we want to check
    size = 0
    for disk_file in disk_files:
        # determine the size of the file
        size += os.path.getsize(disk_file)
    return (disk_files, size)


class DesyStager(Component):
    """
    DesyStager is a Long Term Archive component.

    A DesyStager is responsible for moving files from the Bundler staging
    area to the upload area.
    """

    def __init__(self, config: Dict[str, str], logger: Logger) -> None:
        """
        Create a DesyStager component.

        config - A dictionary of required configuration values.
        logger - The object the desy_stager should use for logging.
        """
        super(DesyStager, self).__init__("desy_stager", config, logger)
        self.bundle_dest_path = config["BUNDLE_DEST_PATH"]
        self.bundle_source_path = config["BUNDLE_SOURCE_PATH"]
        self.dest_site = config["DEST_SITE"]
        self.work_retries = int(config["WORK_RETRIES"])
        self.work_timeout_seconds = float(config["WORK_TIMEOUT_SECONDS"])

    def _do_status(self) -> Dict[str, Any]:
        """Contribute no additional status."""
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
        # 1. Ask the LTA DB for the next Bundle to be staged
        # configure a RestClient to talk to the LTA DB
        lta_rc = RestClient(self.lta_rest_url,
                            token=self.lta_rest_token,
                            timeout=self.work_timeout_seconds,
                            retries=self.work_retries)
        self.logger.info("Asking the LTA DB for a Bundle to stage.")
        pop_body = {
            "claimant": f"{self.name}-{self.instance_uuid}"
        }
        response = await lta_rc.request('POST', f'/Bundles/actions/pop?source={self.source_site}&dest={self.dest_site}&status=created', pop_body)
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

    async def _stage_bundle(self, lta_rc: RestClient, bundle: BundleType) -> bool:
        """Stage the Bundle for transfer to DESY."""
        bundle_id = bundle["uuid"]
        # this bundle is ready to be staged
        bundle_name = os.path.basename(bundle["bundle_path"])
        src_path = os.path.join(self.bundle_source_path, bundle_name)
        dst_path = os.path.join(self.bundle_dest_path, bundle_name)
        self.logger.info(f"Moving Bundle {src_path} -> {dst_path}")
        shutil.move(src_path, dst_path)
        # update the Bundle in the LTA DB
        self.logger.info("Bundle has been staged for transfer to DESY.")
        patch_body = {
            "bundle_path": dst_path,
            "claimed": False,
            "status": "staged",
            "reason": "",
            "update_timestamp": now(),
        }
        self.logger.info(f"PATCH /Bundles/{bundle_id} - '{patch_body}'")
        await lta_rc.request('PATCH', f'/Bundles/{bundle_id}', patch_body)
        return True


def runner() -> None:
    """Configure a DesyStager component from the environment and set it running."""
    # obtain our configuration from the environment
    config = from_environment(EXPECTED_CONFIG)
    # configure structured logging for the application
    structured_formatter = StructuredFormatter(
        component_type='DesyStager',
        component_name=config["COMPONENT_NAME"],
        ndjson=True)
    stream_handler = logging.StreamHandler(sys.stdout)
    stream_handler.setFormatter(structured_formatter)
    root_logger = logging.getLogger(None)
    root_logger.setLevel(logging.NOTSET)
    root_logger.addHandler(stream_handler)
    logger = logging.getLogger("lta.desy_stager")
    # create our DesyStager service
    desy_stager = DesyStager(config, logger)
    # let's get to work
    desy_stager.logger.info("Adding tasks to asyncio loop")
    loop = asyncio.get_event_loop()
    loop.create_task(status_loop(desy_stager))
    loop.create_task(work_loop(desy_stager))


def main() -> None:
    """Configure a DesyStager component from the environment and set it running."""
    runner()
    asyncio.get_event_loop().run_forever()


if __name__ == "__main__":
    main()
