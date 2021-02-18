# desy_verifier.py
"""Module to implement the DesyVerifier component of the Long Term Archive."""

import asyncio
from logging import Logger
import logging
import os
import sys
from typing import Any, Dict, Optional

from rest_tools.client import RestClient  # type: ignore
from rest_tools.server import from_environment  # type: ignore

from .component import COMMON_CONFIG, Component, now, status_loop, work_loop
from .joiner import join_smart
from .log_format import StructuredFormatter
from .lta_types import BundleType


EXPECTED_CONFIG = COMMON_CONFIG.copy()
EXPECTED_CONFIG.update({
    "FILE_CATALOG_REST_TOKEN": None,
    "FILE_CATALOG_REST_URL": None,
    "TAPE_BASE_PATH": None,
    "WORK_RETRIES": "3",
    "WORK_TIMEOUT_SECONDS": "30",
})


def as_catalog_record(bundle_record: BundleType) -> Dict[str, Any]:
    """Cherry pick keys from a File Catalog record to include in Bundle metadata."""
    catalog_record = bundle_record.copy()
    del catalog_record["files"]
    return catalog_record


class DesyVerifier(Component):
    """
    DesyVerifier is a Long Term Archive component.

    A DesyVerifier uses the LTA DB to find bundles that have been verified as
    successfully transferred to DESY. It registers the bundle itself with the
    File Catalog, and updates the records for the constituent files to indicate
    that a copy of that files lives in archive at DESY.
    """

    def __init__(self, config: Dict[str, str], logger: Logger) -> None:
        """
        Create a DesyVerifier component.

        config - A dictionary of required configuration values.
        logger - The object the desy_verifier should use for logging.
        """
        super(DesyVerifier, self).__init__("desy_verifier", config, logger)
        self.file_catalog_rest_token = config["FILE_CATALOG_REST_TOKEN"]
        self.file_catalog_rest_url = config["FILE_CATALOG_REST_URL"]
        self.tape_base_path = config["TAPE_BASE_PATH"]
        self.work_retries = int(config["WORK_RETRIES"])
        self.work_timeout_seconds = float(config["WORK_TIMEOUT_SECONDS"])

    def _do_status(self) -> Dict[str, Any]:
        """DesyVerifier has no additional status to contribute."""
        return {}

    def _expected_config(self) -> Dict[str, Optional[str]]:
        """DesyVerifier provides our expected configuration dictionary."""
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
        # 1. Ask the LTA DB for the next Bundle to be verified
        self.logger.info("Asking the LTA DB for a Bundle to record as verified at DESY.")
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
            self.logger.info("LTA DB did not provide a Bundle to record as verified at DESY. Going on vacation.")
            return False
        # process the Bundle that we were given
        try:
            await self._add_bundle_to_file_catalog(bundle)
            await self._update_bundle_in_lta_db(lta_rc, bundle)
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

    async def _add_bundle_to_file_catalog(self, bundle: BundleType) -> bool:
        """Add a FileCatalog entry for the bundle, then update existing records."""
        # configure a RestClient to talk to the File Catalog
        fc_rc = RestClient(self.file_catalog_rest_url,
                           token=self.file_catalog_rest_token,
                           timeout=self.work_timeout_seconds,
                           retries=self.work_retries)
        # determine the path where the bundle is stored at DESY
        data_warehouse_path = bundle["path"]  # /data/exp/IceCube/2015/filtered/level2/0320
        basename = os.path.basename(bundle["bundle_path"])  # 604b6c80659c11eb8ad66224ddddaab7.zip
        desy_tape_path = join_smart([self.tape_base_path, data_warehouse_path, basename])
        # create a File Catalog entry for the bundle itself
        file_record = {
            "uuid": bundle["uuid"],
            "logical_name": desy_tape_path,
            "checksum": bundle["checksum"],
            "locations": [
                {
                    "site": "DESY",
                    "path": desy_tape_path,
                    "online": False,
                }
            ],
            "file_size": bundle["size"],
            # note: 'lta' is an application-private metadata field
            "lta": as_catalog_record(bundle)
        }
        # add the bundle file to the File Catalog
        try:
            self.logger.info(f"POST /api/files - {desy_tape_path}")
            await fc_rc.request("POST", "/api/files", file_record)
        except Exception as e:
            self.logger.error(f"Error: POST /api/files - {desy_tape_path}")
            self.logger.error(f"Message: {e}")
            uuid = bundle["uuid"]
            self.logger.info(f"PATCH /api/files/{uuid}")
            await fc_rc.request("PATCH", f"/api/files/{uuid}", file_record)
        # for each file contained in the bundle
        for fc_file in bundle["files"]:
            fc_file_uuid = fc_file["uuid"]
            # read the current file entry in the File Catalog
            fc_record = await fc_rc.request("GET", f"/api/files/{fc_file_uuid}")
            logical_name = fc_record["logical_name"]
            # add a location indicating the bundle archive
            new_location = {
                "locations": [
                    {
                        "site": "DESY",
                        "path": f"{desy_tape_path}:{logical_name}",
                        "archive": True,
                    }
                ]
            }
            self.logger.info(f"POST /api/files/{fc_file_uuid}/locations - {new_location}")
            # POST /api/files/{uuid}/locations will de-dupe locations for us
            await fc_rc.request("POST", f"/api/files/{fc_file_uuid}/locations", new_location)
        # indicate that our file catalog updates were successful
        return True

    async def _update_bundle_in_lta_db(self, lta_rc: RestClient, bundle: BundleType) -> bool:
        """Update the LTA DB to indicate the Bundle is verified."""
        bundle_id = bundle["uuid"]
        patch_body = {
            "status": self.output_status,
            "reason": "",
            "update_timestamp": now(),
            "claimed": False,
        }
        self.logger.info(f"PATCH /Bundles/{bundle_id} - '{patch_body}'")
        await lta_rc.request('PATCH', f'/Bundles/{bundle_id}', patch_body)
        # the morning sun has vanquished the horrible night
        return True


def runner() -> None:
    """Configure a DesyVerifier component from the environment and set it running."""
    # obtain our configuration from the environment
    config = from_environment(EXPECTED_CONFIG)
    # configure structured logging for the application
    structured_formatter = StructuredFormatter(
        component_type='DesyVerifier',
        component_name=config["COMPONENT_NAME"],
        ndjson=True)
    stream_handler = logging.StreamHandler(sys.stdout)
    stream_handler.setFormatter(structured_formatter)
    root_logger = logging.getLogger(None)
    root_logger.setLevel(logging.NOTSET)
    root_logger.addHandler(stream_handler)
    logger = logging.getLogger("lta.desy_verifier")
    # create our DesyVerifier service
    desy_verifier = DesyVerifier(config, logger)
    # let's get to work
    desy_verifier.logger.info("Adding tasks to asyncio loop")
    loop = asyncio.get_event_loop()
    loop.create_task(status_loop(desy_verifier))
    loop.create_task(work_loop(desy_verifier))

def main() -> None:
    """Configure a DesyVerifier component from the environment and set it running."""
    runner()
    asyncio.get_event_loop().run_forever()

if __name__ == "__main__":
    main()
