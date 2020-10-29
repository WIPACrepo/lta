# unpacker.py
"""Module to implement the Unpacker component of the Long Term Archive."""

import asyncio
import json
from logging import Logger
import logging
import os
import shutil
import sys
from typing import Any, Dict, Optional
from zipfile import ZipFile

from rest_tools.client import RestClient  # type: ignore
from rest_tools.server import from_environment  # type: ignore

from .component import COMMON_CONFIG, Component, now, status_loop, work_loop
from .crypto import lta_checksums
from .log_format import StructuredFormatter
from .lta_types import BundleType

EXPECTED_CONFIG = COMMON_CONFIG.copy()
EXPECTED_CONFIG.update({
    "DEST_SITE": None,
    "FILE_CATALOG_REST_TOKEN": None,
    "FILE_CATALOG_REST_URL": None,
    "UNPACKER_OUTBOX_PATH": None,
    "UNPACKER_WORKBOX_PATH": None,
    "WORK_RETRIES": "3",
    "WORK_TIMEOUT_SECONDS": "30",
})

class Unpacker(Component):
    """
    Unpacker is a Long Term Archive component.

    An Unpacker is responsible for unpacking large ZIP64 archives of files that
    have been recalled from long term archive. It requests work from the LTA
    REST API to ask for bundles that are ready for unpacking. It unpacks the
    files and writes them to appropriate locations in the Data Warehouse. It
    updates the File Catalog to add the new warehouse location of the file.
    It then updates the LTA REST API to indicate that the provided bundle was
    unpacked and can now be deleted.
    """

    def __init__(self, config: Dict[str, str], logger: Logger) -> None:
        """
        Create a Unpacker component.

        config - A dictionary of required configuration values.
        logger - The object the unpacker should use for logging.
        """
        super(Unpacker, self).__init__("unpacker", config, logger)
        self.dest_site = config["DEST_SITE"]
        self.file_catalog_rest_token = config["FILE_CATALOG_REST_TOKEN"]
        self.file_catalog_rest_url = config["FILE_CATALOG_REST_URL"]
        self.outbox_path = config["UNPACKER_OUTBOX_PATH"]
        self.work_retries = int(config["WORK_RETRIES"])
        self.work_timeout_seconds = float(config["WORK_TIMEOUT_SECONDS"])
        self.workbox_path = config["UNPACKER_WORKBOX_PATH"]

    def _do_status(self) -> Dict[str, Any]:
        """Unpacker has no additional status to contribute."""
        return {}

    def _expected_config(self) -> Dict[str, Optional[str]]:
        """Unpacker provides our expected configuration dictionary."""
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
        # 1. Ask the LTA DB for the next Bundle to be unpacked
        # configure a RestClient to talk to the LTA DB
        lta_rc = RestClient(self.lta_rest_url,
                            token=self.lta_rest_token,
                            timeout=self.work_timeout_seconds,
                            retries=self.work_retries)
        self.logger.info("Asking the LTA DB for a Bundle to unpack.")
        pop_body = {
            "claimant": f"{self.name}-{self.instance_uuid}"
        }
        response = await lta_rc.request('POST', f'/Bundles/actions/pop?dest={self.dest_site}&status=unpacking', pop_body)
        self.logger.info(f"LTA DB responded with: {response}")
        bundle = response["bundle"]
        if not bundle:
            self.logger.info("LTA DB did not provide a Bundle to unpack. Going on vacation.")
            return False
        # process the Bundle that we were given
        try:
            await self._do_work_bundle(lta_rc, bundle)
        except Exception as e:
            await self._quarantine_bundle(lta_rc, bundle, f"{e}")
            raise e
        # signal the work was processed successfully
        return True

    async def _do_work_bundle(self, lta_rc: RestClient, bundle: BundleType) -> None:
        """Unpack the bundle to the Data Warehouse and update the File Catalog and LTA DB."""
        # 0. Get our ducks in a row about what we're doing here
        bundle_file = os.path.basename(bundle["bundle_path"])
        bundle_uuid = bundle_file.split(".")[0]
        bundle_file_path = os.path.join(self.workbox_path, f"{bundle_uuid}.zip")
        # 1. Unpack the archive from our workbox to our outbox
        self.logger.info(f"Unpacking bundle {bundle_file_path} to {self.outbox_path}")
        with ZipFile(bundle_file_path, mode="r", allowZip64=True) as bundle_zip:
            bundle_zip.extractall(path=self.outbox_path)
        # 2. Load the bundle's manifest metadata; structure example below:
        # metadata_dict = {
        #     "uuid": bundle_id,
        #     "component": "bundler",
        #     "version": 2,
        #     "create_timestamp": now(),
        #     "files": [
        #         {
        #             "checksum": {
        #                 "sha512": "09de7c539b724dee9543669309f978b172f6c7449d0269fecbb57d0c9cf7db51713fed3a94573c669fe0aa08fa122b41f84a0ea107c62f514b1525efbd08846b",
        #             },
        #             "file_size": 105311728,
        #             "logical_name": "/data/exp/IceCube/2013/filtered/PFFilt/1109/PFFilt_PhysicsFiltering_Run00123231_Subrun00000000_00000066.tar.bz2",
        #             "meta_modify_date": "2020-02-20 22:47:25.180303",
        #             "uuid": "2f0cb3c8-6cba-49b1-8eeb-13e13fed41dd",
        #         }
        #     ],
        # }
        metadata_file_path = os.path.join(self.outbox_path, f"{bundle_uuid}.metadata.json")
        with open(metadata_file_path) as metadata_file:
            metadata_dict = json.load(metadata_file)
        # 3. Move and verify each file described within the bundle's manifest metadata
        count_idx = 0
        count_max = len(metadata_dict["files"])
        for bundle_file in metadata_dict["files"]:
            # bump up the counter for the next file
            count_idx += 1
            # determine where the file lives on disk
            file_basename = os.path.basename(bundle_file["logical_name"])
            file_path = os.path.join(self.outbox_path, file_basename)
            self.logger.info(f"File {count_idx}/{count_max}: {file_basename}")
            # check that the size matches the expected size
            manifest_size = bundle_file["file_size"]
            disk_size = os.path.getsize(file_path)
            if disk_size != manifest_size:
                self.logger.error(f"Error: File '{file_basename}' has size {disk_size} bytes on disk, but the bundle metadata supplied size is {manifest_size} bytes.")
                raise ValueError(f"File:{file_basename} size Calculated:{disk_size} size Expected:{manifest_size}")
            # move the file to the appropriate location in the data warehouse
            dest_path = bundle_file["logical_name"]
            self.logger.info(f"Moving {file_basename} to the Data Warehouse at {dest_path}")
            shutil.move(file_path, dest_path)
            # check that the checksum matches the expected checksum
            self.logger.info(f"Verifying checksum for {dest_path}")
            manifest_checksum = bundle_file["checksum"]["sha512"]
            disk_checksum = lta_checksums(dest_path)
            if disk_checksum["sha512"] != manifest_checksum:
                self.logger.error(f"Error: File '{file_basename}' has sha512 checksum '{disk_checksum['sha512']}' but the bundle metadata supplied checksum '{manifest_checksum}'")
                raise ValueError(f"File:{file_basename} sha512 Calculated:{disk_checksum['sha512']} sha512 Expected:{manifest_checksum}")
            # add the new location to the file catalog
            await self._add_location_to_file_catalog(bundle_uuid, dest_path)
        # 4. Clean up the metadata file
        self.logger.info(f"Deleting bundle metadata file: '{metadata_file_path}'")
        os.remove(metadata_file_path)
        self.logger.info(f"Bundle metadata '{metadata_file_path}' was deleted.")
        # 5. Update the bundle record in the LTA DB
        await self._update_bundle_in_lta_db(lta_rc, bundle)

    async def _add_location_to_file_catalog(self, fc_uuid: str, fc_path: str) -> bool:
        """Update File Catalog record with new Data Warehouse location."""
        # configure a RestClient to talk to the File Catalog
        fc_rc = RestClient(self.file_catalog_rest_url,
                           token=self.file_catalog_rest_token,
                           timeout=self.work_timeout_seconds,
                           retries=self.work_retries)
        # add the new location to the File Catalog
        new_location = {
            "locations": [
                {
                    "site": "WIPAC",
                    "path": f"{fc_path}",
                }
            ]
        }
        self.logger.info(f"POST /api/files/{fc_uuid}/locations - {new_location}")
        # POST /api/files/{uuid}/locations will de-dupe locations for us
        await fc_rc.request("POST", f"/api/files/{fc_uuid}/locations", new_location)
        # indicate that our file catalog updates were successful
        return True

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

    async def _update_bundle_in_lta_db(self, lta_rc: RestClient, bundle: BundleType) -> bool:
        """Update the LTA DB to indicate the Bundle is unpacked."""
        bundle_id = bundle["uuid"]
        patch_body = {
            "status": "completed",
            "reason": "",
            "update_timestamp": now(),
            "claimed": False,
        }
        self.logger.info(f"PATCH /Bundles/{bundle_id} - '{patch_body}'")
        await lta_rc.request('PATCH', f'/Bundles/{bundle_id}', patch_body)
        # the morning sun has vanquished the horrible night
        return True

def runner() -> None:
    """Configure a Unpacker component from the environment and set it running."""
    # obtain our configuration from the environment
    config = from_environment(EXPECTED_CONFIG)
    # configure structured logging for the application
    structured_formatter = StructuredFormatter(
        component_type='Unpacker',
        component_name=config["COMPONENT_NAME"],
        ndjson=True)
    stream_handler = logging.StreamHandler(sys.stdout)
    stream_handler.setFormatter(structured_formatter)
    root_logger = logging.getLogger(None)
    root_logger.setLevel(logging.NOTSET)
    root_logger.addHandler(stream_handler)
    logger = logging.getLogger("lta.unpacker")
    # create our Unpacker service
    unpacker = Unpacker(config, logger)
    # let's get to work
    unpacker.logger.info("Adding tasks to asyncio loop")
    loop = asyncio.get_event_loop()
    loop.create_task(status_loop(unpacker))
    loop.create_task(work_loop(unpacker))


def main() -> None:
    """Configure a Unpacker component from the environment and set it running."""
    runner()
    asyncio.get_event_loop().run_forever()


if __name__ == "__main__":
    main()
