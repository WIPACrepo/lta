# unpacker.py
"""Module to implement the Unpacker component of the Long Term Archive."""

# fmt:off

import asyncio
import json
import logging
import os
from pathlib import Path
import shutil
import sys
from typing import Any, cast, Dict, Optional
from zipfile import ZipFile

from prometheus_client import start_http_server
from rest_tools.client import ClientCredentialsAuth, RestClient
from wipac_dev_tools import strtobool

from .utils import LTANounEnum
from .component import COMMON_CONFIG, Component, QuarantineNow, now, work_loop
from .crypto import lta_checksums
from .lta_tools import from_environment
from .lta_types import BundleType


Logger = logging.Logger

LOG = logging.getLogger(__name__)

EXPECTED_CONFIG = COMMON_CONFIG.copy()
EXPECTED_CONFIG.update({
    "CLEAN_OUTBOX": "TRUE",
    "FILE_CATALOG_CLIENT_ID": None,
    "FILE_CATALOG_CLIENT_SECRET": None,
    "FILE_CATALOG_REST_URL": None,
    "PATH_MAP_JSON": None,
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
        super().__init__("unpacker", LTANounEnum.BUNDLE, config, logger)
        self.clean_outbox = strtobool(config["CLEAN_OUTBOX"])
        self.file_catalog_client_id = config["FILE_CATALOG_CLIENT_ID"]
        self.file_catalog_client_secret = config["FILE_CATALOG_CLIENT_SECRET"]
        self.file_catalog_rest_url = config["FILE_CATALOG_REST_URL"]
        self.outbox_path = config["UNPACKER_OUTBOX_PATH"]
        self.work_retries = int(config["WORK_RETRIES"])
        self.work_timeout_seconds = float(config["WORK_TIMEOUT_SECONDS"])
        self.workbox_path = config["UNPACKER_WORKBOX_PATH"]
        path_map_json = Path(config["PATH_MAP_JSON"]).read_text()
        self.path_map = json.loads(path_map_json)

    def _do_status(self) -> Dict[str, Any]:
        """Unpacker has no additional status to contribute."""
        return {}

    def _expected_config(self) -> Dict[str, Optional[str]]:
        """Unpacker provides our expected configuration dictionary."""
        return EXPECTED_CONFIG

    async def _do_work_claim(self, lta_rc: RestClient) -> bool | QuarantineNow:
        """Claim a bundle and perform work on it -- see super for return value meanings."""
        # 1. Ask the LTA DB for the next Bundle to be unpacked
        self.logger.info("Asking the LTA DB for a Bundle to unpack.")
        pop_body = {
            "claimant": f"{self.name}-{self.instance_uuid}"
        }
        response = await lta_rc.request('POST', f'/Bundles/actions/pop?source={self.source_site}&dest={self.dest_site}&status={self.input_status}', pop_body)
        self.logger.info(f"LTA DB responded with: {response}")
        bundle = response["bundle"]
        if not bundle:
            self.logger.info("LTA DB did not provide a Bundle to unpack. Going on vacation.")
            return False
        # process the Bundle that we were given
        try:
            await self._do_work_bundle(lta_rc, bundle)
            return True
        except Exception as e:
            return QuarantineNow(bundle, e)

    async def _do_work_bundle(self, lta_rc: RestClient, bundle: BundleType) -> None:
        """Unpack the bundle to the Data Warehouse and update the File Catalog and LTA DB."""
        # 0. Get our ducks in a row about what we're doing here
        bundle_file = os.path.basename(bundle["bundle_path"])
        bundle_uuid = bundle_file.split(".")[0]
        bundle_file_path = os.path.join(self.workbox_path, f"{bundle_uuid}.zip")
        # 1. Obtain the ZipInfo objects for the bundle zip file
        bundle_zip_file = ZipFile(bundle_file_path, mode="r", allowZip64=True)
        zip_info_list = bundle_zip_file.infolist()
        # 2. Extract the bundle's metadata manifest
        manifest_info = zip_info_list[0]  # assume metadata manifest at index 0
        if not manifest_info.filename.startswith(bundle_uuid):
            self.logger.error(f"Error: zip_info_list[0] in Bundle '{bundle_file_path}' is not JSON/NDJSON metadata. Expected: '{bundle_uuid}.metadata.json' or '{bundle_uuid}.metadata.ndjson'. Found: '{manifest_info.filename}'")
            raise ValueError(f"Error: zip_info_list[0] in Bundle '{bundle_file_path}' is not JSON/NDJSON metadata. Expected: '{bundle_uuid}.metadata.json' or '{bundle_uuid}.metadata.ndjson'. Found: '{manifest_info.filename}'")
        self.logger.info(f"Extracting '{manifest_info.filename}' to {self.outbox_path}")
        bundle_zip_file.extract(manifest_info, path=self.outbox_path)
        # 3. Read the bundle's metadata manifest into a dictionary
        metadata_dict = self._read_manifest_metadata(bundle_uuid)
        # 4. Move and verify each file described within the bundle's manifest metadata
        count_idx = 0
        count_max = len(metadata_dict["files"])
        for bundle_file in metadata_dict["files"]:
            # bump up the counter for the next file
            count_idx += 1
            # get the ZipInfo object for the file
            file_zipinfo = zip_info_list[count_idx]
            # determine where the file will live on disk
            logical_name = self._map_dest_path(bundle_file["logical_name"])
            file_basename = os.path.basename(logical_name)
            file_path = os.path.join(self.outbox_path, file_basename)
            # do a sanity check to make sure our metadata matches in filename
            if file_zipinfo.filename != file_basename:
                self.logger.error(f"Error: Unpacking metadata mismatch on index {count_idx}. ZipInfo.filename:'{file_zipinfo.filename}' vs file_basename:'{file_basename}'.")
                raise ValueError(f"Error: Unpacking metadata mismatch on index {count_idx}. ZipInfo.filename:'{file_zipinfo.filename}' vs file_basename:'{file_basename}'.")
            # extract the file from the bundle zip to the work directory
            self.logger.info(f"File {count_idx}/{count_max}: {file_basename} ({file_path})")
            bundle_zip_file.extract(file_zipinfo, path=self.outbox_path)
            # check that the size matches the expected size
            manifest_size = bundle_file["file_size"]
            disk_size = os.path.getsize(file_path)
            if disk_size != manifest_size:
                self.logger.error(f"Error: File '{file_basename}' has size {disk_size} bytes on disk, but the bundle metadata supplied size is {manifest_size} bytes.")
                raise ValueError(f"File:{file_basename} size Calculated:{disk_size} size Expected:{manifest_size}")
            # do a sanity check to make sure our metadata matches in file size
            if file_zipinfo.file_size != disk_size:
                self.logger.error(f"Error: Unpacking metadata mismatch on index {count_idx}. ZipInfo.file_size:'{file_zipinfo.file_size}' vs disk_size:'{disk_size}'.")
                raise ValueError(f"Error: Unpacking metadata mismatch on index {count_idx}. ZipInfo.file_size:'{file_zipinfo.file_size}' vs disk_size:'{disk_size}'.")
            # move the file to the appropriate location in the data warehouse
            self._ensure_dest_directory(logical_name)
            self.logger.info(f"Moving {file_basename} from {file_path} to the Data Warehouse at {logical_name}")
            shutil.move(file_path, logical_name)
            # check that the checksum matches the expected checksum
            self.logger.info(f"Verifying checksum for {logical_name}")
            manifest_checksum = bundle_file["checksum"]["sha512"]
            disk_checksum = lta_checksums(logical_name)
            if disk_checksum["sha512"] != manifest_checksum:
                self.logger.error(f"Error: File '{file_basename}' has sha512 checksum '{disk_checksum['sha512']}' but the bundle metadata supplied checksum '{manifest_checksum}'")
                raise ValueError(f"File:{file_basename} sha512 Calculated:{disk_checksum['sha512']} sha512 Expected:{manifest_checksum}")
            # add the new location to the file catalog
            await self._add_location_to_file_catalog(bundle_file, logical_name)
        # 4. Clean up the metadata file
        self._delete_manifest_metadata(bundle_uuid)
        # 5. Clean up the outbox directory (remove unzip subdirectories, if necessary)
        self._clean_outbox_directory()
        # 6. Update the bundle record in the LTA DB
        await self._update_bundle_in_lta_db(lta_rc, bundle)

    async def _add_location_to_file_catalog(self,
                                            bundle_file: Dict[str, Any],
                                            dest_path: str) -> bool:
        """Update File Catalog record with new Data Warehouse location."""
        # configure a RestClient to talk to the File Catalog
        fc_rc = ClientCredentialsAuth(address=self.file_catalog_rest_url,
                                      token_url=self.lta_auth_openid_url,
                                      client_id=self.file_catalog_client_id,
                                      client_secret=self.file_catalog_client_secret)
        # extract the right variables from the metadata structure
        fc_path = dest_path
        fc_uuid = bundle_file["uuid"]
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

    def _clean_outbox_directory(self) -> None:
        # if we don't care about subdirectories in the work directory, bail
        if not self.clean_outbox:
            self.logger.info(f"CLEAN_OUTBOX == False; will not remove entries from '{self.outbox_path}'")
            return
        # list all of the items in the work directory
        self.logger.info(f"Scanning '{self.outbox_path}' for entries to remove")
        with os.scandir(path=self.outbox_path) as it:
            for entry in it:
                self.logger.info(f"Processing '{entry.name}' at '{entry.path}'")
                # if it's a file, remove it
                if entry.is_file():
                    self.logger.info(f"'{entry.name}' is a file, will os.remove '{entry.path}'")
                    os.remove(entry.path)
                    continue
                # if it's a directory, remove the tree
                if entry.is_dir():
                    self.logger.info(f"'{entry.name}' is a directory, will shutil.rmtree '{entry.path}'")
                    shutil.rmtree(path=entry.path, ignore_errors=True)
                    continue
                # if we can't figure it out, log an error
                self.logger.error(f"'{entry.name}' was neither a file, nor a directory; nothing will be done")
        # inform the caller that we finished
        self.logger.info(f"Finished processing '{self.outbox_path}' for entries to remove")

    def _delete_manifest_metadata(self, bundle_uuid: str) -> None:
        metadata_file_path = os.path.join(self.outbox_path, f"{bundle_uuid}.metadata.json")
        self.logger.info(f"Deleting bundle metadata file: '{metadata_file_path}'")
        try:
            os.remove(metadata_file_path)
        except Exception:
            metadata_file_path = os.path.join(self.outbox_path, f"{bundle_uuid}.metadata.ndjson")
            try:
                os.remove(metadata_file_path)
            except Exception as e:
                raise e
        self.logger.info(f"Bundle metadata '{metadata_file_path}' was deleted.")

    def _ensure_dest_directory(self, dest_path: str) -> None:
        """Ensure the destination directory exists in the Data Warehouse."""
        dest_dir = os.path.dirname(dest_path)
        self.logger.info(f"Creating Data Warehouse directory: {dest_dir}")
        Path(dest_dir).mkdir(parents=True, exist_ok=True)

    def _map_dest_path(self, dest_path: str) -> str:
        """Use the configured path map to remap the destination path if necessary."""
        for prefix, remap in self.path_map.items():
            if dest_path.startswith(prefix):
                return dest_path.replace(prefix, remap)
        return dest_path

    def _read_manifest_metadata(self, bundle_uuid: str) -> Dict[str, Any]:
        """Read the bundle metadata from the manifest file."""
        # try with version 2
        metadata_dict = self._read_manifest_metadata_v2(bundle_uuid)
        if metadata_dict:
            return metadata_dict
        # try with version 3
        metadata_dict = self._read_manifest_metadata_v3(bundle_uuid)
        if metadata_dict:
            return metadata_dict
        # whoops, we have no idea how to read the manifest
        raise Exception("Unknown bundle manifest version")

    def _read_manifest_metadata_v2(self, bundle_uuid: str) -> Optional[Dict[str, Any]]:
        """Read the bundle metadata from an older (version 2) manifest file."""
        metadata_file_path = os.path.join(self.outbox_path, f"{bundle_uuid}.metadata.json")
        try:
            with open(metadata_file_path) as metadata_file:
                metadata_dict = json.load(metadata_file)
        except Exception:
            return None
        return cast(Dict[str, Any], metadata_dict)

    def _read_manifest_metadata_v3(self, bundle_uuid: str) -> Optional[Dict[str, Any]]:
        """Read the bundle metadata from a newer (version 3) manifest file."""
        metadata_file_path = os.path.join(self.outbox_path, f"{bundle_uuid}.metadata.ndjson")
        try:
            with open(metadata_file_path) as metadata_file:
                # read the JSON for the bundle
                line = metadata_file.readline()
                metadata_dict = json.loads(line)
                metadata_dict["files"] = []
                # read the JSON for each file in the manifest
                line = metadata_file.readline()
                while line:
                    file_dict = json.loads(line)
                    metadata_dict["files"].append(file_dict)
                    line = metadata_file.readline()
        except Exception:
            return None
        return cast(Dict[str, Any], metadata_dict)

    async def _update_bundle_in_lta_db(self, lta_rc: RestClient, bundle: BundleType) -> bool:
        """Update the LTA DB to indicate the Bundle is unpacked."""
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


async def main(unpacker: Unpacker) -> None:
    """Execute the work loop of the Unpacker component."""
    LOG.info("Starting asynchronous code")
    await work_loop(unpacker)
    LOG.info("Ending asynchronous code")


def main_sync() -> None:
    """Configure a Unpacker component from the environment and set it running."""
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
    # create our Unpacker service
    LOG.info("Starting synchronous code")
    unpacker = Unpacker(config, LOG)
    # let's get to work
    metrics_port = int(config["PROMETHEUS_METRICS_PORT"])
    start_http_server(metrics_port)
    asyncio.run(main(unpacker))
    LOG.info("Ending synchronous code")


if __name__ == "__main__":
    main_sync()
