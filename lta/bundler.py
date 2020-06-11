# bundler.py
"""Module to implement the Bundler component of the Long Term Archive."""

import asyncio
import json
from logging import Logger
import logging
import os
import shutil
import sys
from typing import Any, Dict, Optional
from zipfile import ZIP_STORED, ZipFile

from rest_tools.client import RestClient  # type: ignore
from rest_tools.server import from_environment  # type: ignore

from .component import COMMON_CONFIG, Component, now, status_loop, work_loop
from .crypto import lta_checksums
from .log_format import StructuredFormatter
from .lta_types import BundleType

EXPECTED_CONFIG = COMMON_CONFIG.copy()
EXPECTED_CONFIG.update({
    "BUNDLER_OUTBOX_PATH": None,
    "BUNDLER_WORKBOX_PATH": None,
    "MYSQL_DB": None,
    "MYSQL_HOST": None,
    "MYSQL_PASSWORD": None,
    "MYSQL_PORT": "3306",
    "MYSQL_USER": None,
    "WORK_RETRIES": "3",
    "WORK_TIMEOUT_SECONDS": "30",
})

class Bundler(Component):
    """
    Bundler is a Long Term Archive component.

    A Bundler is responsible for creating large ZIP64 archives of files that
    should be moved to long term archive. It requests work from the LTA REST
    API in the form of files to put into a large archive. It creates the ZIP64
    archive and moves the file to staging disk. It then updates the LTA REST
    API to indicate that the provided files were so bundled.
    """

    def __init__(self, config: Dict[str, str], logger: Logger) -> None:
        """
        Create a Bundler component.

        config - A dictionary of required configuration values.
        logger - The object the bundler should use for logging.
        """
        super(Bundler, self).__init__("bundler", config, logger)
        self.db = config["MYSQL_DB"]
        self.host = config["MYSQL_HOST"]
        self.outbox_path = config["BUNDLER_OUTBOX_PATH"]
        self.password = config["MYSQL_PASSWORD"]
        self.port = int(config["MYSQL_PORT"])
        self.user = config["MYSQL_USER"]
        self.work_retries = int(config["WORK_RETRIES"])
        self.work_timeout_seconds = float(config["WORK_TIMEOUT_SECONDS"])
        self.workbox_path = config["BUNDLER_WORKBOX_PATH"]

    def _do_status(self) -> Dict[str, Any]:
        """Bundler has no additional status to contribute."""
        return {}

    def _expected_config(self) -> Dict[str, Optional[str]]:
        """Bundler provides our expected configuration dictionary."""
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
        # 1. Ask the LTA DB for the next Bundle to be built
        # configure a RestClient to talk to the LTA DB
        lta_rc = RestClient(self.lta_rest_url,
                            token=self.lta_rest_token,
                            timeout=self.work_timeout_seconds,
                            retries=self.work_retries)
        self.logger.info("Asking the LTA DB for a Bundle to build.")
        pop_body = {
            "claimant": f"{self.name}-{self.instance_uuid}"
        }
        response = await lta_rc.request('POST', f'/Bundles/actions/pop?source={self.source_site}&status=specified', pop_body)
        self.logger.info(f"LTA DB responded with: {response}")
        bundle = response["bundle"]
        if not bundle:
            self.logger.info("LTA DB did not provide a Bundle to build. Going on vacation.")
            return False
        # process the Bundle that we were given
        await self._do_work_bundle(lta_rc, bundle)
        return True

    async def _do_work_bundle(self, lta_rc: RestClient, bundle: BundleType) -> None:
        """Build the archive file for a bundle and update the LTA DB."""
        # 0. Get our ducks in a row about what we're doing here
        num_files = len(bundle["files"])
        source = bundle["source"]
        dest = bundle["dest"]
        self.logger.info(f"There are {num_files} Files to bundle from '{source}' to '{dest}'.")
        # 1. Create a manifest of the bundle, including all metadata
        bundle_id = bundle["uuid"]
        self.logger.info(f"Bundle archive file will be '{bundle_id}.zip'")
        metadata_dict = {
            "uuid": bundle_id,
            "component": "bundler",
            "version": 2,
            "create_timestamp": now(),
            "files": bundle["files"],
        }
        metadata_file_path = os.path.join(self.workbox_path, f"{bundle_id}.metadata.json")
        with open(metadata_file_path, mode="w") as metadata_file:
            self.logger.info(f"Writing bundle metadata to '{metadata_file_path}'")
            metadata_file.write(json.dumps(metadata_dict))
        # 2. Create a ZIP bundle by writing constituent files to it
        bundle_file_path = os.path.join(self.workbox_path, f"{bundle_id}.zip")
        self.logger.info(f"Creating bundle as ZIP archive: '{bundle_file_path}'")
        with ZipFile(bundle_file_path, mode="x", compression=ZIP_STORED, allowZip64=True) as bundle_zip:
            self.logger.info(f"Adding bundle metadata '{metadata_file_path}' to bundle '{bundle_file_path}'")
            bundle_zip.write(metadata_file_path, os.path.basename(metadata_file_path))
            self.logger.info(f"Writing {num_files} files to bundle '{bundle_file_path}'")
            file_count = 1
            for bundle_me in bundle["files"]:
                bundle_me_path = bundle_me["logical_name"]
                self.logger.info(f"Writing file {file_count}/{num_files}: '{bundle_me_path}' to bundle '{bundle_file_path}'")
                bundle_zip.write(bundle_me_path, os.path.basename(bundle_me_path))
                file_count = file_count + 1
        # 3. Clean up generated JSON metadata file
        self.logger.info(f"Deleting bundle metadata file: '{metadata_file_path}'")
        os.remove(metadata_file_path)
        self.logger.info(f"Bundle metadata '{metadata_file_path}' was deleted.")
        # 4. Compute the size of the bundle
        bundle_size = os.path.getsize(bundle_file_path)
        self.logger.info(f"Archive bundle has size {bundle_size} bytes")
        # 5. Compute the LTA checksums for the bundle
        self.logger.info(f"Computing LTA checksums for bundle: '{bundle_file_path}'")
        checksum = lta_checksums(bundle_file_path)
        self.logger.info(f"Bundle '{bundle_file_path}' has adler32 checksum '{checksum['adler32']}'")
        self.logger.info(f"Bundle '{bundle_file_path}' has SHA512 checksum '{checksum['sha512']}'")
        # 6. Determine the final destination path of the bundle
        final_bundle_path = bundle_file_path
        if self.outbox_path != self.workbox_path:
            final_bundle_path = os.path.join(self.outbox_path, f"{bundle_id}.zip")
        self.logger.info(f"Finished archive bundle will be located at: '{final_bundle_path}'")
        # 7. Update the bundle record we have with all the information we collected
        bundle["status"] = "created"
        bundle["update_timestamp"] = now()
        bundle["bundle_path"] = final_bundle_path
        bundle["size"] = bundle_size
        bundle["checksum"] = checksum
        bundle["verified"] = False
        bundle["claimed"] = False
        # 8. Move the bundle from the work box to the outbox
        if final_bundle_path != bundle_file_path:
            self.logger.info(f"Moving bundle from '{bundle_file_path}' to '{final_bundle_path}'")
            shutil.move(bundle_file_path, final_bundle_path)
        self.logger.info(f"Finished archive bundle now located at: '{final_bundle_path}'")
        # 9. Update the Bundle record in the LTA DB
        self.logger.info(f"PATCH /Bundles/{bundle_id} - '{bundle}'")
        await lta_rc.request('PATCH', f'/Bundles/{bundle_id}', bundle)

def runner() -> None:
    """Configure a Bundler component from the environment and set it running."""
    # obtain our configuration from the environment
    config = from_environment(EXPECTED_CONFIG)
    # configure structured logging for the application
    structured_formatter = StructuredFormatter(
        component_type='Bundler',
        component_name=config["COMPONENT_NAME"],
        ndjson=True)
    stream_handler = logging.StreamHandler(sys.stdout)
    stream_handler.setFormatter(structured_formatter)
    root_logger = logging.getLogger(None)
    root_logger.setLevel(logging.NOTSET)
    root_logger.addHandler(stream_handler)
    logger = logging.getLogger("lta.bundler")
    # create our Bundler service
    bundler = Bundler(config, logger)
    # let's get to work
    bundler.logger.info("Adding tasks to asyncio loop")
    loop = asyncio.get_event_loop()
    loop.create_task(status_loop(bundler))
    loop.create_task(work_loop(bundler))


def main() -> None:
    """Configure a Bundler component from the environment and set it running."""
    runner()
    asyncio.get_event_loop().run_forever()


if __name__ == "__main__":
    main()
