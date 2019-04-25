# bundler.py
"""Module to implement the Bundler component of the Long Term Archive."""

import asyncio
from datetime import datetime
import json
from logging import Logger
import logging
import os
import shutil
import sys
from typing import Any, Dict, List, Optional
from uuid import uuid4
from zipfile import ZIP_STORED, ZipFile

from rest_tools.client import RestClient  # type: ignore

from .component import COMMON_CONFIG, Component, status_loop, work_loop
from .config import from_environment
from .crypto import sha512sum
from .log_format import StructuredFormatter

EXPECTED_CONFIG = COMMON_CONFIG.copy()
EXPECTED_CONFIG.update({
    "BUNDLER_OUTBOX_PATH": None,
    "BUNDLER_WORKBOX_PATH": None,
    "LTA_SITE_CONFIG": "etc/site.json",
    "WORK_RETRIES": "3",
    "WORK_TIMEOUT_SECONDS": "30",
})

def now() -> str:
    """Return string timestamp for current time, to the second."""
    return datetime.utcnow().isoformat(timespec='seconds')

def unique_id() -> str:
    """Return a unique ID for a Bundle."""
    return str(uuid4())


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
        self.outbox_path = config["BUNDLER_OUTBOX_PATH"]
        self.work_retries = int(config["WORK_RETRIES"])
        self.work_timeout_seconds = float(config["WORK_TIMEOUT_SECONDS"])
        self.workbox_path = config["BUNDLER_WORKBOX_PATH"]
        with open(config["LTA_SITE_CONFIG"]) as site_data:
            self.lta_site_config = json.load(site_data)
        self.sites = self.lta_site_config["sites"]

    def _do_status(self) -> Dict[str, Any]:
        """Bundler has no additional status to contribute."""
        return {}

    def _expected_config(self) -> Dict[str, Optional[str]]:
        """Bundler provides our expected configuration dictionary."""
        return EXPECTED_CONFIG

    async def _do_work(self) -> None:
        """Perform a work cycle for this component."""
        await self._consume_files_to_bundle_to_destination_sites()

    async def _consume_files_to_bundle_to_destination_sites(self) -> None:
        """Check each of the sites to see if we can bundle for them."""
        # for each destination to which we could bundle
        for site in self.sites:
            # see if we have any work to do bundling files there
            if site != self.source_site:
                self.logger.info(f"Processing bundles from {self.source_site} to {site}")
                await self._consume_files_for_destination_site(site)
        # inform the log that we've worked on each site and now we're taking a break
        self.logger.info(f"Bundling work cycle complete. Going on vacation.")

    async def _consume_files_for_destination_site(self, dest: str) -> None:
        """Check a specific site to see if we can bundle for it."""
        # 1. Get Files to bundle from LTA DB
        lta_rc = RestClient(self.lta_rest_url,
                            token=self.lta_rest_token,
                            timeout=self.work_timeout_seconds,
                            retries=self.work_retries)
        self.logger.info("Asking the LTA DB for Files to bundle.")
        pop_body = {
            "bundler": self.name
        }
        source = self.source_site
        response = await lta_rc.request('POST', f'/Files/actions/pop?source={source}&dest={dest}', pop_body)
        self.logger.info(f"LTA DB responded with: {response}")
        results = response["results"]
        if not results:
            self.logger.info(f"No Files are available to work on. Going on vacation.")
            return
        await self._build_bundle_for_destination_site(dest, lta_rc, results)

    async def _build_bundle_for_destination_site(self, dest: str, lta_rc: RestClient, results: List[Dict[str, Any]]) -> None:
        """Build a bundle for a specific site with the supplied files."""
        source = self.source_site
        num_files = len(results)
        self.logger.info(f"There are {num_files} Files to bundle from '{source}' to '{dest}'.")
        # 1. Create a manifest of the bundle, including all metadata
        bundle_id = unique_id()
        self.logger.info(f"Bundle from {source} to {dest} will be '{bundle_id}.zip'")
        metadata_dict = {
            "uuid": bundle_id,
            "component": "bundler",
            "version": 2,
            "date_created": now(),
            "files": [x["catalog"] for x in results],
        }
        metadata_file_path = os.path.join(self.workbox_path, f"{bundle_id}.metadata.json")
        with open(metadata_file_path, mode="w") as metadata_file:
            self.logger.info(f"Writing bundle metadata to '{metadata_file_path}'")
            metadata_file.write(json.dumps(metadata_dict))
        # 2. Make a bundle
        bundle_file_path = os.path.join(self.workbox_path, f"{bundle_id}.zip")
        self.logger.info(f"Creating bundle as ZIP archive: '{bundle_file_path}'")
        with ZipFile(bundle_file_path, mode="x", compression=ZIP_STORED, allowZip64=True) as bundle_zip:
            self.logger.info(f"Adding bundle metadata '{metadata_file_path}' to bundle '{bundle_file_path}'")
            bundle_zip.write(metadata_file_path, os.path.basename(metadata_file_path))
            self.logger.info(f"Writing {num_files} files to bundle '{bundle_file_path}'")
            file_count = 1
            for bundle_me in results:
                bundle_me_path = bundle_me["catalog"]["logical_name"]
                self.logger.info(f"Writing file {file_count}/{num_files}: '{bundle_me_path}' to bundle '{bundle_file_path}'")
                bundle_zip.write(bundle_me_path, os.path.basename(bundle_me_path))
                file_count = file_count + 1
        # 3. Put bundle in staging storage
        final_bundle_path = bundle_file_path
        if self.outbox_path != self.workbox_path:
            final_bundle_path = os.path.join(self.outbox_path, f"{bundle_id}.zip")
            self.logger.info(f"Moving bundle from '{bundle_file_path}' to '{final_bundle_path}'")
            shutil.move(bundle_file_path, final_bundle_path)
        self.logger.info(f"Finished archive bundle now located at: '{final_bundle_path}'")
        # 3.1. Compute the SHA512 checksum of the bundle
        self.logger.info(f"Computing checksum for bundle: '{final_bundle_path}'")
        checksum = sha512sum(final_bundle_path)
        self.logger.info(f"Bundle '{final_bundle_path}' has checksum '{checksum}'")
        # 3.2. Clean up generated JSON metadata file
        self.logger.info(f"Deleting bundle metadata file: '{metadata_file_path}'")
        os.remove(metadata_file_path)
        self.logger.info(f"Bundle metadata '{metadata_file_path}' was deleted.")
        # 4. Add bundle to REST
        bundle_obj = {
            "bundles": [
                {
                    "bundle_uuid": bundle_id,
                    "location": f"{source}:{final_bundle_path}",
                    "checksum": {
                        "sha512": checksum,
                    },
                    "status": "accessible",
                    "dest": dest,
                    "verified": False,
                    "manifest": [x["catalog"] for x in results],
                },
            ],
        }
        self.logger.info(f"POST /Bundles/actions/bulk_create - '{bundle_obj}'")
        await lta_rc.request('POST', '/Bundles/actions/bulk_create', bundle_obj)
        # 4.1. Update the files as bundled
        self.logger.info(f"Marking Files as complete in the LTA DB")
        update_body = {
            "update": {
                "complete": {
                    "timestamp": now(),
                    "bundler": self.name,
                    "bundle_uuid": bundle_id,
                }
            },
            "files": [x["catalog"]["uuid"] for x in results]
        }
        await lta_rc.request('POST', '/Files/actions/bulk_update', update_body)


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
