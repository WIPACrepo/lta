# bundler.py
"""Module to implement the Bundler component of the Long Term Archive."""

import asyncio
from datetime import datetime
import json
from logging import Logger
import logging
import os
from pathlib import Path
import platform
import shutil
import sys
from typing import Any, Dict, List
from uuid import uuid4
from zipfile import ZIP_STORED, ZipFile

from rest_tools.client import RestClient  # type: ignore
from urllib.parse import urljoin

from .config import from_environment
from .crypto import sha512sum
from .log_format import StructuredFormatter
from .lta_const import DRAIN_SEMAPHORE_FILENAME, STOP_SEMAPHORE_FILENAME

EXPECTED_CONFIG = {
    "BUNDLER_NAME": f"{platform.node()}-bundler",
    "BUNDLER_SITE_SOURCE": None,
    "HEARTBEAT_PATCH_RETRIES": "3",
    "HEARTBEAT_PATCH_TIMEOUT_SECONDS": "30",
    "HEARTBEAT_SLEEP_DURATION_SECONDS": "60",
    "LTA_REST_TOKEN": None,
    "LTA_REST_URL": None,
    "LTA_SITE_CONFIG": "etc/site.json",
    "OUTBOX_PATH": None,
    "WORK_RETRIES": "3",
    "WORK_SLEEP_DURATION_SECONDS": "300",
    "WORK_TIMEOUT_SECONDS": "30",
    "WORKBOX_PATH": None,
}

HEARTBEAT_STATE = [
    "last_work_begin_timestamp",
    "last_work_end_timestamp",
    "lta_ok"
]

def now() -> str:
    """Return string timestamp for current time, to the second."""
    return datetime.utcnow().isoformat(timespec='seconds')

def unique_id() -> str:
    """Return a unique ID for a Bundle."""
    return str(uuid4())


class Bundler:
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
        # validate provided configuration
        for name in EXPECTED_CONFIG:
            if name not in config:
                raise ValueError(f"Missing expected configuration parameter: '{name}'")
            if not config[name]:
                raise ValueError(f"Missing expected configuration parameter: '{name}'")
        # assimilate provided configuration
        self.bundler_name = config["BUNDLER_NAME"]
        self.bundler_site_source = config["BUNDLER_SITE_SOURCE"]
        self.heartbeat_patch_retries = int(config["HEARTBEAT_PATCH_RETRIES"])
        self.heartbeat_patch_timeout_seconds = float(config["HEARTBEAT_PATCH_TIMEOUT_SECONDS"])
        self.heartbeat_sleep_duration_seconds = float(config["HEARTBEAT_SLEEP_DURATION_SECONDS"])
        self.lta_rest_token = config["LTA_REST_TOKEN"]
        self.lta_rest_url = config["LTA_REST_URL"]
        with open(config["LTA_SITE_CONFIG"]) as site_data:
            self.lta_site_config = json.load(site_data)
        self.sites = self.lta_site_config["sites"]
        self.outbox_path = config["OUTBOX_PATH"]
        self.work_retries = int(config["WORK_RETRIES"])
        self.work_sleep_duration_seconds = float(config["WORK_SLEEP_DURATION_SECONDS"])
        self.work_timeout_seconds = float(config["WORK_TIMEOUT_SECONDS"])
        self.workbox_path = config["WORKBOX_PATH"]
        # assimilate provided logger
        self.logger = logger
        # record some default state
        timestamp = datetime.utcnow().isoformat()
        self.last_work_begin_timestamp = timestamp
        self.last_work_end_timestamp = timestamp
        self.lta_ok = False
        self.logger.info(f"Bundler '{self.bundler_name}' is configured:")
        for name in EXPECTED_CONFIG:
            self.logger.info(f"{name} = {config[name]}")

    async def run(self) -> None:
        """Perform the component's work cycle."""
        self.logger.info("Starting bundler work cycle")
        # start the work cycle stopwatch
        self.last_work_begin_timestamp = datetime.utcnow().isoformat()
        # perform the work
        try:
            await self._do_work()
            self.lta_ok = True
        except Exception as e:
            # ut oh, something went wrong; log about it
            self.logger.error("Error occurred during the Bundler work cycle")
            self.logger.error(f"Error was: '{e}'", exc_info=True)
        # stop the work cycle stopwatch
        self.last_work_end_timestamp = datetime.utcnow().isoformat()
        self.logger.info("Ending bundler work cycle")

    async def _do_work(self) -> None:
        """Perform a work cycle for this component."""
        await self._consume_files_to_bundle_to_destination_sites()

    async def _consume_files_to_bundle_to_destination_sites(self) -> None:
        """Check each of the sites to see if we can bundle for them."""
        # for each destination to which we could bundle
        for site in self.sites:
            # see if we have any work to do bundling files there
            if site != self.bundler_site_source:
                self.logger.info(f"Processing bundles from {self.bundler_site_source} to {site}")
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
            "bundler": self.bundler_name
        }
        source = self.bundler_site_source
        response = await lta_rc.request('POST', f'/Files/actions/pop?source={source}&dest={dest}', pop_body)
        self.logger.info(f"LTA DB responded with: {response}")
        results = response["results"]
        if not results:
            self.logger.info(f"No Files are available to work on. Going on vacation.")
            return
        await self._build_bundle_for_destination_site(dest, lta_rc, results)

    async def _build_bundle_for_destination_site(self, dest: str, lta_rc: RestClient, results: List[Dict[str, Any]]) -> None:
        """Build a bundle for a specific site with the supplied files."""
        source = self.bundler_site_source
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
                    "status": "none",
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
                    "bundler": self.bundler_name,
                    "bundle_uuid": bundle_id,
                }
            },
            "files": [x["catalog"]["uuid"] for x in results]
        }
        await lta_rc.request('POST', '/Files/actions/bulk_update', update_body)


def check_drain_semaphore() -> bool:
    """Check if a drain semaphore exists in the current working directory."""
    cwd = os.getcwd()
    drain_filename = os.path.join(cwd, DRAIN_SEMAPHORE_FILENAME)
    return Path(drain_filename).exists()


def check_stop_semaphore() -> bool:
    """Check if a stop semaphore exists in the current working directory."""
    cwd = os.getcwd()
    drain_filename = os.path.join(cwd, STOP_SEMAPHORE_FILENAME)
    return Path(drain_filename).exists()


async def patch_status_heartbeat(bundler: Bundler) -> bool:
    """PATCH /status/bundler to update LTA with a status heartbeat."""
    bundler.logger.info("Sending status heartbeat")
    # determine which resource to PATCH
    status_url = urljoin(bundler.lta_rest_url, "/status/bundler")
    # determine the body to PATCH with
    status_body = {
        bundler.bundler_name: {
            "timestamp": datetime.utcnow().isoformat()
        }
    }
    for name in HEARTBEAT_STATE:
        status_body[bundler.bundler_name][name] = getattr(bundler, name)  # smh; bundler[name]
    # attempt to PATCH the status resource
    bundler.logger.info(f"PATCH {status_url} - {status_body}")
    try:
        rc = RestClient(bundler.lta_rest_url,
                        token=bundler.lta_rest_token,
                        timeout=bundler.heartbeat_patch_timeout_seconds,
                        retries=bundler.heartbeat_patch_retries)
        # Use the RestClient to PATCH our heartbeat to the LTA DB
        await rc.request('PATCH', "/status/bundler", status_body)
        bundler.lta_ok = True
    except Exception as e:
        # if there was a problem, yo I'll solve it
        bundler.logger.error("Error trying to PATCH /status/bundler with heartbeat")
        bundler.logger.error(f"Error was: '{e}'", exc_info=True)
        bundler.lta_ok = False
    # indicate to the caller if the heartbeat was successful
    return bundler.lta_ok


async def lifecycle_loop(bundler: Bundler) -> None:
    """Run a check for a stop semaphore as an infinite loop."""
    bundler.logger.info("Starting lifecycle loop")
    while True:
        # if there is a stop semaphore, terminate the program
        if check_stop_semaphore():
            bundler.logger.info("Component stopped; shutting down.")
            sys.exit(0)
        # sleep until we check again
        await asyncio.sleep(1.0)


async def status_loop(bundler: Bundler) -> None:
    """Run status heartbeat updates as an infinite loop."""
    bundler.logger.info("Starting status loop")
    while True:
        # if there is a drain semaphore, stop sending status updates
        if check_drain_semaphore():
            break
        # PATCH /status/bundler
        await patch_status_heartbeat(bundler)
        # sleep until we PATCH the next heartbeat
        await asyncio.sleep(bundler.heartbeat_sleep_duration_seconds)


async def work_loop(bundler: Bundler) -> None:
    """Run bundler work cycles as an infinite loop."""
    bundler.logger.info("Starting work loop")
    while True:
        # if there is a drain semaphore, don't do any additional work
        if check_drain_semaphore():
            bundler.logger.info("Component drained; shutting down.")
            sys.exit(0)
        # Do the work of the bundler
        await bundler.run()
        # sleep until we need to work again
        await asyncio.sleep(bundler.work_sleep_duration_seconds)


def main() -> None:
    """Configure a Bundler component from the environment and set it running."""
    # obtain our configuration from the environment
    config = from_environment(EXPECTED_CONFIG)
    # configure structured logging for the application
    structured_formatter = StructuredFormatter(
        component_type='Bundler',
        component_name=config["BUNDLER_NAME"],
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
    bundler.logger.info("Starting asyncio loop")
    loop = asyncio.get_event_loop()
    loop.create_task(lifecycle_loop(bundler))
    loop.create_task(status_loop(bundler))
    loop.create_task(work_loop(bundler))
    loop.run_forever()


if __name__ == "__main__":
    main()
