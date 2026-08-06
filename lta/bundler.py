# bundler.py
"""Module to implement the Bundler component of the Long Term Archive."""

# fmt:off

import asyncio
import json
import logging
import os
import shutil
import sys
from pathlib import Path
from typing import Any
from zipfile import ZIP_STORED, ZipFile

import aiofiles
from prometheus_client import start_http_server
from rest_tools.client import ClientCredentialsAuth, RestClient

from .component import COMMON_CONFIG, Component, PrometheusResultTracker, work_loop
from .crypto import lta_checksums
from .lta_tools import from_environment
from .lta_types import BundleType
from .utils import now, quarantine_now

Logger = logging.Logger

LOG = logging.getLogger(__name__)

# maximum number of Metadata UUIDs to work with at a time
CREATE_CHUNK_SIZE = 1000

EXPECTED_CONFIG = COMMON_CONFIG.copy()
EXPECTED_CONFIG.update({
    "BLOCKING_IO_MAX_RETRIES": "3",
    "BLOCKING_IO_SLEEP_SECONDS": "60",
    "BUNDLER_OUTBOX_PATH": None,
    "BUNDLER_WORKBOX_PATH": None,
    "FILE_CATALOG_CLIENT_ID": None,
    "FILE_CATALOG_CLIENT_SECRET": None,
    "FILE_CATALOG_REST_URL": None,
    "WORK_RETRIES": "3",
    "WORK_TIMEOUT_SECONDS": "30",
})


class BundleFileSanityCheckException(Exception):
    """Raised when a bundle file fails a sanity check."""


class BundleZipMissingFileDescriptorException(Exception):
    """Raised when a bundle zip file is missing a file descriptor."""


class MetadataFileSanityCheckException(Exception):
    """Raised when a metadata file fails a sanity check."""


class Bundler(Component):
    """
    Bundler is a Long Term Archive component.

    A Bundler is responsible for creating large ZIP64 archives of files that
    should be moved to long term archive. It requests work from the LTA REST
    API in the form of files to put into a large archive. It creates the ZIP64
    archive and moves the file to staging disk. It then updates the LTA REST
    API to indicate that the provided files were so bundled.
    """

    def __init__(self, config: dict[str, str], logger: Logger) -> None:
        """
        Create a Bundler component.

        config - A dictionary of required configuration values.
        logger - The object the bundler should use for logging.
        """
        super().__init__("bundler", config, logger)
        self.blocking_io_max_retries = int(config["BLOCKING_IO_MAX_RETRIES"])
        self.blocking_io_sleep_seconds = int(config["BLOCKING_IO_SLEEP_SECONDS"])
        self.file_catalog_client_id = config["FILE_CATALOG_CLIENT_ID"]
        self.file_catalog_client_secret = config["FILE_CATALOG_CLIENT_SECRET"]
        self.file_catalog_rest_url = config["FILE_CATALOG_REST_URL"]
        self.outbox_path = config["BUNDLER_OUTBOX_PATH"]
        self.work_retries = int(config["WORK_RETRIES"])
        self.work_timeout_seconds = float(config["WORK_TIMEOUT_SECONDS"])
        self.workbox_path = config["BUNDLER_WORKBOX_PATH"]

    def _do_status(self) -> dict[str, Any]:
        """Bundler has no additional status to contribute."""
        return {}

    def _expected_config(self) -> dict[str, str | None]:
        """Bundler provides our expected configuration dictionary."""
        return EXPECTED_CONFIG

    async def _do_work_claim(
        self,
        lta_rc: RestClient,
        prom_tracker: PrometheusResultTracker,
    ) -> bool:
        """Claim a bundle and perform work on it -- see super for return value meanings."""
        # 1. Ask the LTA DB for the next Bundle to be built
        self.logger.info("Asking the LTA DB for a Bundle to build.")
        pop_body = {
            "claimant": f"{self.name}-{self.instance_uuid}"
        }
        response = await lta_rc.request('POST', f'/Bundles/actions/pop?source={self.source_site}&dest={self.dest_site}&status={self.input_status}', pop_body)
        self.logger.info(f"LTA DB responded with: {response}")
        bundle = response["bundle"]
        if not bundle:
            self.logger.info("LTA DB did not provide a Bundle to build. Going on vacation.")
            return False
        # configure a RestClient to talk to the File Catalog
        fc_rc = ClientCredentialsAuth(address=self.file_catalog_rest_url,
                                      token_url=self.lta_auth_openid_url,
                                      client_id=self.file_catalog_client_id,
                                      client_secret=self.file_catalog_client_secret)
        # process the Bundle that we were given
        try:
            await self._do_work_bundle(fc_rc, lta_rc, bundle)
        except Exception as e:
            prom_tracker.record_failure()
            await quarantine_now(
                lta_rc,
                bundle,
                e,
                self.name,
                self.instance_uuid,
                self.logger,
            )
            raise
        else:
            prom_tracker.record_success()
            return True

    async def _do_work_bundle(self, fc_rc: RestClient, lta_rc: RestClient, bundle: BundleType) -> None:
        # 0. Get our ducks in a row about what we're doing here
        bundle_uuid = bundle["uuid"]
        dest = bundle["dest"]
        file_count = bundle["file_count"]
        source = bundle["source"]
        self.logger.info("There are %s Files to bundle from '%s' to '%s'.", file_count, source, dest)
        self.logger.info("Bundle archive file will be '%s.zip'", bundle_uuid)
        workbox_path = Path(self.workbox_path)
        outbox_path = Path(self.outbox_path)
        # 1. Create a manifest of the bundle, including all metadata
        metadata_file_path = workbox_path / f"{bundle_uuid}.metadata.ndjson"
        await self._create_metadata_file(fc_rc, lta_rc, bundle, os.fspath(metadata_file_path), file_count)
        # 2. Create a ZIP bundle by writing constituent files to it
        bundle_file_path = workbox_path / f"{bundle_uuid}.zip"
        await self._create_bundle_archive(fc_rc, lta_rc, bundle, os.fspath(bundle_file_path), os.fspath(metadata_file_path), file_count)
        # 3. Clean up generated JSON metadata file
        self.logger.info("Deleting bundle metadata file: '%s'", metadata_file_path)
        await asyncio.to_thread(metadata_file_path.unlink)
        self.logger.info("Bundle metadata '%s' was deleted.", metadata_file_path)
        # 4. Compute the size of the bundle
        bundle_stat = await asyncio.to_thread(bundle_file_path.stat)
        bundle_size = bundle_stat.st_size
        self.logger.info("Archive bundle has size %s bytes", bundle_size)
        # 5. Compute the LTA checksums for the bundle
        self.logger.info("Computing LTA checksums for bundle: '%s'", bundle_file_path)
        checksum = await asyncio.to_thread(lta_checksums, os.fspath(bundle_file_path))
        self.logger.info("Bundle '%s' has adler32 checksum '%s'", bundle_file_path, checksum['adler32'])
        self.logger.info("Bundle '%s' has SHA512 checksum '%s'", bundle_file_path, checksum['sha512'])
        # 6. Determine the final destination path of the bundle
        final_bundle_path = bundle_file_path
        if outbox_path != workbox_path:
            final_bundle_path = outbox_path / f"{bundle_uuid}.zip"
        self.logger.info("Finished archive bundle will be located at: '%s'", final_bundle_path)
        # 7. Update the bundle record we have with all the information we collected
        bundle["status"] = self.output_status
        bundle["reason"] = ""
        bundle["update_timestamp"] = now()
        bundle["bundle_path"] = os.fspath(final_bundle_path)
        bundle["size"] = bundle_size
        bundle["checksum"] = checksum
        bundle["verified"] = False
        bundle["claimed"] = False
        # 8. Move the bundle from the work box to the outbox
        if final_bundle_path != bundle_file_path:
            self.logger.info("Moving bundle from '%s' to '%s'", bundle_file_path, final_bundle_path)
            await asyncio.to_thread(shutil.move, bundle_file_path, final_bundle_path)
        self.logger.info("Finished archive bundle now located at: '%s'", final_bundle_path)
        # 9. Update the Bundle record in the LTA DB
        self.logger.info("PATCH /Bundles/%s - '%s'", bundle_uuid, bundle)
        await lta_rc.request('PATCH', f'/Bundles/{bundle_uuid}', bundle)

    async def _create_bundle_archive(self,
                                     fc_rc: RestClient,
                                     lta_rc: RestClient,
                                     bundle: BundleType,
                                     bundle_file_path: str,
                                     metadata_file_path: str,
                                     file_count: int) -> None:
        """Create the bundle archive ZIP file; retry on transient BlockingIOError."""
        retry_count = self.blocking_io_max_retries
        while retry_count > 0:
            try:
                await self._create_bundle_archive_once(fc_rc, lta_rc, bundle, bundle_file_path, metadata_file_path, file_count)
            except BlockingIOError:
                retry_count = retry_count - 1
                self.logger.exception("Transient BlockingIOError; %d tries remain", retry_count)
                if retry_count == 0:
                    raise
                self.logger.info(f"Sleeping for {self.blocking_io_sleep_seconds} seconds until retry.")
                await asyncio.sleep(self.blocking_io_sleep_seconds)
            else:
                return

    async def _create_bundle_archive_once(self,
                                          fc_rc: RestClient,
                                          lta_rc: RestClient,
                                          bundle: BundleType,
                                          bundle_file_path: str,
                                          metadata_file_path: str,
                                          file_count: int) -> None:
        """Create the bundle archive ZIP file."""
        # 0. Remove an existing bundle, if we are re-trying
        await asyncio.to_thread(Path(bundle_file_path).unlink, missing_ok=True)

        # 2. Create a ZIP bundle by writing constituent files to it
        bundle_uuid = bundle["uuid"]
        request_path = bundle["path"]
        count = 0
        done = False
        limit = CREATE_CHUNK_SIZE
        skip = 0
        self.logger.info("Creating bundle as ZIP archive at: %s", bundle_file_path)
        with ZipFile(bundle_file_path, mode="x", compression=ZIP_STORED, allowZip64=True) as bundle_zip:
            # write the metadata file to the bundle archive
            self.logger.info("Adding bundle metadata '%s' to bundle '%s'", metadata_file_path, bundle_file_path)
            await asyncio.to_thread(bundle_zip.write, metadata_file_path, os.path.basename(metadata_file_path))

            # until we've finished processing all the Metadata records
            while not done:
                # ask the LTA DB for the next chunk of Metadata records
                self.logger.info("GET /Metadata?bundle_uuid=%s&limit=%s&skip=%s", bundle_uuid, limit, skip)
                lta_response = await lta_rc.request('GET', f'/Metadata?bundle_uuid={bundle_uuid}&limit={limit}&skip={skip}')
                num_files = len(lta_response["results"])
                done = (num_files == 0)
                skip = skip + num_files
                self.logger.info('LTA returned %s Metadata documents to process.', num_files)

                # for each Metadata record returned by the LTA DB
                for metadata_record in lta_response["results"]:
                    # load the record from the File Catalog and add the warehouse file to the ZIP archive
                    count = count + 1
                    file_catalog_uuid = metadata_record["file_catalog_uuid"]
                    fc_response = await fc_rc.request('GET', f'/api/files/{file_catalog_uuid}')
                    bundle_me_path = os.fsdecode(fc_response["logical_name"])
                    request_path_str = os.fsdecode(request_path)
                    self.logger.info("Writing file %s/%s: '%s' to bundle '%s'", count, file_count, bundle_me_path, bundle_file_path)
                    zip_path: str = os.path.relpath(bundle_me_path, request_path_str)  # noqa: ASYNC240 -- lexical path manipulation; no filesystem I/O
                    await asyncio.to_thread(bundle_zip.write, bundle_me_path, zip_path)

        # do a last minute sanity check on our data
        if count != file_count:
            error_message = f'Bad mojo creating bundle archive file. Expected {file_count} Metadata records, but only processed {count} records.'
            self.logger.error(error_message)
            raise BundleFileSanityCheckException(error_message)

    async def _create_metadata_file(self,
                                    fc_rc: RestClient,
                                    lta_rc: RestClient,
                                    bundle: BundleType,
                                    metadata_file_path: str,
                                    file_count: int) -> None:
        # 0. Remove an existing manifest, if we are re-trying
        await asyncio.to_thread(Path(metadata_file_path).unlink, missing_ok=True)

        # 1. Create a manifest of the bundle, including all metadata
        bundle_uuid = bundle["uuid"]
        self.logger.info(f"Bundle metadata file will be created at: {metadata_file_path}")
        metadata_dict = {
            "uuid": bundle_uuid,
            "component": "bundler",
            "version": 3,
            "create_timestamp": now(),
            "file_count": file_count,
        }

        # open the metadata file and write our data
        count = 0
        done = False
        limit = CREATE_CHUNK_SIZE
        skip = 0
        async with aiofiles.open(metadata_file_path, mode="w") as metadata_file:
            self.logger.info(f"Writing metadata_dict to '{metadata_file_path}'")
            await metadata_file.write(json.dumps(metadata_dict))
            await metadata_file.write("\n")

            # until we've finished processing all the Metadata records
            while not done:
                # ask the LTA DB for the next chunk of Metadata records
                lta_response = await lta_rc.request('GET', f'/Metadata?bundle_uuid={bundle_uuid}&limit={limit}&skip={skip}')
                num_files = len(lta_response["results"])
                done = (num_files == 0)
                skip = skip + num_files
                self.logger.info(f'LTA returned {num_files} Metadata documents to process.')

                # for each Metadata record returned by the LTA DB
                for metadata_record in lta_response["results"]:
                    # load the record from the File Catalog and preserve it in carbonite
                    count = count + 1
                    file_catalog_uuid = metadata_record["file_catalog_uuid"]
                    fc_response = await fc_rc.request('GET', f'/api/files/{file_catalog_uuid}')
                    self.logger.info(f"Writing File Catalog record {file_catalog_uuid} to '{metadata_file_path}'")
                    await metadata_file.write(json.dumps(fc_response))
                    await metadata_file.write("\n")

        # do a last minute sanity check on our data
        if count != file_count:
            error_message = f'Bad mojo creating metadata file. Expected {file_count} Metadata records, but only processed {count} records.'
            self.logger.error(error_message)
            raise MetadataFileSanityCheckException(error_message)


async def main(bundler: Bundler) -> None:
    """Execute the work loop of the Bundler component."""
    LOG.info("Starting asynchronous code")
    await work_loop(bundler)
    LOG.info("Ending asynchronous code")


def main_sync() -> None:
    """Configure a Bundler component from the environment and set it running."""
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
    # create our Bundler service
    LOG.info("Starting synchronous code")
    bundler = Bundler(config, LOG)
    # let's get to work
    metrics_port = int(config["PROMETHEUS_METRICS_PORT"])
    start_http_server(metrics_port)
    asyncio.run(main(bundler))
    LOG.info("Ending synchronous code")


if __name__ == "__main__":
    main_sync()
