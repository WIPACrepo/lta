# bundler.py
"""Module to implement the Bundler component of the Long Term Archive."""

import asyncio
import json
import logging
import os
from pathlib import Path
import shutil
import sys
from typing import Any, Dict, Optional
from zipfile import ZIP_STORED, ZipFile

from rest_tools.client import ClientCredentialsAuth, RestClient
from wipac_dev_tools import from_environment
import wipac_telemetry.tracing_tools as wtt

from .component import COMMON_CONFIG, Component, now, work_loop
from .crypto import lta_checksums
from .lta_types import BundleType

Logger = logging.Logger

LOG = logging.getLogger(__name__)

# maximum number of Metadata UUIDs to work with at a time
CREATE_CHUNK_SIZE = 1000

EXPECTED_CONFIG = COMMON_CONFIG.copy()
EXPECTED_CONFIG.update({
    "BUNDLER_OUTBOX_PATH": None,
    "BUNDLER_WORKBOX_PATH": None,
    "FILE_CATALOG_CLIENT_ID": None,
    "FILE_CATALOG_CLIENT_SECRET": None,
    "FILE_CATALOG_REST_URL": None,
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
        self.file_catalog_client_id = config["FILE_CATALOG_CLIENT_ID"]
        self.file_catalog_client_secret = config["FILE_CATALOG_CLIENT_SECRET"]
        self.file_catalog_rest_url = config["FILE_CATALOG_REST_URL"]
        self.outbox_path = config["BUNDLER_OUTBOX_PATH"]
        self.work_retries = int(config["WORK_RETRIES"])
        self.work_timeout_seconds = float(config["WORK_TIMEOUT_SECONDS"])
        self.workbox_path = config["BUNDLER_WORKBOX_PATH"]

    def _do_status(self) -> Dict[str, Any]:
        """Bundler has no additional status to contribute."""
        return {}

    def _expected_config(self) -> Dict[str, Optional[str]]:
        """Bundler provides our expected configuration dictionary."""
        return EXPECTED_CONFIG

    @wtt.spanned()
    async def _do_work(self) -> None:
        """Perform a work cycle for this component."""
        self.logger.info("Starting work on Bundles.")
        work_claimed = True
        while work_claimed:
            work_claimed = await self._do_work_claim()
            # if we are configured to run once and die, then die
            if self.run_once_and_die:
                sys.exit()
        self.logger.info("Ending work on Bundles.")

    @wtt.spanned()
    async def _do_work_claim(self) -> bool:
        """Claim a bundle and perform work on it."""
        # 1. Ask the LTA DB for the next Bundle to be built
        # configure a RestClient to talk to the File Catalog
        fc_rc = ClientCredentialsAuth(address=self.file_catalog_rest_url,
                                      token_url=self.lta_auth_openid_url,
                                      client_id=self.file_catalog_client_id,
                                      client_secret=self.file_catalog_client_secret)
        # configure a RestClient to talk to the LTA DB
        lta_rc = ClientCredentialsAuth(address=self.lta_rest_url,
                                       token_url=self.lta_auth_openid_url,
                                       client_id=self.client_id,
                                       client_secret=self.client_secret,
                                       timeout=self.work_timeout_seconds,
                                       retries=self.work_retries)
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
        # process the Bundle that we were given
        try:
            await self._do_work_bundle(fc_rc, lta_rc, bundle)
        except Exception as e:
            await self._quarantine_bundle(lta_rc, bundle, f"{e}")
            raise e
        # signal the work was processed successfully
        return True

    @wtt.spanned()
    async def _do_work_bundle(self, fc_rc: RestClient, lta_rc: RestClient, bundle: BundleType) -> None:
        # 0. Get our ducks in a row about what we're doing here
        bundle_uuid = bundle["uuid"]
        dest = bundle["dest"]
        file_count = bundle["file_count"]
        source = bundle["source"]
        self.logger.info(f"There are {file_count} Files to bundle from '{source}' to '{dest}'.")
        self.logger.info(f"Bundle archive file will be '{bundle_uuid}.zip'")
        # 1. Create a manifest of the bundle, including all metadata
        metadata_file_path = os.path.join(self.workbox_path, f"{bundle_uuid}.metadata.ndjson")
        await self._create_metadata_file(fc_rc, lta_rc, bundle, metadata_file_path, file_count)
        # 2. Create a ZIP bundle by writing constituent files to it
        bundle_file_path = os.path.join(self.workbox_path, f"{bundle_uuid}.zip")
        await self._create_bundle_archive(fc_rc, lta_rc, bundle, bundle_file_path, metadata_file_path, file_count)
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
            final_bundle_path = os.path.join(self.outbox_path, f"{bundle_uuid}.zip")
        self.logger.info(f"Finished archive bundle will be located at: '{final_bundle_path}'")
        # 7. Update the bundle record we have with all the information we collected
        bundle["status"] = self.output_status
        bundle["reason"] = ""
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
        self.logger.info(f"PATCH /Bundles/{bundle_uuid} - '{bundle}'")
        await lta_rc.request('PATCH', f'/Bundles/{bundle_uuid}', bundle)

    @wtt.spanned()
    async def _create_bundle_archive(self,
                                     fc_rc: RestClient,
                                     lta_rc: RestClient,
                                     bundle: BundleType,
                                     bundle_file_path: str,
                                     metadata_file_path: str,
                                     file_count: int) -> None:
        # 0. Remove an existing bundle, if we are re-trying
        Path(bundle_file_path).unlink(missing_ok=True)

        # 2. Create a ZIP bundle by writing constituent files to it
        bundle_uuid = bundle["uuid"]
        request_path = bundle["path"]
        count = 0
        done = False
        limit = CREATE_CHUNK_SIZE
        skip = 0
        self.logger.info(f"Creating bundle as ZIP archive at: {bundle_file_path}")
        with ZipFile(bundle_file_path, mode="x", compression=ZIP_STORED, allowZip64=True) as bundle_zip:
            # write the metadata file to the bundle archive
            self.logger.info(f"Adding bundle metadata '{metadata_file_path}' to bundle '{bundle_file_path}'")
            bundle_zip.write(metadata_file_path, os.path.basename(metadata_file_path))

            # until we've finished processing all the Metadata records
            while not done:
                # ask the LTA DB for the next chunk of Metadata records
                self.logger.info(f"GET /Metadata?bundle_uuid={bundle_uuid}&limit={limit}&skip={skip}")
                lta_response = await lta_rc.request('GET', f'/Metadata?bundle_uuid={bundle_uuid}&limit={limit}&skip={skip}')
                num_files = len(lta_response["results"])
                done = (num_files == 0)
                skip = skip + num_files
                self.logger.info(f'LTA returned {num_files} Metadata documents to process.')

                # for each Metadata record returned by the LTA DB
                for metadata_record in lta_response["results"]:
                    # load the record from the File Catalog and add the warehouse file to the ZIP archive
                    count = count + 1
                    file_catalog_uuid = metadata_record["file_catalog_uuid"]
                    fc_response = await fc_rc.request('GET', f'/api/files/{file_catalog_uuid}')
                    bundle_me_path = fc_response["logical_name"]
                    self.logger.info(f"Writing file {count}/{num_files}: '{bundle_me_path}' to bundle '{bundle_file_path}'")
                    zip_path = os.path.relpath(bundle_me_path, request_path)
                    bundle_zip.write(bundle_me_path, zip_path)

        # do a last minute sanity check on our data
        if count != file_count:
            error_message = f'Bad mojo creating bundle archive file. Expected {file_count} Metadata records, but only processed {count} records.'
            self.logger.error(error_message)
            raise Exception(error_message)

    @wtt.spanned()
    async def _create_metadata_file(self,
                                    fc_rc: RestClient,
                                    lta_rc: RestClient,
                                    bundle: BundleType,
                                    metadata_file_path: str,
                                    file_count: int) -> None:
        # 0. Remove an existing manifest, if we are re-trying
        Path(metadata_file_path).unlink(missing_ok=True)

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
        with open(metadata_file_path, mode="w") as metadata_file:
            self.logger.info(f"Writing metadata_dict to '{metadata_file_path}'")
            metadata_file.write(json.dumps(metadata_dict))
            metadata_file.write("\n")

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
                    metadata_file.write(json.dumps(fc_response))
                    metadata_file.write("\n")

        # do a last minute sanity check on our data
        if count != file_count:
            error_message = f'Bad mojo creating metadata file. Expected {file_count} Metadata records, but only processed {count} records.'
            self.logger.error(error_message)
            raise Exception(error_message)

    @wtt.spanned()
    async def _quarantine_bundle(self,
                                 lta_rc: RestClient,
                                 bundle: BundleType,
                                 reason: str) -> None:
        """Quarantine the supplied bundle using the supplied reason."""
        self.logger.error(f'Sending Bundle {bundle["uuid"]} to quarantine: {reason}.')
        right_now = now()
        patch_body = {
            "original_status": bundle["status"],
            "status": "quarantined",
            "reason": f"BY:{self.name}-{self.instance_uuid} REASON:{reason}",
            "work_priority_timestamp": right_now,
        }
        try:
            await lta_rc.request('PATCH', f'/Bundles/{bundle["uuid"]}', patch_body)
        except Exception as e:
            self.logger.error(f'Unable to quarantine Bundle {bundle["uuid"]}: {e}.')


def runner() -> None:
    """Configure a Bundler component from the environment and set it running."""
    # obtain our configuration from the environment
    config = from_environment(EXPECTED_CONFIG)
    # configure logging for the application
    log_level = getattr(logging, str(config["LOG_LEVEL"]).upper())
    logging.basicConfig(
        format="{asctime} [{threadName}] {levelname:5} ({filename}:{lineno}) - {message}",
        level=log_level,
        stream=sys.stdout,
        style="{",
    )
    # create our Bundler service
    bundler = Bundler(config, LOG)  # type: ignore[arg-type]
    # let's get to work
    bundler.logger.info("Adding tasks to asyncio loop")
    loop = asyncio.get_event_loop()
    loop.create_task(work_loop(bundler))


def main() -> None:
    """Configure a Bundler component from the environment and set it running."""
    runner()
    asyncio.get_event_loop().run_forever()


if __name__ == "__main__":
    main()
