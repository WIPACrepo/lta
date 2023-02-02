# desy_verifier.py
"""Module to implement the DesyVerifier component of the Long Term Archive."""

import asyncio
import logging
import os
import sys
from typing import Any, Dict, Optional

from rest_tools.client import ClientCredentialsAuth, RestClient
from wipac_dev_tools import from_environment
import wipac_telemetry.tracing_tools as wtt

from .component import COMMON_CONFIG, Component, now, work_loop
from .joiner import join_smart
from .lta_types import BundleType

Logger = logging.Logger

LOG = logging.getLogger(__name__)

EXPECTED_CONFIG = COMMON_CONFIG.copy()
EXPECTED_CONFIG.update({
    "FILE_CATALOG_REST_TOKEN": None,
    "FILE_CATALOG_REST_URL": None,
    "TAPE_BASE_PATH": None,
    "WORK_RETRIES": "3",
    "WORK_TIMEOUT_SECONDS": "30",
})

# maximum number of Metadata UUIDs to work with at a time
UPDATE_CHUNK_SIZE = 1000

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

    @wtt.spanned()
    async def _do_work(self) -> None:
        """Perform a work cycle for this component."""
        self.logger.info("Starting work on Bundles.")
        work_claimed = True
        while work_claimed:
            work_claimed = await self._do_work_claim()
            work_claimed &= not self.run_once_and_die
        self.logger.info("Ending work on Bundles.")

    @wtt.spanned()
    async def _do_work_claim(self) -> bool:
        """Claim a bundle and perform work on it."""
        # 1. Ask the LTA DB for the next Bundle to be verified
        self.logger.info("Asking the LTA DB for a Bundle to record as verified at DESY.")
        # configure a RestClient to talk to the LTA DB
        lta_rc = ClientCredentialsAuth(address=self.lta_rest_url,
                                       token_url=self.lta_auth_openid_url,
                                       client_id=self.client_id,
                                       client_secret=self.client_secret,
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
            await self._add_bundle_to_file_catalog(lta_rc, bundle)
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

    @wtt.spanned()
    async def _add_bundle_to_file_catalog(self, lta_rc: RestClient, bundle: BundleType) -> bool:
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
        right_now = now()
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
            "lta": {
                "date_archived": right_now,
            },
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
        # update the File Catalog for each file contained in the bundle
        await self._update_files_in_file_catalog(fc_rc, lta_rc, bundle, desy_tape_path)
        # indicate that our file catalog updates were successful
        return True

    @wtt.spanned()
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

    @wtt.spanned()
    async def _update_files_in_file_catalog(self,
                                            fc_rc: RestClient,
                                            lta_rc: RestClient,
                                            bundle: BundleType,
                                            desy_tape_path: str) -> bool:
        """Update the file records in the File Catalog."""
        bundle_uuid = bundle["uuid"]
        count = 0
        done = False
        limit = UPDATE_CHUNK_SIZE
        # until we've finished processing all the Metadata records
        while not done:
            # ask the LTA DB for the next chunk of Metadata records
            self.logger.info(f"GET /Metadata?bundle_uuid={bundle_uuid}&limit={limit}")
            lta_response = await lta_rc.request('GET', f'/Metadata?bundle_uuid={bundle_uuid}&limit={limit}')
            results = lta_response["results"]
            num_files = len(results)
            done = (num_files == 0)
            self.logger.info(f'LTA returned {num_files} Metadata documents to process.')

            # for each Metadata record returned by the LTA DB
            for metadata_record in results:
                # load the record from the File Catalog and add the new location to the record
                count = count + 1
                file_catalog_uuid = metadata_record["file_catalog_uuid"]
                fc_response = await fc_rc.request('GET', f'/api/files/{file_catalog_uuid}')
                logical_name = fc_response["logical_name"]
                # add a location indicating the bundle archive
                new_location = {
                    "locations": [
                        {
                            "site": self.dest_site,
                            "path": f"{desy_tape_path}:{logical_name}",
                            "archive": True,
                        }
                    ]
                }
                self.logger.info(f"POST /api/files/{file_catalog_uuid}/locations - {new_location}")
                # POST /api/files/{uuid}/locations will de-dupe locations for us
                await fc_rc.request("POST", f"/api/files/{file_catalog_uuid}/locations", new_location)

            # if we processed any Metadata records, we can now delete them
            if num_files > 0:
                delete_query = {
                    "metadata": [x['uuid'] for x in results]
                }
                self.logger.info(f"POST /Metadata/actions/bulk_delete - {num_files} Metadata records")
                bulk_response = await lta_rc.request('POST', '/Metadata/actions/bulk_delete', delete_query)
                delete_count = bulk_response['count']
                self.logger.info(f"LTA DB reports {delete_count} Metadata records are deleted.")
                if delete_count != num_files:
                    raise Exception(f"LTA DB gave us {num_files} records to process, but we only deleted {delete_count} records! BAD MOJO!")

        # the morning sun has vanquished the horrible night
        return True

def runner() -> None:
    """Configure a DesyVerifier component from the environment and set it running."""
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
    # create our DesyVerifier service
    desy_verifier = DesyVerifier(config, LOG)  # type: ignore[arg-type]
    # let's get to work
    desy_verifier.logger.info("Adding tasks to asyncio loop")
    loop = asyncio.get_event_loop()
    loop.create_task(work_loop(desy_verifier))

def main() -> None:
    """Configure a DesyVerifier component from the environment and set it running."""
    runner()
    asyncio.get_event_loop().run_forever()

if __name__ == "__main__":
    main()
