# transfer_request_finisher.py
"""Module to implement the TransferRequestFinisher component of the Long Term Archive."""

# fmt:off

import asyncio
import logging
import sys
from typing import Any, Dict, Optional, Union

from prometheus_client import Counter, Gauge, start_http_server
from rest_tools.client import ClientCredentialsAuth, RestClient

from .component import COMMON_CONFIG, Component, now, work_loop
from .lta_tools import from_environment
from .lta_types import BundleType

Logger = logging.Logger

LOG = logging.getLogger(__name__)

EXPECTED_CONFIG = COMMON_CONFIG.copy()
EXPECTED_CONFIG.update({
    "WORK_RETRIES": "3",
    "WORK_TIMEOUT_SECONDS": "30",
    "FILE_CATALOG_CLIENT_ID": None,
    "FILE_CATALOG_CLIENT_SECRET": None,
    "FILE_CATALOG_REST_URL": None,
})

# maximum number of Metadata UUIDs to work with at a time
UPDATE_CHUNK_SIZE = 1000


class TransferRequestFinisher(Component):
    """
    TransferRequestFinisher is a Long Term Archive component.

    A TransferRequestFinisher is responsible for moving a TransferRequest to
    status 'completed' once all of its constituent bundles have been fully
    processed. It looks for bundles in the 'deleted' status and checks to
    ensure that all bundles associated with the request also share this status.
    If they do, then the LTA DB is updated. The TransferRequest is moved to
    status 'completed' and the bundles are moved to status 'finished'.
    """

    def __init__(self, config: Dict[str, str], logger: Logger) -> None:
        """
        Create a TransferRequestFinisher component.

        config - A dictionary of required configuration values.
        logger - The object the transfer_request_finisher should use for logging.
        """
        super(TransferRequestFinisher, self).__init__("transfer_request_finisher", config, logger)
        self.work_retries = int(config["WORK_RETRIES"])
        self.work_timeout_seconds = float(config["WORK_TIMEOUT_SECONDS"])
        self.file_catalog_client_id = config["FILE_CATALOG_CLIENT_ID"]
        self.file_catalog_client_secret = config["FILE_CATALOG_CLIENT_SECRET"]
        self.file_catalog_rest_url = config["FILE_CATALOG_REST_URL"]

        # even if we are successful, take breaks between bundles
        self.pause_after_each_success = True

    def _do_status(self) -> Dict[str, Any]:
        """Provide no additional status."""
        return {}

    def _expected_config(self) -> Dict[str, Optional[str]]:
        """Provide expected configuration dictionary."""
        return EXPECTED_CONFIG

    async def _do_work_claim(self, lta_rc: RestClient) -> bool:
        """Claim a bundle and perform work on it.

        Returns:
            False - if no work was claimed.
            True  - if work was claimed, and the component was successful in processing it.
        Raises:
            Any Exception - if an error occurs during work claim processing.
        """
        fc_rc = ClientCredentialsAuth(
            address=self.file_catalog_rest_url,
            token_url=self.lta_auth_openid_url,
            client_id=self.file_catalog_client_id,
            client_secret=self.file_catalog_client_secret,
        )

        # 1. Ask the LTA DB for the next Bundle to be deleted
        self.logger.info("Asking the LTA DB for a Bundle to check for TransferRequest being finished.")
        pop_body = {
            "claimant": f"{self.name}-{self.instance_uuid}"
        }
        response = await lta_rc.request('POST', f'/Bundles/actions/pop?source={self.source_site}&dest={self.dest_site}&status={self.input_status}', pop_body)
        self.logger.info(f"LTA DB responded with: {response}")
        bundle = response["bundle"]
        if not bundle:
            self.logger.info("LTA DB did not provide a Bundle to check. Going on vacation.")
            return False

        # 2. update the File Catalog + LTA metadata
        await self._migrate_bundle_files_to_file_catalog(fc_rc, lta_rc, bundle)

        # 3. update the TransferRequest that spawned the Bundle, if necessary
        await self._update_transfer_request(lta_rc, bundle)

        return True

    async def _migrate_bundle_files_to_file_catalog(
        self,
        fc_rc: RestClient,
        lta_rc: RestClient,
        bundle: BundleType,
    ) -> None:
        """Update the File Catalog + LTA metadata."""
        await self._add_bundle_to_file_catalog(fc_rc, bundle)
        await self._update_files_in_fc_and_delete_lta_metadata(fc_rc, lta_rc, bundle)

    async def _add_bundle_to_file_catalog(self, fc_rc: RestClient, bundle: BundleType) -> None:
        """Add a FileCatalog entry for the bundle."""

        # create a File Catalog entry for the bundle itself
        bundle_uuid = bundle["uuid"]
        right_now = now()
        file_record = {
            "uuid": bundle_uuid,
            "logical_name": bundle["final_dest_location"]["path"],
            "checksum": bundle["checksum"],
            "locations": [
                {
                    "site": bundle["dest"],
                    **bundle["final_dest_location"],  # "path" + other special keys
                }
            ],
            "file_size": bundle["size"],
            "lta": {
                "date_archived": right_now,
            },
        }

        # add the bundle file to the File Catalog
        logical_name = file_record["logical_name"]
        try:
            self.logger.info(f"POST /api/files - {logical_name}")
            await fc_rc.request("POST", "/api/files", file_record)
        except Exception as e:
            self.logger.error(f"Error: POST /api/files - {logical_name}")
            self.logger.error(f"Message: {e}")
            bundle_uuid = bundle["uuid"]
            self.logger.info(f"PATCH /api/files/{bundle_uuid}")
            await fc_rc.request("PATCH", f"/api/files/{bundle_uuid}", file_record)

    async def _update_files_in_fc_and_delete_lta_metadata(
        self,
        fc_rc: RestClient,
        lta_rc: RestClient,
        bundle: BundleType,
    ) -> None:
        """Update the file records in the File Catalog, then delete their LTA metadata."""
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
                # add a location indicating the bundle archive
                new_location = {
                    "locations": [
                        {
                            "site": bundle['dest'],
                            "path": f'{bundle["final_dest_location"]["path"]}:{fc_response["logical_name"]}',
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

    async def _update_transfer_request(self, lta_rc: RestClient, bundle: BundleType) -> None:
        """
        Update the TransferRequest that spawned the Bundle.

        If all of the Bundles created by the TransferRequest are now status
        "deleted" or "finished", then mark the TransferRequest as status "completed".
        """
        # look up the TransferRequest associated with the bundle
        request_uuid = bundle["request"]
        self.logger.info(f"Querying status of all bundles for TransferRequest {request_uuid}")
        response = await lta_rc.request('GET', f'/Bundles?request={request_uuid}')
        results = response["results"]
        deleted_count = len(results)
        self.logger.info(f"Found {deleted_count} bundles for TransferRequest {request_uuid}")
        # check each constituent bundle for "deleted" or "finished" status
        for bundle_uuid in results:
            result = await lta_rc.request('GET', f'/Bundles/{bundle_uuid}')
            self.logger.info(f"Bundle {result['uuid']} has status {result['status']}")
            if (result["status"] == "deleted") or (result["status"] == "finished"):
                deleted_count = deleted_count - 1
            else:
                self.logger.info(f'{result["status"]} is not "deleted" or "finished"; TransferRequest {request_uuid} will not be updated.')
        # if there are some bundles that have not reached "deleted" or "finished" status
        if deleted_count > 0:
            self.logger.info(f'TransferRequest {request_uuid} has {deleted_count} Bundles still waiting for status "deleted" or "finished"')
            # put the bundle at the back of the line to be checked later
            bundle_id = bundle["uuid"]
            right_now = now()
            patch_body: Dict[str, Union[bool, str]] = {
                "claimed": False,
                "update_timestamp": right_now,
                "work_priority_timestamp": right_now,
            }
            self.logger.info(f"PATCH /Bundles/{bundle_id} - '{patch_body}'")
            await lta_rc.request('PATCH', f'/Bundles/{bundle_id}', patch_body)
            return
        # otherwise, we're ready to complete the TransferRequest
        self.logger.info(f"Updating TransferRequest {request_uuid} to mark as completed.")
        right_now = now()
        patch_body = {
            "claimant": f"{self.name}-{self.instance_uuid}",
            "claimed": False,
            "claim_timestamp": right_now,
            "status": "completed",
            "reason": "",
            "update_timestamp": right_now,
        }
        self.logger.info(f"PATCH /TransferRequests/{request_uuid} - '{patch_body}'")
        await lta_rc.request('PATCH', f'/TransferRequests/{request_uuid}', patch_body)
        # update each of the constituent bundles to status "finished"
        for bundle_id in results:
            patch_body = {
                "claimant": f"{self.name}-{self.instance_uuid}",
                "claimed": False,
                "claim_timestamp": right_now,
                "status": self.output_status,
                "reason": "",
                "update_timestamp": right_now,
            }
            self.logger.info(f"PATCH /Bundles/{bundle_id} - '{patch_body}'")
            await lta_rc.request('PATCH', f'/Bundles/{bundle_id}', patch_body)


async def main(transfer_request_finisher: TransferRequestFinisher) -> None:
    """Execute the work loop of the TransferRequestFinisher component."""
    LOG.info("Starting asynchronous code")
    await work_loop(transfer_request_finisher)
    LOG.info("Ending asynchronous code")


def main_sync() -> None:
    """Configure a TransferRequestFinisher component from the environment and set it running."""
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
    # create our TransferRequestFinisher service
    LOG.info("Starting synchronous code")
    transfer_request_finisher = TransferRequestFinisher(config, LOG)
    # let's get to work
    metrics_port = int(config["PROMETHEUS_METRICS_PORT"])
    start_http_server(metrics_port)
    asyncio.run(main(transfer_request_finisher))
    LOG.info("Ending synchronous code")


if __name__ == "__main__":
    main_sync()
