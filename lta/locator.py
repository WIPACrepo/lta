# locator.py
"""Module to implement the Locator component of the Long Term Archive."""

# fmt:off

import asyncio
import json
import logging
import os
import sys
from typing import Any, Dict, List, Optional

from prometheus_client import start_http_server
from rest_tools.client import ClientCredentialsAuth, RestClient

from .utils import NoFileCatalogFilesException
from .component import COMMON_CONFIG, Component, DoWorkClaimResult, work_loop
from .lta_tools import from_environment
from .lta_types import BundleType, TransferRequestType

Logger = logging.Logger

LOG = logging.getLogger(__name__)

EXPECTED_CONFIG = COMMON_CONFIG.copy()
EXPECTED_CONFIG.update({
    "FILE_CATALOG_CLIENT_ID": None,
    "FILE_CATALOG_CLIENT_SECRET": None,
    "FILE_CATALOG_PAGE_SIZE": "1000",
    "FILE_CATALOG_REST_URL": None,
    "WORK_RETRIES": "3",
    "WORK_TIMEOUT_SECONDS": "30",
})


def as_lta_record(catalog_record: Dict[str, Any]) -> Dict[str, Any]:
    """Cherry pick keys from a File Catalog record to include in Bundle metadata."""
    # As created by the nersc_verifier component...
    # ---------------------------------------------
    # "uuid": bundle["uuid"],
    # "logical_name": hpss_path,
    # "checksum": bundle["checksum"],
    # "locations": [
    #     {
    #         "site": "NERSC",
    #         "path": hpss_path,
    #         "hpss": True,
    #         "online": False,
    #     }
    # ],
    # "file_size": bundle["size"],
    # # note: 'lta' is an application-private metadata field
    # "lta": bundle,
    KEYS = ['checksum', 'file_size', 'logical_name', 'meta_modify_date', 'uuid']
    lta_record = {k: catalog_record[k] for k in KEYS}
    return lta_record


class Locator(Component):
    """
    Locator is a Long Term Archive component.

    A Locator is responsible for choosing bundles from remote archival
    destinations that should be copied back for restoration into the
    Data Warehouse. It requests work from the LTA DB and then queries the
    file catalog to determine which bundles to add to the LTA DB.
    """

    def __init__(self, config: Dict[str, str], logger: Logger) -> None:
        """
        Create a Locator component.

        config - A dictionary of required configuration values.
        logger - The object the locator should use for logging.
        """
        super(Locator, self).__init__("locator", config, logger)
        self.file_catalog_client_id = config["FILE_CATALOG_CLIENT_ID"]
        self.file_catalog_client_secret = config["FILE_CATALOG_CLIENT_SECRET"]
        self.file_catalog_page_size = int(config["FILE_CATALOG_PAGE_SIZE"])
        self.file_catalog_rest_url = config["FILE_CATALOG_REST_URL"]
        self.work_retries = int(config["WORK_RETRIES"])
        self.work_timeout_seconds = float(config["WORK_TIMEOUT_SECONDS"])

    def _do_status(self) -> Dict[str, Any]:
        """Locator has no additional status to contribute."""
        return {}

    def _expected_config(self) -> Dict[str, Optional[str]]:
        """Locator provides our expected configuration dictionary."""
        return EXPECTED_CONFIG

    async def _do_work_claim(self, lta_rc: RestClient) -> DoWorkClaimResult.ReturnType:
        """Claim a transfer request and perform work on it -- see super for return value meanings."""
        # 1. Ask the LTA DB for the next TransferRequest to be picked
        self.logger.info("Asking the LTA DB for a TransferRequest to work on.")
        pop_body = {
            "claimant": f"{self.name}-{self.instance_uuid}"
        }
        response = await lta_rc.request('POST', f'/TransferRequests/actions/pop?source={self.source_site}&dest={self.dest_site}', pop_body)
        self.logger.info(f"LTA DB responded with: {response}")
        tr = response["transfer_request"]
        if not tr:
            self.logger.info("LTA DB did not provide a TransferRequest to work on. Going on vacation.")
            return DoWorkClaimResult.NothingClaimed("PAUSE")
        # process the TransferRequest that we were given
        try:
            await self._do_work_transfer_request(lta_rc, tr)
            return DoWorkClaimResult.Successful("CONTINUE")
        except Exception as e:
            return DoWorkClaimResult.QuarantineNow("PAUSE", tr, "TRANSFER_REQUEST", e)

    async def _do_work_transfer_request(self,
                                        lta_rc: RestClient,
                                        tr: TransferRequestType) -> None:
        self.logger.info(f"Processing TransferRequest: {tr}")
        # configure a RestClient to talk to the File Catalog
        fc_rc = ClientCredentialsAuth(address=self.file_catalog_rest_url,
                                      token_url=self.lta_auth_openid_url,
                                      client_id=self.file_catalog_client_id,
                                      client_secret=self.file_catalog_client_secret)
        # figure out which files need to come back
        source = tr["source"]
        dest = tr["dest"]
        path = tr["path"]
        # query the file catalog for the source files
        self.logger.info(f"Asking the File Catalog about files in {path} archived at {source}")
        query_dict = {
            "locations.archive": {
                "$eq": True,
            },
            "locations.site": {
                "$eq": source,
            },
            "locations.path": {
                "$regex": f"^{path}",
            },
            # this isn't going to work; searching 'logical_name' by regular expression
            # "logical_name": {
            #     "$regex": f"^{path}",
            # },
        }
        query_json = json.dumps(query_dict)
        bundle_uuids: List[str] = []
        page_start = 0
        done = False
        # until we're finished processing file catalog records
        while not done:
            # ask the file catalog for records relevant to the path
            self.logger.info(f"GET /api/files?query=QUERY&keys=uuid&limit={self.file_catalog_page_size}&start={page_start}")
            fc_response = await fc_rc.request('GET', f'/api/files?query={query_json}&keys=uuid&limit={self.file_catalog_page_size}&start={page_start}')
            num_files = len(fc_response["files"])
            self.logger.info(f'File Catalog returned {num_files} file(s) to process.')
            page_start += num_files
            done = (num_files == 0)
            # for each result that we got back, look up the full record
            for catalog_file in fc_response["files"]:
                self.logger.info(f"GET /api/files/{catalog_file['uuid']}")
                catalog_record = await fc_rc.request('GET', f'/api/files/{catalog_file["uuid"]}')
                # using bundle_uuids as an accumulator, reduce the provided record into unique bundle uuids
                bundle_uuids = self._reduce_unique_archive_uuid(bundle_uuids, catalog_record, source)
        # if we didn't get any bundle_uuids, this is bad mojo
        if not bundle_uuids:
            raise NoFileCatalogFilesException(
                f"LTA File Catalog returned zero files for the TransferRequest: {tr['uuid']}"
            )
        # query the file catalog for the bundle records
        bundle_records = []
        for bundle_uuid in bundle_uuids:
            bundle_record = await fc_rc.request('GET', f'/api/files/{bundle_uuid}')
            bundle_records.append(bundle_record)
        # for each bundle record that we obtained, we create a bundle in the LTA DB
        self.logger.info(f"Creating {len(bundle_records)} new Bundles in the LTA DB.")
        for bundle_record in bundle_records:
            await self._create_bundle(lta_rc, {
                "type": "Bundle",
                # "uuid": unique_id(),  # provided by LTA DB
                "status": self.output_status,
                "claimed": False,
                "verified": False,
                "reason": "",
                # "create_timestamp": right_now,  # provided by LTA DB
                # "update_timestamp": right_now,  # provided by LTA DB
                "request": tr["uuid"],
                "source": source,
                "dest": dest,
                "path": path,
                "size": bundle_record["file_size"],
                "bundle_path": bundle_record["logical_name"],
                "checksum": bundle_record["checksum"],
                "catalog": as_lta_record(bundle_record),
            })

    async def _create_bundle(self,
                             lta_rc: RestClient,
                             bundle: BundleType) -> Any:
        """Create a new Bundle entity in the LTA DB."""
        self.logger.info('Creating new bundle in the LTA DB.')
        create_body = {
            "bundles": [bundle]
        }
        result = await lta_rc.request('POST', '/Bundles/actions/bulk_create', create_body)
        uuid = result["bundles"][0]
        return uuid

    def _reduce_unique_archive_uuid(self,
                                    bundle_uuids: List[str],
                                    catalog_record: Dict[str, Any],
                                    source: str) -> List[str]:
        """Obtain the set of archive bundle UUIDs that have the provided files."""
        bundle_paths = []
        # for each location in that record
        for location in catalog_record["locations"]:
            # if this location is not an archive, just skip it
            if "archive" not in location:
                continue
            # if the file is contained in a bundle at the source
            if (location["archive"] is True) and (location["site"] == source):
                # add the path to our list of bundle paths
                bundle_paths.append(location["path"])
        # for each bundle path we collected
        for bundle_path in bundle_paths:
            # extract the archive portion of the path
            # bundle_path: /home/projects/icecube/data/exp/IceCube/2018/internal-system/pDAQ-2ndBld/0803/9a1cab0a395211eab1cbce3a3da73f88.zip:ukey_5667ab7c-919d-40d6-b3bb-31deecf39e3a_SPS-pDAQ-2ndBld-000_20180803_231701_000000.tar.gz
            # split(":"):  |                                                                                                                | |                                                                                         |
            # [0]:         |                                                                                                                |
            keep_path = bundle_path.split(":")[0]
            # extract the uuid portion of the bundle
            # /some/path/to/an/archive/8abe369e59a111ea81bb534d1a62b1fe.zip
            # basename:                |                                  |
            # split("."):              |                              | | |
            # [0]:                     |                              |
            uuid = os.path.basename(keep_path).split(".")[0]
            # and if we don't already have it, add it to the list
            if uuid not in bundle_uuids:
                self.logger.info(f"Found unique bundle UUID: {uuid}")
                bundle_uuids.append(uuid)
        # return the unique list of bundle UUIDs that we collected
        return bundle_uuids


async def main(locator: Locator) -> None:
    """Execute the work loop of the Locator component."""
    LOG.info("Starting asynchronous code")
    await work_loop(locator)
    LOG.info("Ending asynchronous code")


def main_sync() -> None:
    """Configure a Locator component from the environment and set it running."""
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
    # create our Locator service
    LOG.info("Starting synchronous code")
    locator = Locator(config, LOG)
    # let's get to work
    metrics_port = int(config["PROMETHEUS_METRICS_PORT"])
    start_http_server(metrics_port)
    asyncio.run(main(locator))
    LOG.info("Ending synchronous code")


if __name__ == "__main__":
    main_sync()
