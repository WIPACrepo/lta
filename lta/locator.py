# locator.py
"""Module to implement the Locator component of the Long Term Archive."""

import asyncio
import json
import logging
from logging import Logger
import os
import sys
from typing import Any, Dict, List, Optional

# from binpacking import to_constant_bin_number  # type: ignore
from rest_tools.client import RestClient  # type: ignore
from rest_tools.server import from_environment  # type: ignore

from .component import COMMON_CONFIG, Component, status_loop, work_loop
from .log_format import StructuredFormatter
from .lta_types import BundleType, TransferRequestType


EXPECTED_CONFIG = COMMON_CONFIG.copy()
EXPECTED_CONFIG.update({
    "FILE_CATALOG_REST_TOKEN": None,
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
        self.file_catalog_rest_token = config["FILE_CATALOG_REST_TOKEN"]
        self.file_catalog_rest_url = config["FILE_CATALOG_REST_URL"]
        self.work_retries = int(config["WORK_RETRIES"])
        self.work_timeout_seconds = float(config["WORK_TIMEOUT_SECONDS"])

    def _do_status(self) -> Dict[str, Any]:
        """Locator has no additional status to contribute."""
        return {}

    def _expected_config(self) -> Dict[str, Optional[str]]:
        """Locator provides our expected configuration dictionary."""
        return EXPECTED_CONFIG

    async def _do_work(self) -> None:
        """Perform a work cycle for this component."""
        self.logger.info("Starting work on TransferRequests.")
        work_claimed = True
        while work_claimed:
            work_claimed = await self._do_work_claim()
            work_claimed &= not self.run_once_and_die
        self.logger.info("Ending work on TransferRequests.")

    async def _do_work_claim(self) -> bool:
        """Claim a transfer request and perform work on it."""
        # 1. Ask the LTA DB for the next TransferRequest to be picked
        # configure a RestClient to talk to the LTA DB
        lta_rc = RestClient(self.lta_rest_url,
                            token=self.lta_rest_token,
                            timeout=self.work_timeout_seconds,
                            retries=self.work_retries)
        self.logger.info("Asking the LTA DB for a TransferRequest to work on.")
        pop_body = {
            "claimant": f"{self.name}-{self.instance_uuid}"
        }
        response = await lta_rc.request('POST', f'/TransferRequests/actions/pop?dest={self.source_site}', pop_body)
        self.logger.info(f"LTA DB responded with: {response}")
        tr = response["transfer_request"]
        if not tr:
            self.logger.info("LTA DB did not provide a TransferRequest to work on. Going on vacation.")
            return False
        # process the TransferRequest that we were given
        try:
            await self._do_work_transfer_request(lta_rc, tr)
        except Exception as e:
            await self._quarantine_transfer_request(lta_rc, tr, f"{e}")
            raise e
        # if we were successful at processing work, let the caller know
        return True

    async def _do_work_transfer_request(self,
                                        lta_rc: RestClient,
                                        tr: TransferRequestType) -> None:
        self.logger.info(f"Processing TransferRequest: {tr}")
        # configure a RestClient to talk to the File Catalog
        fc_rc = RestClient(self.file_catalog_rest_url,
                           token=self.file_catalog_rest_token,
                           timeout=self.work_timeout_seconds,
                           retries=self.work_retries)
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
                "$eq": source
            },
            "logical_name": {
                "$regex": f"^{path}"
            },
        }
        query_json = json.dumps(query_dict)
        fc_response = await fc_rc.request('GET', f'/api/files?query={query_json}')
        num_files = len(fc_response["files"])
        self.logger.info(f'File Catalog returned {num_files} file(s) to process.')
        # if we didn't get any files, this is bad mojo
        if num_files < 1:
            await self._quarantine_transfer_request(lta_rc, tr, "File Catalog returned zero files for the TransferRequest")
            return
        # query the file catalog for the full records
        catalog_records = []
        for catalog_file in fc_response["files"]:
            catalog_record = await fc_rc.request('GET', f'/api/files/{catalog_file["uuid"]}')
            catalog_records.append(catalog_record)
        # filter to unique bundle uuids
        bundle_uuids = self._get_unique_archives(catalog_records, source)
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
                "status": "located",
                # "create_timestamp": right_now,  # provided by LTA DB
                # "update_timestamp": right_now,  # provided by LTA DB
                "request": tr["uuid"],
                "source": source,
                "dest": dest,
                "path": path,
                "catalog": as_lta_record(bundle_record),
            })

    async def _create_bundle(self,
                             lta_rc: RestClient,
                             bundle: BundleType) -> Any:
        """Create a new Bundle entity in the LTA DB."""
        self.logger.info(f'Creating new bundle in the LTA DB.')
        create_body = {
            "bundles": [bundle]
        }
        result = await lta_rc.request('POST', '/Bundles/actions/bulk_create', create_body)
        uuid = result["bundles"][0]
        return uuid

    def _get_unique_archives(self,
                             records: List[Dict[str, Any]],
                             source: str) -> List[str]:
        """Obtain the set of archive bundle UUIDs that have the provided files."""
        # for each file record that we are given
        bundle_paths = []
        for record in records:
            # for each location in that record
            for location in record["locations"]:
                # if the file is contained in a bundle at the source
                if (location["archive"] is True) and (location["site"] == source):
                    # add the path to our list of bundle paths
                    bundle_paths.append(location["path"])
        # for each bundle path we collected
        bundle_uuids: List[str] = []
        for bundle_path in bundle_paths:
            # extract the uuid portion of the bundle
            # /some/path/to/an/archive/8abe369e59a111ea81bb534d1a62b1fe.zip
            # basename:                |                                  |
            # split("."):              |                              | | |
            # [0]:                     |                              |
            uuid = os.path.basename(bundle_path).split(".")[0]
            # and if we don't already have it, add it to the list
            if uuid not in bundle_uuids:
                bundle_uuids.append(uuid)
        # return the unique list of bundle UUIDs that we collected
        return bundle_uuids

    async def _quarantine_transfer_request(self,
                                           lta_rc: RestClient,
                                           tr: TransferRequestType,
                                           reason: str) -> None:
        """Update the LTA DB to indicate the TransferRequest should be quarantined."""
        self.logger.error(f'Sending TransferRequest {tr["uuid"]} to quarantine: {reason}.')
        patch_body = {
            "status": "quarantined",
            "reason": reason,
        }
        try:
            await lta_rc.request('PATCH', f'/TransferRequests/{tr["uuid"]}', patch_body)
        except Exception as e:
            self.logger.error(f'Unable to quarantine TransferRequest {tr["uuid"]}: {e}.')

def runner() -> None:
    """Configure a Locator component from the environment and set it running."""
    # obtain our configuration from the environment
    config = from_environment(EXPECTED_CONFIG)
    # configure structured logging for the application
    structured_formatter = StructuredFormatter(
        component_type='Locator',
        component_name=config["COMPONENT_NAME"],
        ndjson=True)
    stream_handler = logging.StreamHandler(sys.stdout)
    stream_handler.setFormatter(structured_formatter)
    root_logger = logging.getLogger(None)
    root_logger.setLevel(logging.NOTSET)
    root_logger.addHandler(stream_handler)
    logger = logging.getLogger("lta.locator")
    # create our Locator service
    locator = Locator(config, logger)
    # let's get to work
    locator.logger.info("Adding tasks to asyncio loop")
    loop = asyncio.get_event_loop()
    loop.create_task(status_loop(locator))
    loop.create_task(work_loop(locator))


def main() -> None:
    """Configure a Locator component from the environment and set it running."""
    runner()
    asyncio.get_event_loop().run_forever()


if __name__ == "__main__":
    main()
