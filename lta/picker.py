# picker.py
"""Module to implement the Picker component of the Long Term Archive."""

import asyncio
import json
import logging
from logging import Logger
import math
import sys
from typing import Any, Dict, Optional

from binpacking import to_constant_bin_number  # type: ignore
from rest_tools.client import RestClient  # type: ignore

from .component import COMMON_CONFIG, Component, status_loop, work_loop
from .config import from_environment
from .log_format import StructuredFormatter
from .lta_types import BundleList, TransferRequestType


EXPECTED_CONFIG = COMMON_CONFIG.copy()
EXPECTED_CONFIG.update({
    "FILE_CATALOG_REST_TOKEN": None,
    "FILE_CATALOG_REST_URL": None,
    "LTA_SITE_CONFIG": "etc/site.json",
    "WORK_RETRIES": "3",
    "WORK_TIMEOUT_SECONDS": "30",
})


class Picker(Component):
    """
    Picker is a Long Term Archive component.

    A Picker is responsible for choosing the files that need to be bundled
    and sent to remote archival destinations. It requests work from the
    LTA REST API and then queries the file catalog to determine which files
    to add to the LTA REST API.
    """

    def __init__(self, config: Dict[str, str], logger: Logger) -> None:
        """
        Create a Picker component.

        config - A dictionary of required configuration values.
        logger - The object the picker should use for logging.
        """
        super(Picker, self).__init__("picker", config, logger)
        self.file_catalog_rest_token = config["FILE_CATALOG_REST_TOKEN"]
        self.file_catalog_rest_url = config["FILE_CATALOG_REST_URL"]
        self.work_retries = int(config["WORK_RETRIES"])
        self.work_timeout_seconds = float(config["WORK_TIMEOUT_SECONDS"])
        with open(config["LTA_SITE_CONFIG"]) as site_data:
            self.lta_site_config = json.load(site_data)
        self.sites = self.lta_site_config["sites"]

    def _do_status(self) -> Dict[str, Any]:
        """Picker has no additional status to contribute."""
        return {}

    def _expected_config(self) -> Dict[str, Optional[str]]:
        """Picker provides our expected configuration dictionary."""
        return EXPECTED_CONFIG

    async def _do_work(self) -> None:
        """Perform a work cycle for this component."""
        self.logger.info("Starting work on TransferRequests.")
        work_claimed = True
        while work_claimed:
            work_claimed = await self._do_work_claim()
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
        response = await lta_rc.request('POST', '/TransferRequests/actions/pop?source=WIPAC', pop_body)
        self.logger.info(f"LTA DB responded with: {response}")
        tr = response["transfer_request"]
        if not tr:
            self.logger.info("LTA DB did not provide a TransferRequest to work on. Going on vacation.")
            return False
        # for each TransferRequest that we were given
        await self._do_work_transfer_request(lta_rc, tr)
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
        # figure out which files need to go
        source = tr["source"]
        dest = tr["dest"]
        path = tr["path"]
        # query the file catalog for the source files
        self.logger.info(f"Asking the File Catalog about files in {source}:{path}")
        query_dict = {
            "locations.site": {
                "$eq": source
            },
            "locations.path": {
                "$regex": f"^{path}"
            }
        }
        query_json = json.dumps(query_dict)
        fc_response = await fc_rc.request('GET', f'/api/files?query={query_json}')
        num_files = len(fc_response["files"])
        self.logger.info(f'File Catalog returned {num_files} file(s) to process.')
        # if we didn't get any files, this is bad mojo
        if num_files < 1:
            self.logger.info(f'There are no files to process for TransferRequest {tr["uuid"]}.')
            quarantine = {
                "status": "quarantined",
                "reason": "File Catalog returned zero files for the TransferRequest",
            }
            await lta_rc.request('PATCH', f'/TransferRequests/{tr["uuid"]}', quarantine)
            return
        # query the file catalog for the full records
        catalog_records = []
        for catalog_file in fc_response["files"]:
            catalog_record = await fc_rc.request('GET', f'/api/files/{catalog_file["uuid"]}')
            catalog_records.append(catalog_record)
        # add up the sizes of everything returned by the catalog
        packing_list = []
        total_size = 0
        for catalog_record in catalog_records:
            file_size = catalog_record["file_size"]
            #                    0: size    1: full record
            packing_list.append((file_size, catalog_record))
            total_size += file_size
        # divide that by the size requested at the destination
        bundle_size = self.sites[dest]["bundle_size"]
        num_bundles = math.floor((float(total_size) / float(bundle_size)) + 0.5)
        num_bundles = max(num_bundles, 1)
        packing_spec = to_constant_bin_number(packing_list, num_bundles, 0)  # 0: size
        # for each packing list, we create a bundle in the LTA DB
        bulk_create: BundleList = []
        for spec in packing_spec:
            bulk_create.append({
                "type": "Bundle",
                # "uuid": unique_id(),  # provided by LTA DB
                "status": "specified",
                # "create_timestamp": right_now,  # provided by LTA DB
                # "update_timestamp": right_now,  # provided by LTA DB
                "request": tr["uuid"],
                "source": source,
                "dest": dest,
                "path": path,
                "files": [x[1] for x in spec],  # 1: full record
            })
        # now we post our new bundles to the LTA DB
        self.logger.info(f"Creating {len(bulk_create)} new Bundles in the LTA DB.")
        create_body = {
            "bundles": bulk_create
        }
        await lta_rc.request('POST', '/Bundles/actions/bulk_create', create_body)


def runner() -> None:
    """Configure a Picker component from the environment and set it running."""
    # obtain our configuration from the environment
    config = from_environment(EXPECTED_CONFIG)
    # configure structured logging for the application
    structured_formatter = StructuredFormatter(
        component_type='Picker',
        component_name=config["COMPONENT_NAME"],
        ndjson=True)
    stream_handler = logging.StreamHandler(sys.stdout)
    stream_handler.setFormatter(structured_formatter)
    root_logger = logging.getLogger(None)
    root_logger.setLevel(logging.NOTSET)
    root_logger.addHandler(stream_handler)
    logger = logging.getLogger("lta.picker")
    # create our Picker service
    picker = Picker(config, logger)
    # let's get to work
    picker.logger.info("Adding tasks to asyncio loop")
    loop = asyncio.get_event_loop()
    loop.create_task(status_loop(picker))
    loop.create_task(work_loop(picker))


def main() -> None:
    """Configure a Picker component from the environment and set it running."""
    runner()
    asyncio.get_event_loop().run_forever()


if __name__ == "__main__":
    main()
