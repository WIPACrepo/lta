# picker.py
"""Module to implement the Picker component of the Long Term Archive."""

import asyncio
from datetime import datetime
import json
import logging
from logging import Logger
import sys
from typing import Any, Dict, Optional

from rest_tools.client import RestClient  # type: ignore

from .component import COMMON_CONFIG, Component, status_loop, work_loop
from .config import from_environment
from .log_format import StructuredFormatter
from .lta_types import CatalogFileType, DestList, FileList, TransferRequestType


EXPECTED_CONFIG = COMMON_CONFIG.copy()
EXPECTED_CONFIG.update({
    "FILE_CATALOG_REST_TOKEN": None,
    "FILE_CATALOG_REST_URL": None,
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

    def _do_status(self) -> Dict[str, Any]:
        """Picker has no additional status to contribute."""
        return {}

    def _expected_config(self) -> Dict[str, Optional[str]]:
        """Picker provides our expected configuration dictionary."""
        return EXPECTED_CONFIG

    async def _do_work(self) -> None:
        """Perform a work cycle for this component."""
        # 1. Ask the LTA DB for the next TransferRequest to be picked
        # configure a RestClient to talk to the LTA DB
        lta_rc = RestClient(self.lta_rest_url,
                            token=self.lta_rest_token,
                            timeout=self.work_timeout_seconds,
                            retries=self.work_retries)
        self.logger.info("Asking the LTA DB for a TransferRequest to work on.")
        pop_body = {
            "picker": self.name
        }
        response = await lta_rc.request('POST', '/TransferRequests/actions/pop?source=WIPAC', pop_body)
        self.logger.info(f"LTA DB responded with: {response}")
        results = response["results"]
        if not results:
            self.logger.info(f"No TransferRequests are available to work on. Going on vacation.")
            return
        self.logger.info(f"There are {len(results)} TransferRequest(s) to work on.")
        # for each TransferRequest that we were given
        for tr in results:
            # TODO: what do we do with broken transfer requests?
            # try:
            #     await self._do_work_transfer_request(lta_rc, tr)
            # except Exception as e:
            #     self.logger.error("Unable to process TR {UUID}")
            #     lta_rc.request('PATCH', '/TransferRequest/{uuid}', {"quarantine": f"exception: {e}"})
            await self._do_work_transfer_request(lta_rc, tr)
        # log a friendly message
        self.logger.info(f'Done working on all TransferRequests.')

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
        src_split = tr['source'].split(':', 1)
        src_site = src_split[0]
        src_path = src_split[1]
        # figure out where they need to go to
        dests: DestList = []
        for dst in tr['dest']:
            dst_split = dst.split(':', 1)
            dests.append((dst_split[0], dst_split[1]))
        # query the file catalog for the source files
        self.logger.info(f"Asking the File Catalog about files in {src_split}:{src_path}")
        query_dict = {
            "locations.site": {
                "$eq": f"{src_site}"
            },
            "locations.path": {
                "$regex": f"^{src_path}"
            }
        }
        query_json = json.dumps(query_dict)
        fc_response = await fc_rc.request('GET', f'/api/files?query={query_json}')
        self.logger.info(f'File Catalog returned {len(fc_response["files"])} file(s) to process.')
        # TODO: If there are no files, that seems like something to alert on
        # if len(fc_response["files"]) < 1:
        #     self.alerter.error("TR {UUID} wanted to archive {site}:{path} but the File Catalog reported 0 files!")
        #     lta_rc.request('PATCH', '/TransferRequest/{uuid}', {"quarantine": "file_catalog_count == 0"})
        #     return
        # for each file provided by the catalog
        bulk_create: FileList = []
        for catalog_file in fc_response["files"]:
            bulk_create.extend(await self._do_work_catalog_file(lta_rc, tr, fc_rc, dests, catalog_file))
        # 3. Update the LTA DB with Files needed for bundling
        self.logger.info(f'Identified {len(bulk_create)} transfer(s) to add to the LTA DB.')
        create_body = {
            "files": bulk_create
        }
        await lta_rc.request('POST', '/Files/actions/bulk_create', create_body)
        # 4. Return the TransferRequest to the LTA DB as picked
        self.logger.info(f'Marking TransferRequest {tr["uuid"]} as complete in the LTA DB.')
        complete = {
            "complete": {
                "timestamp": datetime.utcnow().isoformat(),
                "picker": self.name
            }
        }
        await lta_rc.request('PATCH', f'/TransferRequests/{tr["uuid"]}', complete)
        self.logger.info(f'Deleting TransferRequest {tr["uuid"]} from the LTA DB.')
        await lta_rc.request('DELETE', f'/TransferRequests/{tr["uuid"]}')
        self.logger.info(f'Done working on TransferRequest {tr["uuid"]}.')

    async def _do_work_catalog_file(self,
                                    lta_rc: RestClient,
                                    tr: TransferRequestType,
                                    fc_rc: RestClient,
                                    dests: DestList,
                                    catalog_file: CatalogFileType) -> FileList:
        self.logger.info(f'Processing catalog file: {catalog_file["logical_name"]}')
        # ask the File Catalog for the full record of the file
        fc_response2 = await fc_rc.request('GET', f'/api/files/{catalog_file["uuid"]}')
        # create a container to hold our results
        bulk_create: FileList = []
        # for each destination in the transfer request
        for dest in dests:
            # check to see if our full record contains that location
            already_there = False
            for loc in fc_response2["locations"]:
                if (loc["site"] is dest[0]) and (loc["path"].startswith(dest[1])):
                    already_there = True
                    break
            # if the file is already at that destination
            if already_there:
                self.logger.info(f'Catalog file {catalog_file["logical_name"]} is already at {dest[0]}:{dest[1]}')
                # move on to the next destination
                continue
            # otherwise, we need to create this File object in the LTA DB
            self.logger.info(f'Adding catalog file {catalog_file["logical_name"]} transfer {tr["source"]} -> {dest[0]}:{dest[1]} to bulk create list.')
            file_obj = {
                "source": tr["source"],
                "dest": f"{dest[0]}:{dest[1]}",
                "request": tr["uuid"],
                "catalog": fc_response2
            }
            bulk_create.append(file_obj)
        # return our list to the caller
        return bulk_create


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
