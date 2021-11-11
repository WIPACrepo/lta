# picker.py
"""Module to implement the Picker component of the Long Term Archive."""

import asyncio
import json
import logging
import sys
from typing import Any, Dict, List, Optional, Tuple

from binpacking import to_constant_volume  # type: ignore
from rest_tools.client import RestClient  # type: ignore
from rest_tools.server import from_environment  # type: ignore
import wipac_telemetry.tracing_tools as wtt

from .component import COMMON_CONFIG, Component, now, status_loop, work_loop
from .log_format import StructuredFormatter
from .lta_types import BundleType, TransferRequestType

Logger = logging.Logger

# maximum number of Metadata UUIDs to supply to LTA DB for bulk_create
CREATE_CHUNK_SIZE = 1000

EXPECTED_CONFIG = COMMON_CONFIG.copy()
EXPECTED_CONFIG.update({
    "FILE_CATALOG_PAGE_SIZE": "1000",
    "FILE_CATALOG_REST_TOKEN": None,
    "FILE_CATALOG_REST_URL": None,
    "MAX_BUNDLE_SIZE": "107374182400",  # 100 GiB
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
        self.file_catalog_page_size = int(config["FILE_CATALOG_PAGE_SIZE"])
        self.file_catalog_rest_token = config["FILE_CATALOG_REST_TOKEN"]
        self.file_catalog_rest_url = config["FILE_CATALOG_REST_URL"]
        self.max_bundle_size = int(config["MAX_BUNDLE_SIZE"])
        self.work_retries = int(config["WORK_RETRIES"])
        self.work_timeout_seconds = float(config["WORK_TIMEOUT_SECONDS"])

    def _do_status(self) -> Dict[str, Any]:
        """Picker has no additional status to contribute."""
        return {}

    def _expected_config(self) -> Dict[str, Optional[str]]:
        """Picker provides our expected configuration dictionary."""
        return EXPECTED_CONFIG

    @wtt.spanned()
    async def _do_work(self) -> None:
        """Perform a work cycle for this component."""
        self.logger.info("Starting work on TransferRequests.")
        work_claimed = True
        while work_claimed:
            work_claimed = await self._do_work_claim()
            work_claimed &= not self.run_once_and_die
        self.logger.info("Ending work on TransferRequests.")

    @wtt.spanned()
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
        response = await lta_rc.request('POST', f'/TransferRequests/actions/pop?source={self.source_site}&dest={self.dest_site}', pop_body)
        self.logger.info(f"LTA DB responded with: {response}")
        tr = response["transfer_request"]
        if not tr:
            self.logger.info("LTA DB did not provide a TransferRequest to work on. Going on vacation.")
            return False
        # process the TransferRequest that we were given
        try:
            await self._do_work_transfer_request(lta_rc, tr)
        except Exception as e:
            self.logger.info(f"There was an error while processing the transfer request: {e}")
            self.logger.info("Will now attempt to send the transfer request to 'quarantined' status.")
            await self._quarantine_transfer_request(lta_rc, tr, f"{e}")
            self.logger.info("Done sending the transfer request to 'quarantined' status, will end work cycle.")
            return False
        # if we were successful at processing work, let the caller know
        return True

    @wtt.spanned()
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
            },
            "logical_name": {
                "$regex": f"^{path}"
            },
        }
        query_json = json.dumps(query_dict)
        page_start = 0
        catalog_files = []
        fc_response = await fc_rc.request('GET', f'/api/files?query={query_json}&keys=uuid&limit={self.file_catalog_page_size}&start={page_start}')
        num_files = len(fc_response["files"])
        self.logger.info(f'File Catalog returned {num_files} file(s) to process.')
        catalog_files.extend(fc_response["files"])
        while num_files == self.file_catalog_page_size:
            self.logger.info(f'Paging File Catalog. start={page_start}')
            page_start += num_files
            fc_response = await fc_rc.request('GET', f'/api/files?query={query_json}&keys=uuid&limit={self.file_catalog_page_size}&start={page_start}')
            num_files = len(fc_response["files"])
            self.logger.info(f'File Catalog returned {num_files} file(s) to process.')
            catalog_files.extend(fc_response["files"])
        # if we didn't get any files, this is bad mojo
        if not catalog_files:
            await self._quarantine_transfer_request(lta_rc, tr, "File Catalog returned zero files for the TransferRequest")
            return
        # create a packing list by querying the File Catalog for size information
        num_catalog_files = len(catalog_files)
        self.logger.info(f'Processing {num_catalog_files} UUIDs returned by the File Catalog.')
        packing_list = []
        for catalog_file in catalog_files:
            catalog_file_uuid = catalog_file["uuid"]
            catalog_record = await fc_rc.request('GET', f'/api/files/{catalog_file_uuid}')
            file_size = catalog_record["file_size"]
            #                    0: uuid            1: size
            packing_list.append((catalog_file_uuid, file_size))
        # divide the packing list into an array of packing specifications
        packing_spec = to_constant_volume(packing_list, self.max_bundle_size, 1)  # 1: size
        # for each packing list, we create a bundle in the LTA DB
        self.logger.info(f"Creating {len(packing_spec)} new Bundles in the LTA DB.")
        for spec in packing_spec:
            self.logger.info(f"Packing specification contains {len(spec)} files.")
            bundle_uuid = await self._create_bundle(lta_rc, {
                "type": "Bundle",
                # "uuid": unique_id(),  # provided by LTA DB
                "status": self.output_status,
                "reason": "",
                # "create_timestamp": right_now,  # provided by LTA DB
                # "update_timestamp": right_now,  # provided by LTA DB
                "request": tr["uuid"],
                "source": source,
                "dest": dest,
                "path": path,
                "file_count": len(spec),
            })
            await self._create_metadata_mapping(lta_rc, spec, bundle_uuid)

    @wtt.spanned()
    async def _create_bundle(self,
                             lta_rc: RestClient,
                             bundle: BundleType) -> Any:
        self.logger.info('Creating new bundle in the LTA DB.')
        create_body = {
            "bundles": [bundle]
        }
        result = await lta_rc.request('POST', '/Bundles/actions/bulk_create', create_body)
        uuid = result["bundles"][0]
        return uuid

    @wtt.spanned()
    async def _create_metadata_mapping(self,
                                       lta_rc: RestClient,
                                       spec: List[Tuple[str, int]],
                                       bundle_uuid: str) -> None:
        self.logger.info(f'Creating {len(spec)} Metadata mappings between the File Catalog and pending bundle {bundle_uuid}.')
        slice_index = 0
        NUM_UUIDS = len(spec)
        for i in range(slice_index, NUM_UUIDS, CREATE_CHUNK_SIZE):
            slice_index = i
            create_slice = spec[slice_index:slice_index+CREATE_CHUNK_SIZE]
            create_body = {
                "bundle_uuid": bundle_uuid,
                "files": [x[0] for x in create_slice],  # 0: uuid
            }
            result = await lta_rc.request('POST', '/Metadata/actions/bulk_create', create_body)
            self.logger.info(f'Created {result["count"]} Metadata documents linking to pending bundle {bundle_uuid}.')

    @wtt.spanned()
    async def _quarantine_transfer_request(self,
                                           lta_rc: RestClient,
                                           tr: TransferRequestType,
                                           reason: str) -> None:
        self.logger.error(f'Sending TransferRequest {tr["uuid"]} to quarantine: {reason}.')
        right_now = now()
        patch_body = {
            "status": "quarantined",
            "reason": f"BY:{self.name}-{self.instance_uuid} REASON:{reason}",
            "work_priority_timestamp": right_now,
        }
        try:
            await lta_rc.request('PATCH', f'/TransferRequests/{tr["uuid"]}', patch_body)
        except Exception as e:
            self.logger.error(f'Unable to quarantine TransferRequest {tr["uuid"]}: {e}.')

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
