# picker.py
"""Module to implement the Picker component of the Long Term Archive."""

# fmt:off

import asyncio
import dataclasses
import json
import logging
import math
import sys
from typing import Any, Dict, Optional

from binpacking import to_constant_bin_number  # type: ignore
from prometheus_client import start_http_server
from rest_tools.client import ClientCredentialsAuth, RestClient

from .utils import LTANounEnum, NoFileCatalogFilesException, QuarantinableException
from .component import COMMON_CONFIG, Component, work_loop
from .lta_tools import from_environment
from .lta_types import BundleType, TransferRequestType

Logger = logging.Logger

LOG = logging.getLogger(__name__)

# maximum number of Metadata UUIDs to supply to LTA DB for bulk_create
CREATE_CHUNK_SIZE = 1000

BUNDLE_SIZE_MAX_FACTOR = 1.2  # between 1 and 1.5

EXPECTED_CONFIG = COMMON_CONFIG.copy()
EXPECTED_CONFIG.update({
    "FILE_CATALOG_CLIENT_ID": None,
    "FILE_CATALOG_CLIENT_SECRET": None,
    "FILE_CATALOG_PAGE_SIZE": "1000",
    "FILE_CATALOG_REST_URL": None,
    "IDEAL_BUNDLE_SIZE": "107374182400",  # 100 GiB
    "WORK_RETRIES": "3",
    "WORK_TIMEOUT_SECONDS": "30",
})


@dataclasses.dataclass
class FileCatalogFile:
    """An encapsulated file object from the file catalog."""

    uuid: str
    file_size: int


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
        super().__init__("picker", LTANounEnum.TRANSFER_REQUEST, config, logger)
        self.file_catalog_client_id = config["FILE_CATALOG_CLIENT_ID"]
        self.file_catalog_client_secret = config["FILE_CATALOG_CLIENT_SECRET"]
        self.file_catalog_page_size = int(config["FILE_CATALOG_PAGE_SIZE"])
        self.file_catalog_rest_url = config["FILE_CATALOG_REST_URL"]
        self.ideal_bundle_size = int(config["IDEAL_BUNDLE_SIZE"])
        self.work_retries = int(config["WORK_RETRIES"])
        self.work_timeout_seconds = float(config["WORK_TIMEOUT_SECONDS"])

    def _do_status(self) -> Dict[str, Any]:
        """Picker has no additional status to contribute."""
        return {}

    def _expected_config(self) -> Dict[str, Optional[str]]:
        """Picker provides our expected configuration dictionary."""
        return EXPECTED_CONFIG

    async def _do_work_claim(self, lta_rc: RestClient) -> bool:
        """Claim a transfer request and perform work on it.

        Returns:
            False - if no work was claimed.
            True  - if work was claimed, and the component was successful in processing it.
        Raises:
            Any Exception - if an error occurs during work claim processing.
        """
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
            return False
        # process the TransferRequest that we were given
        try:
            await self._do_work_transfer_request(lta_rc, tr)
            return True
        except Exception as e:
            raise QuarantinableException(tr, e)

    async def _do_work_transfer_request(
        self,
        lta_rc: RestClient,
        tr: TransferRequestType,
    ) -> None:
        """Process a TransferRequest by fetching files, grouping them, and bundling for LTA."""
        self.logger.info(f"Processing TransferRequest: {tr}")

        # step 1: get files
        catalog_files = await self._get_files_from_file_catalog(tr)
        # if we didn't get any files, this is bad mojo
        if not catalog_files:
            raise NoFileCatalogFilesException(
                f"LTA File Catalog returned zero files for the TransferRequest: {tr['uuid']}"
            )

        # step 2: group those files
        packing_spec = self._group_catalog_files_evenly(catalog_files)

        # step 3: bundle those files
        await self._bundle_files_for_lta(tr, packing_spec, lta_rc)

    async def _get_files_page(
        self,
        fc_rc: RestClient,
        query_json: str,
        start: int,
    ) -> list[FileCatalogFile]:
        """Query FC for files, return one page according to 'start' + 'limit'."""
        self.logger.info(f'Querying File Catalog. start={start}')
        resp = await fc_rc.request(
            'GET',
            (
                f'/api/files?query={query_json}'
                f'&keys=uuid|file_size'
                f'&limit={self.file_catalog_page_size}'
                f'&start={start}'
            )
        )
        ret = [
            FileCatalogFile(f["uuid"], f["file_size"])  # is everyone here?
            for f in resp["files"]
        ]
        self.logger.info(f'File Catalog returned {len(ret)} file(s) to process.')
        return ret

    async def _get_files_from_file_catalog(
        self,
        tr: TransferRequestType,
    ) -> list[FileCatalogFile]:
        """Get the files for the transfer request from the File Catalog."""

        fc_rc = ClientCredentialsAuth(address=self.file_catalog_rest_url,
                                      token_url=self.lta_auth_openid_url,
                                      client_id=self.file_catalog_client_id,
                                      client_secret=self.file_catalog_client_secret)

        # figure out which files need to go
        source = tr["source"]
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
            # this isn't going to work; searching 'logical_name' by regular expression
            # "logical_name": {
            #     "$regex": f"^{path}"
            # },
        }
        query_json = json.dumps(query_dict)
        page_start = 0
        catalog_files: list[FileCatalogFile] = []
        # query (and paginate) until the FC gives us nothing — don't assume 'limit' is respected
        while files := await self._get_files_page(fc_rc, query_json, page_start):
            page_start += len(files)
            catalog_files.extend(files)

        return catalog_files

    def _group_catalog_files_evenly(
        self,
        catalog_files: list[FileCatalogFile],
    ) -> list[list[FileCatalogFile]]:
        """Group catalog_files into reasonably even-sized chunks for bundling, by file size."""
        self.logger.info(f'Processing {len(catalog_files)} UUIDs returned by the File Catalog.')
        if not catalog_files:
            return [[]]

        total_size = sum(f.file_size for f in catalog_files)
        n_bins = max(
            # we want even bundles...
            round(total_size / self.ideal_bundle_size),
            # if that would make bundles too large, then +1 bin count.
            math.ceil(total_size / (self.ideal_bundle_size * BUNDLE_SIZE_MAX_FACTOR)),
            # edge case: always at least one bundle
            1,
        )

        return to_constant_bin_number(catalog_files, n_bins, key=lambda f: f.file_size)

    async def _bundle_files_for_lta(
        self,
        tr: TransferRequestType,
        packing_spec: list[list[FileCatalogFile]],
        lta_rc: RestClient,
    ) -> None:
        """Bundle files and give to LTA, including metadata objects."""

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
                "source": tr["source"],
                "dest": tr["dest"],
                "path": tr["path"],
                "file_count": len(spec),
            })
            await self._create_metadata_mapping(lta_rc, spec, bundle_uuid)

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

    async def _create_metadata_mapping(self,
                                       lta_rc: RestClient,
                                       spec: list[FileCatalogFile],
                                       bundle_uuid: str) -> None:
        """Create metadata mappings between File Catalog and new bundle."""
        self.logger.info(f'Creating {len(spec)} Metadata mappings between the File Catalog and pending bundle {bundle_uuid}.')
        slice_index = 0
        NUM_UUIDS = len(spec)
        for i in range(slice_index, NUM_UUIDS, CREATE_CHUNK_SIZE):
            slice_index = i
            create_slice = spec[slice_index:(slice_index + CREATE_CHUNK_SIZE)]
            create_body = {
                "bundle_uuid": bundle_uuid,
                "files": [x.uuid for x in create_slice],
            }
            result = await lta_rc.request('POST', '/Metadata/actions/bulk_create', create_body)
            self.logger.info(f'Created {result["count"]} Metadata documents linking to pending bundle {bundle_uuid}.')


async def main(picker: Picker) -> None:
    """Execute the work loop of the Picker component."""
    LOG.info("Starting asynchronous code")
    await work_loop(picker)
    LOG.info("Ending asynchronous code")


def main_sync() -> None:
    """Configure a Picker component from the environment and set it running."""
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
    # create our Picker service
    LOG.info("Starting synchronous code")
    picker = Picker(config, LOG)
    # let's get to work
    metrics_port = int(config["PROMETHEUS_METRICS_PORT"])
    start_http_server(metrics_port)
    asyncio.run(main(picker))
    LOG.info("Ending synchronous code")


if __name__ == "__main__":
    main_sync()
