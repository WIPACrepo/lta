#!/usr/bin/env python3
# export_bundle_metadata.py

"""
Given a list of distinct Bundle UUIDs,
export the Metadata records associated with those Bundles.
"""

import asyncio
import json
import logging
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any, cast

import aiofiles
from rest_tools.client import ClientCredentialsAuth, RestClient
from wipac_dev_tools import from_environment

EXIT_FAILURE = 1

EXPECTED_CONFIG = {
    "CLIENT_ID": None,
    "CLIENT_SECRET": None,
    "FILE_CATALOG_CLIENT_ID": None,
    "FILE_CATALOG_CLIENT_SECRET": None,
    "FILE_CATALOG_REST_URL": None,
    "LTA_AUTH_OPENID_URL": None,
    "LTA_REST_URL": None,
    "METADATA_BUNDLE_UUID_JSON": None,
    "METADATA_EXPORT_PATH": None,
}

LOG = logging.getLogger(__name__)

UPDATE_CHUNK_SIZE = 1000


@dataclass
class DistinctBundleUUIDs:
    count: int
    missing_count: int
    results: list[str]


def load_metadata_bundle_uuids(path: str) -> DistinctBundleUUIDs:
    """Load results from a JSON file."""
    with Path(path).open(encoding="utf-8") as f:
        data: dict[str, Any] = json.load(f)

    return DistinctBundleUUIDs(
        count=data["count"],
        missing_count=data["missing_count"],
        results=data["results"],
    )


async def export_metadata_records(lta_rc: RestClient,
                                  records: DistinctBundleUUIDs,
                                  export_path: Path) -> None:
    """Collect the metadata records for a bundle and export them."""
    # for each Bundle UUID in the results
    for bundle_uuid in records.results:
        # determine to where we'll export the metadata records
        records_json = export_path / f"{bundle_uuid}.json"

        # ask the LTA DB for the next chunk of Metadata records
        limit = UPDATE_CHUNK_SIZE
        LOG.info(f"GET /Metadata?bundle_uuid={bundle_uuid}&limit={limit}")
        lta_response = await lta_rc.request('GET', f'/Metadata?bundle_uuid={bundle_uuid}&limit={limit}')
        results = lta_response["results"]
        num_files = len(results)
        LOG.info(f'LTA returned {num_files} Metadata documents to process.')

        # ensure that we got them all in one shot, and aren't leaving anything out
        if num_files == UPDATE_CHUNK_SIZE:
            LOG.error(f"Bundle {bundle_uuid} has more than {UPDATE_CHUNK_SIZE} Metadata records.")
            LOG.error(f"Increase UPDATE_CHUNK_SIZE")
            sys.exit(EXIT_FAILURE)

        # export the metadata records
        LOG.info(f"Exporting {num_files} Metadata records to: {records_json}")
        async with aiofiles.open(records_json, "w", encoding="utf-8") as f:
            await f.write(json.dumps(lta_response, indent=4, sort_keys=True))


async def main() -> None:
    config = from_environment(EXPECTED_CONFIG)

    # load the list of distinct bundle uuids that have metadata in LTA
    metadata_bundle_uuid_json = config["METADATA_BUNDLE_UUID_JSON"]
    metadata_bundle_uuid = load_metadata_bundle_uuids(metadata_bundle_uuid_json)

    # if there is anything wrong, bail out
    missing_count = metadata_bundle_uuid.missing_count
    if missing_count > 0:
        LOG.error("missing_count == %s (> 0); will not process metadata records", missing_count)
        sys.exit(EXIT_FAILURE)

    count = metadata_bundle_uuid.count
    num_results = len(metadata_bundle_uuid.results)
    if num_results != count:
        LOG.error("(num_results == %s) != (count == %s); will not process metadata records", num_results, count)
        sys.exit(EXIT_FAILURE)

    # export the metadata records
    lta_rc = ClientCredentialsAuth(address=cast(str, config["LTA_REST_URL"]),
                                   token_url=cast(str, config["LTA_AUTH_OPENID_URL"]),
                                   client_id=cast(str, config["CLIENT_ID"]),
                                   client_secret=cast(str, config["CLIENT_SECRET"]))
    export_path = Path(config["METADATA_EXPORT_PATH"])
    await export_metadata_records(lta_rc, metadata_bundle_uuid, export_path)


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    asyncio.run(main())
