#!/usr/bin/env python3
# populate_file_catalog.py

"""
Populate File Catalog archive locations using exported LTA Metadata records.

Each JSON file in METADATA_INBOX_PATH is expected to be named:

    <bundle_uuid>.json

and contain the Metadata records associated with that Bundle.

After all Metadata records in a file have been successfully processed,
the JSON file is moved to METADATA_OUTBOX_PATH.
"""

import asyncio
import json
import logging
import shutil
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
    "METADATA_INBOX_PATH": None,
    "METADATA_OUTBOX_PATH": None,
    "METADATA_QUARANTINE_PATH": None,
}

LOG = logging.getLogger(__name__)


@dataclass
class MetadataRecord:
    bundle_uuid: str
    file_catalog_uuid: str
    uuid: str


@dataclass
class MetadataRecords:
    results: list[MetadataRecord]


async def ensure_bundle_record(
    lta_rc: RestClient,
    bundle_uuid: str) -> bool:
    """Ensure that the Bundle record exists."""
    try:
        bundle: dict[str, Any] = await lta_rc.request(
            "GET",
            f"/Bundle/{bundle_uuid}",
        )
    except Exception as e:
        LOG.error("Unable to get Bundle record for %s: %s", bundle_uuid, e)
        return False
    else:
        print(json.dumps(bundle, indent=4, sort_keys=True))
        return True


async def load_metadata_records(path: Path) -> MetadataRecords:
    """Load exported Metadata records from a JSON file."""
    async with aiofiles.open(path, encoding="utf-8") as f:
        contents = await f.read()

    data: dict[str, Any] = json.loads(contents)

    return MetadataRecords(
        results=[
            MetadataRecord(
                bundle_uuid=record["bundle_uuid"],
                file_catalog_uuid=record["file_catalog_uuid"],
                uuid=record["uuid"],
            )
            for record in data["results"]
        ]
    )


async def process_metadata_file(
    fc_rc: RestClient,
    lta_rc: RestClient,
    metadata_path: Path,
) -> None:
    """Update File Catalog locations using one exported Metadata file."""
    bundle_uuid = metadata_path.stem

    LOG.info("Processing Bundle %s from %s", bundle_uuid, metadata_path)

    # Load the exported Metadata records.
    metadata_records = await load_metadata_records(metadata_path)
    num_files = len(metadata_records.results)

    LOG.info(
        "Loaded %s Metadata records for Bundle %s.",
        num_files,
        bundle_uuid,
    )

    # Make sure this file really contains Metadata for the Bundle UUID
    # encoded in its filename.
    for metadata_record in metadata_records.results:
        if metadata_record.bundle_uuid != bundle_uuid:
            msg = (
                f"Metadata record {metadata_record.uuid} belongs to Bundle "
                f"{metadata_record.bundle_uuid}, but was found in "
                f"{metadata_path.name}."
            )
            raise ValueError(msg)

    # Load the Bundle so we can determine the archive location.
    LOG.info("GET /Bundle/%s", bundle_uuid)
    bundle: dict[str, Any] = await lta_rc.request(
        "GET",
        f"/Bundle/{bundle_uuid}",
    )

    bundle_dest = bundle["dest"]
    bundle_path = bundle["final_dest_location"]["path"]

    LOG.info(
        "Bundle %s archive location is site=%s path=%s",
        bundle_uuid,
        bundle_dest,
        bundle_path,
    )

    # Update each associated File Catalog record.
    for count, metadata_record in enumerate(metadata_records.results, start=1):
        file_catalog_uuid = metadata_record.file_catalog_uuid

        LOG.info(
            "[%s/%s] GET /api/files/%s",
            count,
            num_files,
            file_catalog_uuid,
        )
        fc_response = await fc_rc.request(
            "GET",
            f"/api/files/{file_catalog_uuid}",
        )

        logical_name = fc_response["logical_name"]

        new_location = {
            "locations": [
                {
                    "site": bundle_dest,
                    "path": f"{bundle_path}:{logical_name}",
                    "archive": True,
                }
            ]
        }

        LOG.info(
            "[%s/%s] POST /api/files/%s/locations - %s",
            count,
            num_files,
            file_catalog_uuid,
            new_location,
        )

        # POST /api/files/{uuid}/locations de-dupes locations for us,
        # so reprocessing a record is safe.
        await fc_rc.request(
            "POST",
            f"/api/files/{file_catalog_uuid}/locations",
            new_location,
        )

    LOG.info(
        "Successfully updated %s File Catalog records for Bundle %s.",
        num_files,
        bundle_uuid,
    )


async def process_metadata_files(
    fc_rc: RestClient,
    lta_rc: RestClient,
    inbox_path: Path,
    outbox_path: Path,
    quarantine_path: Path,
) -> None:
    """Process all Metadata JSON files in the inbox."""
    metadata_files = await asyncio.to_thread(
        lambda: sorted(inbox_path.glob("*.json"))
    )

    num_metadata_files = len(metadata_files)

    LOG.info(
        "Found %s Metadata JSON files in %s.",
        num_metadata_files,
        inbox_path,
    )

    for count, metadata_path in enumerate(metadata_files, start=1):
        LOG.info(
            "Processing Metadata file %s/%s: %s",
            count,
            num_metadata_files,
            metadata_path,
        )

        if not await ensure_bundle_record(
            lta_rc,
            metadata_path.stem
        ):
            error_path = quarantine_path / metadata_path.name
            LOG.info(
                "Moving completed Metadata file %s -> %s",
                metadata_path,
                error_path,
            )

            await asyncio.to_thread(
                shutil.move,
                metadata_path,
                error_path,
            )

            continue

        print("We got a live one!!!")
        sys.exit(EXIT_FAILURE)

        # If anything below raises, the input file remains in the inbox.
        await process_metadata_file(
            fc_rc,
            lta_rc,
            metadata_path,
        )

        # Processing succeeded, so move the Metadata export to the outbox.
        finished_path = outbox_path / metadata_path.name

        LOG.info(
            "Moving completed Metadata file %s -> %s",
            metadata_path,
            finished_path,
        )

        await asyncio.to_thread(
            shutil.move,
            metadata_path,
            finished_path,
        )

    LOG.info("Finished processing all Metadata JSON files.")


async def main() -> None:
    print("This tool was never completely finished; USE AT YOUR OWN RISK!!!")
    sys.exit(EXIT_FAILURE)

    config = from_environment(EXPECTED_CONFIG)

    inbox_path = Path(config["METADATA_INBOX_PATH"])
    outbox_path = Path(config["METADATA_OUTBOX_PATH"])
    quarantine_path = Path(config["METADATA_QUARANTINE_PATH"])

    # Ensure the output directory exists.
    await asyncio.to_thread(
        outbox_path.mkdir,
        parents=True,
        exist_ok=True,
    )

    # Ensure the quarantine directory exists.
    await asyncio.to_thread(
        quarantine_path.mkdir,
        parents=True,
        exist_ok=True,
    )

    # LTA DB client.
    lta_rc = ClientCredentialsAuth(
        address=cast(str, config["LTA_REST_URL"]),
        token_url=cast(str, config["LTA_AUTH_OPENID_URL"]),
        client_id=cast(str, config["CLIENT_ID"]),
        client_secret=cast(str, config["CLIENT_SECRET"]),
    )

    # File Catalog client.
    fc_rc = ClientCredentialsAuth(
        address=cast(str, config["FILE_CATALOG_REST_URL"]),
        token_url=cast(str, config["LTA_AUTH_OPENID_URL"]),
        client_id=cast(str, config["FILE_CATALOG_CLIENT_ID"]),
        client_secret=cast(str, config["FILE_CATALOG_CLIENT_SECRET"]),
    )

    await process_metadata_files(
        fc_rc,
        lta_rc,
        inbox_path,
        outbox_path,
        quarantine_path,
    )


if __name__ == "__main__":
    print("This tool was never completely finished; USE AT YOUR OWN RISK!!!")
    sys.exit(EXIT_FAILURE)
    logging.basicConfig(level=logging.INFO)
    asyncio.run(main())
