"""A simple script that does a 'globus ls' using lta's GlobusTransfer."""

import argparse
import logging
import os
from pathlib import Path
import json
import asyncio

from lta.transfer.globus import GlobusTransfer


def _globus_ls(
    gt: GlobusTransfer,
    collection: str,
    path: Path,
    do_recursive: bool,
):
    """Like 'globus ls <collection>' but recursively for every subdir."""

    print(f"\n\nglobus ls {path}")

    entries = gt._transfer_client.operation_ls(collection, path=str(path))["DATA"]

    for e in entries:
        fullpath = path / e["name"]
        print(f"\n-> {fullpath}")
        print(json.dumps(e, indent=4))
        # recurse
        if do_recursive and e["type"] == "dir":
            _globus_ls(gt, collection, fullpath, do_recursive)


def globus_ls(root: Path, do_recursive: bool) -> None:
    """Do the ls."""
    print("Initializing GlobusTransfer...")

    gt = GlobusTransfer()  # uses env vars for auth
    collection = gt._env.GLOBUS_SOURCE_COLLECTION_ID

    print(f"Listing contents for {collection=}…")

    _globus_ls(gt, collection, root, do_recursive)

    print("\nOK — Python SDK authentication is working.")


async def main():
    """Main function."""
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--collection",
        required=bool("GLOBUS_SOURCE_COLLECTION_ID" not in os.environ),
        help="The collection name",
    )
    parser.add_argument(
        "--root",
        type=Path,
        required=True,
        help="The directory tree root to 'globus ls'",
    )
    parser.add_argument(
        "-r",
        "--recursive",
        action="store_true",
        help="Recursively list the contents of 'globus ls'",
    )
    args = parser.parse_args()

    # override if given
    if args.collection:
        os.environ["GLOBUS_SOURCE_COLLECTION_ID"] = args.collection

    # mock out the otherwise required env vars — we aren't using this
    os.environ["GLOBUS_DEST_COLLECTION_ID"] = "n/a"

    globus_ls(args.root, args.recursive)


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    asyncio.run(main())
    print("Done.")
