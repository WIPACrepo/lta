"""A simple script that does a 'globus ls' using lta's GlobusTransfer."""

from pathlib import Path
import json
import asyncio
from lta.transfer.globus import GlobusTransfer


def globus_ls_recursive(gt: GlobusTransfer, collection: str, path: Path):
    """Like 'globus ls <collection>' but recursively for every subdir."""

    print(f"\n\nglobus ls {path}")

    entries = gt._transfer_client.operation_ls(collection, path=path)["DATA"]

    for e in entries:
        fullpath = path / e["name"]
        print(f"\n-> {fullpath}")
        print(json.dumps(e, indent=4))
        # recurse
        if e["type"] == "dir":
            globus_ls_recursive(gt, collection, fullpath)


async def main():
    """Main function."""
    print("Initializing GlobusTransfer…")
    gt = GlobusTransfer()  # uses env vars for auth
    collection = gt._env.GLOBUS_SOURCE_COLLECTION_ID

    print(f"Listing contents for {collection=}…")

    globus_ls_recursive(gt, collection, Path("/"))

    print("\nOK — Python SDK authentication is working.")


if __name__ == "__main__":
    asyncio.run(main())
    print("Done.")
