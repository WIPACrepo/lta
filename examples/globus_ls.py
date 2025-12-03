"""A simple script that does a 'globus ls' using lta's GlobusTransfer."""

import argparse
import logging
import os
from pathlib import Path
import json
import asyncio

from lta.transfer.globus import GlobusTransfer

TREE_TO_PRINT: list[str] = []  # this is global so we can print even if exceptions raise


def print_tree():
    print(flush=True)
    for ln in TREE_TO_PRINT:
        print(ln, flush=True)


def _add_tree_line(
    name: str,
    is_dir: bool,
    _draw_verticals: list[bool],
    is_last: bool,
) -> None:
    """Construct an entry's line for TREE_TO_PRINT."""

    # Build prefix using _draw_verticals
    prefix = ""
    for do_it in _draw_verticals:
        prefix += "│   " if do_it else "    "

    connector = "└── " if is_last else "├── "

    ending = "/" if is_dir else ""

    TREE_TO_PRINT.append(f"{prefix}{connector}{name}{ending}")


def _globus_ls(
    gt: GlobusTransfer,
    collection: str,
    path: Path,
    do_recursive: bool,
    max_depth: int | None,
    show_fullpath: bool,
    _depth: int = 0,
    _draw_verticals: list[bool] | None = None,
):
    if _draw_verticals is None:
        _draw_verticals = []

    # too deep?
    if (max_depth is not None) and (_depth > max_depth):
        return

    print(f"\n\nglobus ls {path}")

    if _depth == 0:
        TREE_TO_PRINT.append(str(path))

    # call api
    entries_sorted = sorted(
        gt._transfer_client.operation_ls(collection, path=str(path))["DATA"],
        key=lambda e: (e["type"] != "dir", e["name"]),
    )

    # traverse
    for idx, e in enumerate(entries_sorted):
        fullpath = path / e["name"]
        print(json.dumps(e, indent=4))

        is_last = bool(idx == len(entries_sorted) - 1)
        _add_tree_line(
            fullpath if show_fullpath else e["name"],
            e["type"] == "dir",
            _draw_verticals,
            is_last,
        )

        # recurse
        if do_recursive and e["type"] == "dir":
            _globus_ls(
                gt,
                collection,
                fullpath,
                do_recursive,
                max_depth,
                show_fullpath,
                _depth + 1,
                _draw_verticals + [not is_last],  # NEW: track sibling state
            )


def globus_ls(
    root: Path,
    do_recursive: bool,
    max_depth: int | None,
    show_fullpath: bool,
) -> None:
    """Do the ls."""
    print("Initializing GlobusTransfer...")

    gt = GlobusTransfer()  # uses env vars for auth
    collection = gt._env.GLOBUS_SOURCE_COLLECTION_ID

    print(f"Listing contents for {collection=}…")

    _globus_ls(gt, collection, root, do_recursive, max_depth, show_fullpath)

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
    parser.add_argument(
        "--max-depth",
        type=int,
        default=None,
        help="The maximum depth of recursive 'globus ls'",
    )
    parser.add_argument(
        "-f",
        "--show-fullpath",
        action="store_true",
        help="Show full path",
    )
    args = parser.parse_args()

    if (not args.recursive) and (args.max_depth is not None):
        raise ValueError("when not using -r (--recursive), --max-depth cannot be used")

    # override if given
    if args.collection:
        os.environ["GLOBUS_SOURCE_COLLECTION_ID"] = args.collection

    # mock out the otherwise required env vars — we aren't using this
    os.environ["GLOBUS_DEST_COLLECTION_ID"] = "n/a"

    globus_ls(args.root, args.recursive, args.max_depth, args.show_fullpath)


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)

    try:
        asyncio.run(main())
        print_tree()
    except BaseException as e:
        logging.exception(e)
        print_tree()
        print(f"\n< Interrupted by {type(e).__name__} — see above >")
    else:
        print("Done.")
