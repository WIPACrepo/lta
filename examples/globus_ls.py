"""A simple script that does a 'globus ls' using lta's GlobusTransfer."""

import argparse
import copy
import logging
import os
from pathlib import Path
import json
import asyncio
from datetime import datetime

from lta.transfer.globus import GlobusTransfer

TREE_TO_PRINT: list[str] = []  # this is global so we can print even if exceptions raise


def print_tree():
    """Print the tree: TREE_TO_PRINT."""
    to_print = copy.deepcopy(TREE_TO_PRINT)

    # this will shave each metadata section so all have the same left-padding
    #   in:  '├── [  12.0B Dec  1 02:10]'
    #   out: '├── [12.0B Dec  1 02:10]'
    while all(("─ [ " in x) for x in to_print[1:]):  # note: first line is root path
        to_print = [x.replace("─ [ ", "─ [") for x in to_print]

    print(flush=True)
    for ln in to_print:
        print(ln, flush=True)


def _fmt_meta(e: dict) -> str:
    """Return metadata like: [4.5K Dec  3 16:05]."""

    # human-readable byte size
    def _hr(n: float) -> str:
        for u in ("B", "K", "M", "G", "T", "P"):
            if n < 1024:
                out = f"{n:6.1f}{u}"
                return out.rstrip("0").rstrip(".")
            n /= 1024
        out = f"{n:.1f}E"
        return out.rstrip("0").rstrip(".")

    ts = datetime.fromisoformat(e["last_modified"]).strftime("%b %e %H:%M")
    return f"[{_hr(e['size'])} {ts}]"


def _add_tree_line(
    entry: dict,
    name_override: str | None,
    _draw_verticals: list[bool],
    is_last: bool,
) -> None:
    """Construct an entry's line for TREE_TO_PRINT."""

    # Build prefix using _draw_verticals
    prefix = ""
    for do_it in _draw_verticals:
        prefix += "│   " if do_it else "    "

    connector = "└──" if is_last else "├──"

    meta = _fmt_meta(entry)

    name = name_override or entry["name"]

    ending = "/" if entry["type"] == "dir" else ""

    TREE_TO_PRINT.append(f"{prefix}{connector} {meta}  {name}{ending}")


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
            e,
            fullpath if show_fullpath else None,
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
        "-d",
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
