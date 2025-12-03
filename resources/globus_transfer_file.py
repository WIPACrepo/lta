"""A simple script that does a "globus transfer" using lta's GlobusTransfer."""

import argparse
import asyncio
import json
import logging
import os
from pathlib import Path


from lta.transfer.globus import GlobusTransfer


async def main():
    """Main function."""
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--src",
        type=Path,
        required=True,
        help="File source path",
    )
    parser.add_argument(
        "--dest",
        type=Path,
        required=True,
        help="File destination path",
    )
    parser.add_argument(
        "--confirm",
        action="store_true",
        default=False,
        help="Wait for transfer confirmation",
    )
    args = parser.parse_args()

    # setup
    os.environ["GLOBUS_POLL_INTERVAL_SECONDS"] = str(5)
    gt = GlobusTransfer()

    # transfer
    task_id = await gt.transfer_file(
        source_path=args.src,
        dest_path=args.dest,
    )
    print(f"Task id: {task_id}")

    # wait for transfer confirmation
    if args.confirm:
        res = await gt.wait_for_transfer_to_finish(task_id)
        print(json.dumps(res, indent=4))


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    asyncio.run(main())
    print("Done.")
