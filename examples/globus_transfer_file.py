"""A simple script that does a "globus transfer" using lta's GlobusTransfer."""

import argparse
import asyncio
import logging
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
    args = parser.parse_args()

    gt = GlobusTransfer()
    await gt.transfer_file(
        source_path=args.src,
        dest_path=args.dest,
    )


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    asyncio.run(main())
    print("Done.")
