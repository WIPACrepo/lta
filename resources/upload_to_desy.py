#!/usr/bin/env python3
# upload_to_desy.py
# Upload a file to DESY using Sync

import asyncio
import logging
import sys

from lta.lta_tools import from_environment
from lta.transfer.sync import Sync


EXPECTED_CONFIG = {
    "CLIENT_ID": "long-term-archive",
    "CLIENT_SECRET": None,
    "DEST_BASE_PATH": "/pnfs/ifh.de/acs/icecube/archive",
    "DEST_URL": "https://globe-door.ifh.de:2880",
    "LOG_LEVEL": "DEBUG",
    "LTA_AUTH_OPENID_URL": "https://keycloak.icecube.wisc.edu/auth/realms/IceCube",
    "MAX_PARALLEL": "100",
    "WORK_RETRIES": "3",
    "WORK_TIMEOUT_SECONDS": "30",
}

LOG = logging.getLogger(__name__)


async def upload_file_to_desy(config: dict[str, str], src_path: str, dest_path: str, timeout: int) -> None:
    """Upload the file to DESY using Sync."""
    # create Sync to transfer to DESY
    sync = Sync(config)
    # upload to DESY
    try:
        LOG.info(f"Replicating {src_path} -> {dest_path}")
        await sync.put_path(src_path, dest_path, timeout)
    except Exception as e:
        LOG.error(f'DESY Sync raised an Exception: {e}')
        raise e
    # all done
    LOG.info("Upload complete.")


async def main(config: dict[str, str], src_path: str, dest_path: str, timeout: int) -> None:
    """Run the async parts of the upload."""
    LOG.info("Starting asynchronous code")
    await upload_file_to_desy(config, src_path, dest_path, timeout)
    LOG.info("Ending asynchronous code")


def main_sync() -> None:
    """Run the sync parts of the upload."""
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
    # create our DesyMirrorReplicator service
    LOG.info("Starting synchronous code")
    if len(sys.argv) < 3:
        print("Usage: resources/upload-to-desy.sh [src_path] [dest_path] <timeout:60>")
        sys.exit(1)
    timeout = 60
    if len(sys.argv) >= 4:
        timeout = int(sys.argv[3])
    src_path = sys.argv[1]
    dest_path = sys.argv[2]
    asyncio.run(main(config, src_path, dest_path, timeout))
    LOG.info("Ending synchronous code")


if __name__ == "__main__":
    main_sync()
