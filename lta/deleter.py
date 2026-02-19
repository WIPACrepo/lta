# deleter.py
"""Module to implement the Deleter component of the Long Term Archive."""

# fmt:off

import asyncio
import logging
import os
import sys
from typing import Any, Dict, Optional

from prometheus_client import start_http_server
from rest_tools.client import RestClient

from .component import COMMON_CONFIG, Component, work_loop
from .utils import now
from .lta_tools import from_environment
from .lta_types import BundleType

Logger = logging.Logger

LOG = logging.getLogger(__name__)

EXPECTED_CONFIG = COMMON_CONFIG.copy()
EXPECTED_CONFIG.update({
    "DISK_BASE_PATH": None,
    "WORK_RETRIES": "3",
    "WORK_TIMEOUT_SECONDS": "30",
})


class Deleter(Component):
    """
    Deleter is a Long Term Archive component.

    A Deleter is responsible for removing the physical files from the staging
    area after archival verification has been completed. The transfer service
    is queried for files ready to be deleted. Those files are removed, and
    the bundle is moved to another state.
    """

    def __init__(self, config: Dict[str, str], logger: Logger) -> None:
        """
        Create a Deleter component.

        config - A dictionary of required configuration values.
        logger - The object the deleter should use for logging.
        """
        super(Deleter, self).__init__("deleter", config, logger)
        self.disk_base_path = config["DISK_BASE_PATH"]
        self.work_retries = int(config["WORK_RETRIES"])
        self.work_timeout_seconds = float(config["WORK_TIMEOUT_SECONDS"])

    def _do_status(self) -> Dict[str, Any]:
        """Contribute no additional status."""
        return {}

    def _expected_config(self) -> Dict[str, Optional[str]]:
        """Provide expected configuration dictionary."""
        return EXPECTED_CONFIG

    async def _do_work_claim(self, lta_rc: RestClient) -> bool:
        """Claim a bundle and perform work on it -- see super for return value meanings."""
        # 1. Ask the LTA DB for the next Bundle to be deleted
        self.logger.info("Asking the LTA DB for a Bundle to delete.")
        pop_body = {
            "claimant": f"{self.name}-{self.instance_uuid}"
        }
        response = await lta_rc.request('POST', f'/Bundles/actions/pop?source={self.source_site}&dest={self.dest_site}&status={self.input_status}', pop_body)
        self.logger.info(f"LTA DB responded with: {response}")
        bundle = response["bundle"]
        if not bundle:
            self.logger.info("LTA DB did not provide a Bundle to delete. Going on vacation.")
            return DoWorkClaimResult.NothingClaimed("PAUSE")
        # process the Bundle that we were given
        try:
            await self._delete_bundle(lta_rc, bundle)
            return DoWorkClaimResult.Successful("CONTINUE")
        except Exception as e:
            return DoWorkClaimResult.QuarantineNow("PAUSE", bundle, "BUNDLE", e)

    async def _delete_bundle(self, lta_rc: RestClient, bundle: BundleType) -> bool:
        """Delete the provided Bundle and update the LTA DB."""
        # determine the name of the file to be deleted
        bundle_id = bundle["uuid"]
        bundle_name = os.path.basename(bundle["bundle_path"])
        bundle_path = os.path.join(self.disk_base_path, bundle_name)
        # delete the file from the disk
        self.logger.info(f"Removing file {bundle_path} from the disk.")
        os.remove(bundle_path)
        # update the Bundle in the LTA DB
        self.logger.info(f"File {bundle_path} was deleted from the disk.")
        patch_body = {
            "status": self.output_status,
            "reason": "",
            "update_timestamp": now(),
            "claimed": False,
        }
        self.logger.info(f"PATCH /Bundles/{bundle_id} - '{patch_body}'")
        await lta_rc.request('PATCH', f'/Bundles/{bundle_id}', patch_body)
        return True


async def main(deleter: Deleter) -> None:
    """Execute the work loop of the Deleter component."""
    LOG.info("Starting asynchronous code")
    await work_loop(deleter)
    LOG.info("Ending asynchronous code")


def main_sync() -> None:
    """Configure a Deleter component from the environment and set it running."""
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
    # create our Deleter service
    LOG.info("Starting synchronous code")
    deleter = Deleter(config, LOG)
    # let's get to work
    metrics_port = int(config["PROMETHEUS_METRICS_PORT"])
    start_http_server(metrics_port)
    asyncio.run(main(deleter))
    LOG.info("Ending synchronous code")


if __name__ == "__main__":
    main_sync()
