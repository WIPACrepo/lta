# deleter.py
"""Module to implement the Deleter component of the Long Term Archive."""

import asyncio
import logging
import os
import sys
from typing import Any, Dict, Optional

from rest_tools.client import RestClient
from rest_tools.server import from_environment
import wipac_telemetry.tracing_tools as wtt

from .component import COMMON_CONFIG, Component, now, status_loop, work_loop
from .log_format import StructuredFormatter
from .lta_types import BundleType

Logger = logging.Logger

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

    @wtt.spanned()
    async def _do_work(self) -> None:
        """Perform a work cycle for this component."""
        self.logger.info("Starting work on Bundles.")
        work_claimed = True
        while work_claimed:
            work_claimed = await self._do_work_claim()
            work_claimed &= not self.run_once_and_die
        self.logger.info("Ending work on Bundles.")

    @wtt.spanned()
    async def _do_work_claim(self) -> bool:
        """Claim a bundle and perform work on it."""
        # 1. Ask the LTA DB for the next Bundle to be deleted
        # configure a RestClient to talk to the LTA DB
        lta_rc = RestClient(self.lta_rest_url,
                            token=self.lta_rest_token,
                            timeout=self.work_timeout_seconds,
                            retries=self.work_retries)
        self.logger.info("Asking the LTA DB for a Bundle to delete.")
        pop_body = {
            "claimant": f"{self.name}-{self.instance_uuid}"
        }
        response = await lta_rc.request('POST', f'/Bundles/actions/pop?source={self.source_site}&dest={self.dest_site}&status={self.input_status}', pop_body)
        self.logger.info(f"LTA DB responded with: {response}")
        bundle = response["bundle"]
        if not bundle:
            self.logger.info("LTA DB did not provide a Bundle to delete. Going on vacation.")
            return False
        # process the Bundle that we were given
        try:
            await self._delete_bundle(lta_rc, bundle)
        except Exception as e:
            await self._quarantine_bundle(lta_rc, bundle, f"{e}")
            raise e
        # if we were successful at processing work, let the caller know
        return True

    @wtt.spanned()
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

    @wtt.spanned()
    async def _quarantine_bundle(self,
                                 lta_rc: RestClient,
                                 bundle: BundleType,
                                 reason: str) -> None:
        """Quarantine the supplied bundle using the supplied reason."""
        self.logger.error(f'Sending Bundle {bundle["uuid"]} to quarantine: {reason}.')
        right_now = now()
        patch_body = {
            "status": "quarantined",
            "reason": f"BY:{self.name}-{self.instance_uuid} REASON:{reason}",
            "work_priority_timestamp": right_now,
        }
        try:
            await lta_rc.request('PATCH', f'/Bundles/{bundle["uuid"]}', patch_body)
        except Exception as e:
            self.logger.error(f'Unable to quarantine Bundle {bundle["uuid"]}: {e}.')


def runner() -> None:
    """Configure a Deleter component from the environment and set it running."""
    # obtain our configuration from the environment
    config = from_environment(EXPECTED_CONFIG)
    # configure structured logging for the application
    structured_formatter = StructuredFormatter(
        component_type='Deleter',
        component_name=config["COMPONENT_NAME"],  # type: ignore[arg-type]
        ndjson=True)
    stream_handler = logging.StreamHandler(sys.stdout)
    stream_handler.setFormatter(structured_formatter)
    root_logger = logging.getLogger(None)
    root_logger.setLevel(logging.NOTSET)
    root_logger.addHandler(stream_handler)
    logger = logging.getLogger("lta.deleter")
    # create our Deleter service
    deleter = Deleter(config, logger)  # type: ignore[arg-type]
    # let's get to work
    deleter.logger.info("Adding tasks to asyncio loop")
    loop = asyncio.get_event_loop()
    loop.create_task(status_loop(deleter))
    loop.create_task(work_loop(deleter))


def main() -> None:
    """Configure a Deleter component from the environment and set it running."""
    runner()
    asyncio.get_event_loop().run_forever()


if __name__ == "__main__":
    main()
