# deleter.py
"""Module to implement the Deleter component of the Long Term Archive."""

import asyncio
import json
from logging import Logger
import logging
import sys
from typing import Any, Dict, Optional

from rest_tools.client import RestClient  # type: ignore

from .component import COMMON_CONFIG, Component, now, status_loop, work_loop
from .config import from_environment
from .log_format import StructuredFormatter
from .lta_types import BundleType
from .transfer.service import instantiate


EXPECTED_CONFIG = COMMON_CONFIG.copy()
EXPECTED_CONFIG.update({
    "TRANSFER_CONFIG_PATH": "etc/rucio.json",
    "WORK_RETRIES": "3",
    "WORK_TIMEOUT_SECONDS": "30",
})


class Deleter(Component):
    """
    Deleter is a Long Term Archive component.

    A Deleter is responsible for deleteing intermediate copies of archive
    bundles that have finished processing at their destination site(s). The
    archive bundles are marked for deletion using the Rucio transfer service.
    Rucio will then remove the intermediate bundle files from the
    destination site(s).

    It uses the LTA DB to find verified bundles that need to be deleted.
    It de-registers the bundles with Rucio. It updates the Bundle and the
    corresponding TransferRequest in the LTA DB with a 'deleted' status.
    """

    def __init__(self, config: Dict[str, str], logger: Logger) -> None:
        """
        Create a Deleter component.

        config - A dictionary of required configuration values.
        logger - The object the deleter should use for logging.
        """
        super(Deleter, self).__init__("deleter", config, logger)
        with open(config["TRANSFER_CONFIG_PATH"]) as config_data:
            self.transfer_config = json.load(config_data)
        self.work_retries = int(config["WORK_RETRIES"])
        self.work_timeout_seconds = float(config["WORK_TIMEOUT_SECONDS"])
        pass

    def _do_status(self) -> Dict[str, Any]:
        """Provide additional status for the Deleter."""
        return {}

    def _expected_config(self) -> Dict[str, Optional[str]]:
        """Provide expected configuration dictionary."""
        return EXPECTED_CONFIG

    async def _do_work(self) -> None:
        """Perform a work cycle for this component."""
        self.logger.info("Starting work on Bundles.")
        work_claimed = True
        while work_claimed:
            work_claimed = await self._do_work_claim()
            work_claimed &= not self.run_once_and_die
        self.logger.info("Ending work on Bundles.")

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
        response = await lta_rc.request('POST', '/Bundles/actions/pop?source=WIPAC&status=completed', pop_body)
        self.logger.info(f"LTA DB responded with: {response}")
        bundle = response["bundle"]
        if not bundle:
            self.logger.info("LTA DB did not provide a Bundle to delete. Going on vacation.")
            return False
        # process the Bundle that we were given
        await self._delete_bundle(lta_rc, bundle)
        # update the TransferRequest that spawned the Bundle, if necessary
        await self._update_transfer_request(lta_rc, bundle)
        return True

    async def _delete_bundle(self, lta_rc: RestClient, bundle: BundleType) -> None:
        """Delete the provided Bundle with the transfer service and update the LTA DB."""
        bundle_id = bundle["uuid"]
        # instantiate a TransferService to delete the bundle
        xfer_service = instantiate(self.transfer_config)
        # ask the transfer service to cancel (i.e.: delete) the transfer
        await xfer_service.cancel(bundle["transfer_reference"])
        # update the Bundle in the LTA DB
        bundle["status"] = "deleted"
        bundle["update_timestamp"] = now()
        bundle["claimed"] = False
        self.logger.info(f"PATCH /Bundles/{bundle_id} - '{bundle}'")
        await lta_rc.request('PATCH', f'/Bundles/{bundle_id}', bundle)

    async def _update_transfer_request(self, lta_rc: RestClient, bundle: BundleType) -> None:
        """
        Update the TransferRequest that spawned the Bundle.

        If all of the Bundles created by the TransferRequest are now status
        "deleted", then mark the TransferRequest as status "completed".
        """
        request_uuid = bundle["request"]
        self.logger.info(f"Querying status of all bundles for TransferRequest {request_uuid}")
        response = await lta_rc.request('GET', f'/Bundles?request={request_uuid}')
        results = response["results"]
        deleted_count = len(results)
        self.logger.info(f"Found {deleted_count} bundles for TransferRequest {request_uuid}")
        for result in results:
            self.logger.info(f"Bundle {result['uuid']} has status {result['status']}")
            if result["status"] == "deleted":
                deleted_count = deleted_count - 1
            else:
                self.logger.info(f'{result["status"]} is not "deleted"; TransferRequest {request_uuid} will not be updated.')
        if deleted_count > 0:
            self.logger.info(f'TransferRequest {request_uuid} has {deleted_count} Bundles still waiting for status "deleted"')
            return
        # update the TransferRequest in the LTA DB
        self.logger.info(f"Updating TransferRequest {request_uuid} to mark as completed.")
        tr = await lta_rc.request('GET', f'/TransferRequest/{request_uuid}')
        right_now = now()
        tr["status"] = "completed"
        tr["update_timestamp"] = right_now
        tr["claimed"] = False
        tr["claimant"] = f"{self.name}-{self.instance_uuid}"
        tr["claim_timestamp"] = right_now
        self.logger.info(f"PATCH /TransferRequest/{request_uuid} - '{tr}'")
        await lta_rc.request('PATCH', f'/TransferRequest/{request_uuid}', tr)


def runner() -> None:
    """Configure a Deleter component from the environment and set it running."""
    # obtain our configuration from the environment
    config = from_environment(EXPECTED_CONFIG)
    # configure structured logging for the application
    structured_formatter = StructuredFormatter(
        component_type='Deleter',
        component_name=config["COMPONENT_NAME"],
        ndjson=True)
    stream_handler = logging.StreamHandler(sys.stdout)
    stream_handler.setFormatter(structured_formatter)
    root_logger = logging.getLogger(None)
    root_logger.setLevel(logging.NOTSET)
    root_logger.addHandler(stream_handler)
    logger = logging.getLogger("lta.deleter")
    # create our Deleter service
    deleter = Deleter(config, logger)
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
