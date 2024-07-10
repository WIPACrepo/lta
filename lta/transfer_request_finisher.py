# transfer_request_finisher.py
"""Module to implement the TransferRequestFinisher component of the Long Term Archive."""

import asyncio
import logging
import sys
from typing import Any, Dict, Optional, Union

from prometheus_client import Counter, Gauge, start_http_server
from rest_tools.client import RestClient
import wipac_telemetry.tracing_tools as wtt

from .component import COMMON_CONFIG, Component, now, work_loop
from .lta_tools import from_environment
from .lta_types import BundleType

Logger = logging.Logger

LOG = logging.getLogger(__name__)

EXPECTED_CONFIG = COMMON_CONFIG.copy()
EXPECTED_CONFIG.update({
    "WORK_RETRIES": "3",
    "WORK_TIMEOUT_SECONDS": "30",
})

# prometheus metrics
failure_counter = Counter('lta_failures', 'lta processing failures', ['component', 'level', 'type'])
load_gauge = Gauge('lta_load_level', 'lta work processed', ['component', 'level', 'type'])
success_counter = Counter('lta_successes', 'lta processing successes', ['component', 'level', 'type'])


class TransferRequestFinisher(Component):
    """
    TransferRequestFinisher is a Long Term Archive component.

    A TransferRequestFinisher is responsible for moving a TransferRequest to
    status 'completed' once all of its constituent bundles have been fully
    processed. It looks for bundles in the 'deleted' status and checks to
    ensure that all bundles associated with the request also share this status.
    If they do, then the LTA DB is updated. The TransferRequest is moved to
    status 'completed' and the bundles are moved to status 'finished'.
    """

    def __init__(self, config: Dict[str, str], logger: Logger) -> None:
        """
        Create a TransferRequestFinisher component.

        config - A dictionary of required configuration values.
        logger - The object the transfer_request_finisher should use for logging.
        """
        super(TransferRequestFinisher, self).__init__("transfer_request_finisher", config, logger)
        self.work_retries = int(config["WORK_RETRIES"])
        self.work_timeout_seconds = float(config["WORK_TIMEOUT_SECONDS"])

    def _do_status(self) -> Dict[str, Any]:
        """Provide no additional status."""
        return {}

    def _expected_config(self) -> Dict[str, Optional[str]]:
        """Provide expected configuration dictionary."""
        return EXPECTED_CONFIG

    @wtt.spanned()
    async def _do_work(self, lta_rc: RestClient) -> None:
        """Perform a work cycle for this component."""
        self.logger.info("Starting work on Bundles.")
        load_level = -1
        work_claimed = True
        while work_claimed:
            load_level += 1
            work_claimed = await self._do_work_claim(lta_rc)
            # if we are configured to run once and die, then die
            if self.run_once_and_die:
                sys.exit()
        load_gauge.labels(component='transfer_request_finisher', level='bundle', type='work').set(load_level)
        self.logger.info("Ending work on Bundles.")

    @wtt.spanned()
    async def _do_work_claim(self, lta_rc: RestClient) -> bool:
        """Claim a bundle and perform work on it."""
        # 1. Ask the LTA DB for the next Bundle to be deleted
        self.logger.info("Asking the LTA DB for a Bundle to check for TransferRequest being finished.")
        pop_body = {
            "claimant": f"{self.name}-{self.instance_uuid}"
        }
        response = await lta_rc.request('POST', f'/Bundles/actions/pop?source={self.source_site}&dest={self.dest_site}&status={self.input_status}', pop_body)
        self.logger.info(f"LTA DB responded with: {response}")
        bundle = response["bundle"]
        if not bundle:
            self.logger.info("LTA DB did not provide a Bundle to check. Going on vacation.")
            return False
        # update the TransferRequest that spawned the Bundle, if necessary
        await self._update_transfer_request(lta_rc, bundle)
        # even if we processed a Bundle, take a break between Bundles
        return False

    @wtt.spanned()
    async def _update_transfer_request(self, lta_rc: RestClient, bundle: BundleType) -> None:
        """
        Update the TransferRequest that spawned the Bundle.

        If all of the Bundles created by the TransferRequest are now status
        "deleted" or "finished", then mark the TransferRequest as status "completed".
        """
        # look up the TransferRequest associated with the bundle
        request_uuid = bundle["request"]
        self.logger.info(f"Querying status of all bundles for TransferRequest {request_uuid}")
        response = await lta_rc.request('GET', f'/Bundles?request={request_uuid}')
        results = response["results"]
        deleted_count = len(results)
        self.logger.info(f"Found {deleted_count} bundles for TransferRequest {request_uuid}")
        # check each constituent bundle for "deleted" or "finished" status
        for bundle_uuid in results:
            result = await lta_rc.request('GET', f'/Bundles/{bundle_uuid}')
            self.logger.info(f"Bundle {result['uuid']} has status {result['status']}")
            if (result["status"] == "deleted") or (result["status"] == "finished"):
                deleted_count = deleted_count - 1
            else:
                self.logger.info(f'{result["status"]} is not "deleted" or "finished"; TransferRequest {request_uuid} will not be updated.')
        # if there are some bundles that have not reached "deleted" or "finished" status
        if deleted_count > 0:
            self.logger.info(f'TransferRequest {request_uuid} has {deleted_count} Bundles still waiting for status "deleted" or "finished"')
            # put the bundle at the back of the line to be checked later
            bundle_id = bundle["uuid"]
            right_now = now()
            patch_body: Dict[str, Union[bool, str]] = {
                "claimed": False,
                "update_timestamp": right_now,
                "work_priority_timestamp": right_now,
            }
            self.logger.info(f"PATCH /Bundles/{bundle_id} - '{patch_body}'")
            await lta_rc.request('PATCH', f'/Bundles/{bundle_id}', patch_body)
            return
        # otherwise, we're ready to complete the TransferRequest
        self.logger.info(f"Updating TransferRequest {request_uuid} to mark as completed.")
        right_now = now()
        patch_body = {
            "claimant": f"{self.name}-{self.instance_uuid}",
            "claimed": False,
            "claim_timestamp": right_now,
            "status": "completed",
            "reason": "",
            "update_timestamp": right_now,
        }
        self.logger.info(f"PATCH /TransferRequests/{request_uuid} - '{patch_body}'")
        await lta_rc.request('PATCH', f'/TransferRequests/{request_uuid}', patch_body)
        success_counter.labels(component='transfer_request_finisher', level='transfer_request', type='work').inc()
        # update each of the constituent bundles to status "finished"
        for bundle_id in results:
            patch_body = {
                "claimant": f"{self.name}-{self.instance_uuid}",
                "claimed": False,
                "claim_timestamp": right_now,
                "status": self.output_status,
                "reason": "",
                "update_timestamp": right_now,
            }
            self.logger.info(f"PATCH /Bundles/{bundle_id} - '{patch_body}'")
            await lta_rc.request('PATCH', f'/Bundles/{bundle_id}', patch_body)
            success_counter.labels(component='transfer_request_finisher', level='bundle', type='work').inc()


async def main(transfer_request_finisher: TransferRequestFinisher) -> None:
    """Execute the work loop of the TransferRequestFinisher component."""
    LOG.info("Starting asynchronous code")
    await work_loop(transfer_request_finisher)
    LOG.info("Ending asynchronous code")


def main_sync() -> None:
    """Configure a TransferRequestFinisher component from the environment and set it running."""
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
    # create our TransferRequestFinisher service
    LOG.info("Starting synchronous code")
    transfer_request_finisher = TransferRequestFinisher(config, LOG)
    # let's get to work
    metrics_port = int(config["PROMETHEUS_METRICS_PORT"])
    start_http_server(metrics_port)
    asyncio.run(main(transfer_request_finisher))
    LOG.info("Ending synchronous code")


if __name__ == "__main__":
    main_sync()
