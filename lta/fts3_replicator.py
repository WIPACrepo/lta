# fts3_replicator.py
"""Module to implement the FTS3Replicator component of the Long Term Archive."""

import asyncio
import json
from logging import Logger
import logging
import sys
from typing import Any, Dict, Optional

import fts3.rest.client.easy as fts3  # type: ignore
from rest_tools.client import RestClient  # type: ignore
from rest_tools.server import from_environment  # type: ignore

from .component import COMMON_CONFIG, Component, now, status_loop, work_loop
from .log_format import StructuredFormatter
from .lta_types import BundleType


EXPECTED_CONFIG = COMMON_CONFIG.copy()
EXPECTED_CONFIG.update({
    "FTS_ENDPOINT_URL": None,
    "WORK_RETRIES": "3",
    "WORK_TIMEOUT_SECONDS": "30",
    "X509_USER_PROXY": None,  # used by the FTS library for authentication
})


class FTS3Replicator(Component):
    """
    FTS3Replicator is a Long Term Archive component.

    A FTS3Replicator is responsible for registering completed archive bundles
    with the FTS transfer service. FTS will then replicate the bundle from the
    source (i.e.: WIPAC Data Warehouse) to the destination(s),
    (i.e.: DESY, NERSC DTN).

    It uses the LTA DB to find completed bundles that need to be registered.
    It registers the bundles with FTS. It updates the Bundle and the
    corresponding TransferRequest in the LTA DB with a 'transferring' status.
    """

    def __init__(self, config: Dict[str, str], logger: Logger) -> None:
        """
        Create a FTS3Replicator component.

        config - A dictionary of required configuration values.
        logger - The object the fts3_replicator should use for logging.
        """
        super(FTS3Replicator, self).__init__("fts3_replicator", config, logger)
        self.endpoint = config["FTS_ENDPOINT_URL"]
        self.work_retries = int(config["WORK_RETRIES"])
        self.work_timeout_seconds = float(config["WORK_TIMEOUT_SECONDS"])
        self.x509_user_proxy = config["X509_USER_PROXY"]

    def _do_status(self) -> Dict[str, Any]:
        """FTS3Replicator has no additional status to contribute."""
        return {}

    def _expected_config(self) -> Dict[str, Optional[str]]:
        """FTS3Replicator provides our expected configuration dictionary."""
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
        # 1. Ask the LTA DB for the next Bundle to be transferred
        # configure a RestClient to talk to the LTA DB
        lta_rc = RestClient(self.lta_rest_url,
                            token=self.lta_rest_token,
                            timeout=self.work_timeout_seconds,
                            retries=self.work_retries)
        self.logger.info("Asking the LTA DB for a Bundle to transfer.")
        source = self.source_site
        pop_body = {
            "claimant": f"{self.name}-{self.instance_uuid}"
        }
        response = await lta_rc.request('POST', f'/Bundles/actions/pop?source={source}&status=staged', pop_body)
        self.logger.info(f"LTA DB responded with: {response}")
        bundle = response["bundle"]
        if not bundle:
            self.logger.info("LTA DB did not provide a Bundle to transfer. Going on vacation.")
            return False
        # process the Bundle that we were given
        try:
            await self._replicate_bundle_to_destination_site(lta_rc, bundle)
        except Exception as e:
            await self._quarantine_bundle(lta_rc, bundle, f"{e}")
            return False
        # if we were successful at processing work, let the caller know
        return True

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

    async def _replicate_bundle_to_destination_site(self, lta_rc: RestClient, bundle: BundleType) -> None:
        """Replicate the supplied bundle using the FTS transfer service."""
        bundle_id = bundle["uuid"]
        # establish a Context for communicating with FTS3
        context = fts3.Context(self.endpoint, verify=True)
        self.logger.info(f"FTS3 Endpoint Info: {context.get_endpoint_info()}")
        self.logger.info(f"FTS3 whoami: {fts3.whoami(context)}")
        # log some stuff about the FTS instance
        jobs_json = fts3.list_jobs(context)
        jobs = json.load(jobs_json)
        self.logger.debug(f"There are {len(jobs)} at FTS3 {self.endpoint}")
        self.logger.debug(f"{jobs_json}")
        # construct the transfer object
        source = ""
        destination = ""
        checksum = f"sha512:{bundle['checksum']['sha512']}"
        filesize = bundle['bundle_size']
        metadata = f"Bundle {bundle_id}"
        transfer = fts3.new_transfer(
            source, destination, checksum=checksum,
            filesize=filesize, metadata=metadata)
        # construct the job object
        transfers = [transfer]
        job = fts3.new_job(
            transfers, verify_checksum=True,
            reuse=True, overwrite=True, metadata=metadata)
        # submit the job to FTS
        xfer_ref = fts3.submit(context, job)
        # update the Bundle in the LTA DB
        patch_body = {
            "status": "transferring",
            "reason": "",
            "update_timestamp": now(),
            "claimed": False,
            "transfer_reference": xfer_ref,
        }
        self.logger.info(f"PATCH /Bundles/{bundle_id} - '{patch_body}'")
        await lta_rc.request('PATCH', f'/Bundles/{bundle_id}', patch_body)

def runner() -> None:
    """Configure a FTS3Replicator component from the environment and set it running."""
    # obtain our configuration from the environment
    config = from_environment(EXPECTED_CONFIG)
    # configure structured logging for the application
    structured_formatter = StructuredFormatter(
        component_type='FTS3Replicator',
        component_name=config["COMPONENT_NAME"],
        ndjson=True)
    stream_handler = logging.StreamHandler(sys.stdout)
    stream_handler.setFormatter(structured_formatter)
    root_logger = logging.getLogger(None)
    root_logger.setLevel(logging.NOTSET)
    root_logger.addHandler(stream_handler)
    logger = logging.getLogger("lta.replicator")
    # create our FTS3Replicator service
    replicator = FTS3Replicator(config, logger)
    # let's get to work
    replicator.logger.info("Adding tasks to asyncio loop")
    loop = asyncio.get_event_loop()
    loop.create_task(status_loop(replicator))
    loop.create_task(work_loop(replicator))

def main() -> None:
    """Configure a FTS3Replicator component from the environment and set it running."""
    runner()
    asyncio.get_event_loop().run_forever()

if __name__ == "__main__":
    main()
