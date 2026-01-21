# globus_replicator.py
"""Module to implement the GlobusReplicator component of the Long Term Archive."""

import asyncio
import logging
import sys
import uuid
from pathlib import Path
from typing import Any, Dict, Optional

import globus_sdk
from prometheus_client import Counter, Gauge, start_http_server  # type: ignore[import-not-found]
from rest_tools.client import RestClient
from wipac_dev_tools import strtobool

from lta.utils import patch_bundle, quarantine_bundle
from .component import COMMON_CONFIG, Component, now, work_loop
from .lta_tools import from_environment
from .lta_types import BundleType
from .transfer.globus import GlobusTransfer

# fmt:off

Logger = logging.Logger

LOG = logging.getLogger(__name__)

EXPECTED_CONFIG = COMMON_CONFIG.copy()
EXPECTED_CONFIG.update({
    "USE_FULL_BUNDLE_PATH": "FALSE",
    "WORK_RETRIES": "3",
    "WORK_TIMEOUT_SECONDS": "30",
    "GLOBUS_REPLICATOR_DEST_DIRPATH": None,  # required
    "GLOBUS_REPLICATOR_SOURCE_BIND_ROOTPATH": None,  # required
})


class TransferReferenceToolkit:
    """A couple of tools for interacting with LTA transfer_reference."""

    PREFIX = "globus/"

    @staticmethod
    def to_transfer_reference(task_id: uuid.UUID | str) -> str:
        """Convert Globus task_id to LTA transfer_reference."""
        return f"{TransferReferenceToolkit.PREFIX}{task_id}"

    @staticmethod
    def to_task_id(bundle: dict) -> str | None:
        """Convert LTA bundle (w/ transfer_reference) to Globus task_id."""
        transfer_reference = bundle.get("transfer_reference", None)

        if transfer_reference:
            return transfer_reference.removeprefix(TransferReferenceToolkit.PREFIX)
        else:
            return None


class GlobusReplicator(Component):
    """
    GlobusReplicator is a Long Term Archive component.

    A GlobusReplicator is responsible for copying the completed archive
    bundles from a source location to a Globus destination. Globus will
    then replicate the bundle from the source (i.e.: WIPAC Data Warehouse)
    to the destination(s), (i.e.: DESY, NERSC DTN).

    It uses the LTA DB to find completed bundles that need to be replicated.
    It issues a Globus transfer command. It updates the Bundle and the
    corresponding TransferRequest in the LTA DB with a 'transferring' status.
    """

    def __init__(self, config: Dict[str, str], logger: Logger) -> None:
        """
        Create a GlobusReplicator component.

        config - A dictionary of required configuration values.
        logger - The object the replicator should use for logging.
        """
        super().__init__("replicator", config, logger)
        self.use_full_bundle_path = strtobool(config["USE_FULL_BUNDLE_PATH"])
        self.work_retries = int(config["WORK_RETRIES"])
        self.work_timeout_seconds = float(config["WORK_TIMEOUT_SECONDS"])
        self.globus_replicator_dest_dirpath = Path(config["GLOBUS_REPLICATOR_DEST_DIRPATH"])
        self.globus_replicator_source_bind_rootpath = Path(config["GLOBUS_REPLICATOR_SOURCE_BIND_ROOTPATH"])

        self.globus_transfer = GlobusTransfer()

        # prometheus metrics
        self.failure_counter = Counter(
            "lta_failures",
            "lta processing failures",
            ["component", "level", "type"],
        )
        self.load_gauge = Gauge(
            "lta_load_level",
            "lta work processed",
            ["component", "level", "type"],
        )
        self.success_counter = Counter(
            "lta_successes",
            "lta processing successes",
            ["component", "level", "type"],
        )

    def _do_status(self) -> Dict[str, Any]:
        """GlobusReplicator has no additional status to contribute."""
        return {}

    def _expected_config(self) -> Dict[str, Optional[str]]:
        """GlobusReplicator provides our expected configuration dictionary."""
        return EXPECTED_CONFIG

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
        self.load_gauge.labels(component='globus_replicator', level='bundle', type='work').set(load_level)
        self.logger.info("Ending work on Bundles.")

    async def _do_work_claim(self, lta_rc: RestClient) -> bool:
        """Claim a bundle and perform work on it."""
        # 1. Ask the LTA DB for the next Bundle to be transferred
        self.logger.info("Asking the LTA DB for a Bundle to transfer.")
        pop_body = {
            "claimant": f"{self.name}-{self.instance_uuid}"
        }
        response = await lta_rc.request('POST', f'/Bundles/actions/pop?source={self.source_site}&dest={self.dest_site}&status={self.input_status}', pop_body)
        self.logger.info(f"LTA DB responded with: {response}")
        bundle = response["bundle"]
        if not bundle:
            self.logger.info("LTA DB did not provide a Bundle to transfer. Going on vacation.")
            return False
        # process the Bundle that we were given
        try:
            await self._replicate_bundle_to_destination_site(lta_rc, bundle)
            self.success_counter.labels(component='globus_replicator', level='bundle', type='work').inc()
        except Exception as e:
            self.logger.error(f'Globus transfer threw an error: {e}')
            self.logger.exception(e)
            self.failure_counter.labels(component='globus_replicator', level='bundle', type='exception').inc()
            await quarantine_bundle(
                lta_rc,
                bundle,
                e,
                self.name,
                self.instance_uuid,
                self.logger,
            )
            return False
        # if we were successful at processing work, let the caller know
        return True

    def _extract_paths(self, bundle: BundleType) -> tuple[Path, Path]:
        """Get the source and destination paths for the supplied bundle."""
        rel = Path(bundle["bundle_path"]).relative_to(self.globus_replicator_source_bind_rootpath)
        source_path = Path("/") / rel  # the path on the destination globus collection

        # destination logic
        # -- start with basename of /mnt/lfss/jade-lta/bundler_out/fdd3c3865d1011eb97bb6224ddddaab7.zip
        bundle_name = Path(bundle["bundle_path"]).name
        if self.use_full_bundle_path:
            # /data/exp/IceCube/2015/filtered/level2/0320 + BUNDLE_NAME
            dest_path = self.globus_replicator_dest_dirpath / bundle["path"].lstrip('/') / bundle_name
        else:
            # BUNDLE_NAME
            dest_path = self.globus_replicator_dest_dirpath / bundle_name

        return source_path, dest_path


    async def _replicate_bundle_to_destination_site(self, lta_rc: RestClient, bundle: BundleType) -> None:
        """Replicate the supplied bundle using the configured transfer service."""
        # get our ducks in a row
        bundle_id = bundle["uuid"]
        source_path, dest_path = self._extract_paths(bundle)
        inflight_dup_origin_task_id = None
        task_id: uuid.UUID | str | None = None
        extra_updates = {}

        # Transfer the bundle
        self.logger.info(f'Sending {source_path} to {dest_path}')
        try:
            task_id = await self.globus_transfer.transfer_file(
                source_path=source_path,
                dest_path=dest_path,
            )
            self.logger.info(f'Initiated transfer {source_path} to {dest_path}')
        # ERROR -> globus possibly caught this inflight duplicate transfer
        except globus_sdk.TransferAPIError as e:
            if "A transfer with identical paths has not yet completed" in str(e):
                # Check if there is an task_id/transfer_reference in the LTA bundle obj.
                #   This is our best-effort at recovering the task_id since
                #   globus didn't give it to us in the error message.
                inflight_dup_origin_task_id = TransferReferenceToolkit.to_task_id(bundle)
                self.logger.warning("OK: globus caught inflight duplicate")
            else:
                raise
        # No error -> record task_id in the LTA DB
        else:
            await patch_bundle(
                lta_rc,
                bundle_id,
                {
                    "transfer_dest_path": str(dest_path),
                    "final_dest_path": str(dest_path),
                    "update_timestamp": now(),
                    "transfer_reference": TransferReferenceToolkit.to_transfer_reference(task_id),
                },
                self.logger,
            )

        # Wait for transfer to finish
        if task_id:
            await self.globus_transfer.wait_for_transfer_to_finish(task_id)
        elif inflight_dup_origin_task_id:
            await self.globus_transfer.wait_for_transfer_to_finish(inflight_dup_origin_task_id)
        else:
            # Since we cannot track the bundle transfer, reset 'work_priority_timestamp'.
            #    This way, the Site Move Verifier component will check this bundle
            #    relatively later than it would if we normally just unclaimed+advanced.
            extra_updates = {"work_priority_timestamp": now()}
            self.logger.info("OK: cannot track transfer, assuming it finished")

        # Unclaim and Advance -- update the Bundle in the LTA DB
        await patch_bundle(
            lta_rc,
            bundle_id,
            {
                "status": self.output_status,
                "reason": "",
                "update_timestamp": now(),
                "claimed": False,
                **extra_updates,
            },
            self.logger,
        )


async def main(globus_replicator: GlobusReplicator) -> None:
    """Execute the work loop of the GlobusReplicator component."""
    LOG.info("Starting asynchronous code")
    await work_loop(globus_replicator)
    LOG.info("Ending asynchronous code")


def main_sync() -> None:
    """Configure a GlobusReplicator component from the environment and set it running."""
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
    # create our GlobusReplicator service
    LOG.info("Starting synchronous code")
    globus_replicator = GlobusReplicator(config, LOG)
    # let's get to work
    metrics_port = int(config["PROMETHEUS_METRICS_PORT"])
    start_http_server(metrics_port)
    asyncio.run(main(globus_replicator))
    LOG.info("Ending synchronous code")


if __name__ == "__main__":
    main_sync()
