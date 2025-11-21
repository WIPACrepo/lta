# globus.py
"""Tools to help manage Globus proxies."""

import asyncio
import uuid
import datetime
from pathlib import Path
import logging
import os
import dataclasses
from typing import Any

import globus_sdk
from wipac_dev_tools import from_environment_as_dataclass

LOGGER = logging.getLogger(__name__)


@dataclasses.dataclass(frozen=True)
class GlobusTransferEnv:
    """Typed environment configuration for Globus transfers."""

    # Required
    GLOBUS_CLIENT_ID: str
    GLOBUS_CLIENT_SECRET: str
    GLOBUS_SOURCE_COLLECTION_ID: str
    GLOBUS_DEST_COLLECTION_ID: str

    # Optional
    GLOBUS_HARD_DEADLINE_SECONDS: int | None = None
    GLOBUS_TRANSFER_SCOPE: str = "urn:globus:auth:scope:transfer.api.globus.org:all"
    GLOBUS_POLL_INTERVAL_SECONDS: float = 10.0


class GlobusTransferFailedException(Exception):
    """Raised when globus transfer failed."""


class GlobusTransfer:
    """Submit and wait for single-file Globus transfers."""

    def __init__(self) -> None:
        """Load env config and initialize a TransferClient."""
        self._env = from_environment_as_dataclass(GlobusTransferEnv)
        self._transfer_client = self._create_client()

    # ---------------------------
    # Internal Helpers
    # ---------------------------

    def _create_client(self) -> globus_sdk.TransferClient:
        """Create an authenticated TransferClient."""

        # request token
        auth = globus_sdk.ConfidentialAppAuthClient(
            self._env.GLOBUS_CLIENT_ID,
            self._env.GLOBUS_CLIENT_SECRET,
        )
        token_resp = auth.oauth2_client_credentials_tokens(
            requested_scopes=self._env.GLOBUS_TRANSFER_SCOPE,
        )

        # assemble transfer client
        tc = globus_sdk.TransferClient(
            authorizer=globus_sdk.AccessTokenAuthorizer(
                token_resp.by_resource_server["transfer.api.globus.org"]["access_token"]
            )
        )
        LOGGER.info(
            f"Initialized Globus TransferClient with source collection "
            f"{self._env.GLOBUS_SOURCE_COLLECTION_ID}",
        )
        return tc

    def make_transfer_document(
        self,
        source_path: Path,
        dest_path: Path,
    ) -> globus_sdk.TransferData:
        """Create the object needed for submitting a transfer."""

        # Unfortunately, 'globus_sdk' does not support passing 'None' for its args...
        #   So to avoid accessing its private 'globus_sdk._missing.MISSING',
        #   we'll use dict-kwargs unpacking.
        optionals: dict[str, Any] = {}
        if self._env.GLOBUS_HARD_DEADLINE_SECONDS:
            optionals["deadline"] = (
                    datetime.datetime.now(datetime.timezone.utc)
                    + datetime.timedelta(seconds=self._env.GLOBUS_HARD_DEADLINE_SECONDS)
            ).isoformat(timespec="seconds")

        # Construct
        tdata = globus_sdk.TransferData(
            source_endpoint=self._env.GLOBUS_SOURCE_COLLECTION_ID,
            destination_endpoint=self._env.GLOBUS_DEST_COLLECTION_ID,
            label=f"LTA bundle transfer: {source_path} -> {dest_path}",
            fail_on_quota_errors=True,
            # NOTE: 'sync_level'
            #   LTA doesn't assume the transfer mechanism is reliable, and computes
            #   checksums later in the pipeline. So 'mtime' is fine (and much cheaper).
            sync_level="mtime",
            **optionals,
        )
        tdata.add_item(str(source_path), str(dest_path))

        LOGGER.info(f"Created transfer document for {source_path=} -> {dest_path=}")
        return tdata

    async def _submit_transfer(self, tdata: globus_sdk.TransferData) -> uuid.UUID | str:
        """Submit a transfer via Globus TransferClient."""
        LOGGER.info(f"Submitting transfer: {list(tdata.iter_items())}")
        task_id = self._transfer_client.submit_transfer(tdata)["task_id"]
        LOGGER.info(f"Globus transfer submitted: {task_id=}")
        await asyncio.sleep(0)  # since request is not async, handover to pending tasks

        return task_id

    def _cancel_task(self, task_id: uuid.UUID | str, error_msg: str) -> None:
        # cancel task
        LOGGER.error(error_msg)
        try:
            self._transfer_client.cancel_task(task_id)
        except Exception:
            LOGGER.exception(f"Could not cancel Globus {task_id=}")

    # ---------------------------
    # Public API
    # ---------------------------

    async def transfer_file(
        self,
        *,
        source_path: Path,
        dest_path: Path,
    ) -> uuid.UUID | str:
        """
        Transfer a single file via Globus Transfer and block until completion.

        :param source_path: Absolute path on the source collection.
        :param dest_path: The filepath on the destination collection.

        :returns: Globus task_id for the submitted transfer.
        """
        if not os.path.isabs(source_path):
            raise ValueError(f"source_path must be absolute: {source_path}")

        # do transfer
        tdata = self.make_transfer_document(source_path, dest_path)
        task_id = await self._submit_transfer(tdata)

        return task_id

    async def wait_for_transfer_to_finish(self, task_id: uuid.UUID | str) -> None:
        """Wait (forever) for transfer to finish.

        NOTE: 'globus_sdk.TransferClient.task_wait()' is *NOT* async, so we must diy
        """
        first = True
        while True:

            # sleep each time after first iteration
            if first:
                first = False
            else:
                await asyncio.sleep(self._env.GLOBUS_POLL_INTERVAL_SECONDS)

            # look at status
            LOGGER.debug("checking transfer status...")
            task = self._transfer_client.get_task(task_id)
            status = task["status"]
            LOGGER.debug(f"{status=}")
            match status:
                case "SUCCEEDED":
                    LOGGER.info(f"Globus transfer succeeded: {task_id=} {task=}")
                    return
                case "FAILED" | "INACTIVE":
                    msg = f"Globus transfer failed ({status=}): {task_id=} {task=}"
                    LOGGER.error(msg)
                    raise GlobusTransferFailedException(msg)
                case "ACTIVE":
                    continue
                case _:
                    LOGGER.warning(
                        f"received unknown {status=}: {task_id=} {task=} — continuing..."
                    )
                    continue
