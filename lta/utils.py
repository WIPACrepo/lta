"""Common and simple utility functions."""

import traceback
from logging import Logger
from subprocess import CompletedProcess
from typing import Literal

from rest_tools.client import RestClient

from lta.component import now
from lta.lta_types import BundleType, TransferRequestType


LtaObjectType = Literal["BUNDLE", "TRANSFER_REQUEST"]


_MAX_QUARANTINE_TRACEBACK_LINES = 500


class NoFileCatalogFilesException(Exception):
    """Raised when a query's files cannot be found in the File Catalog."""


class InvalidBundlePathException(Exception):
    """Raised when a bundle path is invalid."""


class InvalidChecksumException(Exception):
    """Raised when a checksum value is invalid."""

    def __init__(self, creation: str, destination: str, logger: Logger):
        logger.error(f"SHA512 checksum at the time of bundle creation: {creation}")
        logger.error(f"SHA512 checksum of the file at the destination: {destination}")
        logger.error(
            "These checksums do NOT match, and the Bundle will NOT be verified."
        )
        super().__init__(
            f"Checksum mismatch between creation and destination: "
            f"{creation=} and {destination=}"
        )


def log_completed_process_outputs(
    completed_process: CompletedProcess,
    command_description: str,
    logger: Logger,
    is_failure: bool = False,
) -> None:
    """Log various outputs of a CompletedProcess."""
    if is_failure:
        log_fn = logger.error
    else:
        log_fn = logger.info

    log_fn(
        f"Command '{command_description}' {'FAILED' if is_failure else ''}: "
        f"{completed_process.args}"
    )
    log_fn(f"returncode: {completed_process.returncode}")
    log_fn(f"stdout: {str(completed_process.stdout)}")
    log_fn(f"stderr: {str(completed_process.stderr)}")


class HSICommandFailedException(Exception):
    """Raised when an HSI command fails."""

    def __init__(
        self,
        hsi_cmd_description: str,
        completed_process: CompletedProcess,
        logger: Logger,
    ):
        log_completed_process_outputs(
            completed_process, hsi_cmd_description, logger, is_failure=True
        )
        super().__init__(
            f"{hsi_cmd_description} - {completed_process.args} - {completed_process.returncode}"
            f" - {str(completed_process.stdout)} - {str(completed_process.stderr)}"
        )


async def patch_bundle(
    lta_rc: RestClient,
    bundle_id: str,
    patch_body: dict,
    logger: Logger,
) -> None:
    """Send PATCH request to LTA REST API for a bundle."""
    logger.info(f"PATCH /Bundles/{bundle_id} - '{patch_body}'")
    await lta_rc.request("PATCH", f"/Bundles/{bundle_id}", patch_body)


async def patch_transfer_request(
    lta_rc: RestClient,
    tr_id: str,
    patch_body: dict,
    logger: Logger,
) -> None:
    """Send PATCH request to LTA REST API for a transfer request."""
    logger.info(f"PATCH /TransferRequests/{tr_id} - '{patch_body}'")
    await lta_rc.request("PATCH", f"/TransferRequests/{tr_id}", patch_body)


def truncate_traceback(exc: Exception) -> str:
    """Return a potentially-truncated traceback string for the Exception instance.

    If the traceback is too long, the middle of the traceback will be omitted.
    """
    lines = traceback.format_exception(exc)

    # Note on traceback.format_exception():
    #   There may be internal newlines in the list entries, so the truncation logic
    #   is best effort. So, assuming no line has unusually many internal
    #   '\n'-concatenations, this should be fine.
    # See https://docs.python.org/3/library/traceback.html#traceback.format_exception

    if len(lines) > _MAX_QUARANTINE_TRACEBACK_LINES:
        half = _MAX_QUARANTINE_TRACEBACK_LINES // 2
        return (
            "".join(lines[:half])
            + f"... truncated middle {len(lines) - _MAX_QUARANTINE_TRACEBACK_LINES} lines ..."
            + "".join(lines[-half:])
        )
    else:
        return "".join(lines)


async def quarantine_now(
    lta_rc: RestClient,
    lta_object: BundleType | TransferRequestType,
    lta_object_type: LtaObjectType,
    causal_exception: Exception,
    name: str,
    instance_uuid: str,
    logger: Logger,
) -> None:
    """Quarantine the supplied 'lta_noun'-type using the supplied reason.

    Args:
        lta_rc:
            RestClient instance for making API requests
        lta_object:
            BundleType or TransferRequestType dictionary containing object to quarantine
        lta_object_type:
            The type of the LTA object to quarantine: 'bundle' or 'transfer_request'
        causal_exception:
            Exception instance for quarantining the lta object. The exception's 'repr()'
            will be used for the 'reason' field. The exception's stack trace will be
            used for the 'reason_details' field.
        name:
            Name of the component performing the quarantine
        instance_uuid:
            UUID of the component instance
        logger:
            Logger instance for logging messages
    """
    reason_details = truncate_traceback(causal_exception)  # get stack trace
    reason = repr(causal_exception)

    logger.error(
        f'Sending {lta_object_type} {lta_object["uuid"]} to quarantine: {reason}.'
    )
    patch_body = {
        "original_status": lta_object["status"],
        "status": "quarantined",
        "reason": f"BY:{name}-{instance_uuid} REASON:{reason}",
        "reason_details": reason_details,
        "work_priority_timestamp": now(),
    }

    try:
        match lta_object_type:
            case "TRANSFER_REQUEST":
                await patch_transfer_request(
                    lta_rc, lta_object["uuid"], patch_body, logger
                )
            case "BUNDLE":
                await patch_bundle(lta_rc, lta_object["uuid"], patch_body, logger)
            case _:
                raise ValueError(f"Invalid {lta_object_type=}")
    except Exception as e:
        logger.error(
            f'Unable to quarantine {lta_object_type} {lta_object["uuid"]}: {e}.'
        )
