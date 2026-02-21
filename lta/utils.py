"""Common and simple utility functions."""

import datetime
import traceback
from collections.abc import Mapping
from logging import Logger
from subprocess import CompletedProcess
from typing import Any

from rest_tools.client import RestClient

_MAX_QUARANTINE_TRACEBACK_LINES = 500


def utcnow_isoformat(*, timespec: str | None = None) -> str:
    """Mimic exactly the result of 'datetime.datetime.utcnow().isoformat(timespec=...)'.

    Note: 'datetime.datetime.utcnow()' is deprecated
    """
    dt = datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None)
    if timespec is None:
        return dt.isoformat()
    return dt.isoformat(timespec=timespec)


def now() -> str:
    """Return string timestamp for current time, to the second."""
    return utcnow_isoformat(timespec="seconds")


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


class _LtaType:
    """LTA object types."""

    TYPE_BUNDLE = "Bundle"
    TYPE_TRANSFER_REQUEST = "TransferRequest"


SUPPORTED_LTA_TYPES: set[str] = {_LtaType.TYPE_BUNDLE, _LtaType.TYPE_TRANSFER_REQUEST}


async def quarantine_now(
    lta_rc: RestClient,
    lta_object: dict[str, Any],
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
    # 1) lta_object isn't a Dict/Mapping
    if not isinstance(lta_object, Mapping):
        err = (
            "Cannot quarantine LTA object: not a dict-like Mapping "
            f"(got {type(lta_object).__name__}: {lta_object!r})."
        )
        logger.error(err)
        raise ValueError(err)

    # 2) missing required keys
    for key in {"type", "uuid", "status"}:
        if key not in lta_object:
            err = (
                f"Cannot quarantine LTA object: missing key '{key}' "
                f"(contains {list(lta_object.keys())}, uuid={lta_object.get('uuid')})."
            )
            logger.error(err)
            raise ValueError(err)

    reason_details = truncate_traceback(causal_exception)
    reason = repr(causal_exception)

    logger.error(
        f'Sending {lta_object["type"]} uuid={lta_object["uuid"]} to quarantine: {reason}.'
    )
    patch_body = {
        "original_status": lta_object["status"],
        "status": "quarantined",
        "reason": f"BY:{name}-{instance_uuid} REASON:{reason}",
        "reason_details": reason_details,
        "work_priority_timestamp": now(),
    }

    try:
        match lta_object["type"]:
            case _LtaType.TYPE_TRANSFER_REQUEST:
                await patch_transfer_request(
                    lta_rc, lta_object["uuid"], patch_body, logger
                )
            case _LtaType.TYPE_BUNDLE:
                await patch_bundle(lta_rc, lta_object["uuid"], patch_body, logger)
            case _:
                err = (
                    f"Cannot quarantine LTA object: unsupported 'type' value, "
                    f"'{lta_object['type']}' (supported={sorted(SUPPORTED_LTA_TYPES)!r}, "
                    f"uuid={lta_object.get('uuid')!r})."
                )
                logger.error(err)
                raise ValueError(err)
    except Exception as e:
        err = f'Failed to quarantine {lta_object["type"]} uuid={lta_object["uuid"]}: {repr(e)}.'
        logger.error(err)
        raise RuntimeError(err) from e
