"""Common and simple utility functions."""

import datetime
import traceback
from collections.abc import Mapping
from enum import StrEnum
from logging import Logger
from subprocess import CompletedProcess
from typing import Any

from rest_tools.client import RestClient

_MAX_QUARANTINE_TRACEBACK_LINES = 500


class HSIOperation(StrEnum):
    """Operations performed through HSI."""

    LIST_CHECKSUM = "list checksum in HPSS (hashlist)"
    READ_BUNDLE = "read bundle from HPSS"
    TAPE_BUNDLE = "tape bundle to HPSS"
    VERIFY_BUNDLE = "verify bundle in HPSS (hashverify)"


def utcnow_isoformat(*, timespec: str | None = None) -> str:
    """Mimic exactly the result of 'datetime.datetime.utcnow().isoformat(timespec=...)'.

    Note: 'datetime.datetime.utcnow()' is deprecated
    """
    dt = datetime.datetime.now(datetime.UTC).replace(tzinfo=None)
    if timespec is None:
        return dt.isoformat()
    return dt.isoformat(timespec=timespec)


def now() -> str:
    """Return string timestamp for current time, to the second."""
    return utcnow_isoformat(timespec="seconds")


class NoFileCatalogFilesException(Exception):
    """Raised when the File Catalog returns no files for a TransferRequest."""

    def __init__(self) -> None:
        super().__init__(
            "File Catalog returned zero files for the TransferRequest"
        )


class InvalidBundlePathException(Exception):
    """Raised when a bundle path is invalid."""

    def __init__(
        self,
        bundle_path: str,
        transfer_dest_path: str,
        dest_root_path: str,
    ) -> None:
        super().__init__(
            f"bundle_path={bundle_path!r} is not within "
            f"transfer_dest_path={transfer_dest_path!r} "
            f"(dest_root_path={dest_root_path!r})"
        )


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


class UnsupportedQuarantineException(Exception):
    """Raised when attempting to quarantine an LTA object of unknown type."""

    def __init__(self, lta_object_type: str, supported_types: set[str]):
        super().__init__(
            f"lta_object['type'] == '{lta_object_type}' appears in "
            f"SUPPORTED_LTA_TYPES == '{supported_types}' "
            f"but we don't have logic to handle it"
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
        operation: HSIOperation,
        completed_process: CompletedProcess,
        logger: Logger,
    ) -> None:
        hsi_cmd_description = str(operation)
        log_completed_process_outputs(
            completed_process, hsi_cmd_description, logger, is_failure=True
        )
        super().__init__(
            f"{hsi_cmd_description} - {completed_process.args}"
            f" - {completed_process.returncode}"
            f" - {completed_process.stdout!r}"
            f" - {completed_process.stderr!r}"
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
        raise TypeError(err)

    # 2) missing required keys
    for key in {"type", "uuid", "status"}:
        if key not in lta_object:
            err = (
                f"Cannot quarantine LTA object: missing key '{key}' "
                f"(contains {list(lta_object.keys())}, uuid={lta_object.get('uuid')})."
            )
            logger.error(err)
            raise ValueError(err)

    # 3) type isn't a known LTA object type
    if lta_object["type"] not in SUPPORTED_LTA_TYPES:
        err = (
            f"Cannot quarantine LTA object: unsupported 'type' value, "
            f"'{lta_object['type']}' (supported={sorted(SUPPORTED_LTA_TYPES)!r}, "
            f"uuid={lta_object.get('uuid')!r})."
        )
        logger.error(err)
        raise ValueError(err)

    # change the status of the object to `quarantined`
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

    # TODO: this kind of poor man's type-dispatch is painful
    # let's not follow this pattern in the rewrite, eh?
    try:
        lta_object_type = lta_object["type"]
        if lta_object_type == _LtaType.TYPE_TRANSFER_REQUEST:
            return await patch_transfer_request(lta_rc, lta_object["uuid"], patch_body, logger)
        elif lta_object_type == _LtaType.TYPE_BUNDLE:
            return await patch_bundle(lta_rc, lta_object["uuid"], patch_body, logger)
    except Exception as e:
        err = f'Failed to quarantine {lta_object["type"]} uuid={lta_object["uuid"]}: {repr(e)}.'
        logger.exception(err)
        # all done (rainy day)
        raise RuntimeError(err) from e

    # whoops, somehow a 'supported' type wasn't all that well supported...
    raise UnsupportedQuarantineException(lta_object_type, SUPPORTED_LTA_TYPES)
