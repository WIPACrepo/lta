"""Common and simple utility functions."""

from logging import Logger
from subprocess import CompletedProcess

from rest_tools.client import RestClient

from lta.component import now
from lta.lta_types import BundleType


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


async def quarantine_bundle(
    lta_rc: RestClient,
    bundle: BundleType,
    reason: Exception | str,
    name: str,
    instance_uuid: str,
    logger: Logger,
) -> None:
    """Quarantine the supplied bundle using the supplied reason."""
    if isinstance(reason, Exception):
        reason = repr(reason)

    logger.error(f'Sending Bundle {bundle["uuid"]} to quarantine: {reason}.')
    patch_body = {
        "original_status": bundle["status"],
        "status": "quarantined",
        "reason": f"BY:{name}-{instance_uuid} REASON:{reason}",
        "work_priority_timestamp": now(),
    }

    try:
        await patch_bundle(lta_rc, bundle["uuid"], patch_body, logger)
    except Exception as e:
        logger.error(f'Unable to quarantine Bundle {bundle["uuid"]}: {e}.')
