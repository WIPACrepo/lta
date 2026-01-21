"""Common and simple utility functions."""

from logging import Logger

from rest_tools.client import RestClient

from lta.component import now
from lta.lta_types import BundleType


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
