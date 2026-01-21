"""Tests for lta/utils.py"""

import pytest
from unittest.mock import AsyncMock, MagicMock, patch

import lta.utils


########################################################################################


@pytest.mark.asyncio
async def test_000_patch_bundle() -> None:
    """PATCH sends request and logs."""
    lta_rc = MagicMock()
    lta_rc.request = AsyncMock()

    logger = MagicMock()

    bundle_id = "B-123"
    patch_body = {"status": "done"}

    await lta.utils.patch_bundle(lta_rc, bundle_id, patch_body, logger)

    logger.info.assert_called_once_with(f"PATCH /Bundles/{bundle_id} - '{patch_body}'")
    lta_rc.request.assert_awaited_once_with(
        "PATCH", f"/Bundles/{bundle_id}", patch_body
    )


########################################################################################


@pytest.mark.asyncio
async def test_100_quarantine_str_reason() -> None:
    """Quarantine builds patch body for string reason."""
    lta_rc = MagicMock()
    logger = MagicMock()

    bundle = {"uuid": "U-1", "status": "processing"}
    name = "scanner"
    instance_uuid = "I-999"
    fixed_now = "2026-01-21T12:34:56Z"

    with (
        patch.object(lta.utils, "now", return_value=fixed_now),
        patch.object(lta.utils, "patch_bundle", new=AsyncMock()) as patch_bundle,
    ):
        await lta.utils.quarantine_bundle(
            lta_rc=lta_rc,
            bundle=bundle,
            reason="something bad happened",
            name=name,
            instance_uuid=instance_uuid,
            logger=logger,
        )

    logger.error.assert_called_once_with(
        f'Sending Bundle {bundle["uuid"]} to quarantine: something bad happened.'
    )

    _args, kwargs = patch_bundle.call_args
    assert kwargs["lta_rc"] is lta_rc
    assert kwargs["bundle_id"] == bundle["uuid"]
    assert kwargs["logger"] is logger
    assert kwargs["patch_body"] == {
        "original_status": bundle["status"],
        "status": "quarantined",
        "reason": f"BY:{name}-{instance_uuid} REASON:something bad happened",
        "work_priority_timestamp": fixed_now,
    }


@pytest.mark.asyncio
async def test_110_quarantine_exc_reason() -> None:
    """Quarantine uses repr() for Exception reason."""
    lta_rc = MagicMock()
    logger = MagicMock()

    bundle = {"uuid": "U-2", "status": "queued"}
    name = "scanner"
    instance_uuid = "I-123"
    fixed_now = "2026-01-21T00:00:00Z"

    reason_exc = ValueError("nope")
    reason_repr = repr(reason_exc)

    with (
        patch.object(lta.utils, "now", return_value=fixed_now),
        patch.object(lta.utils, "patch_bundle", new=AsyncMock()) as patch_bundle,
    ):
        await lta.utils.quarantine_bundle(
            lta_rc=lta_rc,
            bundle=bundle,
            reason=reason_exc,
            name=name,
            instance_uuid=instance_uuid,
            logger=logger,
        )

    logger.error.assert_called_once_with(
        f'Sending Bundle {bundle["uuid"]} to quarantine: {reason_repr}.'
    )

    _args, kwargs = patch_bundle.call_args
    assert (
        kwargs["patch_body"]["reason"]
        == f"BY:{name}-{instance_uuid} REASON:{reason_repr}"
    )
    assert kwargs["patch_body"]["work_priority_timestamp"] == fixed_now


@pytest.mark.asyncio
async def test_120_quarantine_patch_fails() -> None:
    """Quarantine logs and swallows patch failure."""
    lta_rc = MagicMock()
    logger = MagicMock()

    bundle = {"uuid": "U-3", "status": "new"}
    name = "scanner"
    instance_uuid = "I-000"

    err = RuntimeError("network down")

    with patch.object(lta.utils, "patch_bundle", new=AsyncMock(side_effect=err)):
        await lta.utils.quarantine_bundle(
            lta_rc=lta_rc,
            bundle=bundle,
            reason="will fail",
            name=name,
            instance_uuid=instance_uuid,
            logger=logger,
        )

    assert logger.error.call_count == 2
    logger.error.assert_any_call(
        f'Sending Bundle {bundle["uuid"]} to quarantine: will fail.'
    )
    logger.error.assert_any_call(
        f'Unable to quarantine Bundle {bundle["uuid"]}: {err}.'
    )


########################################################################################
