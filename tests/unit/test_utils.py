"""Tests for lta/utils.py"""

import sys

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
async def test_110_quarantine_exc_reason() -> None:
    """Quarantine uses repr() for Exception reason."""
    lta_rc = MagicMock()
    logger = MagicMock()

    bundle = {"uuid": "U-2", "status": "queued"}
    name = "scanner"
    instance_uuid = "I-123"
    fixed_now = "2026-01-21T00:00:00Z"

    causal_exception = ValueError("nope")
    reason_repr = repr(causal_exception)

    with (
        patch.object(lta.utils, "now", return_value=fixed_now),
        patch.object(lta.utils, "patch_bundle", new=AsyncMock()) as patch_bundle,
    ):
        await lta.utils.quarantine_now(
            lta_rc=lta_rc,
            lta_object=bundle,
            lta_object_type="BUNDLE",
            causal_exception=causal_exception,
            name=name,
            instance_uuid=instance_uuid,
            logger=logger,
        )

    logger.error.assert_called_once_with(
        f'Sending BUNDLE {bundle["uuid"]} to quarantine: {reason_repr}.'
    )

    patch_bundle.assert_awaited_once_with(
        lta_rc,
        bundle["uuid"],
        {
            "original_status": bundle["status"],
            "status": "quarantined",
            "reason": f"BY:{name}-{instance_uuid} REASON:{reason_repr}",
            "reason_details": "ValueError: nope\n",
            # ^^^ no stacktrace b/c we just passed verbatim -- see test_111 below
            "work_priority_timestamp": fixed_now,
        },
        logger,
    )


# *******************************************************************
# NOTE:
#   IF LINES ARE ADDED OR REMOVED ABOVE 'raise' IN USE CASE, THE LINE
#   NUMBERS IN THE EXPECTED STACKTRACE VALUE NEED TO BE UPDATED TOO!
TRACEBACK_111 = f"""Traceback (most recent call last):
  File "{__file__}", line 126, in test_111_quarantine_exc_reason_more_stacktrace
    my_func()
    ~~~~~~~^^
  File "{__file__}", line 123, in my_func
    _inner_func()
    ~~~~~~~~~~~^^
  File "{__file__}", line 120, in _inner_func
    raise ValueError("nope")
ValueError: nope
"""

# python pre-3.13 did not have '~~~^^' arrows
TRACEBACK_111_PY_OLD = TRACEBACK_111.replace("    ~~~~~~~^^\n", "")
TRACEBACK_111_PY_OLD = TRACEBACK_111_PY_OLD.replace("    ~~~~~~~~~~~^^\n", "")
# *******************************************************************


@pytest.mark.asyncio
async def test_111_quarantine_exc_reason_more_stacktrace() -> None:
    """Quarantine uses repr() for Exception reason."""
    lta_rc = MagicMock()
    logger = MagicMock()

    bundle = {"uuid": "U-2", "status": "queued"}
    name = "scanner"
    instance_uuid = "I-123"
    fixed_now = "2026-01-21T00:00:00Z"

    # ------------------------------
    # setup some convoluted exception chain so we can test stacktrace handling

    def _inner_func() -> None:
        raise ValueError("nope")

    def my_func() -> None:
        _inner_func()

    try:
        my_func()
    except ValueError as e:
        causal_exception = e
    reason_repr = repr(causal_exception)
    # ------------------------------

    with (
        patch.object(lta.utils, "now", return_value=fixed_now),
        patch.object(lta.utils, "patch_bundle", new=AsyncMock()) as patch_bundle,
    ):
        await lta.utils.quarantine_now(
            lta_rc=lta_rc,
            lta_object=bundle,
            lta_object_type="BUNDLE",
            causal_exception=causal_exception,
            name=name,
            instance_uuid=instance_uuid,
            logger=logger,
        )

    logger.error.assert_called_once_with(
        f'Sending BUNDLE {bundle["uuid"]} to quarantine: {reason_repr}.'
    )

    patch_bundle.assert_awaited_once_with(
        lta_rc,
        bundle["uuid"],
        {
            "original_status": bundle["status"],
            "status": "quarantined",
            "reason": f"BY:{name}-{instance_uuid} REASON:{reason_repr}",
            "reason_details": (
                TRACEBACK_111 if sys.version_info >= (3, 13) else TRACEBACK_111_PY_OLD
            ),
            "work_priority_timestamp": fixed_now,
        },
        logger,
    )


@pytest.mark.asyncio
async def test_120_quarantine_patch_fails() -> None:
    """Quarantine logs and swallows patch failure."""
    lta_rc = MagicMock()
    logger = MagicMock()

    bundle = {"uuid": "U-3", "status": "new"}
    name = "scanner"
    instance_uuid = "I-000"

    causal_exception = ValueError("patch will fail anyways")
    patch_err = RuntimeError("network down")

    with patch.object(lta.utils, "patch_bundle", new=AsyncMock(side_effect=patch_err)):
        await lta.utils.quarantine_now(
            lta_rc=lta_rc,
            lta_object=bundle,
            lta_object_type="BUNDLE",
            causal_exception=causal_exception,
            name=name,
            instance_uuid=instance_uuid,
            logger=logger,
        )

    assert logger.error.call_count == 2
    logger.error.assert_any_call(
        f'Sending BUNDLE {bundle["uuid"]} to quarantine: {repr(causal_exception)}.'
    )
    logger.error.assert_any_call(
        f'Unable to quarantine BUNDLE {bundle["uuid"]}: {patch_err}.'
    )


########################################################################################
