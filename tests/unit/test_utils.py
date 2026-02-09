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


class _DontPassAnything:
    """Sentinel in tests."""


@pytest.mark.parametrize(
    "reason_details",
    # because we're passing a string as 'reason' (as opposed to an Exception),
    #   we have the option of passing a custom 'reason_details' string
    [
        # in              out
        ("custom string", "custom string"),
        (_DontPassAnything, ""),
        ("", ""),
    ],
)
@pytest.mark.asyncio
async def test_100_quarantine_str_reason(reason_details: tuple[str, str]) -> None:
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
            **(
                {"reason_details": reason_details[0]}
                if reason_details[0] != _DontPassAnything
                else {}
            ),
        )

    logger.error.assert_called_once_with(
        f'Sending Bundle {bundle["uuid"]} to quarantine: something bad happened.'
    )

    patch_bundle.assert_awaited_once_with(
        lta_rc,
        bundle["uuid"],
        {
            "original_status": bundle["status"],
            "status": "quarantined",
            "reason": f"BY:{name}-{instance_uuid} REASON:something bad happened",
            "reason_details": reason_details[1],
            "work_priority_timestamp": fixed_now,
        },
        logger,
    )


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
        reason_exc = e
    reason_repr = repr(reason_exc)
    # ------------------------------

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

    nl = "\n"  # python 3.11 does not allow '\n's in f-strings

    patch_bundle.assert_awaited_once_with(
        lta_rc,
        bundle["uuid"],
        {
            "original_status": bundle["status"],
            "status": "quarantined",
            "reason": f"BY:{name}-{instance_uuid} REASON:{reason_repr}",
            # *******************************************************************
            # NOTE:
            #   IF LINES ARE ADDED OR REMOVED FROM ABOVE, THE LINE NUMBERS IN THE
            #   EXPECTED STACKTRACE VALUE NEED TO BE UPDATED AS WELL!
            "reason_details": (
                "Traceback (most recent call last):\n"
                f'  File "{__file__}", '
                "line 165, in test_111_quarantine_exc_reason_more_stacktrace\n"
                "    my_func()\n"
                f"{'    ~~~~~~~^^'+nl if sys.version_info >= (3, 13) else ''}"
                f'  File "{__file__}", '
                "line 162, in my_func\n"
                "    _inner_func()\n"
                f"{'    ~~~~~~~~~~~^^'+nl if sys.version_info >= (3, 13) else ''}"
                f'  File "{__file__}", '
                "line 159, in _inner_func\n"
                '    raise ValueError("nope")\n'
                "ValueError: nope\n"
            ),
            # *******************************************************************
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
