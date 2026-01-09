# tests/unit/test_replicator.py
"""

# fmt:off
Unit tests for LTA replication component (GlobusReplicator).

Goals:
- Mock the transfer mechanism so backend swaps won't break tests.
- Avoid touching real Prometheus metrics and networking.
- Cover success, no-work, error/quarantine vs success-on-error, run-once-and-die,
  and USE_FULL_BUNDLE_PATH toggling.
"""

import logging
from pathlib import Path
from typing import Any, Callable

import lta

import prometheus_client
import pytest
from unittest.mock import AsyncMock, patch

# --------------------------------------------------------------------------------------
# Parametrized implementation helper (Globus)
# --------------------------------------------------------------------------------------

GLOBUS_REPLICATOR_DEST_DIRPATH = Path("/path/to/destination/")

EXPECTED_CONFIG_KEYS = [
    "USE_FULL_BUNDLE_PATH",
    "WORK_RETRIES",
    "WORK_TIMEOUT_SECONDS",
    "GLOBUS_REPLICATOR_DEST_DIRPATH",
]


@pytest.fixture(autouse=True)
def setup(
    request: pytest.FixtureRequest,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Stub Prometheus + GlobusTransfer for all tests in this module."""

    # --- Patch GlobusTransfer at the alias used by globus_replicator ---
    patcher = patch("lta.globus_replicator.GlobusTransfer")
    mock_globus_transfer = patcher.start()
    request.addfinalizer(patcher.stop)

    # Every GlobusTransfer() call returns this instance
    instance = mock_globus_transfer.return_value
    instance.transfer_file = AsyncMock()
    instance.wait_for_transfer_to_finish = AsyncMock()

    # --- Stub prometheus *at the module aliases* used by GlobusReplicator ---
    import lta.globus_replicator as _mod

    class _Counter:
        def labels(self, **_: Any) -> Any:
            return self

        def inc(self, *_: Any, **__: Any) -> None:
            return None

    class _Gauge:
        def labels(self, **_: Any) -> Any:
            return self

        def set(self, *_: Any, **__: Any) -> None:
            return None

    # Patch the names actually used in GlobusReplicator.__init__
    monkeypatch.setattr(_mod, "Counter", lambda *a, **k: _Counter())
    monkeypatch.setattr(_mod, "Gauge", lambda *a, **k: _Gauge())


# --------------------------------------------------------------------------------------
# Fixtures & Helpers
# --------------------------------------------------------------------------------------


@pytest.fixture
def base_config() -> dict[str, str]:
    """Minimal, valid config for the replication component."""
    cfg: dict[str, str] = {
        # ===== Required by COMMON_CONFIG (must be present and non-empty) =====
        "CLIENT_ID": "test-client-id",
        "CLIENT_SECRET": "test-client-secret",
        "COMPONENT_NAME": "replicator",
        "DEST_SITE": "DESY",
        "INPUT_STATUS": "completed",
        "LOG_LEVEL": "DEBUG",
        "LTA_AUTH_OPENID_URL": "https://auth.local/oidc/token",
        "LTA_REST_URL": "https://lta.local/api",
        "OUTPUT_STATUS": "transferring",
        "PROMETHEUS_METRICS_PORT": "9102",
        "RUN_ONCE_AND_DIE": "FALSE",
        "RUN_UNTIL_NO_WORK": "FALSE",
        "SOURCE_SITE": "WIPAC",
        "WORK_RETRIES": "3",
        "WORK_SLEEP_DURATION_SECONDS": "0.01",  # keep tests snappy
        "WORK_TIMEOUT_SECONDS": "30",
        "USE_FULL_BUNDLE_PATH": "FALSE",
        "GLOBUS_REPLICATOR_DEST_DIRPATH": str(GLOBUS_REPLICATOR_DEST_DIRPATH),
        "GLOBUS_REPLICATOR_SOURCE_BIND_ROOTPATH": "/one/two/three",
    }

    return cfg


class DummyRestClient:
    """A stub RestClient that returns queued responses and records requests."""

    def __init__(self, responses: list[dict[str, Any]] | None = None) -> None:
        self._responses: list[dict[str, Any]] = list(responses or [])
        self.calls: list[tuple[str, str, dict[str, Any]]] = []

    async def request(
        self, method: str, url: str, body: dict[str, Any] | None = None
    ) -> dict[str, Any]:
        self.calls.append((method, url, body or {}))
        if self._responses:
            return self._responses.pop(0)
        return {}


@pytest.fixture
def mock_join(
    monkeypatch: pytest.MonkeyPatch,
) -> Callable[[list[str]], str]:
    """Mock join_smart_url to a predictable path joiner."""

    def _join(parts: list[str]) -> str:
        return "/".join(s.strip("/") for s in parts if s is not None)

    return _join


@pytest.fixture
def mock_now(
    monkeypatch: pytest.MonkeyPatch,
) -> str:
    """Freeze now() to a stable string."""
    ts = "2025-01-01T00:00:00Z"
    monkeypatch.setattr(lta.globus_replicator, "now", lambda: ts)
    return ts


# --------------------------------------------------------------------------------------
# Tests
# --------------------------------------------------------------------------------------


def test_000_expected_config_has_keys() -> None:
    """EXPECTED_CONFIG should include keys this component relies on."""
    for key in EXPECTED_CONFIG_KEYS:
        assert key in lta.globus_replicator.EXPECTED_CONFIG


def test_010_init_parses_config(
    base_config: dict[str, str],
) -> None:
    """__init__ should parse and coerce config values correctly."""
    rep = lta.globus_replicator.GlobusReplicator(base_config, logging.getLogger())

    # These are internal to GlobusTransferEnv, not the replicator itself.
    assert not hasattr(rep, "globus_dest_url")
    assert not hasattr(rep, "globus_timeout")

    assert rep.use_full_bundle_path is False
    assert rep.work_retries == 3
    assert rep.work_timeout_seconds == 30.0


@pytest.mark.asyncio
async def test_020_do_status_empty(
    base_config: dict[str, str],
) -> None:
    """_do_status should return an empty dict."""
    rep = lta.globus_replicator.GlobusReplicator(base_config, logging.getLogger())
    assert rep._do_status() == {}


@pytest.mark.asyncio
async def test_030_do_work_claim_no_bundle_returns_false(
    base_config: dict[str, str],
) -> None:
    """When the DB returns no bundle, _do_work_claim should return False."""
    rep = lta.globus_replicator.GlobusReplicator(base_config, logging.getLogger())
    rc = DummyRestClient(responses=[{"bundle": None}])

    got = await rep._do_work_claim(rc)  # type: ignore[arg-type]
    assert got is False
    assert any(url.startswith("/Bundles/actions/pop") for _, url, _ in rc.calls)


@pytest.mark.parametrize(
    "bundle_src",
    [
        "foo.zip",
        "subdir/foo.zip",
    ],
)
@pytest.mark.asyncio
async def test_040_do_work_claim_success_calls_transfer_and_patch(
    base_config: dict[str, str],
    mock_join: Callable[[list[str]], str],
    mock_now: str,
    monkeypatch: pytest.MonkeyPatch,
    bundle_src: str,
) -> None:
    """On success, replicator should invoke transfer and PATCH the bundle."""
    rep = lta.globus_replicator.GlobusReplicator(base_config, logging.getLogger())

    bundle: dict[str, Any] = {
        "uuid": "B-123",
        "status": "completed",
        "bundle_path": f"/one/two/three/{bundle_src}",
        "path": "/data/exp/IceCube/2015/bar",
    }
    rc = DummyRestClient(responses=[{"bundle": bundle}, {}])

    ok = await rep._do_work_claim(rc)  # type: ignore[arg-type]
    assert ok is True

    # GlobusTransfer.transfer_file
    lta.globus_replicator.GlobusTransfer.return_value.transfer_file.assert_called_once()  # type: ignore
    kwargs = (
        lta.globus_replicator.GlobusTransfer.return_value.transfer_file.call_args.kwargs  # type: ignore
    )
    # -- USE_FULL_BUNDLE_PATH is FALSE in base_config → just basename.
    assert str(kwargs["source_path"]) == "/" + bundle_src
    assert (
        kwargs["dest_path"]
        == GLOBUS_REPLICATOR_DEST_DIRPATH / bundle_src.rsplit("/", 1)[-1]
    )

    # GlobusTransfer.wait_for_transfer_to_finish
    lta.globus_replicator.GlobusTransfer.return_value.wait_for_transfer_to_finish.assert_called_once()  # type: ignore
    args = (
        lta.globus_replicator.GlobusTransfer.return_value.wait_for_transfer_to_finish.call_args.args  # type: ignore
    )
    assert args == (  # assert wait_for_transfer_to_finish() was called with the 'task_id'
        lta.globus_replicator.GlobusTransfer.return_value.transfer_file.return_value,  # type: ignore
    )

    # Verify PATCH
    patch_calls = [
        c for c in rc.calls if c[0] == "PATCH" and c[1].startswith("/Bundles/")
    ]
    assert patch_calls
    # -- post transfer_file()
    _, url, body = patch_calls[0]
    assert url == "/Bundles/B-123"
    assert set(body.keys()) == {"update_timestamp", "transfer_reference"}
    ref = f"globus/{lta.globus_replicator.GlobusTransfer.return_value.transfer_file.return_value}"  # type: ignore
    assert body.get("transfer_reference") == ref
    # -- post wait_for_transfer_to_finish()
    _, url, body = patch_calls[1]
    assert url == "/Bundles/B-123"
    assert set(body.keys()) == {"status", "reason", "update_timestamp", "claimed"}
    assert body.get("claimed") is False
    assert body.get("status") == rep.output_status
    assert body.get("reason") == ""


@pytest.mark.asyncio
async def test_050_do_work_claim_transfer_error_behaviour(
    base_config: dict[str, str],
    mock_join: Callable[[list[str]], str],
    mock_now: str,
) -> None:
    """
    If the transfer raises, behaviour differs:

    - GridFTPReplicator: logs error and still marks bundle as successful.
    - GlobusReplicator: quarantines the bundle and returns False.
    """
    rep = lta.globus_replicator.GlobusReplicator(base_config, logging.getLogger())

    bundle: dict[str, Any] = {
        "uuid": "B-ERR",
        "status": "completed",
        "bundle_path": "/one/two/three/bad.zip",
        "path": "/data/exp/IceCube/2015/baz",
    }
    rc = DummyRestClient(responses=[{"bundle": bundle}, {}])

    def _raise(*_a: Any, **_k: Any) -> None:
        raise RuntimeError("boom")

    lta.globus_replicator.GlobusTransfer.return_value.transfer_file.side_effect = _raise  # type: ignore

    ok = await rep._do_work_claim(rc)  # type: ignore[arg-type]
    patch_calls = [c for c in rc.calls if c[0] == "PATCH"]
    assert patch_calls

    # Globus path: quarantined + False
    assert ok is False
    quarantine_calls = [c for c in patch_calls if c[1] == "/Bundles/B-ERR"]
    assert quarantine_calls
    _, _, body = quarantine_calls[0]
    assert body.get("status") == "quarantined"
    assert "BY:replicator-" in body.get("reason", "")


@pytest.mark.asyncio
async def test_060_do_work_runs_until_no_work(
    base_config: dict[str, str],
) -> None:
    """_do_work should loop until _do_work_claim returns False."""
    rep = lta.globus_replicator.GlobusReplicator(base_config, logging.getLogger())

    claim_calls: list[bool] = []

    async def _fake_claim(_rc: DummyRestClient) -> bool:
        claim_calls.append(True)
        return len(claim_calls) == 1  # True once, then False

    rep._do_work_claim = _fake_claim  # type: ignore[assignment]
    rc = DummyRestClient()

    await rep._do_work(rc)  # type: ignore[arg-type]
    assert len(claim_calls) == 2


@pytest.mark.asyncio
async def test_070_do_work_respects_run_once_and_die(
    base_config: dict[str, str],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """If run_once_and_die is set, _do_work should call sys.exit after one claim attempt."""
    rep = lta.globus_replicator.GlobusReplicator(base_config, logging.getLogger())
    rep.run_once_and_die = True

    async def _fake_claim(_rc: DummyRestClient) -> bool:
        return False

    rep._do_work_claim = _fake_claim  # type: ignore[assignment]

    exit_called: dict[str, bool] = {"flag": False}

    def _fake_exit(*_a: Any, **_k: Any) -> None:
        exit_called["flag"] = True
        raise SystemExit

    # Patch the sys.exit used by this module
    monkeypatch.setattr(lta.globus_replicator.sys, "exit", _fake_exit)

    with pytest.raises(SystemExit):
        await rep._do_work(DummyRestClient())  # type: ignore[arg-type]

    assert exit_called["flag"] is True


@pytest.mark.asyncio
async def test_080_replication_use_full_bundle_path_true(
    base_config: dict[str, str],
    mock_join: Callable[[list[str]], str],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """When USE_FULL_BUNDLE_PATH is TRUE, dest includes bundle['path'] + basename."""
    cfg = dict(base_config)
    cfg["USE_FULL_BUNDLE_PATH"] = "TRUE"
    rep = lta.globus_replicator.GlobusReplicator(cfg, logging.getLogger())

    bundle: dict[str, Any] = {
        "uuid": "B-456",
        "status": "completed",
        "bundle_path": "/one/two/three/bar.zip",
        "path": "/data/exp/IC/2015/filtered/level2/0320",
    }
    rc = DummyRestClient(responses=[{"bundle": bundle}, {}])

    ok = await rep._do_work_claim(rc)  # type: ignore[arg-type]
    assert ok is True

    lta.globus_replicator.GlobusTransfer.return_value.transfer_file.assert_called_once()  # type: ignore
    kwargs = (
        lta.globus_replicator.GlobusTransfer.return_value.transfer_file.call_args.kwargs  # type: ignore
    )

    assert (
        kwargs["dest_path"]
        == GLOBUS_REPLICATOR_DEST_DIRPATH
        / "data/exp/IC/2015/filtered/level2/0320/bar.zip"
    )


@pytest.mark.asyncio
async def test_090_replication_use_full_bundle_path_false(
    base_config: dict[str, str],
    mock_join: Callable[[list[str]], str],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """When USE_FULL_BUNDLE_PATH is FALSE, destination includes only the basename."""
    cfg = dict(base_config)
    cfg["USE_FULL_BUNDLE_PATH"] = "FALSE"
    rep = lta.globus_replicator.GlobusReplicator(cfg, logging.getLogger())

    bundle: dict[str, Any] = {
        "uuid": "B-789",
        "status": "completed",
        "bundle_path": "/one/two/three/baz.zip",
        "path": "/data/exp/IC/irrelevant/when/false",
    }
    rc = DummyRestClient(responses=[{"bundle": bundle}, {}])

    ok = await rep._do_work_claim(rc)  # type: ignore[arg-type]
    assert ok is True

    lta.globus_replicator.GlobusTransfer.return_value.transfer_file.assert_called_once()  # type: ignore
    kwargs = (
        lta.globus_replicator.GlobusTransfer.return_value.transfer_file.call_args.kwargs  # type: ignore
    )

    assert kwargs["dest_path"] == GLOBUS_REPLICATOR_DEST_DIRPATH / "baz.zip"
