# test_replicator.py
"""
Unit tests for replicator.GridFTPReplicator.

Goals:
- Mock the transfer mechanism so GridFTP - Globus swaps won't break tests.
- Avoid touching real Prometheus metrics and networking.
- Cover success, no-work, error/quarantine, run-once-and-die, and path toggle.
"""

import sys
from types import SimpleNamespace
from typing import Any, Callable

import pytest
from unittest.mock import MagicMock


from lta import gridftp_replicator as replicator


# --------------------------------------------------------------------------------------
# Fixtures & Helpers
# --------------------------------------------------------------------------------------


@pytest.fixture
def base_config() -> dict[str, str]:
    """Minimal, valid config for replicator.GridFTPReplicator."""
    return {
        # COMMON_CONFIG used by Component base:
        "CANCEL_FRACTION_OF_TIMEOUT": "0.75",
        "COMPONENT_NAME": "replicator",
        "DEST_SITE": "DESY",
        "INPUT_STATUS": "completed",
        "LOG_LEVEL": "DEBUG",
        "MAX_CLAIM_WORK": "1",
        "RUN_ONCE_AND_DIE": "FALSE",
        "SOURCE_SITE": "WIPAC",
        "WORKBOX_PATH": "/tmp",
        "PROMETHEUS_METRICS_PORT": "9102",
        # Module-specific:
        "GRIDFTP_DEST_URLS": "gsiftp://dest.example.org:2811/data;gsiftp://alt.example.org:2811/data",
        "GRIDFTP_TIMEOUT": "1200",
        "USE_FULL_BUNDLE_PATH": "FALSE",
        "WORK_RETRIES": "3",
        "WORK_TIMEOUT_SECONDS": "30",
    }


@pytest.fixture
def logger() -> Any:
    """Simple logger-like stub with no-op methods."""

    class _L:
        def __getattr__(self, _name: str) -> Callable[..., None]:
            return lambda *a, **k: None

    return _L()


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


@pytest.fixture(autouse=True)
def no_prometheus(monkeypatch: pytest.MonkeyPatch) -> None:
    """Neutralize Prometheus metrics to avoid global state conflicts."""

    class _Counter:
        def labels(self, **_kw: Any) -> _Counter:  # type: ignore[override]
            return self

        def inc(self, *_a: Any, **_k: Any) -> None:
            return None

    class _Gauge:
        def labels(self, **_kw: Any) -> _Gauge:  # type: ignore[override]
            return self

        def set(self, *_a: Any, **_k: Any) -> None:
            return None

    monkeypatch.setattr(replicator, "success_counter", _Counter())
    monkeypatch.setattr(replicator, "failure_counter", _Counter())
    monkeypatch.setattr(replicator, "load_gauge", _Gauge())


@pytest.fixture
def mock_join(monkeypatch: pytest.MonkeyPatch) -> Callable[[list[str]], str]:
    """Mock join_smart_url to a predictable path joiner."""

    def _join(parts: list[str]) -> str:
        return "/".join(s.strip("/") for s in parts if s is not None)

    monkeypatch.setattr(replicator, "join_smart_url", _join)
    return _join


@pytest.fixture
def mock_now(monkeypatch: pytest.MonkeyPatch) -> str:
    """Freeze now() to a stable string."""
    ts = "2025-01-01T00:00:00Z"
    monkeypatch.setattr(replicator, "now", lambda: ts)
    return ts


@pytest.fixture
def mock_proxy(monkeypatch: pytest.MonkeyPatch) -> MagicMock:
    """Mock SiteGlobusProxy.update_proxy (no-op)."""
    cls = replicator.SiteGlobusProxy
    inst = cls.__new__(cls)
    inst.update_proxy = MagicMock()
    monkeypatch.setattr(replicator, "SiteGlobusProxy", MagicMock(return_value=inst))
    return inst


@pytest.fixture
def mock_transfer(monkeypatch: pytest.MonkeyPatch) -> SimpleNamespace:
    """
    Mock the transfer mechanism, abstracting away GridFTP details.
    Provides `.put` as a MagicMock so future swaps (e.g., to Globus) only need this fixture updated.
    """
    transfer = SimpleNamespace(put=MagicMock())
    monkeypatch.setattr(replicator, "GridFTP", transfer)
    return transfer


# --------------------------------------------------------------------------------------
# Tests
# --------------------------------------------------------------------------------------


def test_000_expected_config_has_keys() -> None:
    """replicator.EXPECTED_CONFIG should include keys this component relies on."""
    for key in [
        "GRIDFTP_DEST_URLS",
        "GRIDFTP_TIMEOUT",
        "USE_FULL_BUNDLE_PATH",
        "WORK_RETRIES",
        "WORK_TIMEOUT_SECONDS",
    ]:
        assert key in replicator.EXPECTED_CONFIG


def test_010_init_parses_config(base_config: dict[str, str], logger: Any) -> None:
    """__init__ should parse and coerce config values correctly."""
    rep = replicator.GridFTPReplicator(base_config, logger)
    assert rep.gridftp_dest_urls == [
        "gsiftp://dest.example.org:2811/data",
        "gsiftp://alt.example.org:2811/data",
    ]
    assert rep.gridftp_timeout == 1200
    assert rep.use_full_bundle_path is False
    assert rep.work_retries == 3
    assert rep.work_timeout_seconds == 30.0


@pytest.mark.asyncio
async def test_020_do_status_empty(base_config: dict[str, str], logger: Any) -> None:
    """_do_status should return an empty dict."""
    rep = replicator.GridFTPReplicator(base_config, logger)
    assert rep._do_status() == {}


@pytest.mark.asyncio
async def test_030_do_work_claim_no_bundle_returns_false(
    base_config: dict[str, str],
    logger: Any,
    mock_proxy: MagicMock,
    mock_transfer: SimpleNamespace,
) -> None:
    """When the DB returns no bundle, _do_work_claim should return False."""
    rep = replicator.GridFTPReplicator(base_config, logger)
    rc = DummyRestClient(responses=[{"bundle": None}])

    got = await rep._do_work_claim(rc)
    assert got is False
    assert any(url.startswith("/Bundles/actions/pop") for _, url, _ in rc.calls)


@pytest.mark.asyncio
async def test_040_do_work_claim_success_calls_transfer_and_patch(
    base_config: dict[str, str],
    logger: Any,
    mock_proxy: MagicMock,
    mock_transfer: SimpleNamespace,
    mock_join: Callable[[list[str]], str],
    mock_now: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """On success, replicator should invoke transfer.put and PATCH the bundle."""
    rep = replicator.GridFTPReplicator(base_config, logger)

    bundle: dict[str, Any] = {
        "uuid": "B-123",
        "status": "completed",
        "bundle_path": "/mnt/lfss/jade-lta/bundler_out/foo.zip",
        "path": "/data/exp/IceCube/2015/bar",
    }
    # First POST /pop => bundle; then PATCH => ack
    rc = DummyRestClient(responses=[{"bundle": bundle}, {}])

    # Deterministic URL choice
    monkeypatch.setattr(replicator.random, "choice", lambda urls: urls[0])

    ok = await rep._do_work_claim(rc)
    assert ok is True

    # Transfer called with basename only (USE_FULL_BUNDLE_PATH is FALSE)
    mock_transfer.put.assert_called_once()
    args, kwargs = mock_transfer.put.call_args
    assert kwargs["filename"] == bundle["bundle_path"]
    assert kwargs["request_timeout"] == rep.gridftp_timeout
    assert args[0].endswith("/foo.zip")

    # PATCH to update bundle status to output_status
    patch_calls = [
        c for c in rc.calls if c[0] == "PATCH" and c[1].startswith("/Bundles/")
    ]
    assert patch_calls
    _, url, body = patch_calls[0]
    assert url == "/Bundles/B-123"
    assert body.get("claimed") is False
    assert body.get("status") == rep.output_status


@pytest.mark.asyncio
async def test_050_do_work_claim_exception_quarantines_and_returns_false(
    base_config: dict[str, str],
    logger: Any,
    mock_proxy: MagicMock,
    mock_transfer: SimpleNamespace,
    mock_join: Callable[[list[str]], str],
    mock_now: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """If transfer raises, the bundle is quarantined and function returns False."""
    rep = replicator.GridFTPReplicator(base_config, logger)

    bundle: dict[str, Any] = {
        "uuid": "B-ERR",
        "status": "completed",
        "bundle_path": "/mnt/lfss/jade-lta/bundler_out/bad.zip",
        "path": "/data/exp/IceCube/2015/baz",
    }

    rc = DummyRestClient(responses=[{"bundle": bundle}, {}])
    mock_transfer.put.side_effect = RuntimeError("boom")
    monkeypatch.setattr(replicator.random, "choice", lambda urls: urls[0])

    ok = await rep._do_work_claim(rc)
    assert ok is False

    quarantine_calls = [
        c
        for c in rc.calls
        if c[0] == "PATCH"
        and c[1] == "/Bundles/B-ERR"
        and c[2].get("status") == "quarantined"
    ]
    assert quarantine_calls

    # Ensure we didn't set output_status after failure
    assert not any(
        c
        for c in rc.calls
        if c[0] == "PATCH"
        and c[1] == "/Bundles/B-ERR"
        and c[2].get("status") == rep.output_status
    )


@pytest.mark.asyncio
async def test_060_do_work_runs_until_no_work(
    base_config: dict[str, str],
    logger: Any,
    mock_proxy: MagicMock,
    mock_transfer: SimpleNamespace,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """_do_work should loop until _do_work_claim returns False."""
    rep = replicator.GridFTPReplicator(base_config, logger)

    claim_calls: list[bool] = []

    async def _fake_claim(_rc: DummyRestClient) -> bool:
        claim_calls.append(True)
        return len(claim_calls) == 1  # True once, then False

    monkeypatch.setattr(rep, "_do_work_claim", _fake_claim)
    rc = DummyRestClient()

    await rep._do_work(rc)
    assert len(claim_calls) == 2


@pytest.mark.asyncio
async def test_070_do_work_respects_run_once_and_die(
    base_config: dict[str, str], logger: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    """If run_once_and_die is set, _do_work should call sys.exit after one claim attempt."""
    rep = replicator.GridFTPReplicator(base_config, logger)
    rep.run_once_and_die = True

    async def _fake_claim(_rc: DummyRestClient) -> bool:
        return False

    monkeypatch.setattr(rep, "_do_work_claim", _fake_claim)

    exit_called: dict[str, bool] = {"flag": False}

    def _fake_exit(*_a: Any, **_k: Any) -> None:
        exit_called["flag"] = True
        raise SystemExit

    monkeypatch.setattr(sys, "exit", _fake_exit)

    with pytest.raises(SystemExit):
        await rep._do_work(DummyRestClient())

    assert exit_called["flag"] is True


@pytest.mark.asyncio
async def test_080_replication_use_full_bundle_path_true(
    base_config: dict[str, str],
    logger: Any,
    mock_proxy: MagicMock,
    mock_transfer: SimpleNamespace,
    mock_join: Callable[[list[str]], str],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """When USE_FULL_BUNDLE_PATH is TRUE, destination includes bundle['path'] + basename."""
    cfg = dict(base_config)
    cfg["USE_FULL_BUNDLE_PATH"] = "TRUE"
    rep = replicator.GridFTPReplicator(cfg, logger)

    bundle: dict[str, Any] = {
        "uuid": "B-456",
        "status": "completed",
        "bundle_path": "/mnt/lfss/jade-lta/bundler_out/bar.zip",
        "path": "/data/exp/IC/2015/filtered/level2/0320",
    }
    rc = DummyRestClient(responses=[{"bundle": bundle}, {}])
    monkeypatch.setattr(replicator.random, "choice", lambda urls: urls[0])

    ok = await rep._do_work_claim(rc)
    assert ok is True

    dest_url: str = mock_transfer.put.call_args[0][0]
    assert "/data/exp/IC/2015/filtered/level2/0320/bar.zip" in dest_url


@pytest.mark.asyncio
async def test_090_replication_use_full_bundle_path_false(
    base_config: dict[str, str],
    logger: Any,
    mock_proxy: MagicMock,
    mock_transfer: SimpleNamespace,
    mock_join: Callable[[list[str]], str],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """When USE_FULL_BUNDLE_PATH is FALSE, destination includes only the basename."""
    cfg = dict(base_config)
    cfg["USE_FULL_BUNDLE_PATH"] = "FALSE"
    rep = replicator.GridFTPReplicator(cfg, logger)

    bundle: dict[str, Any] = {
        "uuid": "B-789",
        "status": "completed",
        "bundle_path": "/mnt/lfss/jade-lta/bundler_out/baz.zip",
        "path": "/data/exp/IC/irrelevant/when/false",
    }
    rc = DummyRestClient(responses=[{"bundle": bundle}, {}])
    monkeypatch.setattr(replicator.random, "choice", lambda urls: urls[0])

    ok = await rep._do_work_claim(rc)
    assert ok is True

    dest_url: str = mock_transfer.put.call_args[0][0]
    assert dest_url.endswith("/baz.zip")
    assert "irrelevant" not in dest_url
