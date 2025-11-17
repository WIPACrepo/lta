# tests/unit/test_replicator.py
"""
Unit tests for LTA replication components (GridFTPReplicator / GlobusReplicator).

Goals:
- Mock the transfer mechanism so backend swaps won't break tests.
- Avoid touching real Prometheus metrics and networking.
- Cover success, no-work, error/quarantine vs success-on-error, run-once-and-die,
  and USE_FULL_BUNDLE_PATH toggling.
"""
from dataclasses import dataclass
import importlib
import sys
from types import SimpleNamespace
from typing import Any, Callable, Literal

import prometheus_client
import pytest
from unittest.mock import MagicMock


# --------------------------------------------------------------------------------------
# Parametrized implementation helper (GridFTP vs Globus)
# --------------------------------------------------------------------------------------


@dataclass(slots=True)
class ReplicatorTestHelper:
    """Describe which replicator implementation is under test."""

    classname: Literal["GridFTPReplicator", "GlobusReplicator"]
    mod: Any  # imported module object
    transfer_mock: MagicMock  # the mocked backend transfer object

    dest_config_key: str
    timeout_config_key: str
    dest_attr: str
    timeout_attr: str
    expected_config_keys: list[str]
    has_transfer_reference: bool
    error_quarantines: bool

    def instantiate_replicator(self, *args: Any, **kwargs: Any) -> Any:
        """Instantiate replicator object."""
        cls = getattr(self.mod, self.classname)
        return cls(*args, **kwargs)


@pytest.fixture(
    params=[
        "lta.gridftp_replicator.GridFTPReplicator",
        "lta.globus_replicator.GlobusReplicator",
    ]
)
def rep_helper(
    request: pytest.FixtureRequest,
    monkeypatch: pytest.MonkeyPatch,
) -> ReplicatorTestHelper:
    """Import replicator module, stub Prometheus, and return metadata + class."""

    # Stub prometheus BEFORE import so module-level metrics use our stubs.
    class _Counter:
        def labels(self, **_: Any) -> Any:
            return self

        def inc(self, *a: Any, **k: Any) -> None:
            return None

    class _Gauge:
        def labels(self, **_: Any) -> Any:
            return self

        def set(self, *a: Any, **k: Any) -> None:
            return None

    monkeypatch.setattr(prometheus_client, "Counter", lambda *a, **k: _Counter())
    monkeypatch.setattr(prometheus_client, "Gauge", lambda *a, **k: _Gauge())

    def import_fresh(
        modname: Literal["lta.gridftp_replicator", "lta.globus_replicator"],
    ) -> Any:
        """Fresh import so the stubs take effect for each parametrized run."""
        sys.modules.pop(modname, None)
        return importlib.import_module(modname)

    # Make ReplicatorTestHelper instance
    match request.param:

        # GRIDFTP
        case "lta.gridftp_replicator.GridFTPReplicator":
            mod = import_fresh("lta.gridftp_replicator")

            class _DummyProxy:
                def update_proxy(self) -> None:
                    return None

            monkeypatch.setattr(mod, "SiteGlobusProxy", _DummyProxy)

            # GridFTPReplicator calls module-level GridFTP.put(...)
            xfer_mock = MagicMock()
            gridftp_obj = SimpleNamespace(put=xfer_mock)
            monkeypatch.setattr(mod, "GridFTP", gridftp_obj)

            rep_helper = ReplicatorTestHelper(
                classname="GridFTPReplicator",
                mod=mod,
                transfer_mock=xfer_mock,
                dest_config_key="GRIDFTP_DEST_URLS",
                timeout_config_key="GRIDFTP_TIMEOUT",
                dest_attr="gridftp_dest_urls",
                timeout_attr="gridftp_timeout",
                expected_config_keys=[
                    "GRIDFTP_DEST_URLS",
                    "GRIDFTP_TIMEOUT",
                    "USE_FULL_BUNDLE_PATH",
                    "WORK_RETRIES",
                    "WORK_TIMEOUT_SECONDS",
                ],
                has_transfer_reference=False,
                error_quarantines=False,
            )

        # GLOBUS
        case "lta.globus_replicator.GlobusReplicator":
            mod = import_fresh("lta.globus_replicator")

            # GlobusReplicator instantiates GlobusTransfer and calls transfer_file(...)
            xfer_mock = MagicMock()
            xfer_mock.return_value = "TASK-123"

            class _GlobusStub:
                def __init__(self) -> None:
                    self.transfer_file = xfer_mock

            monkeypatch.setattr(mod, "GlobusTransfer", _GlobusStub)

            rep_helper = ReplicatorTestHelper(
                classname="GlobusReplicator",
                mod=mod,
                transfer_mock=xfer_mock,
                dest_config_key="GLOBUS_DEST_URL",
                timeout_config_key="GLOBUS_TIMEOUT",
                dest_attr="globus_dest_url",
                timeout_attr="globus_timeout",
                expected_config_keys=[
                    "GLOBUS_DEST_URL",
                    "GLOBUS_TIMEOUT",
                    "USE_FULL_BUNDLE_PATH",
                    "WORK_RETRIES",
                    "WORK_TIMEOUT_SECONDS",
                ],
                has_transfer_reference=True,
                error_quarantines=True,
            )

        # ???
        case _:
            raise AssertionError(f"Unknown replicator implementation: {request.param}")

    return rep_helper


# --------------------------------------------------------------------------------------
# Fixtures & Helpers
# --------------------------------------------------------------------------------------


@pytest.fixture
def base_config(rep_helper: ReplicatorTestHelper) -> dict[str, str]:
    """Minimal, valid config for either replication component."""
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
    }

    match rep_helper.classname:
        case "GridFTPReplicator":
            cfg[rep_helper.dest_config_key] = (
                "gsiftp://dest.example.org:2811/data;"
                "gsiftp://alt.example.org:2811/data"
            )
        case "GlobusReplicator":
            cfg[rep_helper.dest_config_key] = "globus://dest.example.org/collection"
        case _:
            raise AssertionError(
                f"Unhandled impl in base_config: {rep_helper.classname}"
            )

    cfg[rep_helper.timeout_config_key] = "1200"
    return cfg


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


@pytest.fixture
def mock_join(
    rep_helper: ReplicatorTestHelper,
    monkeypatch: pytest.MonkeyPatch,
) -> Callable[[list[str]], str]:
    """Mock join_smart_url to a predictable path joiner."""

    def _join(parts: list[str]) -> str:
        return "/".join(s.strip("/") for s in parts if s is not None)

    monkeypatch.setattr(rep_helper.mod, "join_smart_url", _join)
    return _join


@pytest.fixture
def mock_now(
    rep_helper: ReplicatorTestHelper,
    monkeypatch: pytest.MonkeyPatch,
) -> str:
    """Freeze now() to a stable string."""
    ts = "2025-01-01T00:00:00Z"
    monkeypatch.setattr(rep_helper.mod, "now", lambda: ts)
    return ts


# --------------------------------------------------------------------------------------
# Tests
# --------------------------------------------------------------------------------------


def test_000_expected_config_has_keys(rep_helper: ReplicatorTestHelper) -> None:
    """EXPECTED_CONFIG should include keys this component relies on."""
    for key in rep_helper.expected_config_keys:
        assert key in rep_helper.mod.EXPECTED_CONFIG


def test_010_init_parses_config(
    rep_helper: ReplicatorTestHelper,
    base_config: dict[str, str],
    logger: Any,
) -> None:
    """__init__ should parse and coerce config values correctly."""
    rep = rep_helper.instantiate_replicator(base_config, logger)

    dest_attr_value = getattr(rep, rep_helper.dest_attr)
    match rep_helper.classname:
        case "GridFTPReplicator":
            assert dest_attr_value == base_config[rep_helper.dest_config_key].split(";")
        case "GlobusReplicator":
            assert dest_attr_value == base_config[rep_helper.dest_config_key]
        case _:
            raise AssertionError(f"Unhandled impl in init test: {rep_helper.classname}")

    timeout_value = getattr(rep, rep_helper.timeout_attr)
    assert timeout_value == int(base_config[rep_helper.timeout_config_key])

    assert rep.use_full_bundle_path is False
    assert rep.work_retries == 3
    assert rep.work_timeout_seconds == 30.0


@pytest.mark.asyncio
async def test_020_do_status_empty(
    rep_helper: ReplicatorTestHelper,
    base_config: dict[str, str],
    logger: Any,
) -> None:
    """_do_status should return an empty dict."""
    rep = rep_helper.instantiate_replicator(base_config, logger)
    assert rep._do_status() == {}


@pytest.mark.asyncio
async def test_030_do_work_claim_no_bundle_returns_false(
    rep_helper: ReplicatorTestHelper,
    base_config: dict[str, str],
    logger: Any,
) -> None:
    """When the DB returns no bundle, _do_work_claim should return False."""
    rep = rep_helper.instantiate_replicator(base_config, logger)
    rc = DummyRestClient(responses=[{"bundle": None}])

    got = await rep._do_work_claim(rc)  # type: ignore[arg-type]
    assert got is False
    assert any(url.startswith("/Bundles/actions/pop") for _, url, _ in rc.calls)


@pytest.mark.asyncio
async def test_040_do_work_claim_success_calls_transfer_and_patch(
    rep_helper: ReplicatorTestHelper,
    base_config: dict[str, str],
    logger: Any,
    mock_join: Callable[[list[str]], str],
    mock_now: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """On success, replicator should invoke transfer and PATCH the bundle."""
    rep = rep_helper.instantiate_replicator(base_config, logger)

    bundle: dict[str, Any] = {
        "uuid": "B-123",
        "status": "completed",
        "bundle_path": "/mnt/lfss/jade-lta/bundler_out/foo.zip",
        "path": "/data/exp/IceCube/2015/bar",
    }
    rc = DummyRestClient(responses=[{"bundle": bundle}, {}])

    match rep_helper.classname:
        case "GridFTPReplicator":
            # GridFTP chooses among multiple dest URLs; make deterministic.
            monkeypatch.setattr(
                rep_helper.mod.random,  # type: ignore[attr-defined]
                "choice",
                lambda urls: urls[0],
            )
        case "GlobusReplicator":
            # GlobusReplicator uses a single dest URL, no random choice.
            pass
        case _:
            raise AssertionError(
                f"Unhandled impl in success test: {rep_helper.classname}"
            )

    ok = await rep._do_work_claim(rc)  # type: ignore[arg-type]
    assert ok is True

    rep_helper.transfer_mock.assert_called_once()
    args, kwargs = rep_helper.transfer_mock.call_args

    match rep_helper.classname:
        case "GridFTPReplicator":
            dest_url = args[0]
            src_path = kwargs["filename"]
            timeout = kwargs["request_timeout"]
        case "GlobusReplicator":
            dest_url = kwargs["dest_url"]
            src_path = kwargs["source_path"]
            timeout = kwargs["request_timeout"]
        case _:
            raise AssertionError(
                f"Unhandled impl in success call inspection: {rep_helper.classname}"
            )

    assert src_path == bundle["bundle_path"]
    assert timeout == getattr(rep, rep_helper.timeout_attr)
    assert dest_url.endswith("/foo.zip")

    # Verify PATCH
    patch_calls = [
        c for c in rc.calls if c[0] == "PATCH" and c[1].startswith("/Bundles/")
    ]
    assert patch_calls
    _, url, body = patch_calls[0]
    assert url == "/Bundles/B-123"
    assert body.get("claimed") is False
    assert body.get("status") == rep.output_status
    assert body.get("reason") == ""

    if rep_helper.has_transfer_reference:
        assert (
            body.get("transfer_reference")
            == f"globus/{rep_helper.transfer_mock.return_value}"
        )
    else:
        assert "transfer_reference" not in body


@pytest.mark.asyncio
async def test_050_do_work_claim_transfer_error_behaviour(
    rep_helper: ReplicatorTestHelper,
    base_config: dict[str, str],
    logger: Any,
    mock_join: Callable[[list[str]], str],
    mock_now: str,
) -> None:
    """
    If the transfer raises, behaviour differs:

    - GridFTPReplicator: logs error and still marks bundle as successful.
    - GlobusReplicator: quarantines the bundle and returns False.
    """
    rep = rep_helper.instantiate_replicator(base_config, logger)

    bundle: dict[str, Any] = {
        "uuid": "B-ERR",
        "status": "completed",
        "bundle_path": "/mnt/lfss/jade-lta/bundler_out/bad.zip",
        "path": "/data/exp/IceCube/2015/baz",
    }
    rc = DummyRestClient(responses=[{"bundle": bundle}, {}])

    def _raise(*_a: Any, **_k: Any) -> None:
        raise RuntimeError("boom")

    rep_helper.transfer_mock.side_effect = _raise

    ok = await rep._do_work_claim(rc)  # type: ignore[arg-type]
    patch_calls = [c for c in rc.calls if c[0] == "PATCH"]
    assert patch_calls

    match rep_helper.classname:
        case "GlobusReplicator":
            # Globus path: quarantined + False
            assert ok is False
            quarantine_calls = [c for c in patch_calls if c[1] == "/Bundles/B-ERR"]
            assert quarantine_calls
            _, _, body = quarantine_calls[0]
            assert body.get("status") == "quarantined"
            assert "BY:replicator-" in body.get("reason", "")
        case "GridFTPReplicator":
            # GridFTP path: success + not quarantined
            assert ok is True
            success_calls = [c for c in patch_calls if c[1] == "/Bundles/B-ERR"]
            assert success_calls
            _, _, body = success_calls[0]
            assert body.get("status") == rep.output_status
            assert body.get("claimed") is False
            assert body.get("reason") == ""
            assert body.get("status") != "quarantined"
        case _:
            raise AssertionError(
                f"Unhandled impl in error behaviour test: {rep_helper.classname}"
            )


@pytest.mark.asyncio
async def test_060_do_work_runs_until_no_work(
    rep_helper: ReplicatorTestHelper,
    base_config: dict[str, str],
    logger: Any,
) -> None:
    """_do_work should loop until _do_work_claim returns False."""
    rep = rep_helper.instantiate_replicator(base_config, logger)

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
    rep_helper: ReplicatorTestHelper,
    base_config: dict[str, str],
    logger: Any,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """If run_once_and_die is set, _do_work should call sys.exit after one claim attempt."""
    rep = rep_helper.instantiate_replicator(base_config, logger)
    rep.run_once_and_die = True

    async def _fake_claim(_rc: DummyRestClient) -> bool:
        return False

    rep._do_work_claim = _fake_claim  # type: ignore[assignment]

    exit_called: dict[str, bool] = {"flag": False}

    def _fake_exit(*_a: Any, **_k: Any) -> None:
        exit_called["flag"] = True
        raise SystemExit

    # Patch the sys.exit used by this module
    monkeypatch.setattr(rep_helper.mod.sys, "exit", _fake_exit)

    with pytest.raises(SystemExit):
        await rep._do_work(DummyRestClient())  # type: ignore[arg-type]

    assert exit_called["flag"] is True


@pytest.mark.asyncio
async def test_080_replication_use_full_bundle_path_true(
    rep_helper: ReplicatorTestHelper,
    base_config: dict[str, str],
    logger: Any,
    mock_join: Callable[[list[str]], str],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """When USE_FULL_BUNDLE_PATH is TRUE, dest includes bundle['path'] + basename."""
    cfg = dict(base_config)
    cfg["USE_FULL_BUNDLE_PATH"] = "TRUE"
    rep = rep_helper.instantiate_replicator(cfg, logger)

    bundle: dict[str, Any] = {
        "uuid": "B-456",
        "status": "completed",
        "bundle_path": "/mnt/lfss/jade-lta/bundler_out/bar.zip",
        "path": "/data/exp/IC/2015/filtered/level2/0320",
    }
    rc = DummyRestClient(responses=[{"bundle": bundle}, {}])

    match rep_helper.classname:
        case "GridFTPReplicator":
            monkeypatch.setattr(
                rep_helper.mod.random,  # type: ignore[attr-defined]
                "choice",
                lambda urls: urls[0],
            )
        case "GlobusReplicator":
            pass
        case _:
            raise AssertionError(
                f"Unhandled impl in USE_FULL_BUNDLE_PATH true: {rep_helper.classname}"
            )

    ok = await rep._do_work_claim(rc)  # type: ignore[arg-type]
    assert ok is True

    rep_helper.transfer_mock.assert_called_once()
    args, kwargs = rep_helper.transfer_mock.call_args

    match rep_helper.classname:
        case "GridFTPReplicator":
            dest_url = args[0]
        case "GlobusReplicator":
            dest_url = kwargs["dest_url"]
        case _:
            raise AssertionError(
                f"Unhandled impl in USE_FULL_BUNDLE_PATH true call: {rep_helper.classname}"
            )

    assert "/data/exp/IC/2015/filtered/level2/0320/bar.zip" in dest_url


@pytest.mark.asyncio
async def test_090_replication_use_full_bundle_path_false(
    rep_helper: ReplicatorTestHelper,
    base_config: dict[str, str],
    logger: Any,
    mock_join: Callable[[list[str]], str],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """When USE_FULL_BUNDLE_PATH is FALSE, destination includes only the basename."""
    cfg = dict(base_config)
    cfg["USE_FULL_BUNDLE_PATH"] = "FALSE"
    rep = rep_helper.instantiate_replicator(cfg, logger)

    bundle: dict[str, Any] = {
        "uuid": "B-789",
        "status": "completed",
        "bundle_path": "/mnt/lfss/jade-lta/bundler_out/baz.zip",
        "path": "/data/exp/IC/irrelevant/when/false",
    }
    rc = DummyRestClient(responses=[{"bundle": bundle}, {}])

    match rep_helper.classname:
        case "GridFTPReplicator":
            monkeypatch.setattr(
                rep_helper.mod.random,  # type: ignore[attr-defined]
                "choice",
                lambda urls: urls[0],
            )
        case "GlobusReplicator":
            pass
        case _:
            raise AssertionError(
                f"Unhandled impl in USE_FULL_BUNDLE_PATH false: {rep_helper.classname}"
            )

    ok = await rep._do_work_claim(rc)  # type: ignore[arg-type]
    assert ok is True

    rep_helper.transfer_mock.assert_called_once()
    args, kwargs = rep_helper.transfer_mock.call_args

    match rep_helper.classname:
        case "GridFTPReplicator":
            dest_url = args[0]
        case "GlobusReplicator":
            dest_url = kwargs["dest_url"]
        case _:
            raise AssertionError(
                f"Unhandled impl in USE_FULL_BUNDLE_PATH false call: {rep_helper.classname}"
            )

    assert dest_url.endswith("/baz.zip")
    assert "irrelevant" not in dest_url
