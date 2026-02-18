# test_nersc_verifier.py
"""Unit tests for lta/nersc_verifier.py."""
from pathlib import Path

# fmt:off

# -----------------------------------------------------------------------------
# reset prometheus registry for unit tests
from prometheus_client import REGISTRY

from lta.utils import HSICommandFailedException, InvalidChecksumException

collectors = list(REGISTRY._collector_to_names.keys())
for collector in collectors:
    REGISTRY.unregister(collector)
from prometheus_client import gc_collector, platform_collector, process_collector
process_collector.ProcessCollector()
platform_collector.PlatformCollector()
gc_collector.GCCollector()
# -----------------------------------------------------------------------------

from typing import Dict
from unittest.mock import AsyncMock, call, MagicMock
from uuid import uuid1

import pytest
from pytest import MonkeyPatch
from pytest_mock import MockerFixture
from tornado.web import HTTPError

from lta.nersc_verifier import main_sync, NerscVerifier
from .utils import ObjectLiteral

TestConfig = Dict[str, str]


@pytest.fixture
def config() -> TestConfig:
    """Supply a stock NerscVerifier component configuration."""
    return {
        "CLIENT_ID": "long-term-archive",
        "CLIENT_SECRET": "hunter2",  # http://bash.org/?244321
        "COMPONENT_NAME": "testing-nersc_verifier",
        "DEST_SITE": "NERSC",
        "HPSS_AVAIL_PATH": "/path/to/hpss_avail.py",
        "INPUT_STATUS": "verifying",
        "LOG_LEVEL": "DEBUG",
        "LTA_AUTH_OPENID_URL": "localhost:12345",
        "LTA_REST_URL": "localhost:12347",
        "OUTPUT_STATUS": "completed",
        "PROMETHEUS_METRICS_PORT": "8080",
        "RUN_ONCE_AND_DIE": "False",
        "RUN_UNTIL_NO_WORK": "False",
        "SOURCE_SITE": "WIPAC",
        "TAPE_BASE_PATH": "/path/to/hpss",
        "WORK_RETRIES": "3",
        "WORK_SLEEP_DURATION_SECONDS": "60",
        "WORK_TIMEOUT_SECONDS": "30",
    }


def test_constructor_config(config: TestConfig, mocker: MockerFixture) -> None:
    """Test that a NerscVerifier can be constructed with a configuration object and a logging object."""
    logger_mock = mocker.MagicMock()
    p = NerscVerifier(config, logger_mock)
    assert p.name == "testing-nersc_verifier"
    assert p.lta_auth_openid_url == "localhost:12345"
    assert p.lta_rest_url == "localhost:12347"
    assert p.source_site == "WIPAC"
    assert p.tape_base_path == "/path/to/hpss"
    assert p.work_retries == 3
    assert p.work_sleep_duration_seconds == 60
    assert p.work_timeout_seconds == 30
    assert p.logger == logger_mock


def test_do_status(config: TestConfig, mocker: MockerFixture) -> None:
    """Verify that the NerscVerifier has no additional state to offer."""
    logger_mock = mocker.MagicMock()
    p = NerscVerifier(config, logger_mock)
    assert p._do_status() == {}


@pytest.mark.asyncio
async def test_nersc_verifier_logs_configuration(mocker: MockerFixture) -> None:
    """Test to make sure the NerscVerifier logs its configuration."""
    logger_mock = mocker.MagicMock()
    nersc_verifier_config = {
        "CLIENT_ID": "long-term-archive",
        "CLIENT_SECRET": "hunter2",  # http://bash.org/?244321
        "COMPONENT_NAME": "logme-testing-nersc_verifier",
        "DEST_SITE": "NERSC",
        "HPSS_AVAIL_PATH": "/log/me/path/to/hpss_avail.py",
        "INPUT_STATUS": "verifying",
        "LOG_LEVEL": "DEBUG",
        "LTA_AUTH_OPENID_URL": "localhost:12345",
        "LTA_REST_URL": "logme-http://RmMNHdPhHpH2ZxfaFAC9d2jiIbf5pZiHDqy43rFLQiM.com/",
        "OUTPUT_STATUS": "completed",
        "PROMETHEUS_METRICS_PORT": "8080",
        "RUN_ONCE_AND_DIE": "False",
        "RUN_UNTIL_NO_WORK": "False",
        "SOURCE_SITE": "WIPAC",
        "TAPE_BASE_PATH": "/logme/path/to/hpss",
        "WORK_RETRIES": "5",
        "WORK_SLEEP_DURATION_SECONDS": "70",
        "WORK_TIMEOUT_SECONDS": "90",
    }
    NerscVerifier(nersc_verifier_config, logger_mock)
    EXPECTED_LOGGER_CALLS = [
        call("nersc_verifier 'logme-testing-nersc_verifier' is configured:"),
        call('CLIENT_ID = long-term-archive'),
        call('CLIENT_SECRET = [秘密]'),
        call('COMPONENT_NAME = logme-testing-nersc_verifier'),
        call('DEST_SITE = NERSC'),
        call('HPSS_AVAIL_PATH = /log/me/path/to/hpss_avail.py'),
        call('INPUT_STATUS = verifying'),
        call('LOG_LEVEL = DEBUG'),
        call('LTA_AUTH_OPENID_URL = localhost:12345'),
        call('LTA_REST_URL = logme-http://RmMNHdPhHpH2ZxfaFAC9d2jiIbf5pZiHDqy43rFLQiM.com/'),
        call('OUTPUT_STATUS = completed'),
        call('PROMETHEUS_METRICS_PORT = 8080'),
        call('RUN_ONCE_AND_DIE = False'),
        call('RUN_UNTIL_NO_WORK = False'),
        call('SOURCE_SITE = WIPAC'),
        call('TAPE_BASE_PATH = /logme/path/to/hpss'),
        call('WORK_RETRIES = 5'),
        call('WORK_SLEEP_DURATION_SECONDS = 70'),
        call('WORK_TIMEOUT_SECONDS = 90')
    ]
    logger_mock.info.assert_has_calls(EXPECTED_LOGGER_CALLS)


@pytest.mark.asyncio
async def test_script_main_sync(config: TestConfig, mocker: MockerFixture, monkeypatch: MonkeyPatch) -> None:
    """
    Verify NerscVerifier component behavior when run as a script.

    Test to make sure running the NerscVerifier as a script does the setup work
    that we expect and then launches the nersc_verifier service.
    """
    for key in config.keys():
        monkeypatch.setenv(key, config[key])
    mock_run = mocker.patch("asyncio.run")
    mock_main = mocker.patch("lta.nersc_verifier.main")
    mock_shs = mocker.patch("lta.nersc_verifier.start_http_server")
    main_sync()
    mock_shs.assert_called()
    mock_main.assert_called()
    mock_run.assert_called()
    await mock_run.call_args.args[0]


@pytest.mark.asyncio
async def test_nersc_verifier_run(config: TestConfig, mocker: MockerFixture) -> None:
    """Test the NerscVerifier does the work the nersc_verifier should do."""
    logger_mock = mocker.MagicMock()
    p = NerscVerifier(config, logger_mock)
    p._do_work = AsyncMock()  # type: ignore[method-assign]
    await p.run()
    p._do_work.assert_called()


@pytest.mark.asyncio
async def test_nersc_verifier_run_exception(config: TestConfig, mocker: MockerFixture) -> None:
    """Test an error doesn't kill the NerscVerifier."""
    logger_mock = mocker.MagicMock()
    p = NerscVerifier(config, logger_mock)
    p._do_work = AsyncMock()  # type: ignore[method-assign]
    p._do_work.side_effect = [Exception("bad thing happen!")]
    await p.run()
    p._do_work.assert_called()


@pytest.mark.asyncio
async def test_nersc_verifier_hpss_not_available(config: TestConfig, mocker: MockerFixture) -> None:
    """Test that a bad returncode on hpss_avail will prevent work."""
    logger_mock = mocker.MagicMock()
    run_mock = mocker.patch("lta.nersc_verifier.run", new_callable=MagicMock)
    run_mock.return_value = ObjectLiteral(
        returncode=1,
        args=["/usr/common/software/bin/hpss_avail", "archive"],
        stdout="some text on stdout",
        stderr="some text on stderr",
    )
    p = NerscVerifier(config, logger_mock)
    assert not await p._do_work_claim(AsyncMock())


@pytest.mark.asyncio
async def test_nersc_verifier_do_work_pop_exception(config: TestConfig, mocker: MockerFixture) -> None:
    """Test that _do_work raises when the RestClient can't pop."""
    logger_mock = mocker.MagicMock()
    run_mock = mocker.patch("lta.nersc_verifier.run", new_callable=MagicMock)
    run_mock.side_effect = [
        ObjectLiteral(
            returncode=0,
            args=["/usr/common/software/bin/hpss_avail", "archive"],
            stdout="some text on stdout",
            stderr="some text on stderr",
        ),
        ObjectLiteral(
            returncode=0,
            args=["/usr/bin/which", "hsi"],
            stdout="some text on stdout",
            stderr="some text on stderr",
        ),
    ]
    lta_rc_mock = AsyncMock()
    lta_rc_mock.request = AsyncMock()
    lta_rc_mock.request.side_effect = HTTPError(500, "LTA DB on fire. Again.")
    p = NerscVerifier(config, logger_mock)
    with pytest.raises(HTTPError):
        await p._do_work(lta_rc_mock)
    lta_rc_mock.request.assert_called_with("POST", '/Bundles/actions/pop?source=WIPAC&dest=NERSC&status=verifying', {'claimant': f'{p.name}-{p.instance_uuid}'})


@pytest.mark.asyncio
async def test_nersc_verifier_do_work_no_results(config: TestConfig, mocker: MockerFixture) -> None:
    """Test that _do_work goes on vacation when the LTA DB has no work."""
    logger_mock = mocker.MagicMock()
    dwc_mock = mocker.patch("lta.nersc_verifier.NerscVerifier._do_work_claim", new_callable=AsyncMock)
    dwc_mock.return_value = False
    p = NerscVerifier(config, logger_mock)
    await p._do_work(AsyncMock())
    dwc_mock.assert_called()


@pytest.mark.asyncio
async def test_nersc_verifier_do_work_yes_results(config: TestConfig, mocker: MockerFixture) -> None:
    """Test that _do_work keeps working until the LTA DB has no work."""
    logger_mock = mocker.MagicMock()
    dwc_mock = mocker.patch("lta.nersc_verifier.NerscVerifier._do_work_claim", new_callable=AsyncMock)
    dwc_mock.side_effect = [True, True, False]
    p = NerscVerifier(config, logger_mock)
    await p._do_work(AsyncMock())
    assert dwc_mock.call_count == 3


@pytest.mark.asyncio
async def test_nersc_verifier_do_work_claim_no_result(config: TestConfig, mocker: MockerFixture) -> None:
    """Test that _do_work_claim does not work when the LTA DB has no work."""
    logger_mock = mocker.MagicMock()
    run_mock = mocker.patch("lta.nersc_verifier.run", new_callable=MagicMock)
    run_mock.side_effect = [
        ObjectLiteral(
            returncode=0,
            args=["/usr/common/software/bin/hpss_avail", "archive"],
            stdout="some text on stdout",
            stderr="some text on stderr",
        ),
        ObjectLiteral(
            returncode=0,
            args=["/usr/bin/which", "hsi"],
            stdout="some text on stdout",
            stderr="some text on stderr",
        ),
    ]
    lta_rc_mock = AsyncMock()
    lta_rc_mock.request = AsyncMock()
    lta_rc_mock.request.return_value = {
        "bundle": None
    }
    vbih_mock = mocker.patch("lta.nersc_verifier.NerscVerifier._verify_bundle_in_hpss", new_callable=MagicMock)
    p = NerscVerifier(config, logger_mock)
    await p._do_work_claim(lta_rc_mock)
    lta_rc_mock.request.assert_called_with("POST", '/Bundles/actions/pop?source=WIPAC&dest=NERSC&status=verifying', {'claimant': f'{p.name}-{p.instance_uuid}'})
    vbih_mock.assert_not_called()


@pytest.mark.asyncio
async def test_nersc_verifier_do_work_claim_yes_result(config: TestConfig, mocker: MockerFixture) -> None:
    """Test that _do_work_claim processes the Bundle that it gets from the LTA DB."""
    logger_mock = mocker.MagicMock()
    run_mock = mocker.patch("lta.nersc_verifier.run", new_callable=MagicMock)
    run_mock.side_effect = [
        ObjectLiteral(
            returncode=0,
            args=["/usr/common/software/bin/hpss_avail", "archive"],
            stdout="some text on stdout",
            stderr="some text on stderr",
        ),
        ObjectLiteral(
            returncode=0,
            args=["/usr/bin/which", "hsi"],
            stdout="some text on stdout",
            stderr="some text on stderr",
        ),
    ]
    lta_rc_mock = AsyncMock()
    lta_rc_mock.request = AsyncMock()
    lta_rc_mock.request.return_value = {
        "bundle": {
            "one": 1,
        },
    }
    vbih_mock = mocker.patch("lta.nersc_verifier.NerscVerifier._verify_bundle_in_hpss", new_callable=MagicMock)
    ubild_mock = mocker.patch("lta.nersc_verifier.NerscVerifier._update_bundle_in_lta_db", new_callable=AsyncMock)
    p = NerscVerifier(config, logger_mock)
    assert await p._do_work_claim(lta_rc_mock)
    lta_rc_mock.request.assert_called_with("POST", '/Bundles/actions/pop?source=WIPAC&dest=NERSC&status=verifying', {'claimant': f'{p.name}-{p.instance_uuid}'})
    vbih_mock.assert_called_with({"one": 1})
    ubild_mock.assert_called_with(lta_rc_mock, {"one": 1}, vbih_mock.return_value)


@pytest.mark.asyncio
async def test_nersc_verifier_do_work_claim_exception_caught(config: TestConfig, mocker: MockerFixture) -> None:
    """Test that _do_work_claim quarantines a Bundle if it catches an Exception."""
    logger_mock = mocker.MagicMock()
    run_mock = mocker.patch("lta.nersc_verifier.run", new_callable=MagicMock)
    run_mock.side_effect = [
        ObjectLiteral(
            returncode=0,
            args=["/usr/common/software/bin/hpss_avail", "archive"],
            stdout="some text on stdout",
            stderr="some text on stderr",
        ),
        ObjectLiteral(
            returncode=0,
            args=["/usr/bin/which", "hsi"],
            stdout="some text on stdout",
            stderr="some text on stderr",
        ),
    ]
    lta_rc_mock = AsyncMock()
    lta_rc_mock.request = AsyncMock()
    lta_rc_mock.request.side_effect = [
        {
            "bundle": {
                "uuid": "45ae2ad39c664fda86e5981be0976d9c",
                "status": "verifying",
                "one": 1,
            },
        },
        {}
    ]
    vbih_mock = mocker.patch("lta.nersc_verifier.NerscVerifier._verify_bundle_in_hpss", new_callable=MagicMock)
    exc = Exception("Database totally on fire, guys")
    vbih_mock.side_effect = exc
    p = NerscVerifier(config, logger_mock)
    with pytest.raises(type(exc)) as excinfo:
        assert not await p._do_work_claim(lta_rc_mock)
    assert excinfo.value == exc
    lta_rc_mock.request.assert_called_with("PATCH", '/Bundles/45ae2ad39c664fda86e5981be0976d9c', mocker.ANY)
    vbih_mock.assert_called_with(
        {"uuid": "45ae2ad39c664fda86e5981be0976d9c", "status": "verifying", "one": 1}
    )


@pytest.mark.asyncio
async def test_nersc_verifier_update_bundle_in_lta_db(config: TestConfig, mocker: MockerFixture) -> None:
    """Test that _update_bundle_in_lta_db updates the bundle as verified in the LTA DB."""
    logger_mock = mocker.MagicMock()
    bundle = {"uuid": "7ec8a8f9-fae3-4f25-ae54-c1f66014f5ef"}
    lta_mock = mocker.MagicMock()
    lta_rc_mock = mocker.patch("rest_tools.client.RestClient.request", new_callable=AsyncMock)
    lta_rc_mock.return_value = True
    lta_mock.request = lta_rc_mock
    p = NerscVerifier(config, logger_mock)
    assert await p._update_bundle_in_lta_db(lta_mock, bundle, Path("foo"))
    lta_rc_mock.assert_called_with("PATCH", '/Bundles/7ec8a8f9-fae3-4f25-ae54-c1f66014f5ef', mocker.ANY)


@pytest.mark.asyncio
async def test_nersc_verifier_verify_bundle_in_hpss_success_no_quarantine(config: TestConfig, mocker: MockerFixture) -> None:
    """Test that _verify_bundle_in_hpss does not quarantine a bundle if the HSI command succeeds."""
    logger_mock = mocker.MagicMock()
    bundle = {
        "uuid": "7ec8a8f9-fae3-4f25-ae54-c1f66014f5ef",
        "path": "/data/exp/IceCube/2019/filtered/PFFilt/1109",
        "bundle_path": "/path/to/source/rse/7ec8a8f9-fae3-4f25-ae54-c1f66014f5ef.zip",
        "checksum": {
            "sha512": "97de2a6ad728f50a381eb1be6ecf015019887fac27e8bf608334fb72caf8d3f654fdcce68c33b0f0f27de499b84e67b8357cd81ef7bba3cdaa9e23a648f43ad2",
        },
    }
    run_mock = mocker.patch("lta.nersc_verifier.run", new_callable=MagicMock)
    run_mock.side_effect = [
        ObjectLiteral(
            returncode=0,
            args=["hsi", "-q", "hashlist", "/home/projects/icecube/data/exp/IceCube/2019/filtered/PFFilt/1109/7ec8a8f9-fae3-4f25-ae54-c1f66014f5ef.zip"],
            stdout=b"97de2a6ad728f50a381eb1be6ecf015019887fac27e8bf608334fb72caf8d3f654fdcce68c33b0f0f27de499b84e67b8357cd81ef7bba3cdaa9e23a648f43ad2 sha512 /home/projects/icecube/data/exp/IceCube/2019/filtered/PFFilt/1109/7ec8a8f9-fae3-4f25-ae54-c1f66014f5ef.zip [hsi]\n",
            stderr=b"",
        ),
        ObjectLiteral(
            returncode=0,
            args=["hsi", "-q", "hashverify", "-A", "/home/projects/icecube/data/exp/IceCube/2019/filtered/PFFilt/1109/7ec8a8f9-fae3-4f25-ae54-c1f66014f5ef.zip"],
            stdout=b"/home/projects/icecube/data/exp/IceCube/2018/unbiased/PFDST/1230/50145c5c-01e1-4727-a9a1-324e5af09a29.zip: (sha512) OK\n",
            stderr=b"",
        ),
    ]
    p = NerscVerifier(config, logger_mock)
    p._verify_bundle_in_hpss(bundle)
    assert run_mock.call_count == 2


@pytest.mark.asyncio
async def test_nersc_verifier_verify_bundle_in_hpss_hsi_failure_quarantine(config: TestConfig, mocker: MockerFixture) -> None:
    """Test that _verify_bundle_in_hpss quarantines a bundle if the HSI command fails."""
    logger_mock = mocker.MagicMock()
    bundle = {
        "uuid": "7ec8a8f9-fae3-4f25-ae54-c1f66014f5ef",
        "path": "/data/exp/IceCube/2019/filtered/PFFilt/1109",
        "bundle_path": "/path/to/source/rse/7ec8a8f9-fae3-4f25-ae54-c1f66014f5ef.zip",
        "checksum": {
            "sha512": "97de2a6ad728f50a381eb1be6ecf015019887fac27e8bf608334fb72caf8d3f654fdcce68c33b0f0f27de499b84e67b8357cd81ef7bba3cdaa9e23a648f43ad2",
        },
        "status": "verifying",
    }
    run_mock = mocker.patch("lta.nersc_verifier.run", new_callable=MagicMock)
    run_mock.side_effect = [
        ObjectLiteral(
            returncode=1,
            args=["hsi", "-q", "hashlist", "/home/projects/icecube/data/exp/IceCube/2019/filtered/PFFilt/1109/7ec8a8f9-fae3-4f25-ae54-c1f66014f5ef.zip"],
            stdout=b"",
            stderr=b"",
        ),
    ]
    p = NerscVerifier(config, logger_mock)
    with pytest.raises(HSICommandFailedException) as excinfo:
        p._verify_bundle_in_hpss(bundle)
    assert "list checksum in HPSS (hashlist)" in str(excinfo.value)
    assert run_mock.call_count == 1


@pytest.mark.asyncio
async def test_nersc_verifier_verify_bundle_in_hpss_mismatch_checksum_quarantine(config: TestConfig, mocker: MockerFixture) -> None:
    """Test that _verify_bundle_in_hpss quarantines a bundle if the checksums do not match."""
    logger_mock = mocker.MagicMock()
    bundle = {
        "uuid": "7ec8a8f9-fae3-4f25-ae54-c1f66014f5ef",
        "path": "/data/exp/IceCube/2019/filtered/PFFilt/1109",
        "bundle_path": "/path/to/source/rse/7ec8a8f9-fae3-4f25-ae54-c1f66014f5ef.zip",
        "checksum": {
            "sha512": "97de2a6ad728f50a381eb1be6ecf015019887fac27e8bf608334fb72caf8d3f654fdcce68c33b0f0f27de499b84e67b8357cd81ef7bba3cdaa9e23a648f43ad2",
        },
        "status": "verifying",
    }
    run_mock = mocker.patch("lta.nersc_verifier.run", new_callable=MagicMock)
    run_mock.side_effect = [
        ObjectLiteral(
            returncode=0,
            args=["hsi", "-q", "hashlist", "/home/projects/icecube/data/exp/IceCube/2019/filtered/PFFilt/1109/7ec8a8f9-fae3-4f25-ae54-c1f66014f5ef.zip"],
            stdout=b"1693e9d0273e3a2995b917c0e72e6bd2f40ea677f3613b6d57eaa14bd3a285c73e8db8b6e556b886c3929afe324bcc718711f2faddfeb43c3e030d9afe697873 sha512 /home/projects/icecube/data/exp/IceCube/2018/unbiased/PFDST/1230/50145c5c-01e1-4727-a9a1-324e5af09a29.zip [hsi]\n",
            stderr=b"",
        ),
    ]
    p = NerscVerifier(config, logger_mock)
    with pytest.raises(InvalidChecksumException) as excinfo:
        p._verify_bundle_in_hpss(bundle)
    assert str(excinfo.value).startswith("Checksum mismatch between creation and destination:")
    assert run_mock.call_count == 1


@pytest.mark.asyncio
async def test_nersc_verifier_verify_bundle_in_hpss_failure_hashverify_quarantine(config: TestConfig, mocker: MockerFixture) -> None:
    """Test that _verify_bundle_in_hpss raises if the HSI hashverify command fails."""
    logger_mock = mocker.MagicMock()
    bundle = {
        "uuid": "7ec8a8f9-fae3-4f25-ae54-c1f66014f5ef",
        "path": "/data/exp/IceCube/2019/filtered/PFFilt/1109",
        "bundle_path": "/path/to/source/rse/7ec8a8f9-fae3-4f25-ae54-c1f66014f5ef.zip",
        "checksum": {
            "sha512": "97de2a6ad728f50a381eb1be6ecf015019887fac27e8bf608334fb72caf8d3f654fdcce68c33b0f0f27de499b84e67b8357cd81ef7bba3cdaa9e23a648f43ad2",
        },
        "status": "verifying",
    }
    run_mock = mocker.patch("lta.nersc_verifier.run", new_callable=MagicMock)
    run_mock.side_effect = [
        ObjectLiteral(
            returncode=0,
            args=["hsi", "-q", "hashlist", "/home/projects/icecube/data/exp/IceCube/2019/filtered/PFFilt/1109/7ec8a8f9-fae3-4f25-ae54-c1f66014f5ef.zip"],
            stdout=b"97de2a6ad728f50a381eb1be6ecf015019887fac27e8bf608334fb72caf8d3f654fdcce68c33b0f0f27de499b84e67b8357cd81ef7bba3cdaa9e23a648f43ad2 sha512 /home/projects/icecube/data/exp/IceCube/2019/filtered/PFFilt/1109/7ec8a8f9-fae3-4f25-ae54-c1f66014f5ef.zip [hsi]\n",
            stderr=b"",
        ),
        ObjectLiteral(
            returncode=1,
            args=["hsi", "-q", "hashverify", "-A", "/home/projects/icecube/data/exp/IceCube/2019/filtered/PFFilt/1109/7ec8a8f9-fae3-4f25-ae54-c1f66014f5ef.zip"],
            stdout=b"",
            stderr=b"",
        ),
    ]
    p = NerscVerifier(config, logger_mock)
    with pytest.raises(HSICommandFailedException) as excinfo:
        p._verify_bundle_in_hpss(bundle)
    assert "verify bundle in HPSS (hashverify)" in str(excinfo.value)
    assert run_mock.call_count == 2


@pytest.mark.asyncio
async def test_nersc_verifier_verify_bundle_in_hpss_hashverify_bad_type_quarantine(config: TestConfig, mocker: MockerFixture) -> None:
    """Test that _verify_bundle_in_hpss raises when hashverify output is not '(sha512) OK'."""
    logger_mock = mocker.MagicMock()
    bundle = {
        "uuid": "7ec8a8f9-fae3-4f25-ae54-c1f66014f5ef",
        "path": "/data/exp/IceCube/2019/filtered/PFFilt/1109",
        "bundle_path": "/path/to/source/rse/7ec8a8f9-fae3-4f25-ae54-c1f66014f5ef.zip",
        "checksum": {
            "sha512": "97de2a6ad728f50a381eb1be6ecf015019887fac27e8bf608334fb72caf8d3f654fdcce68c33b0f0f27de499b84e67b8357cd81ef7bba3cdaa9e23a648f43ad2",
        },
        "status": "verifying",
    }
    run_mock = mocker.patch("lta.nersc_verifier.run", new_callable=MagicMock)
    run_mock.side_effect = [
        ObjectLiteral(
            returncode=0,
            args=["hsi", "-q", "hashlist", "/home/projects/icecube/data/exp/IceCube/2019/filtered/PFFilt/1109/7ec8a8f9-fae3-4f25-ae54-c1f66014f5ef.zip"],
            stdout=b"97de2a6ad728f50a381eb1be6ecf015019887fac27e8bf608334fb72caf8d3f654fdcce68c33b0f0f27de499b84e67b8357cd81ef7bba3cdaa9e23a648f43ad2 sha512 /home/projects/icecube/data/exp/IceCube/2019/filtered/PFFilt/1109/7ec8a8f9-fae3-4f25-ae54-c1f66014f5ef.zip [hsi]\n",
            stderr=b"",
        ),
        ObjectLiteral(
            returncode=0,
            args=["hsi", "-q", "hashverify", "-A", "/home/projects/icecube/data/exp/IceCube/2019/filtered/PFFilt/1109/7ec8a8f9-fae3-4f25-ae54-c1f66014f5ef.zip"],
            stdout=b"/home/projects/icecube/data/exp/IceCube/2018/unbiased/PFDST/1230/50145c5c-01e1-4727-a9a1-324e5af09a29.zip: (sha256) OK\n",
            stderr=b"",
        ),
    ]
    p = NerscVerifier(config, logger_mock)
    with pytest.raises(InvalidChecksumException) as excinfo:
        p._verify_bundle_in_hpss(bundle)
    assert str(excinfo.value).startswith("Checksum mismatch between creation and destination:")
    assert run_mock.call_count == 2


@pytest.mark.asyncio
async def test_nersc_verifier_verify_bundle_in_hpss_hashverify_bad_result_quarantine(config: TestConfig, mocker: MockerFixture) -> None:
    """Test that _verify_bundle_in_hpss raises when hashverify output is not '(sha512) OK'."""
    logger_mock = mocker.MagicMock()
    bundle = {
        "uuid": "7ec8a8f9-fae3-4f25-ae54-c1f66014f5ef",
        "path": "/data/exp/IceCube/2019/filtered/PFFilt/1109",
        "bundle_path": "/path/to/source/rse/7ec8a8f9-fae3-4f25-ae54-c1f66014f5ef.zip",
        "checksum": {
            "sha512": "97de2a6ad728f50a381eb1be6ecf015019887fac27e8bf608334fb72caf8d3f654fdcce68c33b0f0f27de499b84e67b8357cd81ef7bba3cdaa9e23a648f43ad2",
        },
        "status": "verifying",
    }
    run_mock = mocker.patch("lta.nersc_verifier.run", new_callable=MagicMock)
    run_mock.side_effect = [
        ObjectLiteral(
            returncode=0,
            args=["hsi", "-q", "hashlist", "/home/projects/icecube/data/exp/IceCube/2019/filtered/PFFilt/1109/7ec8a8f9-fae3-4f25-ae54-c1f66014f5ef.zip"],
            stdout=b"97de2a6ad728f50a381eb1be6ecf015019887fac27e8bf608334fb72caf8d3f654fdcce68c33b0f0f27de499b84e67b8357cd81ef7bba3cdaa9e23a648f43ad2 sha512 /home/projects/icecube/data/exp/IceCube/2019/filtered/PFFilt/1109/7ec8a8f9-fae3-4f25-ae54-c1f66014f5ef.zip [hsi]\n",
            stderr=b"",
        ),
        ObjectLiteral(
            returncode=0,
            args=["hsi", "-q", "hashverify", "-A", "/home/projects/icecube/data/exp/IceCube/2019/filtered/PFFilt/1109/7ec8a8f9-fae3-4f25-ae54-c1f66014f5ef.zip"],
            stdout=b"/home/projects/icecube/data/exp/IceCube/2018/unbiased/PFDST/1230/50145c5c-01e1-4727-a9a1-324e5af09a29.zip: (sha256) OK\n",
            stderr=b"",
        ),
    ]
    p = NerscVerifier(config, logger_mock)
    with pytest.raises(InvalidChecksumException) as excinfo:
        p._verify_bundle_in_hpss(bundle)
    assert str(excinfo.value).startswith("Checksum mismatch between creation and destination:")
    assert run_mock.call_count == 2
