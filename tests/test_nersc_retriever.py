# test_nersc_retriever.py
"""Unit tests for lta/nersc_retriever.py."""

# -----------------------------------------------------------------------------
# reset prometheus registry for unit tests
from prometheus_client import REGISTRY
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

import pytest
from pytest import MonkeyPatch
from pytest_mock import MockerFixture
from tornado.web import HTTPError

from lta.nersc_retriever import main_sync, NerscRetriever
from .test_util import ObjectLiteral

TestConfig = Dict[str, str]


@pytest.fixture
def config() -> TestConfig:
    """Supply a stock NerscRetriever component configuration."""
    return {
        "CLIENT_ID": "long-term-archive",
        "CLIENT_SECRET": "hunter2",  # http://bash.org/?244321
        "COMPONENT_NAME": "testing-nersc-mover",
        "DEST_SITE": "WIPAC",
        "HPSS_AVAIL_PATH": "/path/to/hpss_avail.py",
        "INPUT_STATUS": "located",
        "LOG_LEVEL": "DEBUG",
        "LTA_AUTH_OPENID_URL": "localhost:12345",
        "LTA_REST_URL": "localhost:12347",
        "MAX_COUNT": "5",
        "OUTPUT_STATUS": "staged",
        "PROMETHEUS_METRICS_PORT": "8080",
        "RSE_BASE_PATH": "/path/to/rse",
        "RUN_ONCE_AND_DIE": "False",
        "RUN_UNTIL_NO_WORK": "False",
        "SOURCE_SITE": "NERSC",
        "TAPE_BASE_PATH": "/path/to/hpss",
        "WORK_RETRIES": "3",
        "WORK_SLEEP_DURATION_SECONDS": "60",
        "WORK_TIMEOUT_SECONDS": "30",
    }


def test_constructor_config_missing_values(mocker: MockerFixture) -> None:
    """Fail with a ValueError if the configuration object is missing required configuration variables."""
    config = {
        "PAN_GALACTIC_GARGLE_BLASTER": "Yummy"
    }
    logger_mock = mocker.MagicMock()
    with pytest.raises(ValueError):
        NerscRetriever(config, logger_mock)


def test_constructor_config_poison_values(config: TestConfig, mocker: MockerFixture) -> None:
    """Fail with a ValueError if the configuration object is missing required configuration variables."""
    nersc_retriever_config = config.copy()
    del nersc_retriever_config["LTA_REST_URL"]
    logger_mock = mocker.MagicMock()
    with pytest.raises(ValueError):
        NerscRetriever(nersc_retriever_config, logger_mock)


def test_constructor_config(config: TestConfig, mocker: MockerFixture) -> None:
    """Test that a NerscRetriever can be constructed with a configuration object and a logging object."""
    logger_mock = mocker.MagicMock()
    p = NerscRetriever(config, logger_mock)
    assert p.lta_rest_url == "localhost:12347"
    assert p.name == "testing-nersc-mover"
    assert p.work_sleep_duration_seconds == 60
    assert p.logger == logger_mock


def test_constructor_config_sleep_type_int(config: TestConfig, mocker: MockerFixture) -> None:
    """Ensure that sleep seconds can also be provided as an integer."""
    logger_mock = mocker.MagicMock()
    p = NerscRetriever(config, logger_mock)
    assert p.lta_rest_url == "localhost:12347"
    assert p.name == "testing-nersc-mover"
    assert p.work_sleep_duration_seconds == 60
    assert p.logger == logger_mock


def test_constructor_state(config: TestConfig, mocker: MockerFixture) -> None:
    """Verify that the NerscRetriever has a reasonable state when it is first constructed."""
    logger_mock = mocker.MagicMock()
    p = NerscRetriever(config, logger_mock)
    assert p.last_work_begin_timestamp is p.last_work_end_timestamp


def test_do_status(config: TestConfig, mocker: MockerFixture) -> None:
    """Verify that the NerscRetriever has no additional state to offer."""
    logger_mock = mocker.MagicMock()
    p = NerscRetriever(config, logger_mock)
    assert p._do_status() == {}


@pytest.mark.asyncio
async def test_script_main_sync(config: TestConfig, mocker: MockerFixture, monkeypatch: MonkeyPatch) -> None:
    """
    Verify NerscRetriever component behavior when run as a script.

    Test to make sure running the NerscRetriever as a script does the setup work
    that we expect and then launches the nersc_retriever service.
    """
    for key in config.keys():
        monkeypatch.setenv(key, config[key])
    mock_run = mocker.patch("asyncio.run")
    mock_main = mocker.patch("lta.nersc_retriever.main")
    mock_shs = mocker.patch("lta.nersc_retriever.start_http_server")
    main_sync()
    mock_shs.assert_called()
    mock_main.assert_called()
    mock_run.assert_called()
    await mock_run.call_args.args[0]


@pytest.mark.asyncio
async def test_nersc_retriever_logs_configuration(mocker: MockerFixture) -> None:
    """Test to make sure the NerscRetriever logs its configuration."""
    logger_mock = mocker.MagicMock()
    nersc_retriever_config = {
        "CLIENT_ID": "long-term-archive",
        "CLIENT_SECRET": "hunter2",  # http://bash.org/?244321
        "COMPONENT_NAME": "logme-testing-nersc-mover",
        "DEST_SITE": "WIPAC",
        "HPSS_AVAIL_PATH": "/log/me/path/to/hpss_avail.py",
        "INPUT_STATUS": "located",
        "LOG_LEVEL": "DEBUG",
        "LTA_AUTH_OPENID_URL": "localhost:12345",
        "LTA_REST_URL": "logme-http://RmMNHdPhHpH2ZxfaFAC9d2jiIbf5pZiHDqy43rFLQiM.com/",
        "MAX_COUNT": "9001",
        "OUTPUT_STATUS": "staged",
        "PROMETHEUS_METRICS_PORT": "8080",
        "RSE_BASE_PATH": "/log/me/path/to/rse",
        "RUN_ONCE_AND_DIE": "False",
        "RUN_UNTIL_NO_WORK": "False",
        "SOURCE_SITE": "NERSC",
        "TAPE_BASE_PATH": "/log/me/path/to/hpss",
        "WORK_RETRIES": "5",
        "WORK_SLEEP_DURATION_SECONDS": "70",
        "WORK_TIMEOUT_SECONDS": "90",
    }
    NerscRetriever(nersc_retriever_config, logger_mock)
    EXPECTED_LOGGER_CALLS = [
        call("nersc_retriever 'logme-testing-nersc-mover' is configured:"),
        call('CLIENT_ID = long-term-archive'),
        call('CLIENT_SECRET = [秘密]'),
        call('COMPONENT_NAME = logme-testing-nersc-mover'),
        call('DEST_SITE = WIPAC'),
        call('HPSS_AVAIL_PATH = /log/me/path/to/hpss_avail.py'),
        call('INPUT_STATUS = located'),
        call('LOG_LEVEL = DEBUG'),
        call('LTA_AUTH_OPENID_URL = localhost:12345'),
        call('LTA_REST_URL = logme-http://RmMNHdPhHpH2ZxfaFAC9d2jiIbf5pZiHDqy43rFLQiM.com/'),
        call('MAX_COUNT = 9001'),
        call('OUTPUT_STATUS = staged'),
        call('PROMETHEUS_METRICS_PORT = 8080'),
        call('RSE_BASE_PATH = /log/me/path/to/rse'),
        call('RUN_ONCE_AND_DIE = False'),
        call('RUN_UNTIL_NO_WORK = False'),
        call('SOURCE_SITE = NERSC'),
        call('TAPE_BASE_PATH = /log/me/path/to/hpss'),
        call('WORK_RETRIES = 5'),
        call('WORK_SLEEP_DURATION_SECONDS = 70'),
        call('WORK_TIMEOUT_SECONDS = 90')
    ]
    logger_mock.info.assert_has_calls(EXPECTED_LOGGER_CALLS)


@pytest.mark.asyncio
async def test_nersc_retriever_run(config: TestConfig, mocker: MockerFixture) -> None:
    """Test the NerscRetriever does the work the nersc_retriever should do."""
    logger_mock = mocker.MagicMock()
    p = NerscRetriever(config, logger_mock)
    p._do_work = AsyncMock()  # type: ignore[method-assign]
    await p.run()
    p._do_work.assert_called()


@pytest.mark.asyncio
async def test_nersc_retriever_run_exception(config: TestConfig, mocker: MockerFixture) -> None:
    """Test an error doesn't kill the NerscRetriever."""
    logger_mock = mocker.MagicMock()
    p = NerscRetriever(config, logger_mock)
    p.last_work_end_timestamp = ""
    p._do_work = AsyncMock()  # type: ignore[method-assign]
    p._do_work.side_effect = [Exception("bad thing happen!")]
    await p.run()
    p._do_work.assert_called()
    assert p.last_work_end_timestamp


@pytest.mark.asyncio
async def test_nersc_retriever_do_work_no_results(config: TestConfig, mocker: MockerFixture) -> None:
    """Test that _do_work goes on vacation when the LTA DB has no work."""
    logger_mock = mocker.MagicMock()
    dwc_mock = mocker.patch("lta.nersc_retriever.NerscRetriever._do_work_claim", new_callable=AsyncMock)
    dwc_mock.return_value = False
    p = NerscRetriever(config, logger_mock)
    await p._do_work(AsyncMock())
    dwc_mock.assert_called()


@pytest.mark.asyncio
async def test_nersc_retriever_do_work_yes_results(config: TestConfig, mocker: MockerFixture) -> None:
    """Test that _do_work keeps working until the LTA DB has no work."""
    logger_mock = mocker.MagicMock()
    dwc_mock = mocker.patch("lta.nersc_retriever.NerscRetriever._do_work_claim", new_callable=AsyncMock)
    dwc_mock.side_effect = [True, True, False]
    p = NerscRetriever(config, logger_mock)
    await p._do_work(AsyncMock())
    dwc_mock.assert_called()


@pytest.mark.asyncio
async def test_nersc_retriever_hpss_not_available(config: TestConfig, mocker: MockerFixture) -> None:
    """Test that a bad returncode on hpss_avail will prevent work."""
    logger_mock = mocker.MagicMock()
    run_mock = mocker.patch("lta.nersc_retriever.run", new_callable=MagicMock)
    run_mock.return_value = ObjectLiteral(
        returncode=1,
        args=["/usr/common/software/bin/hpss_avail", "archive"],
        stdout="some text on stdout",
        stderr="some text on stderr",
    )
    p = NerscRetriever(config, logger_mock)
    assert not await p._do_work_claim(AsyncMock())


@pytest.mark.asyncio
async def test_nersc_retriever_do_work_pop_exception(config: TestConfig, mocker: MockerFixture) -> None:
    """Test that _do_work raises when the RestClient can't pop."""
    logger_mock = mocker.MagicMock()
    run_mock = mocker.patch("lta.nersc_retriever.run", new_callable=MagicMock)
    run_mock.return_value = ObjectLiteral(
        returncode=0,
        args=["/usr/common/software/bin/hpss_avail", "archive"],
        stdout="some text on stdout",
        stderr="some text on stderr",
    )
    lta_rc_mock = AsyncMock()
    lta_rc_mock.request = AsyncMock()
    lta_rc_mock.request.side_effect = [
        HTTPError(500, "LTA DB on fire. Again.")
    ]
    p = NerscRetriever(config, logger_mock)
    with pytest.raises(HTTPError):
        await p._do_work(lta_rc_mock)
    lta_rc_mock.request.assert_called_with("POST", '/Bundles/actions/pop?source=NERSC&dest=WIPAC&status=located', {'claimant': f'{p.name}-{p.instance_uuid}'})


@pytest.mark.asyncio
async def test_nersc_retriever_do_work_claim_no_result(config: TestConfig, mocker: MockerFixture) -> None:
    """Test that _do_work_claim does not work when the LTA DB has no work."""
    logger_mock = mocker.MagicMock()
    run_mock = mocker.patch("lta.nersc_retriever.run", new_callable=MagicMock)
    run_mock.return_value = ObjectLiteral(
        returncode=0,
        args=["/usr/common/software/bin/hpss_avail", "archive"],
        stdout="some text on stdout",
        stderr="some text on stderr",
    )
    lta_rc_mock = AsyncMock()
    lta_rc_mock.request = AsyncMock()
    lta_rc_mock.request.side_effect = [
        {
            "bundle": None
        }
    ]
    wbth_mock = mocker.patch("lta.nersc_retriever.NerscRetriever._read_bundle_from_hpss", new_callable=AsyncMock)
    p = NerscRetriever(config, logger_mock)
    await p._do_work_claim(lta_rc_mock)
    lta_rc_mock.request.assert_called_with("POST", '/Bundles/actions/pop?source=NERSC&dest=WIPAC&status=located', {'claimant': f'{p.name}-{p.instance_uuid}'})
    wbth_mock.assert_not_called()


@pytest.mark.asyncio
async def test_nersc_retriever_do_work_claim_yes_result(config: TestConfig, mocker: MockerFixture) -> None:
    """Test that _do_work_claim processes the Bundle it gets from the LTA DB."""
    logger_mock = mocker.MagicMock()
    run_mock = mocker.patch("lta.nersc_retriever.run", new_callable=MagicMock)
    run_mock.return_value = ObjectLiteral(
        returncode=0,
        args=["/usr/common/software/bin/hpss_avail", "archive"],
        stdout="some text on stdout",
        stderr="some text on stderr",
    )
    lta_rc_mock = AsyncMock()
    lta_rc_mock.request = AsyncMock()
    lta_rc_mock.request.side_effect = [
        {
            "bundle": {
                "one": 1,
            },
        }
    ]
    wbth_mock = mocker.patch("lta.nersc_retriever.NerscRetriever._read_bundle_from_hpss", new_callable=AsyncMock)
    p = NerscRetriever(config, logger_mock)
    await p._do_work_claim(lta_rc_mock)
    lta_rc_mock.request.assert_called_with("POST", '/Bundles/actions/pop?source=NERSC&dest=WIPAC&status=located', {'claimant': f'{p.name}-{p.instance_uuid}'})
    wbth_mock.assert_called_with(mocker.ANY, {"one": 1})


@pytest.mark.asyncio
async def test_nersc_retriever_do_work_claim_write_bundle_raise_exception(config: TestConfig, mocker: MockerFixture) -> None:
    """Test that _do_work_claim will quarantine a bundle if an exception occurs."""
    logger_mock = mocker.MagicMock()
    run_mock = mocker.patch("lta.nersc_retriever.run", new_callable=MagicMock)
    run_mock.return_value = ObjectLiteral(
        returncode=0,
        args=["/usr/common/software/bin/hpss_avail", "archive"],
        stdout="some text on stdout",
        stderr="some text on stderr",
    )
    lta_rc_mock = AsyncMock()
    lta_rc_mock.request = AsyncMock()
    lta_rc_mock.request.side_effect = [
        {
            "bundle": {
                "uuid": "8f03a920-49d6-446b-811e-830e3f7942f5",
            },
        },
        {}
    ]
    wbth_mock = mocker.patch("lta.nersc_retriever.NerscRetriever._read_bundle_from_hpss", new_callable=AsyncMock)
    wbth_mock.side_effect = Exception("BAD THING HAPPEN!")
    p = NerscRetriever(config, logger_mock)
    assert not await p._do_work_claim(lta_rc_mock)
    lta_rc_mock.request.assert_called_with("PATCH", '/Bundles/8f03a920-49d6-446b-811e-830e3f7942f5', mocker.ANY)
    wbth_mock.assert_called_with(mocker.ANY, {"uuid": "8f03a920-49d6-446b-811e-830e3f7942f5"})


@pytest.mark.asyncio
async def test_nersc_retriever_read_bundle_from_hpss_hsi_get(config: TestConfig, mocker: MockerFixture) -> None:
    """Test that _read_bundle_from_hpss executes an HSI command to read the file from tape."""
    logger_mock = mocker.MagicMock()
    run_mock = mocker.patch("lta.nersc_retriever.run", new_callable=MagicMock)
    run_mock.return_value = ObjectLiteral(
        returncode=0,
        args=["/usr/common/software/bin/hpss_avail", "archive"],
        stdout="some text on stdout",
        stderr="some text on stderr",
    )
    lta_rc_mock = AsyncMock()
    lta_rc_mock.request = AsyncMock()
    lta_rc_mock.request.side_effect = [
        {
            "bundle": {
                "uuid": "398ca1ed-0178-4333-a323-8b9158c3dd88",
                "bundle_path": "/path/on/source/rse/398ca1ed-0178-4333-a323-8b9158c3dd88.zip",
                "path": "/data/exp/IceCube/2019/filtered/PFFilt/1109",
            },
        },
        {
            "type": "Bundle",
        },
    ]
    ehc_mock = mocker.patch("lta.nersc_retriever.NerscRetriever._execute_hsi_command", new_callable=AsyncMock)
    ehc_mock.side_effect = [True, False]
    p = NerscRetriever(config, logger_mock)
    await p._do_work_claim(lta_rc_mock)
    ehc_mock.assert_called_with(mocker.ANY, mocker.ANY, ['/usr/bin/hsi', 'get', '-c', 'on', '/path/to/rse/398ca1ed-0178-4333-a323-8b9158c3dd88.zip', ':', '/path/to/hpss/data/exp/IceCube/2019/filtered/PFFilt/1109/398ca1ed-0178-4333-a323-8b9158c3dd88.zip'])
    lta_rc_mock.request.assert_called_with("PATCH", '/Bundles/398ca1ed-0178-4333-a323-8b9158c3dd88', mocker.ANY)


@pytest.mark.asyncio
async def test_nersc_retriever_read_bundle_from_hpss(config: TestConfig, mocker: MockerFixture) -> None:
    """Test that _read_bundle_from_hpss updates the LTA DB after success."""
    logger_mock = mocker.MagicMock()
    run_mock = mocker.patch("lta.nersc_retriever.run", new_callable=MagicMock)
    run_mock.return_value = ObjectLiteral(
        returncode=0,
        args=["/usr/common/software/bin/hpss_avail", "archive"],
        stdout="some text on stdout",
        stderr="some text on stderr",
    )
    lta_rc_mock = AsyncMock()
    lta_rc_mock.request = AsyncMock()
    lta_rc_mock.request.side_effect = [
        {
            "bundle": {
                "uuid": "398ca1ed-0178-4333-a323-8b9158c3dd88",
                "bundle_path": "/path/on/source/rse/398ca1ed-0178-4333-a323-8b9158c3dd88.zip",
                "path": "/data/exp/IceCube/2019/filtered/PFFilt/1109",
            },
        },
        {
            "type": "Bundle",
        },
    ]
    ehc_mock = mocker.patch("lta.nersc_retriever.NerscRetriever._execute_hsi_command", new_callable=AsyncMock)
    ehc_mock.side_effect = [True, True]
    p = NerscRetriever(config, logger_mock)
    await p._do_work_claim(lta_rc_mock)
    ehc_mock.assert_called_with(mocker.ANY, mocker.ANY, ['/usr/bin/hsi', 'get', '-c', 'on', '/path/to/rse/398ca1ed-0178-4333-a323-8b9158c3dd88.zip', ':', '/path/to/hpss/data/exp/IceCube/2019/filtered/PFFilt/1109/398ca1ed-0178-4333-a323-8b9158c3dd88.zip'])
    lta_rc_mock.request.assert_called_with("PATCH", '/Bundles/398ca1ed-0178-4333-a323-8b9158c3dd88', mocker.ANY)


@pytest.mark.asyncio
async def test_nersc_retriever_execute_hsi_command_failed(config: TestConfig, mocker: MockerFixture) -> None:
    """Test that _execute_hsi_command will PATCH a bundle to quarantine on failure."""
    logger_mock = mocker.MagicMock()
    run_mock = mocker.patch("lta.nersc_retriever.run", new_callable=MagicMock)
    run_mock.side_effect = [
        ObjectLiteral(
            returncode=0,
            args=["/usr/common/software/bin/hpss_avail", "archive"],
            stdout="some text on stdout",
            stderr="some text on stderr",
        ),
        ObjectLiteral(
            returncode=1,
            args=['/usr/bin/hsi', 'mkdir', '-p', '/path/to/hpss/data/exp/IceCube/2019/filtered/PFFilt/1109'],
            stdout="some text on stdout",
            stderr="some text on stderr",
        )
    ]
    lta_rc_mock = AsyncMock()
    lta_rc_mock.request = AsyncMock()
    lta_rc_mock.request.side_effect = [
        {
            "bundle": {
                "uuid": "398ca1ed-0178-4333-a323-8b9158c3dd88",
                "bundle_path": "/path/on/source/rse/398ca1ed-0178-4333-a323-8b9158c3dd88.zip",
                "path": "/data/exp/IceCube/2019/filtered/PFFilt/1109",
            },
        },
        {
            "type": "Bundle",
        },
    ]
    p = NerscRetriever(config, logger_mock)
    await p._do_work_claim(lta_rc_mock)
    lta_rc_mock.request.assert_called_with("PATCH", '/Bundles/398ca1ed-0178-4333-a323-8b9158c3dd88', mocker.ANY)


@pytest.mark.asyncio
async def test_nersc_retriever_execute_hsi_command_success(config: TestConfig, mocker: MockerFixture) -> None:
    """Test that _execute_hsi_command will PATCH a bundle to quarantine on failure."""
    logger_mock = mocker.MagicMock()
    run_mock = mocker.patch("lta.nersc_retriever.run", new_callable=MagicMock)
    run_mock.side_effect = [
        ObjectLiteral(
            returncode=0,
            args=["/usr/common/software/bin/hpss_avail", "archive"],
            stdout="some text on stdout",
            stderr="some text on stderr",
        ),
        ObjectLiteral(returncode=0),
        ObjectLiteral(returncode=0)
    ]
    lta_rc_mock = AsyncMock()
    lta_rc_mock.request = AsyncMock()
    lta_rc_mock.request.side_effect = [
        {
            "bundle": {
                "uuid": "398ca1ed-0178-4333-a323-8b9158c3dd88",
                "bundle_path": "/path/on/source/rse/398ca1ed-0178-4333-a323-8b9158c3dd88.zip",
                "path": "/data/exp/IceCube/2019/filtered/PFFilt/1109",
            },
        },
        {
            "type": "Bundle",
        },
    ]
    p = NerscRetriever(config, logger_mock)
    await p._do_work_claim(lta_rc_mock)
    lta_rc_mock.request.assert_called_with("PATCH", '/Bundles/398ca1ed-0178-4333-a323-8b9158c3dd88', mocker.ANY)
