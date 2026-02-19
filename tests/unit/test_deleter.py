# test_deleter.py
"""Unit tests for lta/deleter.py."""

# fmt:off

from .utils import NicheException

from typing import Dict
from unittest.mock import AsyncMock, call, MagicMock

import pytest
from pytest import MonkeyPatch
from pytest_mock import MockerFixture
from tornado.web import HTTPError

from lta.deleter import main_sync, Deleter

TestConfig = Dict[str, str]


@pytest.fixture
def config() -> TestConfig:
    """Supply a stock Deleter component configuration."""
    return {
        "CLIENT_ID": "long-term-archive",
        "CLIENT_SECRET": "hunter2",  # http://bash.org/?244321
        "COMPONENT_NAME": "testing-deleter",
        "DEST_SITE": "NERSC",
        "DISK_BASE_PATH": "/path/to/rucio/rse/root",
        "INPUT_STATUS": "detached",
        "LOG_LEVEL": "DEBUG",
        "LTA_AUTH_OPENID_URL": "localhost:12345",
        "LTA_REST_URL": "localhost:12347",
        "OUTPUT_STATUS": "source-deleted",
        "PROMETHEUS_METRICS_PORT": "8080",
        "RUN_ONCE_AND_DIE": "False",
        "RUN_UNTIL_NO_WORK": "False",
        "SOURCE_SITE": "WIPAC",
        "WORK_RETRIES": "3",
        "WORK_SLEEP_DURATION_SECONDS": "60",
        "WORK_TIMEOUT_SECONDS": "30",
    }


def test_constructor_config(config: TestConfig, mocker: MockerFixture) -> None:
    """Test that a Deleter can be constructed with a configuration object and a logging object."""
    logger_mock = mocker.MagicMock()
    p = Deleter(config, logger_mock)
    assert p.name == "testing-deleter"
    assert p.dest_site == "NERSC"
    assert p.disk_base_path == "/path/to/rucio/rse/root"
    assert p.input_status == "detached"
    assert p.lta_rest_url == "localhost:12347"
    assert p.output_status == "source-deleted"
    assert p.source_site == "WIPAC"
    assert p.work_retries == 3
    assert p.work_sleep_duration_seconds == 60
    assert p.work_timeout_seconds == 30
    assert p.logger == logger_mock


def test_do_status(config: TestConfig, mocker: MockerFixture) -> None:
    """Verify that the Deleter has no additional state to offer."""
    logger_mock = mocker.MagicMock()
    p = Deleter(config, logger_mock)
    assert p._do_status() == {}


@pytest.mark.asyncio
async def test_deleter_logs_configuration(mocker: MockerFixture) -> None:
    """Test to make sure the Deleter logs its configuration."""
    logger_mock = mocker.MagicMock()
    deleter_config = {
        "CLIENT_ID": "long-term-archive",
        "CLIENT_SECRET": "hunter2",  # http://bash.org/?244321
        "COMPONENT_NAME": "logme-testing-deleter",
        "DEST_SITE": "NERSC",
        "DISK_BASE_PATH": "/path/to/rucio/rse/root",
        "INPUT_STATUS": "detached",
        "LOG_LEVEL": "DEBUG",
        "LTA_AUTH_OPENID_URL": "localhost:12345",
        "LTA_REST_URL": "localhost:12347",
        "OUTPUT_STATUS": "source-deleted",
        "PROMETHEUS_METRICS_PORT": "8080",
        "RUN_ONCE_AND_DIE": "False",
        "RUN_UNTIL_NO_WORK": "False",
        "SOURCE_SITE": "WIPAC",
        "WORK_RETRIES": "5",
        "WORK_SLEEP_DURATION_SECONDS": "70",
        "WORK_TIMEOUT_SECONDS": "90",
    }
    Deleter(deleter_config, logger_mock)
    EXPECTED_LOGGER_CALLS = [
        call("deleter 'logme-testing-deleter' is configured:"),
        call('CLIENT_ID = long-term-archive'),
        call('CLIENT_SECRET = [秘密]'),
        call('COMPONENT_NAME = logme-testing-deleter'),
        call('DEST_SITE = NERSC'),
        call('DISK_BASE_PATH = /path/to/rucio/rse/root'),
        call('INPUT_STATUS = detached'),
        call('LOG_LEVEL = DEBUG'),
        call('LTA_AUTH_OPENID_URL = localhost:12345'),
        call('LTA_REST_URL = localhost:12347'),
        call('OUTPUT_STATUS = source-deleted'),
        call('PROMETHEUS_METRICS_PORT = 8080'),
        call('RUN_ONCE_AND_DIE = False'),
        call('RUN_UNTIL_NO_WORK = False'),
        call('SOURCE_SITE = WIPAC'),
        call('WORK_RETRIES = 5'),
        call('WORK_SLEEP_DURATION_SECONDS = 70'),
        call('WORK_TIMEOUT_SECONDS = 90')
    ]
    logger_mock.info.assert_has_calls(EXPECTED_LOGGER_CALLS)


@pytest.mark.asyncio
async def test_script_main(config: TestConfig,
                           mocker: MockerFixture,
                           monkeypatch: MonkeyPatch) -> None:
    """
    Verify Deleter component behavior when run as a script.

    Test to make sure running the Deleter as a script does the setup work
    that we expect and then launches the deleter service.
    """
    for key in config.keys():
        monkeypatch.setenv(key, config[key])
    mock_run = mocker.patch("asyncio.run")
    mock_main = mocker.patch("lta.deleter.main")
    mock_shs = mocker.patch("lta.deleter.start_http_server")
    main_sync()
    mock_shs.assert_called()
    mock_main.assert_called()
    mock_run.assert_called()
    await mock_run.call_args.args[0]


@pytest.mark.asyncio
async def test_deleter_run(config: TestConfig, mocker: MockerFixture) -> None:
    """Test the Deleter does the work the deleter should do."""
    logger_mock = mocker.MagicMock()
    p = Deleter(config, logger_mock)
    p._do_work = AsyncMock()  # type: ignore[method-assign]
    await p.run()
    p._do_work.assert_called()


@pytest.mark.asyncio
async def test_deleter_run_exception(config: TestConfig, mocker: MockerFixture) -> None:
    """Test an error doesn't kill the Deleter."""
    logger_mock = mocker.MagicMock()
    dw_mock = mocker.patch("lta.deleter.Deleter._do_work")  # , new_callable=AsyncMock)
    dw_mock.side_effect = [Exception("bad thing happen!")]
    p = Deleter(config, logger_mock)
    await p.run()
    dw_mock.assert_called()


@pytest.mark.asyncio
async def test_deleter_do_work_pop_exception(config: TestConfig, mocker: MockerFixture) -> None:
    """Test that _do_work raises when the RestClient can't pop."""
    logger_mock = mocker.MagicMock()
    lta_rc_mock = AsyncMock()
    lta_rc_mock.request = AsyncMock()
    lta_rc_mock.request.side_effect = HTTPError(500, "LTA DB on fire. Again.")
    p = Deleter(config, logger_mock)
    with pytest.raises(HTTPError):
        await p._do_work(lta_rc_mock)
    lta_rc_mock.request.assert_called_with("POST", '/Bundles/actions/pop?source=WIPAC&dest=NERSC&status=detached', {'claimant': f'{p.name}-{p.instance_uuid}'})


@pytest.mark.asyncio
async def test_deleter_do_work_no_results(config: TestConfig, mocker: MockerFixture) -> None:
    """Test that _do_work goes on vacation when the LTA DB has no work."""
    logger_mock = mocker.MagicMock()
    lta_rc_mock = AsyncMock()
    lta_rc_mock.request = AsyncMock()
    dwc_mock = mocker.patch("lta.deleter.Deleter._do_work_claim", new_callable=AsyncMock)
    dwc_mock.return_value = False
    p = Deleter(config, logger_mock)
    await p._do_work(lta_rc_mock)
    dwc_mock.assert_called()


@pytest.mark.asyncio
async def test_deleter_do_work_yes_results(config: TestConfig, mocker: MockerFixture) -> None:
    """Test that _do_work keeps working until the LTA DB has no work."""
    logger_mock = mocker.MagicMock()
    lta_rc_mock = AsyncMock()
    lta_rc_mock.request = AsyncMock()
    dwc_mock = mocker.patch("lta.deleter.Deleter._do_work_claim", new_callable=AsyncMock)
    dwc_mock.side_effect = [True, True, False]
    p = Deleter(config, logger_mock)
    await p._do_work(lta_rc_mock)
    dwc_mock.assert_called()


@pytest.mark.asyncio
async def test_deleter_do_work_claim_no_result(config: TestConfig, mocker: MockerFixture) -> None:
    """Test that _do_work_claim does not work when the LTA DB has no work."""
    logger_mock = mocker.MagicMock()
    lta_rc_mock = AsyncMock()
    lta_rc_mock.request = AsyncMock()
    lta_rc_mock.request.return_value = {
        "bundle": None
    }
    db_mock = mocker.patch("lta.deleter.Deleter._delete_bundle", new_callable=AsyncMock)
    p = Deleter(config, logger_mock)
    await p._do_work_claim(lta_rc_mock, MagicMock())
    lta_rc_mock.request.assert_called_with("POST", '/Bundles/actions/pop?source=WIPAC&dest=NERSC&status=detached', {'claimant': f'{p.name}-{p.instance_uuid}'})
    db_mock.assert_not_called()


@pytest.mark.asyncio
async def test_deleter_do_work_claim_yes_result(config: TestConfig, mocker: MockerFixture) -> None:
    """Test that _do_work_claim processes the Bundle that it gets from the LTA DB."""
    logger_mock = mocker.MagicMock()
    lta_rc_mock = AsyncMock()
    lta_rc_mock.request = AsyncMock()
    lta_rc_mock.request.return_value = {
        "bundle": {
            "one": 1,
        },
    }
    db_mock = mocker.patch("lta.deleter.Deleter._delete_bundle", new_callable=AsyncMock)
    p = Deleter(config, logger_mock)
    assert await p._do_work_claim(lta_rc_mock, MagicMock())
    lta_rc_mock.request.assert_called_with("POST", '/Bundles/actions/pop?source=WIPAC&dest=NERSC&status=detached', {'claimant': f'{p.name}-{p.instance_uuid}'})
    db_mock.assert_called_with(mocker.ANY, {"one": 1})


@pytest.mark.asyncio
async def test_deleter_delete_bundle_raises(config: TestConfig, mocker: MockerFixture) -> None:
    """Test that _do_work_claim both calls quarantine_now and re-raises when _delete_bundle raises."""
    logger_mock = mocker.MagicMock()
    lta_rc_mock = AsyncMock()
    lta_rc_mock.request = AsyncMock()
    lta_rc_mock.request.return_value = {
        "bundle": {
            "one": 1,
        },
    }
    db_mock = mocker.patch("lta.deleter.Deleter._delete_bundle", new_callable=AsyncMock)
    qb_mock = mocker.patch("lta.deleter.quarantine_now", new_callable=AsyncMock)
    exc = NicheException("LTA DB unavailable; currently safer at home")
    db_mock.side_effect = exc
    p = Deleter(config, logger_mock)
    with pytest.raises(NicheException):
        await p._do_work_claim(lta_rc_mock, MagicMock())
    lta_rc_mock.request.assert_called_with("POST", '/Bundles/actions/pop?source=WIPAC&dest=NERSC&status=detached', {'claimant': f'{p.name}-{p.instance_uuid}'})
    db_mock.assert_called_with(mocker.ANY, {"one": 1})
    qb_mock.assert_called_with(
        lta_rc_mock,
        {"one": 1},
        "BUNDLE",
        exc,
        p.name,
        p.instance_uuid,
        logger_mock,
    )


@pytest.mark.asyncio
async def test_deleter_delete_bundle(config: TestConfig, mocker: MockerFixture) -> None:
    """Test that _delete_bundle attempts to delete a Bundle."""
    logger_mock = mocker.MagicMock()
    lta_rc_mock = mocker.patch("rest_tools.client.RestClient", new_callable=AsyncMock)
    remove_mock = mocker.patch("os.remove", new_callable=MagicMock)
    p = Deleter(config, logger_mock)
    await p._delete_bundle(lta_rc_mock, {
        "uuid": "c4b345e4-2395-4f9e-b0eb-9cc1c9cdf003",
        "bundle_path": "/icecube/datawarehouse/path/to/c4b345e4-2395-4f9e-b0eb-9cc1c9cdf003.zip",
    })
    remove_mock.assert_called()
    lta_rc_mock.request.assert_called_with("PATCH", "/Bundles/c4b345e4-2395-4f9e-b0eb-9cc1c9cdf003", mocker.ANY)
