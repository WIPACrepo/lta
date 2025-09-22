# test_transfer_request_finisher.py
"""Unit tests for lta/transfer_request_finisher.py."""

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
from unittest.mock import AsyncMock, call

import pytest
from pytest import MonkeyPatch
from pytest_mock import MockerFixture
from tornado.web import HTTPError

from lta.transfer_request_finisher import main_sync, TransferRequestFinisher

TestConfig = Dict[str, str]


@pytest.fixture
def config() -> TestConfig:
    """Supply a stock TransferRequestFinisher component configuration."""
    return {
        "CLIENT_ID": "long-term-archive",
        "CLIENT_SECRET": "hunter2",  # http://bash.org/?244321
        "COMPONENT_NAME": "testing-transfer_request_finisher",
        "DEST_SITE": "NERSC",
        "INPUT_STATUS": "deleted",
        "LOG_LEVEL": "DEBUG",
        "LTA_AUTH_OPENID_URL": "localhost:12345",
        "LTA_REST_URL": "localhost:12347",
        "OUTPUT_STATUS": "finished",
        "PROMETHEUS_METRICS_PORT": "8080",
        "RUCIO_PASSWORD": "hunter2",
        "RUN_ONCE_AND_DIE": "False",
        "RUN_UNTIL_NO_WORK": "False",
        "SOURCE_SITE": "WIPAC",
        "TRANSFER_CONFIG_PATH": "examples/rucio.json",
        "WORK_RETRIES": "3",
        "WORK_SLEEP_DURATION_SECONDS": "60",
        "WORK_TIMEOUT_SECONDS": "30",
    }


def test_constructor_config(config: TestConfig, mocker: MockerFixture) -> None:
    """Test that a TransferRequestFinisher can be constructed with a configuration object and a logging object."""
    logger_mock = mocker.MagicMock()
    p = TransferRequestFinisher(config, logger_mock)
    assert p.name == "testing-transfer_request_finisher"
    assert p.lta_auth_openid_url == "localhost:12345"
    assert p.lta_rest_url == "localhost:12347"
    assert p.source_site == "WIPAC"
    assert p.work_retries == 3
    assert p.work_sleep_duration_seconds == 60
    assert p.work_timeout_seconds == 30
    assert p.logger == logger_mock


def test_do_status(config: TestConfig, mocker: MockerFixture) -> None:
    """Verify that the TransferRequestFinisher has no additional state to offer."""
    logger_mock = mocker.MagicMock()
    p = TransferRequestFinisher(config, logger_mock)
    assert p._do_status() == {}


@pytest.mark.asyncio
async def test_transfer_request_finisher_logs_configuration(mocker: MockerFixture) -> None:
    """Test to make sure the TransferRequestFinisher logs its configuration."""
    logger_mock = mocker.MagicMock()
    transfer_request_finisher_config = {
        "CLIENT_ID": "long-term-archive",
        "CLIENT_SECRET": "hunter2",  # http://bash.org/?244321
        "COMPONENT_NAME": "logme-testing-transfer_request_finisher",
        "DEST_SITE": "NERSC",
        "INPUT_STATUS": "deleted",
        "LOG_LEVEL": "DEBUG",
        "LTA_AUTH_OPENID_URL": "localhost:12345",
        "LTA_REST_URL": "logme-http://zjwdm5ggeEgS1tZDZy9l1DOZU53uiSO4Urmyb8xL0.com/",
        "OUTPUT_STATUS": "finished",
        "PROMETHEUS_METRICS_PORT": "8080",
        "RUCIO_PASSWORD": "hunter3-electric-boogaloo",
        "RUN_ONCE_AND_DIE": "False",
        "RUN_UNTIL_NO_WORK": "False",
        "SOURCE_SITE": "WIPAC",
        "TRANSFER_CONFIG_PATH": "examples/rucio.json",
        "WORK_RETRIES": "5",
        "WORK_SLEEP_DURATION_SECONDS": "70",
        "WORK_TIMEOUT_SECONDS": "90",
    }
    TransferRequestFinisher(transfer_request_finisher_config, logger_mock)
    EXPECTED_LOGGER_CALLS = [
        call("transfer_request_finisher 'logme-testing-transfer_request_finisher' is configured:"),
        call('CLIENT_ID = long-term-archive'),
        call('CLIENT_SECRET = [秘密]'),
        call('COMPONENT_NAME = logme-testing-transfer_request_finisher'),
        call('DEST_SITE = NERSC'),
        call('INPUT_STATUS = deleted'),
        call('LOG_LEVEL = DEBUG'),
        call('LTA_AUTH_OPENID_URL = localhost:12345'),
        call('LTA_REST_URL = logme-http://zjwdm5ggeEgS1tZDZy9l1DOZU53uiSO4Urmyb8xL0.com/'),
        call('OUTPUT_STATUS = finished'),
        call('PROMETHEUS_METRICS_PORT = 8080'),
        call('RUCIO_PASSWORD = hunter3-electric-boogaloo'),
        call('RUN_ONCE_AND_DIE = False'),
        call('RUN_UNTIL_NO_WORK = False'),
        call('SOURCE_SITE = WIPAC'),
        call('TRANSFER_CONFIG_PATH = examples/rucio.json'),
        call('WORK_RETRIES = 5'),
        call('WORK_SLEEP_DURATION_SECONDS = 70'),
        call('WORK_TIMEOUT_SECONDS = 90')
    ]
    logger_mock.info.assert_has_calls(EXPECTED_LOGGER_CALLS)


@pytest.mark.asyncio
async def test_script_main_sync(config: TestConfig, mocker: MockerFixture, monkeypatch: MonkeyPatch) -> None:
    """
    Verify TransferRequestFinisher component behavior when run as a script.

    Test to make sure running the TransferRequestFinisher as a script does the setup work
    that we expect and then launches the transfer_request_finisher service.
    """
    for key in config.keys():
        monkeypatch.setenv(key, config[key])
    mock_run = mocker.patch("asyncio.run")
    mock_main = mocker.patch("lta.transfer_request_finisher.main")
    mock_shs = mocker.patch("lta.transfer_request_finisher.start_http_server")
    main_sync()
    mock_shs.assert_called()
    mock_main.assert_called()
    mock_run.assert_called()
    await mock_run.call_args.args[0]


@pytest.mark.asyncio
async def test_transfer_request_finisher_run(config: TestConfig, mocker: MockerFixture) -> None:
    """Test the TransferRequestFinisher does the work the transfer_request_finisher should do."""
    logger_mock = mocker.MagicMock()
    p = TransferRequestFinisher(config, logger_mock)
    p._do_work = AsyncMock()  # type: ignore[method-assign]
    await p.run()
    p._do_work.assert_called()


@pytest.mark.asyncio
async def test_transfer_request_finisher_run_exception(config: TestConfig, mocker: MockerFixture) -> None:
    """Test an error doesn't kill the TransferRequestFinisher."""
    logger_mock = mocker.MagicMock()
    p = TransferRequestFinisher(config, logger_mock)
    p.last_work_end_timestamp = ""
    p._do_work = AsyncMock()  # type: ignore[method-assign]
    p._do_work.side_effect = [Exception("bad thing happen!")]
    await p.run()
    p._do_work.assert_called()
    assert p.last_work_end_timestamp


@pytest.mark.asyncio
async def test_transfer_request_finisher_do_work_pop_exception(config: TestConfig, mocker: MockerFixture) -> None:
    """Test that _do_work raises when the RestClient can't pop."""
    logger_mock = mocker.MagicMock()
    lta_rc_mock = AsyncMock()
    lta_rc_mock.request = AsyncMock()
    lta_rc_mock.request.side_effect = HTTPError(500, "LTA DB on fire. Again.")
    p = TransferRequestFinisher(config, logger_mock)
    with pytest.raises(HTTPError):
        await p._do_work(lta_rc_mock)
    lta_rc_mock.request.assert_called_with("POST", '/Bundles/actions/pop?source=WIPAC&dest=NERSC&status=deleted', {'claimant': f'{p.name}-{p.instance_uuid}'})


@pytest.mark.asyncio
async def test_transfer_request_finisher_do_work_no_results(config: TestConfig, mocker: MockerFixture) -> None:
    """Test that _do_work goes on vacation when the LTA DB has no work."""
    logger_mock = mocker.MagicMock()
    dwc_mock = mocker.patch("lta.transfer_request_finisher.TransferRequestFinisher._do_work_claim", new_callable=AsyncMock)
    dwc_mock.return_value = False
    p = TransferRequestFinisher(config, logger_mock)
    await p._do_work(AsyncMock())
    dwc_mock.assert_called()


@pytest.mark.asyncio
async def test_transfer_request_finisher_do_work_yes_results(config: TestConfig, mocker: MockerFixture) -> None:
    """Test that _do_work keeps working until the LTA DB has no work."""
    logger_mock = mocker.MagicMock()
    dwc_mock = mocker.patch("lta.transfer_request_finisher.TransferRequestFinisher._do_work_claim", new_callable=AsyncMock)
    dwc_mock.side_effect = [True, True, False]
    p = TransferRequestFinisher(config, logger_mock)
    await p._do_work(AsyncMock())
    dwc_mock.assert_called()


@pytest.mark.asyncio
async def test_transfer_request_finisher_do_work_claim_no_result(config: TestConfig, mocker: MockerFixture) -> None:
    """Test that _do_work_claim does not work when the LTA DB has no work."""
    logger_mock = mocker.MagicMock()
    lta_rc_mock = AsyncMock()
    lta_rc_mock.request = AsyncMock()
    lta_rc_mock.request.return_value = {
        "bundle": None
    }
    utr_mock = mocker.patch("lta.transfer_request_finisher.TransferRequestFinisher._update_transfer_request", new_callable=AsyncMock)
    p = TransferRequestFinisher(config, logger_mock)
    await p._do_work_claim(lta_rc_mock)
    lta_rc_mock.request.assert_called_with("POST", '/Bundles/actions/pop?source=WIPAC&dest=NERSC&status=deleted', {'claimant': f'{p.name}-{p.instance_uuid}'})
    utr_mock.assert_not_called()


@pytest.mark.asyncio
async def test_transfer_request_finisher_do_work_claim_yes_result(config: TestConfig, mocker: MockerFixture) -> None:
    """Test that _do_work_claim processes the Bundle that it gets from the LTA DB."""
    logger_mock = mocker.MagicMock()
    lta_rc_mock = AsyncMock()
    lta_rc_mock.request = AsyncMock()
    lta_rc_mock.request.return_value = {
        "bundle": {
            "one": 1,
        },
    }
    utr_mock = mocker.patch("lta.transfer_request_finisher.TransferRequestFinisher._update_transfer_request", new_callable=AsyncMock)
    p = TransferRequestFinisher(config, logger_mock)
    assert not await p._do_work_claim(lta_rc_mock)
    lta_rc_mock.request.assert_called_with("POST", '/Bundles/actions/pop?source=WIPAC&dest=NERSC&status=deleted', {'claimant': f'{p.name}-{p.instance_uuid}'})
    utr_mock.assert_called_with(mocker.ANY, {"one": 1})


@pytest.mark.asyncio
async def test_transfer_request_finisher_update_transfer_request_no(config: TestConfig, mocker: MockerFixture) -> None:
    """Test that _update_transfer_request does not update an incomplete TransferRequest."""
    deleted_bundle = {
        "uuid": "8286d3ba-fb1b-4923-876d-935bdf7fc99e",
        "request": "a8758a77-2a66-46e6-b43d-b4c74d3078a6",
        "status": "deleted",
    }
    transferring_bundle = {
        "uuid": "90a664cc-e3f9-4421-973f-7bc2bc7407d0",
        "request": "a8758a77-2a66-46e6-b43d-b4c74d3078a6",
        "status": "transferring",
    }
    logger_mock = mocker.MagicMock()
    lta_rc_mock = mocker.patch("rest_tools.client.RestClient", new_callable=AsyncMock)
    lta_rc_mock.request.side_effect = [
        {
            "results": [
                "8286d3ba-fb1b-4923-876d-935bdf7fc99e",
                "90a664cc-e3f9-4421-973f-7bc2bc7407d0",
            ],
        },
        deleted_bundle,
        transferring_bundle,
        deleted_bundle,
    ]
    p = TransferRequestFinisher(config, logger_mock)
    await p._update_transfer_request(lta_rc_mock, deleted_bundle)
    lta_rc_mock.request.assert_called_with("PATCH", '/Bundles/8286d3ba-fb1b-4923-876d-935bdf7fc99e', {
        'claimed': False,
        'update_timestamp': mocker.ANY,
        'work_priority_timestamp': mocker.ANY,
    })


@pytest.mark.asyncio
async def test_transfer_request_finisher_update_transfer_request_yes(config: TestConfig, mocker: MockerFixture) -> None:
    """Test that _update_transfer_request does update a complete TransferRequest."""
    deleted_bundle = {
        "uuid": "8286d3ba-fb1b-4923-876d-935bdf7fc99e",
        "request": "a8758a77-2a66-46e6-b43d-b4c74d3078a6",
        "status": "deleted",
    }
    finished_bundle = {
        "uuid": "90a664cc-e3f9-4421-973f-7bc2bc7407d0",
        "request": "a8758a77-2a66-46e6-b43d-b4c74d3078a6",
        "status": "finished",
    }
    transfer_request = {
        "uuid": "a8758a77-2a66-46e6-b43d-b4c74d3078a6",
    }
    logger_mock = mocker.MagicMock()
    lta_rc_mock = mocker.patch("rest_tools.client.RestClient", new_callable=AsyncMock)
    lta_rc_mock.request.side_effect = [
        {
            "results": [
                "8286d3ba-fb1b-4923-876d-935bdf7fc99e",
                "90a664cc-e3f9-4421-973f-7bc2bc7407d0",
            ],
        },
        deleted_bundle,
        finished_bundle,
        transfer_request,
        deleted_bundle,
        finished_bundle,
    ]
    p = TransferRequestFinisher(config, logger_mock)
    await p._update_transfer_request(lta_rc_mock, deleted_bundle)
    lta_rc_mock.request.assert_called_with("PATCH", '/Bundles/90a664cc-e3f9-4421-973f-7bc2bc7407d0', {
        "claimant": mocker.ANY,
        "claimed": False,
        "claim_timestamp": mocker.ANY,
        "status": "finished",
        "reason": "",
        "update_timestamp": mocker.ANY,
    })
