# test_replicator.py
"""Unit tests for lta/replicator.py."""

from unittest.mock import call  # MagicMock

import pytest  # type: ignore
from tornado.web import HTTPError  # type: ignore

from lta.replicator import main, Replicator
from .test_util import AsyncMock

@pytest.fixture
def config():
    """Supply a stock Replicator component configuration."""
    return {
        "COMPONENT_NAME": "testing-replicator",
        "HEARTBEAT_PATCH_RETRIES": "3",
        "HEARTBEAT_PATCH_TIMEOUT_SECONDS": "30",
        "HEARTBEAT_SLEEP_DURATION_SECONDS": "60",
        "LTA_REST_TOKEN": "fake-lta-rest-token",
        "LTA_REST_URL": "http://RmMNHdPhHpH2ZxfaFAC9d2jiIbf5pZiHDqy43rFLQiM.com/",
        "RUN_ONCE_AND_DIE": "False",
        "SOURCE_SITE": "WIPAC",
        "TRANSFER_CONFIG_PATH": "examples/rucio.json",
        "WORK_RETRIES": "3",
        "WORK_SLEEP_DURATION_SECONDS": "60",
        "WORK_TIMEOUT_SECONDS": "30",
    }

def test_constructor_config(config, mocker):
    """Test that a Replicator can be constructed with a configuration object and a logging object."""
    logger_mock = mocker.MagicMock()
    p = Replicator(config, logger_mock)
    assert p.name == "testing-replicator"
    assert p.heartbeat_patch_retries == 3
    assert p.heartbeat_patch_timeout_seconds == 30
    assert p.heartbeat_sleep_duration_seconds == 60
    assert p.lta_rest_token == "fake-lta-rest-token"
    assert p.lta_rest_url == "http://RmMNHdPhHpH2ZxfaFAC9d2jiIbf5pZiHDqy43rFLQiM.com/"
    assert p.source_site == "WIPAC"
    assert p.transfer_config
    assert p.work_retries == 3
    assert p.work_sleep_duration_seconds == 60
    assert p.work_timeout_seconds == 30
    assert p.logger == logger_mock

def test_do_status(config, mocker):
    """Verify that the Replicator has no additional state to offer."""
    logger_mock = mocker.MagicMock()
    p = Replicator(config, logger_mock)
    assert p._do_status() == {}

@pytest.mark.asyncio
async def test_replicator_logs_configuration(mocker):
    """Test to make sure the Replicator logs its configuration."""
    logger_mock = mocker.MagicMock()
    replicator_config = {
        "COMPONENT_NAME": "logme-testing-replicator",
        "HEARTBEAT_PATCH_RETRIES": "1",
        "HEARTBEAT_PATCH_TIMEOUT_SECONDS": "20",
        "HEARTBEAT_SLEEP_DURATION_SECONDS": "30",
        "LTA_REST_TOKEN": "logme-fake-lta-rest-token",
        "LTA_REST_URL": "logme-http://zjwdm5ggeEgS1tZDZy9l1DOZU53uiSO4Urmyb8xL0.com/",
        "RUN_ONCE_AND_DIE": "False",
        "SOURCE_SITE": "WIPAC",
        "TRANSFER_CONFIG_PATH": "examples/rucio.json",
        "WORK_RETRIES": "5",
        "WORK_SLEEP_DURATION_SECONDS": "70",
        "WORK_TIMEOUT_SECONDS": "90",
    }
    Replicator(replicator_config, logger_mock)
    EXPECTED_LOGGER_CALLS = [
        call("replicator 'logme-testing-replicator' is configured:"),
        call('COMPONENT_NAME = logme-testing-replicator'),
        call('HEARTBEAT_PATCH_RETRIES = 1'),
        call('HEARTBEAT_PATCH_TIMEOUT_SECONDS = 20'),
        call('HEARTBEAT_SLEEP_DURATION_SECONDS = 30'),
        call('LTA_REST_TOKEN = logme-fake-lta-rest-token'),
        call('LTA_REST_URL = logme-http://zjwdm5ggeEgS1tZDZy9l1DOZU53uiSO4Urmyb8xL0.com/'),
        call('RUN_ONCE_AND_DIE = False'),
        call('SOURCE_SITE = WIPAC'),
        call('TRANSFER_CONFIG_PATH = examples/rucio.json'),
        call('WORK_RETRIES = 5'),
        call('WORK_SLEEP_DURATION_SECONDS = 70'),
        call('WORK_TIMEOUT_SECONDS = 90')
    ]
    logger_mock.info.assert_has_calls(EXPECTED_LOGGER_CALLS)

@pytest.mark.asyncio
async def test_script_main(config, mocker, monkeypatch):
    """
    Verify Replicator component behavior when run as a script.

    Test to make sure running the Replicator as a script does the setup work
    that we expect and then launches the replicator service.
    """
    for key in config.keys():
        monkeypatch.setenv(key, config[key])
    mock_event_loop = mocker.patch("asyncio.get_event_loop")
    mock_root_logger = mocker.patch("logging.getLogger")
    mock_status_loop = mocker.patch("lta.replicator.status_loop")
    mock_work_loop = mocker.patch("lta.replicator.work_loop")
    main()
    mock_event_loop.assert_called()
    mock_root_logger.assert_called()
    mock_status_loop.assert_called()
    mock_work_loop.assert_called()

@pytest.mark.asyncio
async def test_replicator_run(config, mocker):
    """Test the Replicator does the work the replicator should do."""
    logger_mock = mocker.MagicMock()
    p = Replicator(config, logger_mock)
    p._do_work = AsyncMock()
    await p.run()
    p._do_work.assert_called()

@pytest.mark.asyncio
async def test_replicator_run_exception(config, mocker):
    """Test an error doesn't kill the Replicator."""
    logger_mock = mocker.MagicMock()
    p = Replicator(config, logger_mock)
    p.last_work_end_timestamp = None
    p._do_work = AsyncMock()
    p._do_work.side_effect = [Exception("bad thing happen!")]
    await p.run()
    p._do_work.assert_called()
    assert p.last_work_end_timestamp

@pytest.mark.asyncio
async def test_replicator_do_work_pop_exception(config, mocker):
    """Test that _do_work raises when the RestClient can't pop."""
    logger_mock = mocker.MagicMock()
    lta_rc_mock = mocker.patch("rest_tools.client.RestClient.request", new_callable=AsyncMock)
    lta_rc_mock.side_effect = HTTPError(500, "LTA DB on fire. Again.")
    p = Replicator(config, logger_mock)
    with pytest.raises(HTTPError):
        await p._do_work()
    lta_rc_mock.assert_called_with("POST", '/Bundles/actions/pop?source=WIPAC&status=created', {'claimant': f'{p.name}-{p.instance_uuid}'})

@pytest.mark.asyncio
async def test_replicator_do_work_no_results(config, mocker):
    """Test that _do_work goes on vacation when the LTA DB has no work."""
    logger_mock = mocker.MagicMock()
    dwc_mock = mocker.patch("lta.replicator.Replicator._do_work_claim", new_callable=AsyncMock)
    dwc_mock.return_value = False
    p = Replicator(config, logger_mock)
    await p._do_work()
    dwc_mock.assert_called()

@pytest.mark.asyncio
async def test_replicator_do_work_yes_results(config, mocker):
    """Test that _do_work keeps working until the LTA DB has no work."""
    logger_mock = mocker.MagicMock()
    dwc_mock = mocker.patch("lta.replicator.Replicator._do_work_claim", new_callable=AsyncMock)
    dwc_mock.side_effect = [True, True, False]
    p = Replicator(config, logger_mock)
    await p._do_work()
    dwc_mock.assert_called()

@pytest.mark.asyncio
async def test_replicator_do_work_claim_no_result(config, mocker):
    """Test that _do_work_claim does not work when the LTA DB has no work."""
    logger_mock = mocker.MagicMock()
    lta_rc_mock = mocker.patch("rest_tools.client.RestClient.request", new_callable=AsyncMock)
    lta_rc_mock.return_value = {
        "bundle": None
    }
    rbtds_mock = mocker.patch("lta.replicator.Replicator._replicate_bundle_to_destination_site", new_callable=AsyncMock)
    p = Replicator(config, logger_mock)
    await p._do_work_claim()
    lta_rc_mock.assert_called_with("POST", '/Bundles/actions/pop?source=WIPAC&status=created', {'claimant': f'{p.name}-{p.instance_uuid}'})
    rbtds_mock.assert_not_called()

@pytest.mark.asyncio
async def test_replicator_do_work_claim_yes_result(config, mocker):
    """Test that _do_work_claim processes the Bundle that it gets from the LTA DB."""
    logger_mock = mocker.MagicMock()
    lta_rc_mock = mocker.patch("rest_tools.client.RestClient.request", new_callable=AsyncMock)
    lta_rc_mock.return_value = {
        "bundle": {
            "one": 1,
        },
    }
    rbtds_mock = mocker.patch("lta.replicator.Replicator._replicate_bundle_to_destination_site", new_callable=AsyncMock)
    p = Replicator(config, logger_mock)
    await p._do_work_claim()
    lta_rc_mock.assert_called_with("POST", '/Bundles/actions/pop?source=WIPAC&status=created', {'claimant': f'{p.name}-{p.instance_uuid}'})
    rbtds_mock.assert_called_with(mocker.ANY, {"one": 1})

@pytest.mark.asyncio
async def test_replicator_replicate_bundle_to_destination_site(config, mocker):
    """Test that _replicate_bundle_to_destination_site initiates a bundle transfer."""
    logger_mock = mocker.MagicMock()
    lta_rc_mock = mocker.patch("rest_tools.client.RestClient", new_callable=AsyncMock)
    inst_mock = mocker.patch("lta.replicator.instantiate")
    xfer_service_mock = AsyncMock()
    inst_mock.return_value = xfer_service_mock
    bundle_obj = {
        "uuid": "8286d3ba-fb1b-4923-876d-935bdf7fc99e"
    }
    p = Replicator(config, logger_mock)
    await p._replicate_bundle_to_destination_site(lta_rc_mock, bundle_obj)
    inst_mock.assert_called_with(p.transfer_config)
    xfer_service_mock.start.assert_called_with(bundle_obj)
    lta_rc_mock.request.assert_called_with("PATCH", '/Bundles/8286d3ba-fb1b-4923-876d-935bdf7fc99e', mocker.ANY)
