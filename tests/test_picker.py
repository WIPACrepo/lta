# test_picker.py
"""Unit tests for lta/picker.py."""

from asyncio import Future
from unittest.mock import call, MagicMock
import logging
import pytest
import requests

from lta.picker import main, patch_status_heartbeat, Picker, status_loop, work_loop

null_logger = logging.getLogger("just_testing")


class AsyncMock(MagicMock):
    """
    AsyncMock is the async version of a MagicMock.

    We use this class in place of MagicMock when we want to mock
    asynchronous callables.

    Source: https://stackoverflow.com/a/32498408
    """

    async def __call__(self, *args, **kwargs):
        """Allow MagicMock to work its magic too."""
        return super(AsyncMock, self).__call__(*args, **kwargs)


class ObjectLiteral:
    """
    ObjectLiteral transforms named arguments into object attributes.

    This is useful for creating object literals to be used as return
    values from mocked API calls.

    Source: https://stackoverflow.com/a/3335732
    """

    def __init__(self, **kwds):
        """Add attributes to ourself with the provided named arguments."""
        self.__dict__.update(kwds)


@pytest.fixture
def config():
    """Supply a stock Picker component configuration."""
    return {
        "FILE_CATALOG_REST_URL": "http://kVj74wBA1AMTDV8zccn67pGuWJqHZzD7iJQHrUJKA.com/",
        "HEARTBEAT_PATCH_RETRIES": "3",
        "HEARTBEAT_PATCH_TIMEOUT_SECONDS": "30",
        "HEARTBEAT_SLEEP_DURATION_SECONDS": "60",
        "LTA_REST_TOKEN": "fake-lta-rest-token",
        "LTA_REST_URL": "http://RmMNHdPhHpH2ZxfaFAC9d2jiIbf5pZiHDqy43rFLQiM.com/",
        "PICKER_NAME": "testing-picker",
        "WORK_SLEEP_DURATION_SECONDS": "60"
    }


def test_constructor_missing_config():
    """Fail with a TypeError if a configuration object isn't provided."""
    with pytest.raises(TypeError):
        Picker()


def test_constructor_missing_logging():
    """Fail with a TypeError if a logging object isn't provided."""
    with pytest.raises(TypeError):
        config = {
            "PAN_GALACTIC_GARGLE_BLASTER": "Yummy"
        }
        Picker(config)


def test_constructor_config_missing_values():
    """Fail with a ValueError if the configuration object is missing required configuration variables."""
    with pytest.raises(ValueError):
        config = {
            "PAN_GALACTIC_GARGLE_BLASTER": "Yummy"
        }
        Picker(config, null_logger)


def test_constructor_config(config):
    """Test that a Picker can be constructed with a configuration object and a logging object."""
    p = Picker(config, null_logger)
    assert p.file_catalog_rest_url == "http://kVj74wBA1AMTDV8zccn67pGuWJqHZzD7iJQHrUJKA.com/"
    assert p.heartbeat_sleep_duration_seconds == 60
    assert p.lta_rest_url == "http://RmMNHdPhHpH2ZxfaFAC9d2jiIbf5pZiHDqy43rFLQiM.com/"
    assert p.picker_name == "testing-picker"
    assert p.work_sleep_duration_seconds == 60
    assert p.logger == null_logger


def test_constructor_config_sleep_type_int(config):
    """Ensure that sleep seconds can also be provided as an integer."""
    p = Picker(config, null_logger)
    assert p.file_catalog_rest_url == "http://kVj74wBA1AMTDV8zccn67pGuWJqHZzD7iJQHrUJKA.com/"
    assert p.heartbeat_sleep_duration_seconds == 60
    assert p.lta_rest_url == "http://RmMNHdPhHpH2ZxfaFAC9d2jiIbf5pZiHDqy43rFLQiM.com/"
    assert p.picker_name == "testing-picker"
    assert p.work_sleep_duration_seconds == 60
    assert p.logger == null_logger


def test_constructor_state(config):
    """Verify that the Picker has a reasonable state when it is first constructed."""
    p = Picker(config, null_logger)
    assert p.file_catalog_ok is False
    assert p.last_work_begin_timestamp is p.last_work_end_timestamp
    assert p.lta_ok is False


@pytest.mark.asyncio
async def test_patch_status_heartbeat_connection_error(config, mocker):
    """
    Verify Picker behavior when status heartbeat patches fail.

    The Picker will change state to indicate that its connection to LTA is
    not OK, and it will log an error, if the PATCH call results in a
    ConnectionError being raised.
    """
    patch_mock = mocker.patch("rest_tools.client.RestClient.request")
    patch_mock.side_effect = requests.exceptions.HTTPError
    logger_mock = mocker.MagicMock()
    p = Picker(config, logger_mock)
    assert p.lta_ok is False
    p.lta_ok = True
    assert p.lta_ok is True
    await patch_status_heartbeat(p)
    assert p.lta_ok is False
    logger_mock.error.assert_called()


@pytest.mark.asyncio
async def test_patch_status_heartbeat_patch_call(config, mocker):
    """
    Verify Picker behavior when status heartbeat patches succeed.

    Test that the Picker calls the proper URL for the PATCH /status/{component}
    route, and on success (200), updates its internal status to say that the
    connection to LTA is OK.
    """
    patch_mock = mocker.patch("rest_tools.client.RestClient.request")
    patch_mock.return_value = Future()
    patch_mock.return_value.set_result(ObjectLiteral(
        status_code=200
    ))
    logger_mock = mocker.MagicMock()
    p = Picker(config, logger_mock)
    assert p.lta_ok is False
    retVal = await patch_status_heartbeat(p)
    assert p.lta_ok is True
    assert retVal is True
    patch_mock.assert_called_with("PATCH", "/status/picker", mocker.ANY)
    logger_mock.assert_not_called()


@pytest.mark.asyncio
async def test_patch_status_heartbeat_patch_call_data(config, mocker):
    """
    Verify Picker behavior when status heartbeat patches succeed.

    Test that the Picker provides proper status data to the
    PATCH /status/{component} route.
    """
    patch_mock = mocker.patch("rest_tools.client.RestClient.request")
    patch_mock.return_value = Future()
    patch_mock.return_value.set_result(ObjectLiteral(
        status_code=200
    ))
    logger_mock = mocker.MagicMock()
    picker_config = config.copy()
    picker_config["PICKER_NAME"] = "special-picker-name"
    p = Picker(picker_config, logger_mock)
    assert p.lta_ok is False
    retVal = await patch_status_heartbeat(p)
    assert p.lta_ok is True
    assert retVal is True
    patch_mock.assert_called_with(mocker.ANY, mocker.ANY, {
        "special-picker-name": {
            "timestamp": mocker.ANY,
            "file_catalog_ok": False,
            "last_work_begin_timestamp": mocker.ANY,
            "last_work_end_timestamp": mocker.ANY,
            "lta_ok": False
        }
    })
    logger_mock.assert_not_called()


@pytest.mark.asyncio
async def test_patch_status_heartbeat_patch_call_4xx(config, mocker):
    """
    Verify Picker behavior when status heartbeat patches fail.

    The Picker will change state to indicate that its connection to LTA is
    not OK, and that it will log an error, if the PATCH call results in a
    4xx series response.
    """
    patch_mock = mocker.patch("rest_tools.client.RestClient.request")
    patch_mock.side_effect = requests.exceptions.HTTPError("400 Bad Request")
    logger_mock = mocker.MagicMock()
    p = Picker(config, logger_mock)
    assert p.lta_ok is False
    p.lta_ok = True
    assert p.lta_ok is True
    await patch_status_heartbeat(p)
    assert p.lta_ok is False
    logger_mock.error.assert_called()


@pytest.mark.asyncio
async def test_status_loop(config, mocker):
    """Ensure the status loop will loop."""
    # NOTE: The Exception() is a hack to get around the infinite loop in status_loop()
    patch_mock = mocker.patch("lta.picker.patch_status_heartbeat", new_callable=AsyncMock)
    patch_mock.side_effect = [True, Exception()]

    sleep_mock = mocker.patch("asyncio.sleep", new_callable=AsyncMock)
    sleep_mock.side_effect = [None, None]

    logger_mock = mocker.MagicMock()
    p = Picker(config, logger_mock)
    # NOTE: This is a hack to get around the infinite loop in status_loop()
    try:
        await status_loop(p)
        assert False, "This should have exited with an Exception"
    except Exception:
        pass
    patch_mock.assert_called_with(p)
    sleep_mock.assert_called_with(60)


@pytest.mark.asyncio
async def test_work_loop(config, mocker):
    """Ensure the work loop will loop."""
    # NOTE: The Exception() is a hack to get around the infinite loop in work_loop()
    run_mock = mocker.patch("lta.picker.Picker.run", new_callable=AsyncMock)
    run_mock.side_effect = [None, Exception()]

    sleep_mock = mocker.patch("asyncio.sleep", new_callable=AsyncMock)
    sleep_mock.side_effect = [None, None]

    logger_mock = mocker.MagicMock()
    picker_config = config.copy()
    picker_config["WORK_SLEEP_DURATION_SECONDS"] = "300"
    p = Picker(picker_config, logger_mock)
    # NOTE: This is a hack to get around the infinite loop in work_loop()
    try:
        await work_loop(p)
        assert False, "This should have exited with an Exception"
    except Exception:
        pass
    run_mock.assert_called()
    sleep_mock.assert_called_with(300)


@pytest.mark.asyncio
async def test_script_main(config, mocker, monkeypatch):
    """
    Verify Picker component behavior when run as a script.

    Test to make sure running the Picker as a script does the setup work
    that we expect and then launches the picker service.
    """
    for key in config.keys():
        monkeypatch.setenv(key, config[key])
    mock_event_loop = mocker.patch("asyncio.get_event_loop")
    mock_root_logger = mocker.patch("logging.getLogger")
    mock_status_loop = mocker.patch("lta.picker.status_loop")
    mock_work_loop = mocker.patch("lta.picker.work_loop")
    main()
    mock_event_loop.assert_called()
    mock_root_logger.assert_called()
    mock_status_loop.assert_called()
    mock_work_loop.assert_called()


@pytest.mark.asyncio
async def test_picker_run(config, mocker):
    """Test to make sure the Picker does the work the picker should do."""
    logger_mock = mocker.MagicMock()
    picker_config = config.copy()
    picker_config["PICKER_NAME"] = "testing-muh-picker"
    p = Picker(picker_config, logger_mock)
    await p.run()
    EXPECTED_LOGGER_CALLS = [
        call("Picker 'testing-muh-picker' is configured:"),
        call('FILE_CATALOG_REST_URL = http://kVj74wBA1AMTDV8zccn67pGuWJqHZzD7iJQHrUJKA.com/'),
        call('HEARTBEAT_PATCH_RETRIES = 3'),
        call('HEARTBEAT_PATCH_TIMEOUT_SECONDS = 30'),
        call('HEARTBEAT_SLEEP_DURATION_SECONDS = 60'),
        call('LTA_REST_TOKEN = fake-lta-rest-token'),
        call('LTA_REST_URL = http://RmMNHdPhHpH2ZxfaFAC9d2jiIbf5pZiHDqy43rFLQiM.com/'),
        call('PICKER_NAME = testing-muh-picker'),
        call('WORK_SLEEP_DURATION_SECONDS = 60'),
        call('Starting picker work cycle'),
        call('Ending picker work cycle')
    ]
    logger_mock.info.assert_has_calls(EXPECTED_LOGGER_CALLS)
