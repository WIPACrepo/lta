# test_picker.py

from unittest.mock import call
from concurrent.futures import Future
import logging
from unittest.mock import MagicMock
from lta.picker import main
from lta.picker import patch_status_heartbeat
from lta.picker import Picker
import pytest
import requests
from lta.picker import status_loop
from lta.picker import work_loop

null_logger = logging.getLogger("just_testing")


class AsyncMock(MagicMock):
    """
    AsyncMock allows us to mock asynchronous callables. The async
    version of a MagicMock.

    Source: https://stackoverflow.com/a/32498408
    """
    async def __call__(self, *args, **kwargs):
        return super(AsyncMock, self).__call__(*args, **kwargs)


class ObjectLiteral:
    """
    ObjectLiteral is a helper class to define object literals. Useful for
    creating objects used as return values from a mocked API call.

    Source: https://stackoverflow.com/a/3335732
    """
    def __init__(self, **kwds):
        self.__dict__.update(kwds)


def test_constructor_missing_config():
    """
    Fail with a TypeError if a configuration object isn't provided.
    """
    with pytest.raises(TypeError):
        Picker()


def test_constructor_missing_logging():
    """
    Fail with a TypeError if a logging object isn't provided.
    """
    with pytest.raises(TypeError):
        config = {
            "PAN_GALACTIC_GARGLE_BLASTER": "Yummy"
        }
        Picker(config)


def test_constructor_config_missing_values():
    """
    Fail with a ValueError if the configuration object is missing required
    configuration variables.
    """
    with pytest.raises(ValueError):
        config = {
            "PAN_GALACTIC_GARGLE_BLASTER": "Yummy"
        }
        Picker(config, null_logger)


def test_constructor_config():
    """
    Test that a Picker can be constructed with a configuration object and a
    logging object.
    """
    config = {
        "FILE_CATALOG_REST_URL": "http://kVj74wBA1AMTDV8zccn67pGuWJqHZzD7iJQHrUJKA.com/",
        "HEARTBEAT_SLEEP_DURATION_SECONDS": "60",
        "LTA_REST_URL": "http://59F9jQx9Z2cVQOTuQXnTgigy7Mq9CfvPatLeVjnvN7c.com/",
        "PICKER_NAME": "picker",
        "WORK_SLEEP_DURATION_SECONDS": "60"
    }
    p = Picker(config, null_logger)
    assert p.file_catalog_rest_url == "http://kVj74wBA1AMTDV8zccn67pGuWJqHZzD7iJQHrUJKA.com/"
    assert p.heartbeat_sleep_duration_seconds == 60
    assert p.lta_rest_url == "http://59F9jQx9Z2cVQOTuQXnTgigy7Mq9CfvPatLeVjnvN7c.com/"
    assert p.picker_name == "picker"
    assert p.work_sleep_duration_seconds == 60
    assert p.logger == null_logger


def test_constructor_config_sleep_type_int():
    """
    Environment variables should always be provided as strings. However, in the
    case where sleep seconds is provided as an integer, we want that to be OK.
    """
    config = {
        "FILE_CATALOG_REST_URL": "http://kVj74wBA1AMTDV8zccn67pGuWJqHZzD7iJQHrUJKA.com/",
        "HEARTBEAT_SLEEP_DURATION_SECONDS": 60,
        "LTA_REST_URL": "http://59F9jQx9Z2cVQOTuQXnTgigy7Mq9CfvPatLeVjnvN7c.com/",
        "PICKER_NAME": "picker",
        "WORK_SLEEP_DURATION_SECONDS": 60
    }
    p = Picker(config, null_logger)
    assert p.file_catalog_rest_url == "http://kVj74wBA1AMTDV8zccn67pGuWJqHZzD7iJQHrUJKA.com/"
    assert p.heartbeat_sleep_duration_seconds == 60
    assert p.lta_rest_url == "http://59F9jQx9Z2cVQOTuQXnTgigy7Mq9CfvPatLeVjnvN7c.com/"
    assert p.picker_name == "picker"
    assert p.work_sleep_duration_seconds == 60
    assert p.logger == null_logger


def test_constructor_state():
    """
    Verify that the Picker has a reasonable state when it is first constructed.
    """
    config = {
        "FILE_CATALOG_REST_URL": "http://kVj74wBA1AMTDV8zccn67pGuWJqHZzD7iJQHrUJKA.com/",
        "HEARTBEAT_SLEEP_DURATION_SECONDS": "60",
        "LTA_REST_URL": "http://59F9jQx9Z2cVQOTuQXnTgigy7Mq9CfvPatLeVjnvN7c.com/",
        "PICKER_NAME": "picker",
        "WORK_SLEEP_DURATION_SECONDS": "60"
    }
    p = Picker(config, null_logger)
    assert p.file_catalog_ok is False
    assert p.last_work_begin_timestamp is p.last_work_end_timestamp
    assert p.lta_ok is False


@pytest.mark.asyncio
async def test_patch_status_heartbeat_connection_error(mocker):
    """
    Verify that the Picker will change state to indicate that its connection to
    LTA is not OK, and that it will log an error, if the PATCH call results in
    a ConnectionError being raised.
    """
    patch_mock = mocker.patch("requests_futures.sessions.FuturesSession.patch")
    patch_mock.side_effect = requests.exceptions.ConnectionError
    logger_mock = mocker.MagicMock()
    config = {
        "FILE_CATALOG_REST_URL": "http://kVj74wBA1AMTDV8zccn67pGuWJqHZzD7iJQHrUJKA.com/",
        "HEARTBEAT_SLEEP_DURATION_SECONDS": "60",
        "LTA_REST_URL": "http://59F9jQx9Z2cVQOTuQXnTgigy7Mq9CfvPatLeVjnvN7c.com/",
        "PICKER_NAME": "picker",
        "WORK_SLEEP_DURATION_SECONDS": "60"
    }
    p = Picker(config, logger_mock)
    assert p.lta_ok is False
    p.lta_ok = True
    assert p.lta_ok is True
    await patch_status_heartbeat(p)
    assert p.lta_ok is False
    logger_mock.error.assert_called()


@pytest.mark.asyncio
async def test_patch_status_heartbeat_patch_call(mocker):
    """
    Test that the Picker calls the proper URL for the PATCH /status/{component}
    route, and on success (200), updates its internal status to say that the
    connection to LTA is OK.
    """
    patch_mock = mocker.patch("requests_futures.sessions.FuturesSession.patch")
    patch_mock.return_value = Future()
    patch_mock.return_value.set_result(ObjectLiteral(
        status_code=200
    ))
    logger_mock = mocker.MagicMock()
    config = {
        "FILE_CATALOG_REST_URL": "http://localhost/",
        "HEARTBEAT_SLEEP_DURATION_SECONDS": "60",
        "LTA_REST_URL": "http://localhost/",
        "PICKER_NAME": "picker",
        "WORK_SLEEP_DURATION_SECONDS": "60"
    }
    p = Picker(config, logger_mock)
    assert p.lta_ok is False
    retVal = await patch_status_heartbeat(p)
    assert p.lta_ok is True
    assert retVal is True
    patch_mock.assert_called_with("http://localhost/status/picker", data=mocker.ANY)
    logger_mock.assert_not_called()


@pytest.mark.asyncio
async def test_patch_status_heartbeat_patch_call_data(mocker):
    """
    Test that the Picker provides proper status data to the PATCH /status/{component} route.
    """
    patch_mock = mocker.patch("requests_futures.sessions.FuturesSession.patch")
    patch_mock.return_value = Future()
    patch_mock.return_value.set_result(ObjectLiteral(
        status_code=200
    ))
    logger_mock = mocker.MagicMock()
    config = {
        "FILE_CATALOG_REST_URL": "http://localhost/",
        "HEARTBEAT_SLEEP_DURATION_SECONDS": "60",
        "LTA_REST_URL": "http://localhost/",
        "PICKER_NAME": "special-picker-name",
        "WORK_SLEEP_DURATION_SECONDS": "60"
    }
    p = Picker(config, logger_mock)
    assert p.lta_ok is False
    retVal = await patch_status_heartbeat(p)
    assert p.lta_ok is True
    assert retVal is True
    patch_mock.assert_called_with(mocker.ANY, data={
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
async def test_patch_status_heartbeat_patch_call_1xx(mocker):
    """
    Verify that the Picker will change state to indicate that its connection to
    LTA is not OK, and that it will log an error, if the PATCH call results in
    a 1xx series response.
    """
    patch_mock = mocker.patch("requests_futures.sessions.FuturesSession.patch")
    patch_mock.return_value = Future()
    patch_mock.return_value.set_result(ObjectLiteral(
        # 103 Early Hints: https://tools.ietf.org/html/rfc8297
        status_code=103
    ))
    logger_mock = mocker.MagicMock()
    config = {
        "FILE_CATALOG_REST_URL": "http://localhost/",
        "HEARTBEAT_SLEEP_DURATION_SECONDS": "60",
        "LTA_REST_URL": "http://localhost/",
        "PICKER_NAME": "picker",
        "WORK_SLEEP_DURATION_SECONDS": "60"
    }
    p = Picker(config, logger_mock)
    assert p.lta_ok is False
    p.lta_ok = True
    assert p.lta_ok is True
    await patch_status_heartbeat(p)
    assert p.lta_ok is False
    logger_mock.error.assert_called()


@pytest.mark.asyncio
async def test_patch_status_heartbeat_patch_call_3xx(mocker):
    """
    Verify that the Picker will change state to indicate that its connection to
    LTA is not OK, and that it will log an error, if the PATCH call results in
    a 3xx series response.
    """
    patch_mock = mocker.patch("requests_futures.sessions.FuturesSession.patch")
    patch_mock.return_value = Future()
    patch_mock.return_value.set_result(ObjectLiteral(
        # 304 Not Modified: https://tools.ietf.org/html/rfc7232
        status_code=304
    ))
    logger_mock = mocker.MagicMock()
    config = {
        "FILE_CATALOG_REST_URL": "http://localhost/",
        "HEARTBEAT_SLEEP_DURATION_SECONDS": "60",
        "LTA_REST_URL": "http://localhost/",
        "PICKER_NAME": "picker",
        "WORK_SLEEP_DURATION_SECONDS": "60"
    }
    p = Picker(config, logger_mock)
    assert p.lta_ok is False
    p.lta_ok = True
    assert p.lta_ok is True
    await patch_status_heartbeat(p)
    assert p.lta_ok is False
    logger_mock.error.assert_called()


@pytest.mark.asyncio
async def test_patch_status_heartbeat_patch_call_4xx(mocker):
    """
    Verify that the Picker will change state to indicate that its connection to
    LTA is not OK, and that it will log an error, if the PATCH call results in
    a 4xx series response.
    """
    patch_mock = mocker.patch("requests_futures.sessions.FuturesSession.patch")
    patch_mock.return_value = Future()
    patch_mock.return_value.set_result(ObjectLiteral(
        # 400 Bad Request
        status_code=400
    ))
    logger_mock = mocker.MagicMock()
    config = {
        "FILE_CATALOG_REST_URL": "http://localhost/",
        "HEARTBEAT_SLEEP_DURATION_SECONDS": "60",
        "LTA_REST_URL": "http://localhost/",
        "PICKER_NAME": "picker",
        "WORK_SLEEP_DURATION_SECONDS": "60"
    }
    p = Picker(config, logger_mock)
    assert p.lta_ok is False
    p.lta_ok = True
    assert p.lta_ok is True
    await patch_status_heartbeat(p)
    assert p.lta_ok is False
    logger_mock.error.assert_called()


@pytest.mark.asyncio
async def test_status_loop(mocker):
    """
    Ensure the status loop will loop
    """
    # NOTE: The Exception() is a hack to get around the infinite loop in status_loop()
    patch_mock = mocker.patch("lta.picker.patch_status_heartbeat", new_callable=AsyncMock)
    patch_mock.side_effect = [True, Exception()]

    sleep_mock = mocker.patch("asyncio.sleep", new_callable=AsyncMock)
    sleep_mock.side_effect = [None, None]

    logger_mock = mocker.MagicMock()
    config = {
        "FILE_CATALOG_REST_URL": "http://localhost/",
        "HEARTBEAT_SLEEP_DURATION_SECONDS": "60",
        "LTA_REST_URL": "http://localhost/",
        "PICKER_NAME": "picker",
        "WORK_SLEEP_DURATION_SECONDS": "300"
    }
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
async def test_work_loop(mocker):
    """
    Ensure the work loop will loop
    """
    # NOTE: The Exception() is a hack to get around the infinite loop in work_loop()
    run_mock = mocker.patch("lta.picker.Picker.run", new_callable=AsyncMock)
    run_mock.side_effect = [None, Exception()]

    sleep_mock = mocker.patch("asyncio.sleep", new_callable=AsyncMock)
    sleep_mock.side_effect = [None, None]

    logger_mock = mocker.MagicMock()
    config = {
        "FILE_CATALOG_REST_URL": "http://localhost/",
        "HEARTBEAT_SLEEP_DURATION_SECONDS": "60",
        "LTA_REST_URL": "http://localhost/",
        "PICKER_NAME": "picker",
        "WORK_SLEEP_DURATION_SECONDS": "300"
    }
    p = Picker(config, logger_mock)
    # NOTE: This is a hack to get around the infinite loop in work_loop()
    try:
        await work_loop(p)
        assert False, "This should have exited with an Exception"
    except Exception:
        pass
    run_mock.assert_called()
    sleep_mock.assert_called_with(300)


@pytest.mark.asyncio
async def test_script_main(mocker, monkeypatch):
    """
    Test to make sure running the Picker as a script does the setup work
    that we expect and then launches the picker service.
    """
    config = {
        "FILE_CATALOG_REST_URL": "http://localhost/",
        "HEARTBEAT_SLEEP_DURATION_SECONDS": "60",
        "LTA_REST_URL": "http://localhost/",
        "PICKER_NAME": "picker",
        "WORK_SLEEP_DURATION_SECONDS": "300"
    }
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
async def test_picker_run(mocker):
    """
    Test to make sure the Picker does the work the picker should do.
    """
    logger_mock = mocker.MagicMock()
    config = {
        "FILE_CATALOG_REST_URL": "http://localhost/fc/api",
        "HEARTBEAT_SLEEP_DURATION_SECONDS": "60",
        "LTA_REST_URL": "http://localhost/lta/api",
        "PICKER_NAME": "testing-muh-picker",
        "WORK_SLEEP_DURATION_SECONDS": "300"
    }
    p = Picker(config, logger_mock)
    await p.run()
    EXPECTED_LOGGER_CALLS = [
        call("Picker 'testing-muh-picker' is configured:"),
        call('FILE_CATALOG_REST_URL = http://localhost/fc/api'),
        call('HEARTBEAT_SLEEP_DURATION_SECONDS = 60'),
        call('LTA_REST_URL = http://localhost/lta/api'),
        call('PICKER_NAME = testing-muh-picker'),
        call('WORK_SLEEP_DURATION_SECONDS = 300'),
        call('Starting picker work cycle'),
        call('Ending picker work cycle')
    ]
    logger_mock.info.assert_has_calls(EXPECTED_LOGGER_CALLS)
