# test_picker.py

import logging
from lta.picker import Picker
import pytest
import requests

null_logger = logging.getLogger("just_testing")


def Any(cls):
    """
    Any is a class that always returns True when compared. This is useful for
    assert_called_with() when we don't really care about the specifics of a
    particular argument.
    """
    class Any(cls):
        def __eq__(self, other):
            return True
    return Any()


class ObjectLiteral:
    """
    ObjectLiteral is a helper class to define object literals. Useful for
    creating objects used as return values from a mocked API call.

    Source: https://stackoverflow.com/a/3335732
    """
    def __init__(self, **kwds):
        self.__dict__.update(kwds)


@pytest.mark.xfail(raises=TypeError, strict=True)
def test_constructor_missing_config():
    """
    Fail with a TypeError if a configuration object isn't provided.
    """
    Picker()


@pytest.mark.xfail(raises=TypeError, strict=True)
def test_constructor_missing_logging():
    """
    Fail with a TypeError if a logging object isn't provided.
    """
    config = {
        "PAN_GALACTIC_GARGLE_BLASTER": "Yummy"
    }
    Picker(config)


@pytest.mark.xfail(raises=ValueError, strict=True)
def test_constructor_config_missing_values():
    """
    Fail with a ValueError if the configuration object is missing required
    configuration variables.
    """
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
        "LTA_REST_URL": "http://59F9jQx9Z2cVQOTuQXnTgigy7Mq9CfvPatLeVjnvN7c.com/",
        "PICKER_NAME": "picker",
        "SLEEP_DURATION_SECONDS": "60"
    }
    p = Picker(config, null_logger)
    assert p.file_catalog_rest_url == "http://kVj74wBA1AMTDV8zccn67pGuWJqHZzD7iJQHrUJKA.com/"
    assert p.lta_rest_url == "http://59F9jQx9Z2cVQOTuQXnTgigy7Mq9CfvPatLeVjnvN7c.com/"
    assert p.picker_name == "picker"
    assert p.sleep_duration_seconds == 60
    assert p.logger == null_logger


def test_constructor_config_sleep_type_int():
    """
    Environment variables should always be provided as strings. However, in the
    case where sleep seconds is provided as an integer, we want that to be OK.
    """
    config = {
        "FILE_CATALOG_REST_URL": "http://kVj74wBA1AMTDV8zccn67pGuWJqHZzD7iJQHrUJKA.com/",
        "LTA_REST_URL": "http://59F9jQx9Z2cVQOTuQXnTgigy7Mq9CfvPatLeVjnvN7c.com/",
        "PICKER_NAME": "picker",
        "SLEEP_DURATION_SECONDS": 60
    }
    p = Picker(config, null_logger)
    assert p.file_catalog_rest_url == "http://kVj74wBA1AMTDV8zccn67pGuWJqHZzD7iJQHrUJKA.com/"
    assert p.lta_rest_url == "http://59F9jQx9Z2cVQOTuQXnTgigy7Mq9CfvPatLeVjnvN7c.com/"
    assert p.picker_name == "picker"
    assert p.sleep_duration_seconds == 60
    assert p.logger == null_logger


def test_constructor_state():
    """
    Verify that the Picker has a reasonable state when it is first constructed.
    """
    config = {
        "FILE_CATALOG_REST_URL": "http://kVj74wBA1AMTDV8zccn67pGuWJqHZzD7iJQHrUJKA.com/",
        "LTA_REST_URL": "http://59F9jQx9Z2cVQOTuQXnTgigy7Mq9CfvPatLeVjnvN7c.com/",
        "PICKER_NAME": "picker",
        "SLEEP_DURATION_SECONDS": "60"
    }
    p = Picker(config, null_logger)
    assert p.file_catalog_OK is False
    assert p.last_work is None
    assert p.lta_OK is False
    assert p.work_duration == 0


def test_patch_status_heartbeat_connection_error(mocker):
    """
    Verify that the Picker will change state to indicate that its connection to
    LTA is not OK, and that it will log an error, if the PATCH call results in
    a ConnectionError being raised.
    """
    patch_mock = mocker.patch("requests.patch")
    patch_mock.side_effect = requests.exceptions.ConnectionError
    logger_mock = mocker.MagicMock()
    config = {
        "FILE_CATALOG_REST_URL": "http://localhost/",
        "LTA_REST_URL": "http://localhost/",
        "PICKER_NAME": "picker",
        "SLEEP_DURATION_SECONDS": "60"
    }
    p = Picker(config, logger_mock)
    assert p.lta_OK is False
    p.lta_OK = True
    assert p.lta_OK is True
    p._patch_status_heartbeat()
    assert p.lta_OK is False
    logger_mock.error.assert_called()


def test_patch_status_heartbeat_patch_call(mocker):
    """
    Test that the Picker calls the proper URL for the PATCH /status/{component}
    route, and on success (200), updates its internal status to say that the
    connection to LTA is OK.
    """
    patch_mock = mocker.patch("requests.patch")
    patch_mock.return_value = ObjectLiteral(
        status_code=200
    )
    logger_mock = mocker.MagicMock()
    config = {
        "FILE_CATALOG_REST_URL": "http://localhost/",
        "LTA_REST_URL": "http://localhost/",
        "PICKER_NAME": "picker",
        "SLEEP_DURATION_SECONDS": "60"
    }
    p = Picker(config, logger_mock)
    assert p.lta_OK is False
    retVal = p._patch_status_heartbeat()
    assert p.lta_OK is True
    assert retVal is True
    patch_mock.assert_called_with("http://localhost/status/picker", data=Any(dict))
    logger_mock.assert_not_called()


def test_patch_status_heartbeat_patch_call_data(mocker):
    """
    Test that the Picker provides proper status data to the PATCH /status/{component} route.
    """
    patch_mock = mocker.patch("requests.patch")
    patch_mock.return_value = ObjectLiteral(
        status_code=200
    )
    logger_mock = mocker.MagicMock()
    config = {
        "FILE_CATALOG_REST_URL": "http://localhost/",
        "LTA_REST_URL": "http://localhost/",
        "PICKER_NAME": "special-picker-name",
        "SLEEP_DURATION_SECONDS": "60"
    }
    p = Picker(config, logger_mock)
    assert p.lta_OK is False
    retVal = p._patch_status_heartbeat()
    assert p.lta_OK is True
    assert retVal is True
    patch_mock.assert_called_with(Any(str), data={
        "special-picker-name": {
            "t": Any(str),
            "fc": False,
            "lta": False,
            "last_work": Any(str),
            "work_duration": Any(int)
        }
    })
    logger_mock.assert_not_called()


def test_patch_status_heartbeat_patch_call_1xx(mocker):
    """
    Verify that the Picker will change state to indicate that its connection to
    LTA is not OK, and that it will log an error, if the PATCH call results in
    a 1xx series response.
    """
    patch_mock = mocker.patch("requests.patch")
    patch_mock.return_value = ObjectLiteral(
        # 103 Early Hints: https://tools.ietf.org/html/rfc8297
        status_code=103
    )
    logger_mock = mocker.MagicMock()
    config = {
        "FILE_CATALOG_REST_URL": "http://localhost/",
        "LTA_REST_URL": "http://localhost/",
        "PICKER_NAME": "picker",
        "SLEEP_DURATION_SECONDS": "60"
    }
    p = Picker(config, logger_mock)
    assert p.lta_OK is False
    p.lta_OK = True
    assert p.lta_OK is True
    p._patch_status_heartbeat()
    assert p.lta_OK is False
    logger_mock.error.assert_called()


def test_patch_status_heartbeat_patch_call_3xx(mocker):
    """
    Verify that the Picker will change state to indicate that its connection to
    LTA is not OK, and that it will log an error, if the PATCH call results in
    a 3xx series response.
    """
    patch_mock = mocker.patch("requests.patch")
    patch_mock.return_value = ObjectLiteral(
        # 304 Not Modified: https://tools.ietf.org/html/rfc7232
        status_code=304
    )
    logger_mock = mocker.MagicMock()
    config = {
        "FILE_CATALOG_REST_URL": "http://localhost/",
        "LTA_REST_URL": "http://localhost/",
        "PICKER_NAME": "picker",
        "SLEEP_DURATION_SECONDS": "60"
    }
    p = Picker(config, logger_mock)
    assert p.lta_OK is False
    p.lta_OK = True
    assert p.lta_OK is True
    p._patch_status_heartbeat()
    assert p.lta_OK is False
    logger_mock.error.assert_called()


def test_patch_status_heartbeat_patch_call_4xx(mocker):
    """
    Verify that the Picker will change state to indicate that its connection to
    LTA is not OK, and that it will log an error, if the PATCH call results in
    a 4xx series response.
    """
    patch_mock = mocker.patch("requests.patch")
    patch_mock.return_value = ObjectLiteral(
        # 400 Bad Request
        status_code=400
    )
    logger_mock = mocker.MagicMock()
    config = {
        "FILE_CATALOG_REST_URL": "http://localhost/",
        "LTA_REST_URL": "http://localhost/",
        "PICKER_NAME": "picker",
        "SLEEP_DURATION_SECONDS": "60"
    }
    p = Picker(config, logger_mock)
    assert p.lta_OK is False
    p.lta_OK = True
    assert p.lta_OK is True
    p._patch_status_heartbeat()
    assert p.lta_OK is False
    logger_mock.error.assert_called()
