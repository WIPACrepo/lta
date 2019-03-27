# test_bundler.py
"""Unit tests for lta/bundler.py."""

from asyncio import Future
from unittest.mock import call, mock_open, patch

import pytest  # type: ignore
import requests
from tornado.web import HTTPError  # type: ignore

from lta.bundler import Bundler, main, patch_status_heartbeat, status_loop, work_loop
from .test_util import AsyncMock, ObjectLiteral


@pytest.fixture
def config():
    """Supply a stock Bundler component configuration."""
    return {
        "BUNDLER_NAME": "testing-bundler",
        "BUNDLER_SITE_SOURCE": "WIPAC",
        "HEARTBEAT_PATCH_RETRIES": "3",
        "HEARTBEAT_PATCH_TIMEOUT_SECONDS": "30",
        "HEARTBEAT_SLEEP_DURATION_SECONDS": "60",
        "LTA_REST_TOKEN": "fake-lta-rest-token",
        "LTA_REST_URL": "http://RmMNHdPhHpH2ZxfaFAC9d2jiIbf5pZiHDqy43rFLQiM.com/",
        "LTA_SITE_CONFIG": "etc/site.json",
        "OUTBOX_PATH": "/tmp/lta/testing/bundler/outbox",
        "WORK_RETRIES": "3",
        "WORK_SLEEP_DURATION_SECONDS": "60",
        "WORK_TIMEOUT_SECONDS": "30",
        "WORKBOX_PATH": "/tmp/lta/testing/bundler/workbox",
    }


def test_constructor_missing_config():
    """Fail with a TypeError if a configuration object isn't provided."""
    with pytest.raises(TypeError):
        Bundler()


def test_constructor_missing_logging():
    """Fail with a TypeError if a logging object isn't provided."""
    with pytest.raises(TypeError):
        config = {
            "PAN_GALACTIC_GARGLE_BLASTER": "Yummy"
        }
        Bundler(config)


def test_constructor_config_missing_values(mocker):
    """Fail with a ValueError if the configuration object is missing required configuration variables."""
    config = {
        "PAN_GALACTIC_GARGLE_BLASTER": "Yummy"
    }
    logger_mock = mocker.MagicMock()
    with pytest.raises(ValueError):
        Bundler(config, logger_mock)


def test_constructor_config_poison_values(config, mocker):
    """Fail with a ValueError if the configuration object is missing required configuration variables."""
    bundler_config = config.copy()
    bundler_config["LTA_REST_URL"] = None
    logger_mock = mocker.MagicMock()
    with pytest.raises(ValueError):
        Bundler(bundler_config, logger_mock)


def test_constructor_config(config, mocker):
    """Test that a Bundler can be constructed with a configuration object and a logging object."""
    logger_mock = mocker.MagicMock()
    p = Bundler(config, logger_mock)
    assert p.heartbeat_sleep_duration_seconds == 60
    assert p.lta_rest_url == "http://RmMNHdPhHpH2ZxfaFAC9d2jiIbf5pZiHDqy43rFLQiM.com/"
    assert p.bundler_name == "testing-bundler"
    assert p.work_sleep_duration_seconds == 60
    assert p.logger == logger_mock


def test_constructor_config_sleep_type_int(config, mocker):
    """Ensure that sleep seconds can also be provided as an integer."""
    logger_mock = mocker.MagicMock()
    p = Bundler(config, logger_mock)
    assert p.heartbeat_sleep_duration_seconds == 60
    assert p.lta_rest_url == "http://RmMNHdPhHpH2ZxfaFAC9d2jiIbf5pZiHDqy43rFLQiM.com/"
    assert p.bundler_name == "testing-bundler"
    assert p.work_sleep_duration_seconds == 60
    assert p.logger == logger_mock


def test_constructor_state(config, mocker):
    """Verify that the Bundler has a reasonable state when it is first constructed."""
    logger_mock = mocker.MagicMock()
    p = Bundler(config, logger_mock)
    assert p.last_work_begin_timestamp is p.last_work_end_timestamp
    assert p.lta_ok is False


@pytest.mark.asyncio
async def test_patch_status_heartbeat_connection_error(config, mocker):
    """
    Verify Bundler behavior when status heartbeat patches fail.

    The Bundler will change state to indicate that its connection to LTA is
    not OK, and it will log an error, if the PATCH call results in a
    ConnectionError being raised.
    """
    patch_mock = mocker.patch("rest_tools.client.RestClient.request")
    patch_mock.side_effect = requests.exceptions.HTTPError
    logger_mock = mocker.MagicMock()
    p = Bundler(config, logger_mock)
    assert p.lta_ok is False
    p.lta_ok = True
    assert p.lta_ok is True
    await patch_status_heartbeat(p)
    assert p.lta_ok is False
    logger_mock.error.assert_called()


@pytest.mark.asyncio
async def test_patch_status_heartbeat_patch_call(config, mocker):
    """
    Verify Bundler behavior when status heartbeat patches succeed.

    Test that the Bundler calls the proper URL for the PATCH /status/{component}
    route, and on success (200), updates its internal status to say that the
    connection to LTA is OK.
    """
    patch_mock = mocker.patch("rest_tools.client.RestClient.request")
    patch_mock.return_value = Future()
    patch_mock.return_value.set_result(ObjectLiteral(
        status_code=200
    ))
    logger_mock = mocker.MagicMock()
    p = Bundler(config, logger_mock)
    assert p.lta_ok is False
    retVal = await patch_status_heartbeat(p)
    assert p.lta_ok is True
    assert retVal is True
    patch_mock.assert_called_with("PATCH", "/status/bundler", mocker.ANY)
    logger_mock.assert_not_called()


@pytest.mark.asyncio
async def test_patch_status_heartbeat_patch_call_data(config, mocker):
    """
    Verify Bundler behavior when status heartbeat patches succeed.

    Test that the Bundler provides proper status data to the
    PATCH /status/{component} route.
    """
    patch_mock = mocker.patch("rest_tools.client.RestClient.request")
    patch_mock.return_value = Future()
    patch_mock.return_value.set_result(ObjectLiteral(
        status_code=200
    ))
    logger_mock = mocker.MagicMock()
    bundler_config = config.copy()
    bundler_config["BUNDLER_NAME"] = "special-bundler-name"
    p = Bundler(bundler_config, logger_mock)
    assert p.lta_ok is False
    retVal = await patch_status_heartbeat(p)
    assert p.lta_ok is True
    assert retVal is True
    patch_mock.assert_called_with(mocker.ANY, mocker.ANY, {
        "special-bundler-name": {
            "timestamp": mocker.ANY,
            "last_work_begin_timestamp": mocker.ANY,
            "last_work_end_timestamp": mocker.ANY,
            "lta_ok": False
        }
    })
    logger_mock.assert_not_called()


@pytest.mark.asyncio
async def test_patch_status_heartbeat_patch_call_4xx(config, mocker):
    """
    Verify Bundler behavior when status heartbeat patches fail.

    The Bundler will change state to indicate that its connection to LTA is
    not OK, and that it will log an error, if the PATCH call results in a
    4xx series response.
    """
    patch_mock = mocker.patch("rest_tools.client.RestClient.request")
    patch_mock.side_effect = requests.exceptions.HTTPError("400 Bad Request")
    logger_mock = mocker.MagicMock()
    p = Bundler(config, logger_mock)
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
    patch_mock = mocker.patch("lta.bundler.patch_status_heartbeat", new_callable=AsyncMock)
    patch_mock.side_effect = [True, Exception()]

    sleep_mock = mocker.patch("asyncio.sleep", new_callable=AsyncMock)
    sleep_mock.side_effect = [None, None]

    logger_mock = mocker.MagicMock()
    p = Bundler(config, logger_mock)
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
    run_mock = mocker.patch("lta.bundler.Bundler.run", new_callable=AsyncMock)
    run_mock.side_effect = [None, Exception()]

    sleep_mock = mocker.patch("asyncio.sleep", new_callable=AsyncMock)
    sleep_mock.side_effect = [None, None]

    logger_mock = mocker.MagicMock()
    bundler_config = config.copy()
    bundler_config["WORK_SLEEP_DURATION_SECONDS"] = "300"
    p = Bundler(bundler_config, logger_mock)
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
    Verify Bundler component behavior when run as a script.

    Test to make sure running the Bundler as a script does the setup work
    that we expect and then launches the bundler service.
    """
    for key in config.keys():
        monkeypatch.setenv(key, config[key])
    mock_event_loop = mocker.patch("asyncio.get_event_loop")
    mock_root_logger = mocker.patch("logging.getLogger")
    mock_status_loop = mocker.patch("lta.bundler.status_loop")
    mock_work_loop = mocker.patch("lta.bundler.work_loop")
    main()
    mock_event_loop.assert_called()
    mock_root_logger.assert_called()
    mock_status_loop.assert_called()
    mock_work_loop.assert_called()


@pytest.mark.asyncio
async def test_bundler_logs_configuration(mocker):
    """Test to make sure the Bundler logs its configuration."""
    logger_mock = mocker.MagicMock()
    bundler_config = {
        "BUNDLER_NAME": "logme-testing-bundler",
        "BUNDLER_SITE_SOURCE": "WIPAC",
        "HEARTBEAT_PATCH_RETRIES": "1",
        "HEARTBEAT_PATCH_TIMEOUT_SECONDS": "20",
        "HEARTBEAT_SLEEP_DURATION_SECONDS": "30",
        "LTA_REST_TOKEN": "logme-fake-lta-rest-token",
        "LTA_REST_URL": "logme-http://RmMNHdPhHpH2ZxfaFAC9d2jiIbf5pZiHDqy43rFLQiM.com/",
        "LTA_SITE_CONFIG": "etc/site.json",
        "OUTBOX_PATH": "logme/tmp/lta/testing/bundler/outbox",
        "WORK_RETRIES": "5",
        "WORK_SLEEP_DURATION_SECONDS": "70",
        "WORK_TIMEOUT_SECONDS": "90",
        "WORKBOX_PATH": "logme/tmp/lta/testing/bundler/workbox",
    }
    Bundler(bundler_config, logger_mock)
    EXPECTED_LOGGER_CALLS = [
        call("Bundler 'logme-testing-bundler' is configured:"),
        call('BUNDLER_NAME = logme-testing-bundler'),
        call('BUNDLER_SITE_SOURCE = WIPAC'),
        call('HEARTBEAT_PATCH_RETRIES = 1'),
        call('HEARTBEAT_PATCH_TIMEOUT_SECONDS = 20'),
        call('HEARTBEAT_SLEEP_DURATION_SECONDS = 30'),
        call('LTA_REST_TOKEN = logme-fake-lta-rest-token'),
        call('LTA_REST_URL = logme-http://RmMNHdPhHpH2ZxfaFAC9d2jiIbf5pZiHDqy43rFLQiM.com/'),
        call('LTA_SITE_CONFIG = etc/site.json'),
        call('OUTBOX_PATH = logme/tmp/lta/testing/bundler/outbox'),
        call('WORK_RETRIES = 5'),
        call('WORK_SLEEP_DURATION_SECONDS = 70'),
        call('WORK_TIMEOUT_SECONDS = 90'),
        call('WORKBOX_PATH = logme/tmp/lta/testing/bundler/workbox'),
    ]
    logger_mock.info.assert_has_calls(EXPECTED_LOGGER_CALLS)


@pytest.mark.asyncio
async def test_bundler_run(config, mocker):
    """Test the Bundler does the work the bundler should do."""
    logger_mock = mocker.MagicMock()
    p = Bundler(config, logger_mock)
    p._do_work = AsyncMock()
    await p.run()
    p._do_work.assert_called()


@pytest.mark.asyncio
async def test_bundler_run_exception(config, mocker):
    """Test an error doesn't kill the Bundler."""
    logger_mock = mocker.MagicMock()
    p = Bundler(config, logger_mock)
    p.last_work_end_timestamp = None
    p._do_work = AsyncMock()
    p._do_work.side_effect = [Exception("bad thing happen!")]
    await p.run()
    p._do_work.assert_called()
    assert p.last_work_end_timestamp


@pytest.mark.asyncio
async def test_bundler_do_work_pop_exception(config, mocker):
    """Test that _do_work raises when the RestClient can't pop."""
    logger_mock = mocker.MagicMock()
    lta_rc_mock = mocker.patch("rest_tools.client.RestClient.request", new_callable=AsyncMock)
    lta_rc_mock.side_effect = HTTPError(500, "LTA DB on fire. Again.")
    p = Bundler(config, logger_mock)
    with pytest.raises(HTTPError):
        await p._do_work()
    lta_rc_mock.assert_called_with("POST", '/Files/actions/pop?source=WIPAC&dest=DESY', {'bundler': 'testing-bundler'})


@pytest.mark.asyncio
async def test_bundler_do_work_no_results(config, mocker):
    """Test that _do_work goes on vacation when the LTA DB has no work."""
    logger_mock = mocker.MagicMock()
    lta_rc_mock = mocker.patch("rest_tools.client.RestClient.request", new_callable=AsyncMock)
    lta_rc_mock.return_value = {
        "results": []
    }
    p = Bundler(config, logger_mock)
    await p._do_work()
    lta_rc_mock.assert_called_with("POST", '/Files/actions/pop?source=WIPAC&dest=NERSC', {'bundler': 'testing-bundler'})


@pytest.mark.asyncio
async def test_bundler_do_work_yes_results(config, mocker):
    """Test that _do_work processes each TransferRequest it gets from the LTA DB."""
    logger_mock = mocker.MagicMock()
    lta_rc_mock = mocker.patch("rest_tools.client.RestClient.request", new_callable=AsyncMock)
    lta_rc_mock.return_value = {
        "results": [
            {
                "one": 1
            },
            {
                "two": 2
            },
            {
                "three": 3
            }
        ]
    }
    dwtr_mock = mocker.patch("lta.bundler.Bundler._build_bundle_for_destination_site", new_callable=AsyncMock)
    p = Bundler(config, logger_mock)
    await p._do_work()
    lta_rc_mock.assert_called_with("POST", '/Files/actions/pop?source=WIPAC&dest=NERSC', {'bundler': 'testing-bundler'})
    dwtr_mock.assert_called_with("NERSC", mocker.ANY, [{"one": 1}, {"two": 2}, {"three": 3}])


@pytest.mark.asyncio
async def test_bundler_do_work_dest_results(config, mocker):
    """Test that _do_work processes each TransferRequest it gets from the LTA DB."""
    logger_mock = mocker.MagicMock()
    lta_rc_mock = mocker.patch("rest_tools.client.RestClient.request", new_callable=AsyncMock)
    mock_zipfile_init = mocker.patch("zipfile.ZipFile.__init__")
    mock_zipfile_init.return_value = None
    mock_zipfile_write = mocker.patch("zipfile.ZipFile.write")
    mock_zipfile_write.return_value = None
    mock_shutil_move = mocker.patch("shutil.move")
    mock_shutil_move.return_value = None
    mock_sha512sum = mocker.patch("lta.bundler.sha512sum")
    mock_sha512sum.return_value = "c919210281b72327c179e26be799b06cdaf48bf6efce56fb9d53f758c1b997099831ad05453fdb1ba65be7b35d0b4c5cebfc439efbdf83317ba0e38bf6f42570"
    mock_os_remove = mocker.patch("os.remove")
    mock_os_remove.return_value = None
    p = Bundler(config, logger_mock)
    results = [
        {
            "catalog": {
                "uuid": "44e2c70c-c111-4d33-9acd-7a617ed28ee4",
                "logical_name": "/tmp/my/data/file1.tar.gz",
            },
        },
        {
            "catalog": {
                "uuid": "7f5b45aa-074d-4e72-9168-890e7473f71d",
                "logical_name": "/tmp/my/data/file2.tar.gz",
            },
        },
        {
            "catalog": {
                "uuid": "115023df-b341-44e8-8083-1d9ddf54f5c1",
                "logical_name": "/tmp/my/data/file3.tar.gz",
            },
        },
    ]
    with patch("builtins.open", mock_open(read_data="data")) as metadata_mock:
        await p._build_bundle_for_destination_site("NERSC", lta_rc_mock, results)
        metadata_mock.assert_called_with(mocker.ANY, mode="w")
        # lta_rc_mock.assert_called_with('POST', '/Bundles/actions/bulk_create', mocker.ANY)
        lta_rc_mock.assert_not_called()
