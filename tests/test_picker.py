# test_picker.py
"""Unit tests for lta/picker.py."""

from asyncio import Future
from unittest.mock import call, MagicMock
import pytest  # type: ignore
import requests
from tornado.web import HTTPError  # type: ignore
from uuid import uuid1

from lta.picker import main, patch_status_heartbeat, Picker, status_loop, work_loop


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
        "FILE_CATALOG_REST_TOKEN": "fake-file-catalog-rest-token",
        "FILE_CATALOG_REST_URL": "http://kVj74wBA1AMTDV8zccn67pGuWJqHZzD7iJQHrUJKA.com/",
        "HEARTBEAT_PATCH_RETRIES": "3",
        "HEARTBEAT_PATCH_TIMEOUT_SECONDS": "30",
        "HEARTBEAT_SLEEP_DURATION_SECONDS": "60",
        "LTA_REST_TOKEN": "fake-lta-rest-token",
        "LTA_REST_URL": "http://RmMNHdPhHpH2ZxfaFAC9d2jiIbf5pZiHDqy43rFLQiM.com/",
        "PICKER_NAME": "testing-picker",
        "WORK_RETRIES": "3",
        "WORK_SLEEP_DURATION_SECONDS": "60",
        "WORK_TIMEOUT_SECONDS": "30"
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


def test_constructor_config_missing_values(mocker):
    """Fail with a ValueError if the configuration object is missing required configuration variables."""
    config = {
        "PAN_GALACTIC_GARGLE_BLASTER": "Yummy"
    }
    logger_mock = mocker.MagicMock()
    with pytest.raises(ValueError):
        Picker(config, logger_mock)


def test_constructor_config_poison_values(config, mocker):
    """Fail with a ValueError if the configuration object is missing required configuration variables."""
    picker_config = config.copy()
    picker_config["LTA_REST_URL"] = None
    logger_mock = mocker.MagicMock()
    with pytest.raises(ValueError):
        Picker(picker_config, logger_mock)


def test_constructor_config(config, mocker):
    """Test that a Picker can be constructed with a configuration object and a logging object."""
    logger_mock = mocker.MagicMock()
    p = Picker(config, logger_mock)
    assert p.file_catalog_rest_url == "http://kVj74wBA1AMTDV8zccn67pGuWJqHZzD7iJQHrUJKA.com/"
    assert p.heartbeat_sleep_duration_seconds == 60
    assert p.lta_rest_url == "http://RmMNHdPhHpH2ZxfaFAC9d2jiIbf5pZiHDqy43rFLQiM.com/"
    assert p.picker_name == "testing-picker"
    assert p.work_sleep_duration_seconds == 60
    assert p.logger == logger_mock


def test_constructor_config_sleep_type_int(config, mocker):
    """Ensure that sleep seconds can also be provided as an integer."""
    logger_mock = mocker.MagicMock()
    p = Picker(config, logger_mock)
    assert p.file_catalog_rest_url == "http://kVj74wBA1AMTDV8zccn67pGuWJqHZzD7iJQHrUJKA.com/"
    assert p.heartbeat_sleep_duration_seconds == 60
    assert p.lta_rest_url == "http://RmMNHdPhHpH2ZxfaFAC9d2jiIbf5pZiHDqy43rFLQiM.com/"
    assert p.picker_name == "testing-picker"
    assert p.work_sleep_duration_seconds == 60
    assert p.logger == logger_mock


def test_constructor_state(config, mocker):
    """Verify that the Picker has a reasonable state when it is first constructed."""
    logger_mock = mocker.MagicMock()
    p = Picker(config, logger_mock)
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
async def test_picker_logs_configuration(mocker):
    """Test to make sure the Picker logs its configuration."""
    logger_mock = mocker.MagicMock()
    picker_config = {
        "FILE_CATALOG_REST_TOKEN": "logme-fake-file-catalog-rest-token",
        "FILE_CATALOG_REST_URL": "logme-http://kVj74wBA1AMTDV8zccn67pGuWJqHZzD7iJQHrUJKA.com/",
        "HEARTBEAT_PATCH_RETRIES": "1",
        "HEARTBEAT_PATCH_TIMEOUT_SECONDS": "20",
        "HEARTBEAT_SLEEP_DURATION_SECONDS": "30",
        "LTA_REST_TOKEN": "logme-fake-lta-rest-token",
        "LTA_REST_URL": "logme-http://RmMNHdPhHpH2ZxfaFAC9d2jiIbf5pZiHDqy43rFLQiM.com/",
        "PICKER_NAME": "logme-testing-picker",
        "WORK_RETRIES": "5",
        "WORK_SLEEP_DURATION_SECONDS": "70",
        "WORK_TIMEOUT_SECONDS": "90"
    }
    Picker(picker_config, logger_mock)
    EXPECTED_LOGGER_CALLS = [
        call("Picker 'logme-testing-picker' is configured:"),
        call('FILE_CATALOG_REST_TOKEN = logme-fake-file-catalog-rest-token'),
        call('FILE_CATALOG_REST_URL = logme-http://kVj74wBA1AMTDV8zccn67pGuWJqHZzD7iJQHrUJKA.com/'),
        call('HEARTBEAT_PATCH_RETRIES = 1'),
        call('HEARTBEAT_PATCH_TIMEOUT_SECONDS = 20'),
        call('HEARTBEAT_SLEEP_DURATION_SECONDS = 30'),
        call('LTA_REST_TOKEN = logme-fake-lta-rest-token'),
        call('LTA_REST_URL = logme-http://RmMNHdPhHpH2ZxfaFAC9d2jiIbf5pZiHDqy43rFLQiM.com/'),
        call('PICKER_NAME = logme-testing-picker'),
        call('WORK_RETRIES = 5'),
        call('WORK_SLEEP_DURATION_SECONDS = 70'),
        call('WORK_TIMEOUT_SECONDS = 90')
    ]
    logger_mock.info.assert_has_calls(EXPECTED_LOGGER_CALLS)


@pytest.mark.asyncio
async def test_picker_run(config, mocker):
    """Test the Picker does the work the picker should do."""
    logger_mock = mocker.MagicMock()
    p = Picker(config, logger_mock)
    p._do_work = AsyncMock()
    await p.run()
    p._do_work.assert_called()


@pytest.mark.asyncio
async def test_picker_run_exception(config, mocker):
    """Test an error doesn't kill the Picker."""
    logger_mock = mocker.MagicMock()
    p = Picker(config, logger_mock)
    p.last_work_end_timestamp = None
    p._do_work = AsyncMock()
    p._do_work.side_effect = [Exception("bad thing happen!")]
    await p.run()
    p._do_work.assert_called()
    assert p.last_work_end_timestamp


@pytest.mark.asyncio
async def test_picker_do_work_pop_exception(config, mocker):
    """Test that _do_work raises when the RestClient can't pop."""
    logger_mock = mocker.MagicMock()
    lta_rc_mock = mocker.patch("rest_tools.client.RestClient.request", new_callable=AsyncMock)
    lta_rc_mock.side_effect = HTTPError(500, "REST DB on fire. Again.")
    p = Picker(config, logger_mock)
    with pytest.raises(HTTPError):
        await p._do_work()
    lta_rc_mock.assert_called_with("POST", '/TransferRequests/actions/pop?source=WIPAC', {'picker': 'testing-picker'})


@pytest.mark.asyncio
async def test_picker_do_work_no_results(config, mocker):
    """Test that _do_work goes on vacation when the REST DB has no work."""
    logger_mock = mocker.MagicMock()
    lta_rc_mock = mocker.patch("rest_tools.client.RestClient.request", new_callable=AsyncMock)
    lta_rc_mock.return_value = {
        "results": []
    }
    p = Picker(config, logger_mock)
    await p._do_work()
    lta_rc_mock.assert_called_with("POST", '/TransferRequests/actions/pop?source=WIPAC', {'picker': 'testing-picker'})


@pytest.mark.asyncio
async def test_picker_do_work_yes_results(config, mocker):
    """Test that _do_work processes each TransferRequest it gets from the REST DB."""
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
    dwtr_mock = mocker.patch("lta.picker.Picker._do_work_transfer_request", new_callable=AsyncMock)
    p = Picker(config, logger_mock)
    await p._do_work()
    lta_rc_mock.assert_called_with("POST", '/TransferRequests/actions/pop?source=WIPAC', {'picker': 'testing-picker'})
    dwtr_mock.assert_called_with(mocker.ANY, {"three": 3})


@pytest.mark.asyncio
async def test_picker_do_work_transfer_request_fc_exception(config, mocker):
    """Test that _do_work_transfer_request raises an exception if the File Catalog has an error."""
    logger_mock = mocker.MagicMock()
    p = Picker(config, logger_mock)
    lta_rc_mock = MagicMock()
    tr = {
        "source": "WIPAC:/tmp/this/is/just/a/test",
        "dest": [
            "DESY:/tmp/this/is/just/a/test",
            "NERSC:/tmp/this/is/just/a/test"
        ]
    }
    fc_rc_mock = mocker.patch("rest_tools.client.RestClient.request", new_callable=AsyncMock)
    fc_rc_mock.side_effect = HTTPError(500, "REST DB on fire. Again.")
    with pytest.raises(HTTPError):
        await p._do_work_transfer_request(lta_rc_mock, tr)
    fc_rc_mock.assert_called_with("GET", '/api/files?query={"locations.site": {"$eq": "WIPAC"}, "locations.path": {"$regex": "^/tmp/this/is/just/a/test"}}')


@pytest.mark.asyncio
async def test_picker_do_work_transfer_request_fc_no_results(config, mocker):
    """Test that _do_work_transfer_request raises an exception when the REST DB refuses to create an empty list."""
    logger_mock = mocker.MagicMock()
    p = Picker(config, logger_mock)
    lta_rc_mock = mocker.MagicMock()
    lta_rc_mock.request = AsyncMock()
    lta_rc_mock.request.side_effect = HTTPError(400, reason="files field is empty")
    tr = {
        "source": "WIPAC:/tmp/this/is/just/a/test",
        "dest": [
            "DESY:/tmp/this/is/just/a/test",
            "NERSC:/tmp/this/is/just/a/test"
        ]
    }
    fc_rc_mock = mocker.patch("rest_tools.client.RestClient.request", new_callable=AsyncMock)
    fc_rc_mock.return_value = {
        "files": []
    }
    with pytest.raises(HTTPError):
        await p._do_work_transfer_request(lta_rc_mock, tr)
    fc_rc_mock.assert_called_with("GET", '/api/files?query={"locations.site": {"$eq": "WIPAC"}, "locations.path": {"$regex": "^/tmp/this/is/just/a/test"}}')
    lta_rc_mock.request.assert_called_with("POST", '/Files/actions/bulk_create', {'files': []})


@pytest.mark.asyncio
async def test_picker_do_work_transfer_request_fc_yes_results(config, mocker):
    """Test that _do_work_transfer_request processes each file it gets back from the File Catalog."""
    logger_mock = mocker.MagicMock()
    lta_rc_mock = mocker.MagicMock()
    lta_rc_mock.request = AsyncMock()
    lta_rc_mock.request.return_value = {
        "files": [uuid1().hex, uuid1().hex, uuid1().hex]
    }
    tr = {
        "uuid": "a2647c96-b12a-4fb4-a9c3-3c527b771f6f",
        "source": "WIPAC:/tmp/this/is/just/a/test",
        "dest": [
            "DESY:/tmp/this/is/just/a/test",
            "NERSC:/tmp/this/is/just/a/test"
        ]
    }
    fc_rc_mock = mocker.patch("rest_tools.client.RestClient.request", new_callable=AsyncMock)
    fc_rc_mock.return_value = {
        "_links": {
            "parent": {
                "href": "/api"
            },
            "self": {
                "href": "/api/files"
            }
        },
        "files": [
            {"logical_name": "/data/exp/IceCube/2013/filtered/PFFilt/1109/PFFilt_PhysicsFiltering_Run00123231_Subrun00000000_00000000.tar.bz2",
             "uuid": "65983278-3322-4754-9e5a-1f1c1e118fbc"},
            {"logical_name": "/data/exp/IceCube/2013/filtered/PFFilt/1109/PFFilt_PhysicsFiltering_Run00123231_Subrun00000000_00000001.tar.bz2",
             "uuid": "aaee52f2-f903-43d3-b5da-2e19880e1312"},
            {"logical_name": "/data/exp/IceCube/2013/filtered/PFFilt/1109/PFFilt_PhysicsFiltering_Run00123231_Subrun00000000_00000002.tar.bz2",
             "uuid": "a336aa2b-83d8-4056-8fc1-1a0b72bce7c4"}]
    }
    dwcf_mock = mocker.patch("lta.picker.Picker._do_work_catalog_file", new_callable=AsyncMock)
    dwcf_mock.return_value = [{}, {}, {}]
    p = Picker(config, logger_mock)
    await p._do_work_transfer_request(lta_rc_mock, tr)
    fc_rc_mock.assert_called_with("GET", '/api/files?query={"locations.site": {"$eq": "WIPAC"}, "locations.path": {"$regex": "^/tmp/this/is/just/a/test"}}')
    lta_rc_mock.request.assert_called_with("DELETE", '/TransferRequests/a2647c96-b12a-4fb4-a9c3-3c527b771f6f')
    dests = [('DESY', '/tmp/this/is/just/a/test'), ('NERSC', '/tmp/this/is/just/a/test')]
    third_file = {"logical_name": "/data/exp/IceCube/2013/filtered/PFFilt/1109/PFFilt_PhysicsFiltering_Run00123231_Subrun00000000_00000002.tar.bz2",
                  "uuid": "a336aa2b-83d8-4056-8fc1-1a0b72bce7c4"}
    dwcf_mock.assert_called_with(lta_rc_mock, tr, mocker.ANY, dests, third_file)


@pytest.mark.asyncio
async def test_picker_do_work_catalog_file_fc_exception(config, mocker):
    """Test that _do_work_catalog_file raises an exception if the File Catalog has an error."""
    logger_mock = mocker.MagicMock()
    p = Picker(config, logger_mock)
    lta_rc_mock = MagicMock()
    lta_rc_mock.request = AsyncMock()
    lta_rc_mock.request.side_effect = HTTPError(500, "hey! this shouldn't get called in this test!")
    tr = {
        "source": "WIPAC:/tmp/this/is/just/a/test",
        "dest": [
            "DESY:/tmp/this/is/just/a/test",
            "NERSC:/tmp/this/is/just/a/test"
        ]
    }
    fc_rc_mock = MagicMock()
    fc_rc_mock.request = AsyncMock()
    fc_rc_mock.request.side_effect = HTTPError(500, "File Catalog on fire. Again.")
    dests = [('DESY', '/tmp/this/is/just/a/test'), ('NERSC', '/tmp/this/is/just/a/test')]
    catalog_file = {"logical_name": "/data/exp/IceCube/2013/filtered/PFFilt/1109/PFFilt_PhysicsFiltering_Run00123231_Subrun00000000_00000002.tar.bz2",
                    "uuid": "a336aa2b-83d8-4056-8fc1-1a0b72bce7c4"}
    with pytest.raises(HTTPError):
        await p._do_work_catalog_file(lta_rc_mock, tr, fc_rc_mock, dests, catalog_file)
    fc_rc_mock.request.assert_called_with("GET", '/api/files/a336aa2b-83d8-4056-8fc1-1a0b72bce7c4')
    lta_rc_mock.request.assert_not_called()


# @pytest.mark.asyncio
# async def test_picker_do_work_catalog_file_fc_no_result(config, mocker):
#     normally we'd write a test here, but it would be the same as the last one
#     except it'd be a 404 instead of a 500 that prompted the HTTPError
#     so, imagine the last test, but with a 404; ahhh, coverage bliss.


@pytest.mark.asyncio
async def test_picker_do_work_catalog_file_fc_yes_result(config, mocker):
    """Test that _do_work_catalog_file returns File objects for both destinations."""
    logger_mock = mocker.MagicMock()
    p = Picker(config, logger_mock)
    lta_rc_mock = MagicMock()
    lta_rc_mock.request = AsyncMock()
    lta_rc_mock.request.side_effect = HTTPError(500, "hey! this shouldn't get called in this test!")
    tr = {
        "uuid": "c9a23a20-92d6-49eb-a63e-0f73ac632146",
        "source": "WIPAC:/tmp/this/is/just/a/test",
        "dest": [
            "DESY:/tmp/this/is/just/a/test",
            "NERSC:/tmp/this/is/just/a/test"
        ]
    }
    fc_rc_mock = MagicMock()
    fc_rc_mock.request = AsyncMock()
    catalog_record = {
        "_id": "5b6df684e1382307f078be02",
        "logical_name": "/data/exp/IceCube/2013/filtered/PFFilt/1109/PFFilt_PhysicsFiltering_Run00123231_Subrun00000000_00000002.tar.bz2",
        "uuid": "a336aa2b-83d8-4056-8fc1-1a0b72bce7c4",
        "checksum": {"sha512": "ae7c1639aeaacbd69b8540a117e71a6a92b5e4eff0d7802150609daa98d99fd650f8285e26af23f97f441f3047afbce88ad54bb3feb4fe243a429934d0ee4211"},
        "locations": [{"path": "/data/exp/IceCube/2013/filtered/PFFilt/1109/PFFilt_PhysicsFiltering_Run00123231_Subrun00000000_00000002.tar.bz2",
                       "site": "WIPAC"}],
        "file_size": 104136149,
        "meta_modify_date": "2018-10-30 17:28:22.757029",
        "final_analysis_sample": {"collection_tag": "bae45fdd-8e26-47a2-92cc-75b96c105c64"}
    }
    fc_rc_mock.request.return_value = catalog_record
    dests = [('DESY', '/tmp/this/is/just/a/test'), ('NERSC', '/tmp/this/is/just/a/test')]
    catalog_file = {"logical_name": "/data/exp/IceCube/2013/filtered/PFFilt/1109/PFFilt_PhysicsFiltering_Run00123231_Subrun00000000_00000002.tar.bz2",
                    "uuid": "a336aa2b-83d8-4056-8fc1-1a0b72bce7c4"}
    bulk_create = await p._do_work_catalog_file(lta_rc_mock, tr, fc_rc_mock, dests, catalog_file)
    fc_rc_mock.request.assert_called_with("GET", '/api/files/a336aa2b-83d8-4056-8fc1-1a0b72bce7c4')
    lta_rc_mock.request.assert_not_called()
    assert bulk_create == [
        {
            "source": "WIPAC:/tmp/this/is/just/a/test",
            "dest": "DESY:/tmp/this/is/just/a/test",
            "request": "c9a23a20-92d6-49eb-a63e-0f73ac632146",
            "catalog": catalog_record
        },
        {
            "source": "WIPAC:/tmp/this/is/just/a/test",
            "dest": "NERSC:/tmp/this/is/just/a/test",
            "request": "c9a23a20-92d6-49eb-a63e-0f73ac632146",
            "catalog": catalog_record
        }
    ]


@pytest.mark.asyncio
async def test_picker_do_work_catalog_file_fc_yes_result_only_one(config, mocker):
    """Test that _do_work_catalog_file returns File objects for one destination."""
    logger_mock = mocker.MagicMock()
    p = Picker(config, logger_mock)
    lta_rc_mock = MagicMock()
    lta_rc_mock.request = AsyncMock()
    lta_rc_mock.request.side_effect = HTTPError(500, "hey! this shouldn't get called in this test!")
    tr = {
        "uuid": "c9a23a20-92d6-49eb-a63e-0f73ac632146",
        "source": "WIPAC:/data/exp/IceCube/2013/filtered/PFFilt/1109",
        "dest": [
            "DESY:/tmp/this/is/just/a/test",
            "NERSC:/tmp/this/is/just/a/test"
        ]
    }
    fc_rc_mock = MagicMock()
    fc_rc_mock.request = AsyncMock()
    catalog_record = {
        "_id": "5b6df684e1382307f078be02",
        "logical_name": "/data/exp/IceCube/2013/filtered/PFFilt/1109/PFFilt_PhysicsFiltering_Run00123231_Subrun00000000_00000002.tar.bz2",
        "uuid": "a336aa2b-83d8-4056-8fc1-1a0b72bce7c4",
        "checksum": {
            "sha512": "ae7c1639aeaacbd69b8540a117e71a6a92b5e4eff0d7802150609daa98d99fd650f8285e26af23f97f441f3047afbce88ad54bb3feb4fe243a429934d0ee4211"
        },
        "locations": [
            {
                "site": "WIPAC",
                "path": "/data/exp/IceCube/2013/filtered/PFFilt/1109/PFFilt_PhysicsFiltering_Run00123231_Subrun00000000_00000002.tar.bz2"
            },
            {
                "site": "NERSC",
                "path": "/tmp/this/is/just/a/test/1b71f86f-18a1-4d90-b88e-7505feda3ce6.zip",
                "archive": True
            }
        ],
        "file_size": 104136149,
        "meta_modify_date": "2018-10-30 17:28:22.757029",
        "final_analysis_sample": {
            "collection_tag": "bae45fdd-8e26-47a2-92cc-75b96c105c64"
        }
    }
    fc_rc_mock.request.return_value = catalog_record
    dests = [('DESY', '/tmp/this/is/just/a/test'), ('NERSC', '/tmp/this/is/just/a/test')]
    catalog_file = {"logical_name": "/data/exp/IceCube/2013/filtered/PFFilt/1109/PFFilt_PhysicsFiltering_Run00123231_Subrun00000000_00000002.tar.bz2", "uuid": "a336aa2b-83d8-4056-8fc1-1a0b72bce7c4"}
    bulk_create = await p._do_work_catalog_file(lta_rc_mock, tr, fc_rc_mock, dests, catalog_file)
    fc_rc_mock.request.assert_called_with("GET", '/api/files/a336aa2b-83d8-4056-8fc1-1a0b72bce7c4')
    lta_rc_mock.request.assert_not_called()
    # this one is failing because the picker code isn't doing startswith() on the dest path
    assert bulk_create == [
        {
            "source": "WIPAC:/data/exp/IceCube/2013/filtered/PFFilt/1109",
            "dest": "DESY:/tmp/this/is/just/a/test",
            "request": "c9a23a20-92d6-49eb-a63e-0f73ac632146",
            "catalog": catalog_record
        }
    ]
