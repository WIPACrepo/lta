# test_picker.py
"""Unit tests for lta/picker.py."""

from unittest.mock import call, MagicMock
from uuid import uuid1

import pytest  # type: ignore
from tornado.web import HTTPError  # type: ignore

from lta.picker import as_bundle_record, main, Picker
from .test_util import AsyncMock

@pytest.fixture
def config():
    """Supply a stock Picker component configuration."""
    return {
        "COMPONENT_NAME": "testing-picker",
        "FILE_CATALOG_REST_TOKEN": "fake-file-catalog-rest-token",
        "FILE_CATALOG_REST_URL": "http://kVj74wBA1AMTDV8zccn67pGuWJqHZzD7iJQHrUJKA.com/",
        "HEARTBEAT_PATCH_RETRIES": "3",
        "HEARTBEAT_PATCH_TIMEOUT_SECONDS": "30",
        "HEARTBEAT_SLEEP_DURATION_SECONDS": "60",
        "LTA_REST_TOKEN": "fake-lta-rest-token",
        "LTA_REST_URL": "http://RmMNHdPhHpH2ZxfaFAC9d2jiIbf5pZiHDqy43rFLQiM.com/",
        "LTA_SITE_CONFIG": "examples/site.json",
        "RUN_ONCE_AND_DIE": "False",
        "SOURCE_SITE": "wipac",
        "WORK_RETRIES": "3",
        "WORK_SLEEP_DURATION_SECONDS": "60",
        "WORK_TIMEOUT_SECONDS": "30",
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
    assert p.name == "testing-picker"
    assert p.work_sleep_duration_seconds == 60
    assert p.logger == logger_mock


def test_constructor_config_sleep_type_int(config, mocker):
    """Ensure that sleep seconds can also be provided as an integer."""
    logger_mock = mocker.MagicMock()
    p = Picker(config, logger_mock)
    assert p.file_catalog_rest_url == "http://kVj74wBA1AMTDV8zccn67pGuWJqHZzD7iJQHrUJKA.com/"
    assert p.heartbeat_sleep_duration_seconds == 60
    assert p.lta_rest_url == "http://RmMNHdPhHpH2ZxfaFAC9d2jiIbf5pZiHDqy43rFLQiM.com/"
    assert p.name == "testing-picker"
    assert p.work_sleep_duration_seconds == 60
    assert p.logger == logger_mock


def test_constructor_state(config, mocker):
    """Verify that the Picker has a reasonable state when it is first constructed."""
    logger_mock = mocker.MagicMock()
    p = Picker(config, logger_mock)
    assert p.last_work_begin_timestamp is p.last_work_end_timestamp


def test_do_status(config, mocker):
    """Verify that the Picker has no additional state to offer."""
    logger_mock = mocker.MagicMock()
    p = Picker(config, logger_mock)
    assert p._do_status() == {}


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
        "COMPONENT_NAME": "logme-testing-picker",
        "FILE_CATALOG_REST_TOKEN": "logme-fake-file-catalog-rest-token",
        "FILE_CATALOG_REST_URL": "logme-http://kVj74wBA1AMTDV8zccn67pGuWJqHZzD7iJQHrUJKA.com/",
        "HEARTBEAT_PATCH_RETRIES": "1",
        "HEARTBEAT_PATCH_TIMEOUT_SECONDS": "20",
        "HEARTBEAT_SLEEP_DURATION_SECONDS": "30",
        "LTA_REST_TOKEN": "logme-fake-lta-rest-token",
        "LTA_REST_URL": "logme-http://RmMNHdPhHpH2ZxfaFAC9d2jiIbf5pZiHDqy43rFLQiM.com/",
        "LTA_SITE_CONFIG": "examples/site.json",
        "RUN_ONCE_AND_DIE": "False",
        "SOURCE_SITE": "wipac",
        "WORK_RETRIES": "5",
        "WORK_SLEEP_DURATION_SECONDS": "70",
        "WORK_TIMEOUT_SECONDS": "90",
    }
    Picker(picker_config, logger_mock)
    EXPECTED_LOGGER_CALLS = [
        call("picker 'logme-testing-picker' is configured:"),
        call('COMPONENT_NAME = logme-testing-picker'),
        call('FILE_CATALOG_REST_TOKEN = logme-fake-file-catalog-rest-token'),
        call('FILE_CATALOG_REST_URL = logme-http://kVj74wBA1AMTDV8zccn67pGuWJqHZzD7iJQHrUJKA.com/'),
        call('HEARTBEAT_PATCH_RETRIES = 1'),
        call('HEARTBEAT_PATCH_TIMEOUT_SECONDS = 20'),
        call('HEARTBEAT_SLEEP_DURATION_SECONDS = 30'),
        call('LTA_REST_TOKEN = logme-fake-lta-rest-token'),
        call('LTA_REST_URL = logme-http://RmMNHdPhHpH2ZxfaFAC9d2jiIbf5pZiHDqy43rFLQiM.com/'),
        call('LTA_SITE_CONFIG = examples/site.json'),
        call('RUN_ONCE_AND_DIE = False'),
        call('SOURCE_SITE = wipac'),
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
    lta_rc_mock.side_effect = HTTPError(500, "LTA DB on fire. Again.")
    p = Picker(config, logger_mock)
    with pytest.raises(HTTPError):
        await p._do_work()
    lta_rc_mock.assert_called_with("POST", '/TransferRequests/actions/pop?source=wipac', {'claimant': f'{p.name}-{p.instance_uuid}'})


@pytest.mark.asyncio
async def test_picker_do_work_no_results(config, mocker):
    """Test that _do_work goes on vacation when the LTA DB has no work."""
    logger_mock = mocker.MagicMock()
    dwc_mock = mocker.patch("lta.picker.Picker._do_work_claim", new_callable=AsyncMock)
    dwc_mock.return_value = False
    p = Picker(config, logger_mock)
    await p._do_work()
    dwc_mock.assert_called()


@pytest.mark.asyncio
async def test_picker_do_work_yes_results(config, mocker):
    """Test that _do_work keeps working until the LTA DB has no work."""
    logger_mock = mocker.MagicMock()
    dwc_mock = mocker.patch("lta.picker.Picker._do_work_claim", new_callable=AsyncMock)
    dwc_mock.side_effect = [True, True, False]
    p = Picker(config, logger_mock)
    await p._do_work()
    dwc_mock.assert_called()


@pytest.mark.asyncio
async def test_picker_do_work_claim_no_result(config, mocker):
    """Test that _do_work_claim does not work when the LTA DB has no work."""
    logger_mock = mocker.MagicMock()
    lta_rc_mock = mocker.patch("rest_tools.client.RestClient.request", new_callable=AsyncMock)
    lta_rc_mock.return_value = {
        "transfer_request": None
    }
    dwtr_mock = mocker.patch("lta.picker.Picker._do_work_transfer_request", new_callable=AsyncMock)
    p = Picker(config, logger_mock)
    await p._do_work_claim()
    lta_rc_mock.assert_called_with("POST", '/TransferRequests/actions/pop?source=wipac', {'claimant': f'{p.name}-{p.instance_uuid}'})
    dwtr_mock.assert_not_called()


@pytest.mark.asyncio
async def test_picker_do_work_claim_yes_result(config, mocker):
    """Test that _do_work_claim processes the TransferRequest it gets from the LTA DB."""
    logger_mock = mocker.MagicMock()
    lta_rc_mock = mocker.patch("rest_tools.client.RestClient.request", new_callable=AsyncMock)
    lta_rc_mock.return_value = {
        "transfer_request": {
            "one": 1,
        },
    }
    dwtr_mock = mocker.patch("lta.picker.Picker._do_work_transfer_request", new_callable=AsyncMock)
    p = Picker(config, logger_mock)
    await p._do_work_claim()
    lta_rc_mock.assert_called_with("POST", '/TransferRequests/actions/pop?source=wipac', {'claimant': f'{p.name}-{p.instance_uuid}'})
    dwtr_mock.assert_called_with(mocker.ANY, {"one": 1})


@pytest.mark.asyncio
async def test_picker_do_work_transfer_request_fc_exception(config, mocker):
    """Test that _do_work_transfer_request raises an exception if the File Catalog has an error."""
    logger_mock = mocker.MagicMock()
    p = Picker(config, logger_mock)
    lta_rc_mock = MagicMock()
    tr = {
        "uuid": uuid1().hex,
        "source": "wipac",
        "dest": "nersc",
        "path": "/tmp/this/is/just/a/test",
    }
    fc_rc_mock = mocker.patch("rest_tools.client.RestClient.request", new_callable=AsyncMock)
    fc_rc_mock.side_effect = HTTPError(500, "LTA DB on fire. Again.")
    with pytest.raises(HTTPError):
        await p._do_work_transfer_request(lta_rc_mock, tr)
    fc_rc_mock.assert_called_with("GET", '/api/files?query={"locations.site": {"$eq": "wipac"}, "locations.path": {"$regex": "^/tmp/this/is/just/a/test"}, "logical_name": {"$regex": "^/tmp/this/is/just/a/test"}}')


@pytest.mark.asyncio
async def test_picker_do_work_transfer_request_fc_no_results(config, mocker):
    """Test that _do_work_transfer_request raises an exception when the LTA DB refuses to create an empty list."""
    QUARANTINE = {'status': 'quarantined', 'reason': 'File Catalog returned zero files for the TransferRequest', 'work_priority_timestamp': mocker.ANY}
    logger_mock = mocker.MagicMock()
    p = Picker(config, logger_mock)
    lta_rc_mock = mocker.MagicMock()
    lta_rc_mock.request = AsyncMock()
    lta_rc_mock.request.return_value = {}
    tr_uuid = uuid1().hex
    tr = {
        "uuid": tr_uuid,
        "source": "wipac",
        "dest": "nersc",
        "path": "/tmp/this/is/just/a/test",
    }
    fc_rc_mock = mocker.patch("rest_tools.client.RestClient.request", new_callable=AsyncMock)
    fc_rc_mock.return_value = {
        "files": []
    }
    await p._do_work_transfer_request(lta_rc_mock, tr)
    fc_rc_mock.assert_called_with("GET", '/api/files?query={"locations.site": {"$eq": "wipac"}, "locations.path": {"$regex": "^/tmp/this/is/just/a/test"}, "logical_name": {"$regex": "^/tmp/this/is/just/a/test"}}')
    lta_rc_mock.request.assert_called_with("PATCH", f'/TransferRequests/{tr_uuid}', QUARANTINE)


@pytest.mark.asyncio
async def test_picker_do_work_transfer_request_fc_yes_results(config, mocker):
    """Test that _do_work_transfer_request processes each file it gets back from the File Catalog."""
    logger_mock = mocker.MagicMock()
    lta_rc_mock = mocker.MagicMock()
    lta_rc_mock.request = AsyncMock()
    lta_rc_mock.request.return_value = {
        "bundles": [uuid1().hex, uuid1().hex, uuid1().hex],
        "count": 3
    }
    tr_uuid = uuid1().hex
    tr = {
        "uuid": tr_uuid,
        "source": "wipac",
        "dest": "nersc",
        "path": "/tmp/this/is/just/a/test",
    }
    fc_rc_mock = mocker.patch("rest_tools.client.RestClient.request", new_callable=AsyncMock)
    fc_rc_mock.side_effect = [
        {
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
                 "uuid": "58a334e6-642e-475e-b642-e92bf08e96d4"},
                {"logical_name": "/data/exp/IceCube/2013/filtered/PFFilt/1109/PFFilt_PhysicsFiltering_Run00123231_Subrun00000000_00000001.tar.bz2",
                 "uuid": "89528506-9950-43dc-a910-f5108a1d25c0"},
                {"logical_name": "/data/exp/IceCube/2013/filtered/PFFilt/1109/PFFilt_PhysicsFiltering_Run00123231_Subrun00000000_00000002.tar.bz2",
                 "uuid": "1e4a88c6-247e-4e59-9c89-1a4edafafb1e"}]
        },
        {
            "logical_name": "/data/exp/IceCube/2013/filtered/PFFilt/1109/PFFilt_PhysicsFiltering_Run00123231_Subrun00000000_00000000.tar.bz2",
            "uuid": "58a334e6-642e-475e-b642-e92bf08e96d4",
            "checksum": {
                "sha512": "63c25d9bcf7bacc8cdb7ccf0a480403eea111a6b8db2d0c54fef0e39c32fe76f75b3632b3582ef888caeaf8a8aac44fb51d0eb051f67e874f9fe694981649b74"
            },
            "locations": [
                {
                    "path": "/data/exp/IceCube/2013/filtered/PFFilt/1109/PFFilt_PhysicsFiltering_Run00123231_Subrun00000000_00000000.tar.bz2",
                    "site": "wipac"
                }],
            "file_size": 103166718,
            "meta_modify_date": "2019-07-26 01:53:20.857303"
        },
        {
            "logical_name": "/data/exp/IceCube/2013/filtered/PFFilt/1109/PFFilt_PhysicsFiltering_Run00123231_Subrun00000000_00000001.tar.bz2",
            "uuid": "89528506-9950-43dc-a910-f5108a1d25c0",
            "checksum": {
                "sha512": "5acee016b1f7a367f549d3a861af32a66e5b577753d6d7b8078a30129f6535108042a74b70b1374a9f7506022e6cc64372a8d01500db5d56afb17071cba6da9e"
            },
            "locations": [
                {
                    "path": "/data/exp/IceCube/2013/filtered/PFFilt/1109/PFFilt_PhysicsFiltering_Run00123231_Subrun00000000_00000001.tar.bz2",
                    "site": "wipac"
                }
            ],
            "file_size": 103064762,
            "meta_modify_date": "2019-07-26 01:53:20.646010"
        },
        {
            "logical_name": "/data/exp/IceCube/2013/filtered/PFFilt/1109/PFFilt_PhysicsFiltering_Run00123231_Subrun00000000_00000002.tar.bz2",
            "uuid": "1e4a88c6-247e-4e59-9c89-1a4edafafb1e",
            "checksum": {
                "sha512": "ae7c1639aeaacbd69b8540a117e71a6a92b5e4eff0d7802150609daa98d99fd650f8285e26af23f97f441f3047afbce88ad54bb3feb4fe243a429934d0ee4211"
            },
            "locations": [
                {
                    "path": "/data/exp/IceCube/2013/filtered/PFFilt/1109/PFFilt_PhysicsFiltering_Run00123231_Subrun00000000_00000002.tar.bz2",
                    "site": "wipac"
                }
            ],
            "file_size": 104136149,
            "meta_modify_date": "2019-07-26 01:53:22.591198"
        }
    ]
    p = Picker(config, logger_mock)
    await p._do_work_transfer_request(lta_rc_mock, tr)
    fc_rc_mock.assert_called_with("GET", '/api/files/1e4a88c6-247e-4e59-9c89-1a4edafafb1e')
    lta_rc_mock.request.assert_called_with("POST", '/Bundles/actions/bulk_create', mocker.ANY)


def test_as_bundle_record(config, mocker):
    """Test that bundle_record cherry picks the right keys."""
    catalog_record = {
        "_links": {
            "parent": {
                "href": "/api/files"
            },
            "self": {
                "href": "/api/files/e6549962-2c91-11ea-9a10-f6a52f4853dd"
            }
        },
        "checksum": {
            "sha512": "e001e7895e9367d20e804eec5cd867ea0758ebed068c52dac9fac55bf2d263695d1e39231b667598edcb16426048f8801341c44c0d9128df67e3cc22599319a0"
        },
        "file_size": 4977182,
        "locations": [
            {
                "path": "/data/exp/IceCube/2018/unbiased/PFDST/1120/ukey_cf9b674a-7620-498a-8d59-a47d39d80245_PFDST_PhysicsFiltering_Run00131763_Subrun00000000_00000398.tar.gz",
                "site": "WIPAC"
            }
        ],
        "logical_name": "/data/exp/IceCube/2018/unbiased/PFDST/1120/ukey_cf9b674a-7620-498a-8d59-a47d39d80245_PFDST_PhysicsFiltering_Run00131763_Subrun00000000_00000398.tar.gz",
        "meta_modify_date": "2020-01-01 12:26:13.440651",
        "uuid": "e6549962-2c91-11ea-9a10-f6a52f4853dd"
    }

    bundle_record = as_bundle_record(catalog_record)

    assert ("_links" not in bundle_record)
    assert bundle_record["checksum"] == {
        "sha512": "e001e7895e9367d20e804eec5cd867ea0758ebed068c52dac9fac55bf2d263695d1e39231b667598edcb16426048f8801341c44c0d9128df67e3cc22599319a0"
    }
    assert bundle_record["file_size"] == 4977182
    assert ("locations" not in bundle_record)
    assert bundle_record["logical_name"] == "/data/exp/IceCube/2018/unbiased/PFDST/1120/ukey_cf9b674a-7620-498a-8d59-a47d39d80245_PFDST_PhysicsFiltering_Run00131763_Subrun00000000_00000398.tar.gz"
    assert bundle_record["meta_modify_date"] == "2020-01-01 12:26:13.440651"
    assert bundle_record["uuid"] == "e6549962-2c91-11ea-9a10-f6a52f4853dd"
