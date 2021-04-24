# test_unpacker.py
"""Unit tests for lta/unpacker.py."""

from unittest.mock import call, mock_open, patch

import pytest  # type: ignore
from tornado.web import HTTPError  # type: ignore

from lta.unpacker import Unpacker, main
from .test_util import AsyncMock


@pytest.fixture
def config():
    """Supply a stock Unpacker component configuration."""
    return {
        "COMPONENT_NAME": "testing-unpacker",
        "DEST_SITE": "WIPAC",
        "FILE_CATALOG_REST_TOKEN": "fake-file-catalog-token",
        "FILE_CATALOG_REST_URL": "http://kVj74wBA1AMTDV8zccn67pGuWJqHZzD7iJQHrUJKA.com/",
        "HEARTBEAT_PATCH_RETRIES": "3",
        "HEARTBEAT_PATCH_TIMEOUT_SECONDS": "30",
        "HEARTBEAT_SLEEP_DURATION_SECONDS": "60",
        "INPUT_STATUS": "unpacking",
        "LTA_REST_TOKEN": "fake-lta-rest-token",
        "LTA_REST_URL": "http://RmMNHdPhHpH2ZxfaFAC9d2jiIbf5pZiHDqy43rFLQiM.com/",
        "OUTPUT_STATUS": "completed",
        "PATH_MAP_JSON": "/tmp/lta/testing/path_map.json",
        "RUN_ONCE_AND_DIE": "False",
        "SOURCE_SITE": "NERSC",
        "UNPACKER_OUTBOX_PATH": "/tmp/lta/testing/unpacker/outbox",
        "UNPACKER_WORKBOX_PATH": "/tmp/lta/testing/unpacker/workbox",
        "WORK_RETRIES": "3",
        "WORK_SLEEP_DURATION_SECONDS": "60",
        "WORK_TIMEOUT_SECONDS": "30",
    }

@pytest.fixture
def path_map_mock(mocker):
    rtm = mocker.patch("pathlib.Path.read_text")
    rtm.return_value = '{"/mnt/lfs7": "/data/exp"}'
    return rtm


def test_always_succeed():
    """Canary test to verify test framework is operating properly."""
    assert True


def test_constructor_missing_config():
    """Fail with a TypeError if a configuration object isn't provided."""
    with pytest.raises(TypeError):
        Unpacker()


def test_constructor_missing_logging():
    """Fail with a TypeError if a logging object isn't provided."""
    with pytest.raises(TypeError):
        config = {
            "PAN_GALACTIC_GARGLE_BLASTER": "Yummy"
        }
        Unpacker(config)


def test_constructor_config_missing_values(mocker):
    """Fail with a ValueError if the configuration object is missing required configuration variables."""
    config = {
        "PAN_GALACTIC_GARGLE_BLASTER": "Yummy"
    }
    logger_mock = mocker.MagicMock()
    with pytest.raises(ValueError):
        Unpacker(config, logger_mock)


def test_constructor_config_poison_values(config, mocker):
    """Fail with a ValueError if the configuration object is missing required configuration variables."""
    unpacker_config = config.copy()
    unpacker_config["LTA_REST_URL"] = None
    logger_mock = mocker.MagicMock()
    with pytest.raises(ValueError):
        Unpacker(unpacker_config, logger_mock)


def test_constructor_config(config, mocker, path_map_mock):
    """Test that a Unpacker can be constructed with a configuration object and a logging object."""
    logger_mock = mocker.MagicMock()
    p = Unpacker(config, logger_mock)
    assert p.heartbeat_sleep_duration_seconds == 60
    assert p.lta_rest_url == "http://RmMNHdPhHpH2ZxfaFAC9d2jiIbf5pZiHDqy43rFLQiM.com/"
    assert p.name == "testing-unpacker"
    assert p.work_sleep_duration_seconds == 60
    assert p.logger == logger_mock


def test_constructor_config_sleep_type_int(config, mocker, path_map_mock):
    """Ensure that sleep seconds can also be provided as an integer."""
    logger_mock = mocker.MagicMock()
    p = Unpacker(config, logger_mock)
    assert p.heartbeat_sleep_duration_seconds == 60
    assert p.lta_rest_url == "http://RmMNHdPhHpH2ZxfaFAC9d2jiIbf5pZiHDqy43rFLQiM.com/"
    assert p.name == "testing-unpacker"
    assert p.work_sleep_duration_seconds == 60
    assert p.logger == logger_mock


def test_constructor_state(config, mocker, path_map_mock):
    """Verify that the Unpacker has a reasonable state when it is first constructed."""
    logger_mock = mocker.MagicMock()
    p = Unpacker(config, logger_mock)
    assert p.last_work_begin_timestamp is p.last_work_end_timestamp


def test_do_status(config, mocker, path_map_mock):
    """Verify that the Unpacker has no additional state to offer."""
    logger_mock = mocker.MagicMock()
    p = Unpacker(config, logger_mock)
    assert p._do_status() == {}


@pytest.mark.asyncio
async def test_script_main(config, mocker, monkeypatch, path_map_mock):
    """
    Verify Unpacker component behavior when run as a script.

    Test to make sure running the Unpacker as a script does the setup work
    that we expect and then launches the unpacker service.
    """
    for key in config.keys():
        monkeypatch.setenv(key, config[key])
    mock_event_loop = mocker.patch("asyncio.get_event_loop")
    mock_root_logger = mocker.patch("logging.getLogger")
    mock_status_loop = mocker.patch("lta.unpacker.status_loop")
    mock_work_loop = mocker.patch("lta.unpacker.work_loop")
    main()
    mock_event_loop.assert_called()
    mock_root_logger.assert_called()
    mock_status_loop.assert_called()
    mock_work_loop.assert_called()


@pytest.mark.asyncio
async def test_unpacker_logs_configuration(mocker, path_map_mock):
    """Test to make sure the Unpacker logs its configuration."""
    logger_mock = mocker.MagicMock()
    unpacker_config = {
        "COMPONENT_NAME": "logme-testing-unpacker",
        "DEST_SITE": "WIPAC",
        "FILE_CATALOG_REST_TOKEN": "fake-file-catalog-token",
        "FILE_CATALOG_REST_URL": "http://kVj74wBA1AMTDV8zccn67pGuWJqHZzD7iJQHrUJKA.com/",
        "HEARTBEAT_PATCH_RETRIES": "1",
        "HEARTBEAT_PATCH_TIMEOUT_SECONDS": "20",
        "HEARTBEAT_SLEEP_DURATION_SECONDS": "30",
        "INPUT_STATUS": "unpacking",
        "LTA_REST_TOKEN": "logme-fake-lta-rest-token",
        "LTA_REST_URL": "logme-http://RmMNHdPhHpH2ZxfaFAC9d2jiIbf5pZiHDqy43rFLQiM.com/",
        "OUTPUT_STATUS": "completed",
        "PATH_MAP_JSON": "logme/tmp/lta/testing/path_map.json",
        "RUN_ONCE_AND_DIE": "False",
        "SOURCE_SITE": "NERSC",
        "UNPACKER_OUTBOX_PATH": "logme/tmp/lta/testing/unpacker/outbox",
        "UNPACKER_WORKBOX_PATH": "logme/tmp/lta/testing/unpacker/workbox",
        "WORK_RETRIES": "5",
        "WORK_SLEEP_DURATION_SECONDS": "70",
        "WORK_TIMEOUT_SECONDS": "90",
    }
    Unpacker(unpacker_config, logger_mock)
    EXPECTED_LOGGER_CALLS = [
        call("unpacker 'logme-testing-unpacker' is configured:"),
        call('COMPONENT_NAME = logme-testing-unpacker'),
        call('DEST_SITE = WIPAC'),
        call('FILE_CATALOG_REST_TOKEN = fake-file-catalog-token'),
        call('FILE_CATALOG_REST_URL = http://kVj74wBA1AMTDV8zccn67pGuWJqHZzD7iJQHrUJKA.com/'),
        call('HEARTBEAT_PATCH_RETRIES = 1'),
        call('HEARTBEAT_PATCH_TIMEOUT_SECONDS = 20'),
        call('HEARTBEAT_SLEEP_DURATION_SECONDS = 30'),
        call('INPUT_STATUS = unpacking'),
        call('LTA_REST_TOKEN = logme-fake-lta-rest-token'),
        call('LTA_REST_URL = logme-http://RmMNHdPhHpH2ZxfaFAC9d2jiIbf5pZiHDqy43rFLQiM.com/'),
        call('OUTPUT_STATUS = completed'),
        call('PATH_MAP_JSON = logme/tmp/lta/testing/path_map.json'),
        call('RUN_ONCE_AND_DIE = False'),
        call('SOURCE_SITE = NERSC'),
        call('UNPACKER_OUTBOX_PATH = logme/tmp/lta/testing/unpacker/outbox'),
        call('UNPACKER_WORKBOX_PATH = logme/tmp/lta/testing/unpacker/workbox'),
        call('WORK_RETRIES = 5'),
        call('WORK_SLEEP_DURATION_SECONDS = 70'),
        call('WORK_TIMEOUT_SECONDS = 90'),
    ]
    logger_mock.info.assert_has_calls(EXPECTED_LOGGER_CALLS)


@pytest.mark.asyncio
async def test_unpacker_run(config, mocker, path_map_mock):
    """Test the Unpacker does the work the unpacker should do."""
    logger_mock = mocker.MagicMock()
    p = Unpacker(config, logger_mock)
    p._do_work = AsyncMock()
    await p.run()
    p._do_work.assert_called()


@pytest.mark.asyncio
async def test_unpacker_run_exception(config, mocker, path_map_mock):
    """Test an error doesn't kill the Unpacker."""
    logger_mock = mocker.MagicMock()
    p = Unpacker(config, logger_mock)
    p.last_work_end_timestamp = None
    p._do_work = AsyncMock()
    p._do_work.side_effect = [Exception("bad thing happen!")]
    await p.run()
    p._do_work.assert_called()
    assert p.last_work_end_timestamp


@pytest.mark.asyncio
async def test_unpacker_do_work_pop_exception(config, mocker, path_map_mock):
    """Test that _do_work raises when the RestClient can't pop."""
    logger_mock = mocker.MagicMock()
    lta_rc_mock = mocker.patch("rest_tools.client.RestClient.request", new_callable=AsyncMock)
    lta_rc_mock.side_effect = HTTPError(500, "LTA DB on fire. Again.")
    p = Unpacker(config, logger_mock)
    with pytest.raises(HTTPError):
        await p._do_work()
    lta_rc_mock.assert_called_with("POST", '/Bundles/actions/pop?source=NERSC&dest=WIPAC&status=unpacking', mocker.ANY)


@pytest.mark.asyncio
async def test_unpacker_do_work_no_results(config, mocker, path_map_mock):
    """Test that _do_work goes on vacation when the LTA DB has no work."""
    logger_mock = mocker.MagicMock()
    claim_mock = mocker.patch("lta.unpacker.Unpacker._do_work_claim", new_callable=AsyncMock)
    claim_mock.return_value = False
    p = Unpacker(config, logger_mock)
    await p._do_work()


@pytest.mark.asyncio
async def test_unpacker_do_work_claim_no_results(config, mocker, path_map_mock):
    """Test that _do_work_claim returns False when the LTA DB has no work."""
    logger_mock = mocker.MagicMock()
    lta_rc_mock = mocker.patch("rest_tools.client.RestClient.request", new_callable=AsyncMock)
    lta_rc_mock.return_value = {
        "bundle": None
    }
    p = Unpacker(config, logger_mock)
    assert not await p._do_work_claim()
    lta_rc_mock.assert_called_with("POST", '/Bundles/actions/pop?source=NERSC&dest=WIPAC&status=unpacking', mocker.ANY)


@pytest.mark.asyncio
async def test_unpacker_do_work_yes_results(config, mocker, path_map_mock):
    """Test that _do_work_claim processes each Bundle that it gets from the LTA DB."""
    BUNDLE_OBJ = {
        "uuid": "f74db80e-9661-40cc-9f01-8d087af23f56"
    }
    logger_mock = mocker.MagicMock()
    lta_rc_mock = mocker.patch("rest_tools.client.RestClient.request", new_callable=AsyncMock)
    lta_rc_mock.return_value = {
        "bundle": BUNDLE_OBJ,
    }
    dwb_mock = mocker.patch("lta.unpacker.Unpacker._do_work_bundle", new_callable=AsyncMock)
    p = Unpacker(config, logger_mock)
    assert await p._do_work_claim()
    lta_rc_mock.assert_called_with("POST", '/Bundles/actions/pop?source=NERSC&dest=WIPAC&status=unpacking', mocker.ANY)
    dwb_mock.assert_called_with(lta_rc_mock, BUNDLE_OBJ)


@pytest.mark.asyncio
async def test_unpacker_do_work_raise_exception(config, mocker, path_map_mock):
    """Test that _do_work_claim processes each Bundle that it gets from the LTA DB."""
    BUNDLE_OBJ = {
        "uuid": "f74db80e-9661-40cc-9f01-8d087af23f56"
    }
    logger_mock = mocker.MagicMock()
    lta_rc_mock = mocker.patch("rest_tools.client.RestClient.request", new_callable=AsyncMock)
    lta_rc_mock.return_value = {
        "bundle": BUNDLE_OBJ,
    }
    dwb_mock = mocker.patch("lta.unpacker.Unpacker._do_work_bundle", new_callable=AsyncMock)
    dwb_mock.side_effect = Exception("LTA DB started on fire again")
    qb_mock = mocker.patch("lta.unpacker.Unpacker._quarantine_bundle", new_callable=AsyncMock)
    p = Unpacker(config, logger_mock)
    with pytest.raises(Exception):
        await p._do_work_claim()
    lta_rc_mock.assert_called_with("POST", '/Bundles/actions/pop?source=NERSC&dest=WIPAC&status=unpacking', mocker.ANY)
    dwb_mock.assert_called_with(lta_rc_mock, BUNDLE_OBJ)
    qb_mock.assert_called_with(lta_rc_mock, BUNDLE_OBJ, "LTA DB started on fire again")


@pytest.mark.asyncio
async def test_unpacker_do_work_bundle_once_and_die(config, mocker, path_map_mock):
    """Test that _do_work goes on vacation when the LTA DB has no work."""
    once = config.copy()
    once["RUN_ONCE_AND_DIE"] = "True"
    logger_mock = mocker.MagicMock()
    claim_mock = mocker.patch("lta.unpacker.Unpacker._do_work_claim", new_callable=AsyncMock)
    claim_mock.return_value = False
    sys_exit_mock = mocker.patch("sys.exit")
    p = Unpacker(once, logger_mock)
    assert not await p._do_work()
    sys_exit_mock.assert_not_called()


@pytest.mark.asyncio
async def test_unpacker_quarantine_bundle_with_reason(config, mocker, path_map_mock):
    """Test that _do_work_claim attempts to quarantine a Bundle that fails to get unpacked."""
    logger_mock = mocker.MagicMock()
    lta_rc_mock = mocker.patch("rest_tools.client.RestClient", new_callable=AsyncMock)
    p = Unpacker(config, logger_mock)
    await p._quarantine_bundle(lta_rc_mock, {"uuid": "c4b345e4-2395-4f9e-b0eb-9cc1c9cdf003"}, "Rucio caught fire, then we roasted marshmellows.")
    lta_rc_mock.request.assert_called_with("PATCH", "/Bundles/c4b345e4-2395-4f9e-b0eb-9cc1c9cdf003", mocker.ANY)


@pytest.mark.asyncio
async def test_unpacker_quarantine_bundle_with_reason_raises(config, mocker, path_map_mock):
    """Test that _do_work_claim attempts to quarantine a Bundle that fails to get unpacked."""
    logger_mock = mocker.MagicMock()
    lta_rc_mock = mocker.patch("rest_tools.client.RestClient", new_callable=AsyncMock)
    lta_rc_mock.request.side_effect = Exception("Marshmellows were poisoned")
    p = Unpacker(config, logger_mock)
    await p._quarantine_bundle(lta_rc_mock, {"uuid": "c4b345e4-2395-4f9e-b0eb-9cc1c9cdf003"}, "Rucio caught fire, then we roasted marshmellows.")
    lta_rc_mock.request.assert_called_with("PATCH", "/Bundles/c4b345e4-2395-4f9e-b0eb-9cc1c9cdf003", mocker.ANY)


@pytest.mark.asyncio
async def test_unpacker_update_bundle_in_lta_db(config, mocker, path_map_mock):
    """Test that _update_bundle_in_lta_db updates the status of the bundle in the LTA DB."""
    logger_mock = mocker.MagicMock()
    lta_rc_mock = mocker.patch("rest_tools.client.RestClient", new_callable=AsyncMock)
    p = Unpacker(config, logger_mock)
    assert await p._update_bundle_in_lta_db(lta_rc_mock, {"uuid": "c4b345e4-2395-4f9e-b0eb-9cc1c9cdf003"})
    lta_rc_mock.request.assert_called_with("PATCH", "/Bundles/c4b345e4-2395-4f9e-b0eb-9cc1c9cdf003", mocker.ANY)


@pytest.mark.asyncio
async def test_unpacker_add_location_to_file_catalog(config, mocker, path_map_mock):
    """Test that _add_location_to_file_catalog adds a location in the File Catalog."""
    logger_mock = mocker.MagicMock()
    fc_rc_mock = mocker.patch("rest_tools.client.RestClient.request", new_callable=AsyncMock)
    p = Unpacker(config, logger_mock)
    bundle_file = {
        "checksum": {
            "sha512": "09de7c539b724dee9543669309f978b172f6c7449d0269fecbb57d0c9cf7db51713fed3a94573c669fe0aa08fa122b41f84a0ea107c62f514b1525efbd08846b",
        },
        "file_size": 105311728,
        "logical_name": "/data/exp/IceCube/2013/filtered/PFFilt/1109/PFFilt_PhysicsFiltering_Run00123231_Subrun00000000_00000066.tar.bz2",
        "meta_modify_date": "2020-02-20 22:47:25.180303",
        "uuid": "2f0cb3c8-6cba-49b1-8eeb-13e13fed41dd",
    }
    dest_path = bundle_file["logical_name"]
    assert await p._add_location_to_file_catalog(bundle_file, dest_path)
    fc_rc_mock.assert_called_with("POST", "/api/files/2f0cb3c8-6cba-49b1-8eeb-13e13fed41dd/locations", {
        "locations": [
            {
                "site": "WIPAC",
                "path": "/data/exp/IceCube/2013/filtered/PFFilt/1109/PFFilt_PhysicsFiltering_Run00123231_Subrun00000000_00000066.tar.bz2",
            }
        ]
    })


@pytest.mark.asyncio
async def test_unpacker_do_work_bundle(config, mocker, path_map_mock):
    """Test that _do_work_bundle does the work of preparing an archive."""
    logger_mock = mocker.MagicMock()
    lta_rc_mock = mocker.patch("rest_tools.client.RestClient.request", new_callable=AsyncMock)
    mock_zipfile_init = mocker.patch("zipfile.ZipFile.__init__")
    mock_zipfile_init.return_value = None
    mock_zipfile_write = mocker.patch("zipfile.ZipFile.extractall")
    mock_zipfile_write.return_value = None
    mock_json_load = mocker.patch("json.load")
    mock_json_load.return_value = {
        "files": [
            {
                "logical_name": "/full/path/to/file/in/data/warehouse.tar.bz2",
                "file_size": 1234567890,
                "checksum": {
                    "adler32": "89d5efeb",
                    "sha512": "c919210281b72327c179e26be799b06cdaf48bf6efce56fb9d53f758c1b997099831ad05453fdb1ba65be7b35d0b4c5cebfc439efbdf83317ba0e38bf6f42570",
                },
            }
        ]
    }
    mock_shutil_move = mocker.patch("shutil.move")
    mock_shutil_move.return_value = None
    mock_lta_checksums = mocker.patch("lta.unpacker.lta_checksums")
    mock_lta_checksums.return_value = {
        "adler32": "89d5efeb",
        "sha512": "c919210281b72327c179e26be799b06cdaf48bf6efce56fb9d53f758c1b997099831ad05453fdb1ba65be7b35d0b4c5cebfc439efbdf83317ba0e38bf6f42570",
    }
    mock_os_path_getsize = mocker.patch("os.path.getsize")
    mock_os_path_getsize.return_value = 1234567890
    mock_os_remove = mocker.patch("os.remove")
    mock_os_remove.return_value = None
    altfc_mock = mocker.patch("lta.unpacker.Unpacker._add_location_to_file_catalog", new_callable=AsyncMock)
    altfc_mock.return_value = False
    p = Unpacker(config, logger_mock)
    BUNDLE_OBJ = {
        "bundle_path": "/mnt/lfss/jade-lta/bundler_out/9a1cab0a395211eab1cbce3a3da73f88.zip",
        "uuid": "f74db80e-9661-40cc-9f01-8d087af23f56",
        "source": "NERSC",
        "dest": "WIPAC",
        "files": [{"logical_name": "/path/to/a/data/file", }],
    }
    with patch("builtins.open", mock_open(read_data="data")) as metadata_mock:
        await p._do_work_bundle(lta_rc_mock, BUNDLE_OBJ)
        metadata_mock.assert_called_with(mocker.ANY)


@pytest.mark.asyncio
async def test_unpacker_do_work_bundle_mismatch_size(config, mocker, path_map_mock):
    """Test that _do_work_bundle does the work of preparing an archive."""
    logger_mock = mocker.MagicMock()
    lta_rc_mock = mocker.patch("rest_tools.client.RestClient.request", new_callable=AsyncMock)
    mock_zipfile_init = mocker.patch("zipfile.ZipFile.__init__")
    mock_zipfile_init.return_value = None
    mock_zipfile_write = mocker.patch("zipfile.ZipFile.extractall")
    mock_zipfile_write.return_value = None
    mock_json_load = mocker.patch("json.load")
    mock_json_load.return_value = {
        "files": [
            {
                "logical_name": "/full/path/to/file/in/data/warehouse.tar.bz2",
                "file_size": 1234567890,
                "checksum": {
                    "adler32": "89d5efeb",
                    "sha512": "c919210281b72327c179e26be799b06cdaf48bf6efce56fb9d53f758c1b997099831ad05453fdb1ba65be7b35d0b4c5cebfc439efbdf83317ba0e38bf6f42570",
                },
            }
        ]
    }
    mock_shutil_move = mocker.patch("shutil.move")
    mock_shutil_move.return_value = None
    mock_lta_checksums = mocker.patch("lta.unpacker.lta_checksums")
    mock_lta_checksums.return_value = {
        "adler32": "89d5efeb",
        "sha512": "c919210281b72327c179e26be799b06cdaf48bf6efce56fb9d53f758c1b997099831ad05453fdb1ba65be7b35d0b4c5cebfc439efbdf83317ba0e38bf6f42570",
    }
    mock_os_path_getsize = mocker.patch("os.path.getsize")
    mock_os_path_getsize.return_value = 234567890
    mock_os_remove = mocker.patch("os.remove")
    mock_os_remove.return_value = None
    altfc_mock = mocker.patch("lta.unpacker.Unpacker._add_location_to_file_catalog", new_callable=AsyncMock)
    altfc_mock.return_value = False
    p = Unpacker(config, logger_mock)
    BUNDLE_OBJ = {
        "bundle_path": "/mnt/lfss/jade-lta/bundler_out/9a1cab0a395211eab1cbce3a3da73f88.zip",
        "uuid": "f74db80e-9661-40cc-9f01-8d087af23f56",
        "source": "NERSC",
        "dest": "WIPAC",
        "files": [{"logical_name": "/path/to/a/data/file", }],
    }
    with patch("builtins.open", mock_open(read_data="data")) as metadata_mock:
        with pytest.raises(Exception):
            await p._do_work_bundle(lta_rc_mock, BUNDLE_OBJ)
        metadata_mock.assert_called_with(mocker.ANY)


@pytest.mark.asyncio
async def test_unpacker_do_work_bundle_mismatch_checksum(config, mocker, path_map_mock):
    """Test that _do_work_bundle does the work of preparing an archive."""
    logger_mock = mocker.MagicMock()
    lta_rc_mock = mocker.patch("rest_tools.client.RestClient.request", new_callable=AsyncMock)
    mock_zipfile_init = mocker.patch("zipfile.ZipFile.__init__")
    mock_zipfile_init.return_value = None
    mock_zipfile_write = mocker.patch("zipfile.ZipFile.extractall")
    mock_zipfile_write.return_value = None
    mock_json_load = mocker.patch("json.load")
    mock_json_load.return_value = {
        "files": [
            {
                "logical_name": "/full/path/to/file/in/data/warehouse.tar.bz2",
                "file_size": 1234567890,
                "checksum": {
                    "adler32": "89d5efeb",
                    "sha512": "c919210281b72327c179e26be799b06cdaf48bf6efce56fb9d53f758c1b997099831ad05453fdb1ba65be7b35d0b4c5cebfc439efbdf83317ba0e38bf6f42570",
                },
            }
        ]
    }
    mock_shutil_move = mocker.patch("shutil.move")
    mock_shutil_move.return_value = None
    mock_lta_checksums = mocker.patch("lta.unpacker.lta_checksums")
    mock_lta_checksums.return_value = {
        "adler32": "89d5efeb",
        "sha512": "919210281b72327c179e26be799b06cdaf48bf6efce56fb9d53f758c1b997099831ad05453fdb1ba65be7b35d0b4c5cebfc439efbdf83317ba0e38bf6f42570c",
    }
    mock_os_path_getsize = mocker.patch("os.path.getsize")
    mock_os_path_getsize.return_value = 1234567890
    mock_os_remove = mocker.patch("os.remove")
    mock_os_remove.return_value = None
    altfc_mock = mocker.patch("lta.unpacker.Unpacker._add_location_to_file_catalog", new_callable=AsyncMock)
    altfc_mock.return_value = False
    p = Unpacker(config, logger_mock)
    BUNDLE_OBJ = {
        "bundle_path": "/mnt/lfss/jade-lta/bundler_out/9a1cab0a395211eab1cbce3a3da73f88.zip",
        "uuid": "f74db80e-9661-40cc-9f01-8d087af23f56",
        "source": "NERSC",
        "dest": "WIPAC",
        "files": [{"logical_name": "/path/to/a/data/file", }],
    }
    with patch("builtins.open", mock_open(read_data="data")) as metadata_mock:
        with pytest.raises(Exception):
            await p._do_work_bundle(lta_rc_mock, BUNDLE_OBJ)
        metadata_mock.assert_called_with(mocker.ANY)


@pytest.mark.asyncio
async def test_unpacker_do_work_bundle_path_remapping(config, mocker, path_map_mock):
    """Test that _do_work_bundle does the work of preparing an archive."""
    logger_mock = mocker.MagicMock()
    lta_rc_mock = mocker.patch("rest_tools.client.RestClient.request", new_callable=AsyncMock)
    mock_zipfile_init = mocker.patch("zipfile.ZipFile.__init__")
    mock_zipfile_init.return_value = None
    mock_zipfile_write = mocker.patch("zipfile.ZipFile.extractall")
    mock_zipfile_write.return_value = None
    mock_json_load = mocker.patch("json.load")
    mock_json_load.return_value = {
        "files": [
            {
                "logical_name": "/mnt/lfs7/IceCube/2013/filtered/PFFilt/1109/PFFilt_PhysicsFiltering_Run00123231_Subrun00000000_00000002.tar.bz2",
                "file_size": 1234567890,
                "checksum": {
                    "adler32": "89d5efeb",
                    "sha512": "c919210281b72327c179e26be799b06cdaf48bf6efce56fb9d53f758c1b997099831ad05453fdb1ba65be7b35d0b4c5cebfc439efbdf83317ba0e38bf6f42570",
                },
            }
        ]
    }
    mock_shutil_move = mocker.patch("shutil.move")
    mock_shutil_move.return_value = None
    mock_lta_checksums = mocker.patch("lta.unpacker.lta_checksums")
    mock_lta_checksums.return_value = {
        "adler32": "89d5efeb",
        "sha512": "c919210281b72327c179e26be799b06cdaf48bf6efce56fb9d53f758c1b997099831ad05453fdb1ba65be7b35d0b4c5cebfc439efbdf83317ba0e38bf6f42570",
    }
    mock_os_path_getsize = mocker.patch("os.path.getsize")
    mock_os_path_getsize.return_value = 1234567890
    mock_os_remove = mocker.patch("os.remove")
    mock_os_remove.return_value = None
    altfc_mock = mocker.patch("lta.unpacker.Unpacker._add_location_to_file_catalog", new_callable=AsyncMock)
    altfc_mock.return_value = False
    p = Unpacker(config, logger_mock)
    BUNDLE_OBJ = {
        "bundle_path": "/mnt/lfss/jade-lta/bundler_out/9a1cab0a395211eab1cbce3a3da73f88.zip",
        "uuid": "f74db80e-9661-40cc-9f01-8d087af23f56",
        "source": "NERSC",
        "dest": "WIPAC",
        "files": [{"logical_name": "/path/to/a/data/file", }],
    }
    with patch("builtins.open", mock_open(read_data="data")) as metadata_mock:
        await p._do_work_bundle(lta_rc_mock, BUNDLE_OBJ)
        metadata_mock.assert_called_with(mocker.ANY)
        mock_lta_checksums.assert_called_with("/data/exp/IceCube/2013/filtered/PFFilt/1109/PFFilt_PhysicsFiltering_Run00123231_Subrun00000000_00000002.tar.bz2")


def test_unpacker_delete_manifest_metadata_v3(config, mocker, path_map_mock):
    """Test that _delete_manifest_metadata will delete metadata of either version."""
    logger_mock = mocker.MagicMock()
    p = Unpacker(config, logger_mock)
    mock_os_remove = mocker.patch("os.remove")
    mock_os_remove.side_effect = [NameError, None]
    p._delete_manifest_metadata("0869ea50-e437-443f-8cdb-31a350f88e57")
    mock_os_remove.assert_called_with("/tmp/lta/testing/unpacker/outbox/0869ea50-e437-443f-8cdb-31a350f88e57.metadata.ndjson")


def test_unpacker_delete_manifest_metadata_unknown(config, mocker, path_map_mock):
    """Test that _delete_manifest_metadata will throw on an unknown version."""
    logger_mock = mocker.MagicMock()
    p = Unpacker(config, logger_mock)
    mock_os_remove = mocker.patch("os.remove")
    mock_os_remove.side_effect = [NameError, NameError]
    with pytest.raises(NameError):
        p._delete_manifest_metadata("0869ea50-e437-443f-8cdb-31a350f88e57")
    mock_os_remove.assert_called_with("/tmp/lta/testing/unpacker/outbox/0869ea50-e437-443f-8cdb-31a350f88e57.metadata.ndjson")


def test_unpacker_read_manifest_metadata_for_v3(config, mocker, path_map_mock):
    """Test that _read_manifest_metadata will read v3 metadata."""
    logger_mock = mocker.MagicMock()
    p = Unpacker(config, logger_mock)
    mock_v2 = mocker.patch("lta.unpacker.Unpacker._read_manifest_metadata_v2")
    mock_v2.side_effect = [None]
    mock_v3 = mocker.patch("lta.unpacker.Unpacker._read_manifest_metadata_v3")
    mock_v3.side_effect = [{"some": "object"}]
    p._read_manifest_metadata("0869ea50-e437-443f-8cdb-31a350f88e57")
    mock_v2.assert_called_with("0869ea50-e437-443f-8cdb-31a350f88e57")
    mock_v3.assert_called_with("0869ea50-e437-443f-8cdb-31a350f88e57")


def test_unpacker_read_manifest_metadata_unknown(config, mocker, path_map_mock):
    """Test that _read_manifest_metadata will throw on an unknown version."""
    logger_mock = mocker.MagicMock()
    p = Unpacker(config, logger_mock)
    mock_v2 = mocker.patch("lta.unpacker.Unpacker._read_manifest_metadata_v2")
    mock_v2.side_effect = [None]
    mock_v3 = mocker.patch("lta.unpacker.Unpacker._read_manifest_metadata_v3")
    mock_v3.side_effect = [None]
    with pytest.raises(Exception):
        p._read_manifest_metadata("0869ea50-e437-443f-8cdb-31a350f88e57")
    mock_v2.assert_called_with("0869ea50-e437-443f-8cdb-31a350f88e57")
    mock_v3.assert_called_with("0869ea50-e437-443f-8cdb-31a350f88e57")


def test_unpacker_read_manifest_metadata_v2(config, mocker, path_map_mock):
    """Test that _read_manifest_metadata_v2 will try to read metadata."""
    logger_mock = mocker.MagicMock()
    p = Unpacker(config, logger_mock)
    with patch("builtins.open", mock_open(read_data='{"some": "object"}')) as metadata_mock:
        assert p._read_manifest_metadata_v2("0869ea50-e437-443f-8cdb-31a350f88e57") == {"some": "object"}
    metadata_mock.assert_called_with(mocker.ANY)


@patch('builtins.open')
def test_unpacker_read_manifest_metadata_v2_no_throw(file_open_mock, config, mocker, path_map_mock):
    """Test that _read_manifest_metadata_v2 will not throw when unable to read metadata."""
    logger_mock = mocker.MagicMock()
    file_open_mock.side_effect = NameError
    p = Unpacker(config, logger_mock)
    assert not p._read_manifest_metadata_v2("0869ea50-e437-443f-8cdb-31a350f88e57")


def test_unpacker_read_manifest_metadata_v3(config, mocker, path_map_mock):
    """Test that _read_manifest_metadata_v3 will try to read metadata."""
    logger_mock = mocker.MagicMock()
    p = Unpacker(config, logger_mock)
    read_data = """{}
        {"some": "object"}"""
    result = {
        "files": [
            {"some": "object"}
        ]
    }
    with patch("builtins.open", mock_open(read_data=read_data)) as metadata_mock:
        assert p._read_manifest_metadata_v3("0869ea50-e437-443f-8cdb-31a350f88e57") == result
    metadata_mock.assert_called_with(mocker.ANY)


@patch('builtins.open')
def test_unpacker_read_manifest_metadata_v3_no_throw(file_open_mock, config, mocker, path_map_mock):
    """Test that _read_manifest_metadata_v3 will not throw when unable to read metadata."""
    logger_mock = mocker.MagicMock()
    file_open_mock.side_effect = NameError
    p = Unpacker(config, logger_mock)
    assert not p._read_manifest_metadata_v3("0869ea50-e437-443f-8cdb-31a350f88e57")


# @pytest.mark.asyncio
# async def test_unpacker_do_work_bundle_ndjson_metadata(config, mocker, path_map_mock):
#     """Test that _do_work_bundle does the work of preparing an archive."""
#     logger_mock = mocker.MagicMock()
#     lta_rc_mock = mocker.patch("rest_tools.client.RestClient.request", new_callable=AsyncMock)
#     mock_zipfile_init = mocker.patch("zipfile.ZipFile.__init__")
#     mock_zipfile_init.return_value = None
#     mock_zipfile_write = mocker.patch("zipfile.ZipFile.extractall")
#     mock_zipfile_write.return_value = None
#     mock_json_load = mocker.patch("json.load")
#     mock_json_load.side_effect = [
#         {
#             "files": [
#                 {
#                     "logical_name": "/full/path/to/file/in/data/warehouse.tar.bz2",
#                     "file_size": 1234567890,
#                     "checksum": {
#                         "adler32": "89d5efeb",
#                         "sha512": "c919210281b72327c179e26be799b06cdaf48bf6efce56fb9d53f758c1b997099831ad05453fdb1ba65be7b35d0b4c5cebfc439efbdf83317ba0e38bf6f42570",
#                     },
#                 }
#             ]
#         },
#         {
#             "logical_name": "/full/path/to/file/in/data/warehouse.tar.bz2",
#             "file_size": 1234567890,
#             "checksum": {
#                 "adler32": "89d5efeb",
#                 "sha512": "c919210281b72327c179e26be799b06cdaf48bf6efce56fb9d53f758c1b997099831ad05453fdb1ba65be7b35d0b4c5cebfc439efbdf83317ba0e38bf6f42570"
#             }
#         },
#     ]
#     mock_shutil_move = mocker.patch("shutil.move")
#     mock_shutil_move.return_value = None
#     mock_lta_checksums = mocker.patch("lta.unpacker.lta_checksums")
#     mock_lta_checksums.return_value = {
#         "adler32": "89d5efeb",
#         "sha512": "c919210281b72327c179e26be799b06cdaf48bf6efce56fb9d53f758c1b997099831ad05453fdb1ba65be7b35d0b4c5cebfc439efbdf83317ba0e38bf6f42570",
#     }
#     mock_os_path_getsize = mocker.patch("os.path.getsize")
#     mock_os_path_getsize.return_value = 1234567890
#     mock_os_remove = mocker.patch("os.remove")
#     mock_os_remove.side_effect = [
#         NameError,
#         None
#     ]
#     altfc_mock = mocker.patch("lta.unpacker.Unpacker._add_location_to_file_catalog", new_callable=AsyncMock)
#     altfc_mock.return_value = False
#     p = Unpacker(config, logger_mock)
#     BUNDLE_OBJ = {
#         "bundle_path": "/mnt/lfss/jade-lta/bundler_out/9a1cab0a395211eab1cbce3a3da73f88.zip",
#         "uuid": "f74db80e-9661-40cc-9f01-8d087af23f56",
#         "source": "NERSC",
#         "dest": "WIPAC",
#         "files": [{"logical_name": "/path/to/a/data/file", }],
#     }
#     with patch("builtins.open", mock_open(read_data="data")) as metadata_mock:
#         await p._do_work_bundle(lta_rc_mock, BUNDLE_OBJ)
#         metadata_mock.assert_called_with(mocker.ANY)


# @pytest.mark.asyncio
# @patch('builtins.open')
# async def test_unpacker_do_work_bundle_ndjson_metadata(file_open_mock, config, mocker, path_map_mock):
#     """Test that _do_work_bundle does the work of preparing an archive."""
#     logger_mock = mocker.MagicMock()
#     lta_rc_mock = mocker.patch("rest_tools.client.RestClient.request", new_callable=AsyncMock)
#     mock_zipfile_init = mocker.patch("zipfile.ZipFile.__init__")
#     mock_zipfile_init.return_value = None
#     mock_zipfile_write = mocker.patch("zipfile.ZipFile.extractall")
#     mock_zipfile_write.return_value = None
#     mock_json_load = mocker.patch("json.load")
#     mock_json_load.return_value = {
#         "files": [
#             {
#                 "logical_name": "/full/path/to/file/in/data/warehouse.tar.bz2",
#                 "file_size": 1234567890,
#                 "checksum": {
#                     "adler32": "89d5efeb",
#                     "sha512": "c919210281b72327c179e26be799b06cdaf48bf6efce56fb9d53f758c1b997099831ad05453fdb1ba65be7b35d0b4c5cebfc439efbdf83317ba0e38bf6f42570",
#                 },
#             }
#         ]
#     }
#     mock_shutil_move = mocker.patch("shutil.move")
#     mock_shutil_move.return_value = None
#     mock_lta_checksums = mocker.patch("lta.unpacker.lta_checksums")
#     mock_lta_checksums.return_value = {
#         "adler32": "89d5efeb",
#         "sha512": "c919210281b72327c179e26be799b06cdaf48bf6efce56fb9d53f758c1b997099831ad05453fdb1ba65be7b35d0b4c5cebfc439efbdf83317ba0e38bf6f42570",
#     }
#     mock_os_path_getsize = mocker.patch("os.path.getsize")
#     mock_os_path_getsize.return_value = 1234567890
#     mock_os_remove = mocker.patch("os.remove")
#     mock_os_remove.return_value = None
#     altfc_mock = mocker.patch("lta.unpacker.Unpacker._add_location_to_file_catalog", new_callable=AsyncMock)
#     altfc_mock.return_value = False
#     p = Unpacker(config, logger_mock)
#     BUNDLE_OBJ = {
#         "bundle_path": "/mnt/lfss/jade-lta/bundler_out/9a1cab0a395211eab1cbce3a3da73f88.zip",
#         "uuid": "f74db80e-9661-40cc-9f01-8d087af23f56",
#         "source": "NERSC",
#         "dest": "WIPAC",
#         "files": [{"logical_name": "/path/to/a/data/file", }],
#     }
#     ndjson_mock_data = MagicMock()
#     ndjson_mock_data.readline = MagicMock()
#     ndjson_mock_data.readline.side_effect = [
#         """{
#             "bundle_path": "/mnt/lfss/jade-lta/bundler_out/9a1cab0a395211eab1cbce3a3da73f88.zip",
#             "uuid": "f74db80e-9661-40cc-9f01-8d087af23f56",
#             "source": "NERSC",
#             "dest": "WIPAC"
#         }""",
#         """{
#             "logical_name": "/full/path/to/file/in/data/warehouse.tar.bz2",
#             "file_size": 1234567890,
#             "checksum": {
#                 "adler32": "89d5efeb",
#                 "sha512": "c919210281b72327c179e26be799b06cdaf48bf6efce56fb9d53f758c1b997099831ad05453fdb1ba65be7b35d0b4c5cebfc439efbdf83317ba0e38bf6f42570"
#             }
#         }""",
#         None
#     ]
#     ndjson_mock = MagicMock()
#     ndjson_mock.__enter__.return_value = ndjson_mock_data
#     file_open_mock.side_effect = [
#         # FileNotFoundError,
#         IOError,
#         ndjson_mock,
#     ]
#     with file_open_mock:
#         await p._do_work_bundle(lta_rc_mock, BUNDLE_OBJ)
