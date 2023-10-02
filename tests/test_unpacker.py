# test_unpacker.py
"""Unit tests for lta/unpacker.py."""

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

from typing import Any, Dict
from unittest.mock import AsyncMock, call, MagicMock, mock_open, patch

import pytest
from pytest import MonkeyPatch
from pytest_mock import MockerFixture
from tornado.web import HTTPError

from lta.unpacker import main, main_sync, Unpacker
from .test_util import ObjectLiteral

TestConfig = Dict[str, str]


@pytest.fixture
def config() -> TestConfig:
    """Supply a stock Unpacker component configuration."""
    return {
        "CLIENT_ID": "long-term-archive",
        "CLIENT_SECRET": "hunter2",  # http://bash.org/?244321
        "CLEAN_OUTBOX": "TRUE",
        "COMPONENT_NAME": "testing-unpacker",
        "DEST_SITE": "WIPAC",
        "FILE_CATALOG_CLIENT_ID": "file-catalog-client-id",
        "FILE_CATALOG_CLIENT_SECRET": "file-catalog-client-secret",
        "FILE_CATALOG_REST_URL": "http://kVj74wBA1AMTDV8zccn67pGuWJqHZzD7iJQHrUJKA.com/",
        "INPUT_STATUS": "unpacking",
        "LOG_LEVEL": "DEBUG",
        "LTA_AUTH_OPENID_URL": "localhost:12345",
        "LTA_REST_URL": "localhost:12347",
        "OUTPUT_STATUS": "completed",
        "PATH_MAP_JSON": "/tmp/lta/testing/path_map.json",
        "PROMETHEUS_METRICS_PORT": "8080",
        "RUN_ONCE_AND_DIE": "False",
        "RUN_UNTIL_NO_WORK": "False",
        "SOURCE_SITE": "NERSC",
        "UNPACKER_OUTBOX_PATH": "/tmp/lta/testing/unpacker/outbox",
        "UNPACKER_WORKBOX_PATH": "/tmp/lta/testing/unpacker/workbox",
        "WORK_RETRIES": "3",
        "WORK_SLEEP_DURATION_SECONDS": "60",
        "WORK_TIMEOUT_SECONDS": "30",
    }


@pytest.fixture
def path_map_mock(mocker: MockerFixture) -> MagicMock:
    rtm = mocker.patch("pathlib.Path.read_text")
    rtm.return_value = '{"/mnt/lfs7": "/data/exp"}'
    return rtm


def test_always_succeed() -> None:
    """Canary test to verify test framework is operating properly."""
    assert True


def test_constructor_config_missing_values(mocker: MockerFixture) -> None:
    """Fail with a ValueError if the configuration object is missing required configuration variables."""
    config = {
        "PAN_GALACTIC_GARGLE_BLASTER": "Yummy"
    }
    logger_mock = mocker.MagicMock()
    with pytest.raises(ValueError):
        Unpacker(config, logger_mock)


def test_constructor_config_poison_values(config: TestConfig, mocker: MockerFixture) -> None:
    """Fail with a ValueError if the configuration object is missing required configuration variables."""
    unpacker_config = config.copy()
    del unpacker_config["LTA_REST_URL"]
    logger_mock = mocker.MagicMock()
    with pytest.raises(ValueError):
        Unpacker(unpacker_config, logger_mock)


def test_constructor_config(config: TestConfig, mocker: MockerFixture, path_map_mock: MagicMock) -> None:
    """Test that a Unpacker can be constructed with a configuration object and a logging object."""
    logger_mock = mocker.MagicMock()
    p = Unpacker(config, logger_mock)
    assert p.lta_auth_openid_url == "localhost:12345"
    assert p.lta_rest_url == "localhost:12347"
    assert p.name == "testing-unpacker"
    assert p.work_sleep_duration_seconds == 60
    assert p.logger == logger_mock


def test_constructor_config_sleep_type_int(config: TestConfig, mocker: MockerFixture, path_map_mock: MagicMock) -> None:
    """Ensure that sleep seconds can also be provided as an integer."""
    logger_mock = mocker.MagicMock()
    p = Unpacker(config, logger_mock)
    assert p.lta_auth_openid_url == "localhost:12345"
    assert p.lta_rest_url == "localhost:12347"
    assert p.name == "testing-unpacker"
    assert p.work_sleep_duration_seconds == 60
    assert p.logger == logger_mock


def test_constructor_state(config: TestConfig, mocker: MockerFixture, path_map_mock: MagicMock) -> None:
    """Verify that the Unpacker has a reasonable state when it is first constructed."""
    logger_mock = mocker.MagicMock()
    p = Unpacker(config, logger_mock)
    assert p.last_work_begin_timestamp is p.last_work_end_timestamp


def test_do_status(config: TestConfig, mocker: MockerFixture, path_map_mock: MagicMock) -> None:
    """Verify that the Unpacker has no additional state to offer."""
    logger_mock = mocker.MagicMock()
    p = Unpacker(config, logger_mock)
    assert p._do_status() == {}


@pytest.mark.asyncio
async def test_script_main_sync(config: TestConfig,
                                mocker: MockerFixture,
                                monkeypatch: MonkeyPatch,
                                path_map_mock: MagicMock) -> None:
    """
    Verify Unpacker component behavior when run as a script.

    Test to make sure running the Unpacker as a script does the setup work
    that we expect and then launches the unpacker service.
    """
    for key in config.keys():
        monkeypatch.setenv(key, config[key])
    mock_run = mocker.patch("asyncio.run")
    mock_main = mocker.patch("lta.unpacker.main")
    mock_prometheus = mocker.patch("lta.unpacker.start_http_server")
    main_sync()
    mock_prometheus.assert_called_with(8080)
    mock_main.assert_called()
    mock_run.assert_called()
    await mock_run.call_args.args[0]


@pytest.mark.asyncio
async def test_script_main(config: TestConfig,
                           mocker: MockerFixture,
                           monkeypatch: MonkeyPatch,
                           path_map_mock: MagicMock) -> None:
    """
    Verify Unpacker component behavior when run as a script.

    Test to make sure running the Unpacker as a script does the async setup
    work that we expect and then launches the unpacker service.
    """
    logger_mock = mocker.MagicMock()
    p = Unpacker(config, logger_mock)
    mock_work_loop = mocker.patch("lta.unpacker.work_loop", side_effect=AsyncMock())
    await main(p)
    mock_work_loop.assert_called()


@pytest.mark.asyncio
async def test_unpacker_logs_configuration(mocker: MockerFixture, path_map_mock: MagicMock) -> None:
    """Test to make sure the Unpacker logs its configuration."""
    logger_mock = mocker.MagicMock()
    unpacker_config = {
        "CLEAN_OUTBOX": "true",
        "CLIENT_ID": "long-term-archive",
        "CLIENT_SECRET": "hunter2",  # http://bash.org/?244321
        "COMPONENT_NAME": "logme-testing-unpacker",
        "DEST_SITE": "WIPAC",
        "FILE_CATALOG_CLIENT_ID": "file-catalog-client-id",
        "FILE_CATALOG_CLIENT_SECRET": "file-catalog-client-secret",
        "FILE_CATALOG_REST_URL": "http://kVj74wBA1AMTDV8zccn67pGuWJqHZzD7iJQHrUJKA.com/",
        "INPUT_STATUS": "unpacking",
        "LOG_LEVEL": "DEBUG",
        "LTA_AUTH_OPENID_URL": "localhost:12345",
        "LTA_REST_URL": "logme-http://RmMNHdPhHpH2ZxfaFAC9d2jiIbf5pZiHDqy43rFLQiM.com/",
        "OUTPUT_STATUS": "completed",
        "PATH_MAP_JSON": "logme/tmp/lta/testing/path_map.json",
        "PROMETHEUS_METRICS_PORT": "8080",
        "RUN_ONCE_AND_DIE": "False",
        "RUN_UNTIL_NO_WORK": "False",
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
        call('CLEAN_OUTBOX = true'),
        call('CLIENT_ID = long-term-archive'),
        call('CLIENT_SECRET = [秘密]'),
        call('COMPONENT_NAME = logme-testing-unpacker'),
        call('DEST_SITE = WIPAC'),
        call('FILE_CATALOG_CLIENT_ID = file-catalog-client-id'),
        call('FILE_CATALOG_CLIENT_SECRET = [秘密]'),
        call('FILE_CATALOG_REST_URL = http://kVj74wBA1AMTDV8zccn67pGuWJqHZzD7iJQHrUJKA.com/'),
        call('INPUT_STATUS = unpacking'),
        call('LOG_LEVEL = DEBUG'),
        call('LTA_AUTH_OPENID_URL = localhost:12345'),
        call('LTA_REST_URL = logme-http://RmMNHdPhHpH2ZxfaFAC9d2jiIbf5pZiHDqy43rFLQiM.com/'),
        call('OUTPUT_STATUS = completed'),
        call('PATH_MAP_JSON = logme/tmp/lta/testing/path_map.json'),
        call('PROMETHEUS_METRICS_PORT = 8080'),
        call('RUN_ONCE_AND_DIE = False'),
        call('RUN_UNTIL_NO_WORK = False'),
        call('SOURCE_SITE = NERSC'),
        call('UNPACKER_OUTBOX_PATH = logme/tmp/lta/testing/unpacker/outbox'),
        call('UNPACKER_WORKBOX_PATH = logme/tmp/lta/testing/unpacker/workbox'),
        call('WORK_RETRIES = 5'),
        call('WORK_SLEEP_DURATION_SECONDS = 70'),
        call('WORK_TIMEOUT_SECONDS = 90'),
    ]
    logger_mock.info.assert_has_calls(EXPECTED_LOGGER_CALLS)


@pytest.mark.asyncio
async def test_unpacker_run(config: TestConfig, mocker: MockerFixture, path_map_mock: MagicMock) -> None:
    """Test the Unpacker does the work the unpacker should do."""
    logger_mock = mocker.MagicMock()
    p = Unpacker(config, logger_mock)
    p._do_work = AsyncMock()  # type: ignore[method-assign]
    await p.run()
    p._do_work.assert_called()


@pytest.mark.asyncio
async def test_unpacker_run_exception(config: TestConfig, mocker: MockerFixture, path_map_mock: MagicMock) -> None:
    """Test an error doesn't kill the Unpacker."""
    logger_mock = mocker.MagicMock()
    p = Unpacker(config, logger_mock)
    p.last_work_end_timestamp = ""
    p._do_work = AsyncMock()  # type: ignore[method-assign]
    p._do_work.side_effect = [Exception("bad thing happen!")]
    await p.run()
    p._do_work.assert_called()
    assert p.last_work_end_timestamp


@pytest.mark.asyncio
async def test_unpacker_do_work_pop_exception(config: TestConfig, mocker: MockerFixture, path_map_mock: MagicMock) -> None:
    """Test that _do_work raises when the RestClient can't pop."""
    logger_mock = mocker.MagicMock()
    lta_rc_mock = mocker.patch("rest_tools.client.RestClient.request", new_callable=AsyncMock)
    lta_rc_mock.side_effect = HTTPError(500, "LTA DB on fire. Again.")
    p = Unpacker(config, logger_mock)
    with pytest.raises(HTTPError):
        await p._do_work()
    lta_rc_mock.assert_called_with("POST", '/Bundles/actions/pop?source=NERSC&dest=WIPAC&status=unpacking', mocker.ANY)


@pytest.mark.asyncio
async def test_unpacker_do_work_no_results(config: TestConfig, mocker: MockerFixture, path_map_mock: MagicMock) -> None:
    """Test that _do_work goes on vacation when the LTA DB has no work."""
    logger_mock = mocker.MagicMock()
    claim_mock = mocker.patch("lta.unpacker.Unpacker._do_work_claim", new_callable=AsyncMock)
    claim_mock.return_value = False
    p = Unpacker(config, logger_mock)
    await p._do_work()


@pytest.mark.asyncio
async def test_unpacker_do_work_claim_no_results(config: TestConfig, mocker: MockerFixture, path_map_mock: MagicMock) -> None:
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
async def test_unpacker_do_work_yes_results(config: TestConfig, mocker: MockerFixture, path_map_mock: MagicMock) -> None:
    """Test that _do_work_claim processes each Bundle that it gets from the LTA DB."""
    BUNDLE_OBJ = {
        "uuid": "f74db80e-9661-40cc-9f01-8d087af23f56"
    }
    logger_mock = mocker.MagicMock()
    request_mock = mocker.patch("rest_tools.client.RestClient.request", new_callable=AsyncMock)
    request_mock.return_value = {
        "bundle": BUNDLE_OBJ,
    }
    dwb_mock = mocker.patch("lta.unpacker.Unpacker._do_work_bundle", new_callable=AsyncMock)
    p = Unpacker(config, logger_mock)
    assert await p._do_work_claim()
    request_mock.assert_called_with("POST", '/Bundles/actions/pop?source=NERSC&dest=WIPAC&status=unpacking', mocker.ANY)
    dwb_mock.assert_called_with(mocker.ANY, BUNDLE_OBJ)


@pytest.mark.asyncio
async def test_unpacker_do_work_raise_exception(config: TestConfig, mocker: MockerFixture, path_map_mock: MagicMock) -> None:
    """Test that _do_work_claim processes each Bundle that it gets from the LTA DB."""
    BUNDLE_OBJ = {
        "uuid": "f74db80e-9661-40cc-9f01-8d087af23f56"
    }
    logger_mock = mocker.MagicMock()
    request_mock = mocker.patch("rest_tools.client.RestClient.request", new_callable=AsyncMock)
    request_mock.return_value = {
        "bundle": BUNDLE_OBJ,
    }
    dwb_mock = mocker.patch("lta.unpacker.Unpacker._do_work_bundle", new_callable=AsyncMock)
    dwb_mock.side_effect = Exception("LTA DB started on fire again")
    qb_mock = mocker.patch("lta.unpacker.Unpacker._quarantine_bundle", new_callable=AsyncMock)
    p = Unpacker(config, logger_mock)
    with pytest.raises(Exception):
        await p._do_work_claim()
    request_mock.assert_called_with("POST", '/Bundles/actions/pop?source=NERSC&dest=WIPAC&status=unpacking', mocker.ANY)
    dwb_mock.assert_called_with(mocker.ANY, BUNDLE_OBJ)
    qb_mock.assert_called_with(mocker.ANY, BUNDLE_OBJ, "LTA DB started on fire again")


@pytest.mark.asyncio
async def test_unpacker_do_work_bundle_once_and_die(config: TestConfig, mocker: MockerFixture, path_map_mock: MagicMock) -> None:
    """Test that _do_work goes on vacation when the LTA DB has no work."""
    once = config.copy()
    once["RUN_ONCE_AND_DIE"] = "True"
    logger_mock = mocker.MagicMock()
    claim_mock = mocker.patch("lta.unpacker.Unpacker._do_work_claim", new_callable=AsyncMock)
    claim_mock.return_value = False
    sys_exit_mock = mocker.patch("sys.exit")
    p = Unpacker(once, logger_mock)
    await p._do_work()
    sys_exit_mock.assert_called()


@pytest.mark.asyncio
async def test_unpacker_quarantine_bundle_with_reason(config: TestConfig, mocker: MockerFixture, path_map_mock: MagicMock) -> None:
    """Test that _do_work_claim attempts to quarantine a Bundle that fails to get unpacked."""
    logger_mock = mocker.MagicMock()
    lta_rc_mock = mocker.patch("rest_tools.client.RestClient", new_callable=AsyncMock)
    p = Unpacker(config, logger_mock)
    await p._quarantine_bundle(
        lta_rc_mock,
        {"uuid": "c4b345e4-2395-4f9e-b0eb-9cc1c9cdf003", "status": "unpacking"},
        "Rucio caught fire, then we roasted marshmellows."
    )
    lta_rc_mock.request.assert_called_with("PATCH", "/Bundles/c4b345e4-2395-4f9e-b0eb-9cc1c9cdf003", mocker.ANY)


@pytest.mark.asyncio
async def test_unpacker_quarantine_bundle_with_reason_raises(config: TestConfig, mocker: MockerFixture, path_map_mock: MagicMock) -> None:
    """Test that _do_work_claim attempts to quarantine a Bundle that fails to get unpacked."""
    logger_mock = mocker.MagicMock()
    lta_rc_mock = mocker.patch("rest_tools.client.RestClient", new_callable=AsyncMock)
    lta_rc_mock.request.side_effect = Exception("Marshmellows were poisoned")
    p = Unpacker(config, logger_mock)
    await p._quarantine_bundle(
        lta_rc_mock,
        {"uuid": "c4b345e4-2395-4f9e-b0eb-9cc1c9cdf003", "status": "unpacking"},
        "Rucio caught fire, then we roasted marshmellows."
    )
    lta_rc_mock.request.assert_called_with("PATCH", "/Bundles/c4b345e4-2395-4f9e-b0eb-9cc1c9cdf003", mocker.ANY)


@pytest.mark.asyncio
async def test_unpacker_update_bundle_in_lta_db(config: TestConfig, mocker: MockerFixture, path_map_mock: MagicMock) -> None:
    """Test that _update_bundle_in_lta_db updates the status of the bundle in the LTA DB."""
    logger_mock = mocker.MagicMock()
    lta_rc_mock = mocker.patch("rest_tools.client.RestClient", new_callable=AsyncMock)
    p = Unpacker(config, logger_mock)
    assert await p._update_bundle_in_lta_db(lta_rc_mock, {"uuid": "c4b345e4-2395-4f9e-b0eb-9cc1c9cdf003"})
    lta_rc_mock.request.assert_called_with("PATCH", "/Bundles/c4b345e4-2395-4f9e-b0eb-9cc1c9cdf003", mocker.ANY)


@pytest.mark.asyncio
async def test_unpacker_add_location_to_file_catalog(config: TestConfig, mocker: MockerFixture, path_map_mock: MagicMock) -> None:
    """Test that _add_location_to_file_catalog adds a location in the File Catalog."""
    logger_mock = mocker.MagicMock()
    fc_rc_mock = mocker.patch("rest_tools.client.RestClient.request", new_callable=AsyncMock)
    p = Unpacker(config, logger_mock)
    bundle_file: Dict[str, Any] = {
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
async def test_unpacker_do_work_bundle(config: TestConfig, mocker: MockerFixture, path_map_mock: MagicMock) -> None:
    """Test that _do_work_bundle does the work of preparing an archive."""
    logger_mock = mocker.MagicMock()
    lta_rc_mock = mocker.patch("rest_tools.client.RestClient.request", new_callable=AsyncMock)
    mock_zipfile_init = mocker.patch("zipfile.ZipFile.__init__")
    mock_zipfile_init.return_value = None
    mock_zipfile_infolist = mocker.patch("zipfile.ZipFile.infolist")
    mock_zipfile_infolist.return_value = [
        ObjectLiteral(
            filename="9a1cab0a395211eab1cbce3a3da73f88.metadata.json",
            file_size=0,
        ),
        ObjectLiteral(
            filename="warehouse.tar.bz2",
            file_size=1234567890,
        ),
    ]
    mock_zipfile_extract = mocker.patch("zipfile.ZipFile.extract")
    mock_zipfile_extract.return_value = None
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
    edd_mock = mocker.patch("lta.unpacker.Unpacker._ensure_dest_directory")
    edd_mock.return_value = None
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
    mock_os_scandir = mocker.patch("os.scandir")
    mock_os_scandir.return_value.__enter__.return_value = []
    altfc_mock = mocker.patch("lta.unpacker.Unpacker._add_location_to_file_catalog", new_callable=AsyncMock)
    altfc_mock.return_value = False
    p = Unpacker(config, logger_mock)
    BUNDLE_OBJ = {
        "bundle_path": "/mnt/lfss/jade-lta/bundler_out/9a1cab0a395211eab1cbce3a3da73f88.zip",
        "uuid": "f74db80e-9661-40cc-9f01-8d087af23f56",
        "source": "NERSC",
        "dest": "WIPAC",
        "path": "/full/path/to/file",
        "files": [{"logical_name": "/full/path/to/file/in/data/warehouse.tar.bz2", }],
    }
    with patch("builtins.open", mock_open(read_data="data")) as metadata_mock:
        await p._do_work_bundle(lta_rc_mock, BUNDLE_OBJ)
        metadata_mock.assert_called_with(mocker.ANY)


@pytest.mark.asyncio
async def test_unpacker_do_work_bundle_manifest_json_filename_mismatch(config: TestConfig, mocker: MockerFixture, path_map_mock: MagicMock) -> None:
    """Test that _do_work_bundle does the work of preparing an archive."""
    logger_mock = mocker.MagicMock()
    # lta_rc_mock = mocker.patch("rest_tools.client.RestClient.request", new_callable=AsyncMock)
    mock_zipfile_init = mocker.patch("zipfile.ZipFile.__init__")
    mock_zipfile_init.return_value = None
    mock_zipfile_infolist = mocker.patch("zipfile.ZipFile.infolist")
    mock_zipfile_infolist.return_value = [
        ObjectLiteral(
            filename="9a1cab0a395211eab1cbce3a3da73f88.metadata.json",
            file_size=0,
        ),
        ObjectLiteral(
            filename="warehouse.tar.bz2",
            file_size=1234567890,
        ),
    ]
    p = Unpacker(config, logger_mock)
    BUNDLE_OBJ = {
        "bundle_path": "/mnt/lfss/jade-lta/bundler_out/58892e329b5111ed805113b05d4dfded.zip",
        "uuid": "58892e32-9b51-11ed-8051-13b05d4dfded",
        "source": "NERSC",
        "dest": "WIPAC",
        "path": "/full/path/to/file",
        "files": [{"logical_name": "/full/path/to/file/in/data/warehouse.tar.bz2", }],
    }
    with patch("builtins.open", mock_open(read_data="data")) as metadata_mock:
        with pytest.raises(Exception):
            await p._do_work_bundle(mocker.AsyncMock(), BUNDLE_OBJ)
        metadata_mock.assert_not_called()


@pytest.mark.asyncio
async def test_unpacker_do_work_bundle_filename_mismatch(config: TestConfig, mocker: MockerFixture, path_map_mock: MagicMock) -> None:
    """Test that _do_work_bundle does the work of preparing an archive."""
    logger_mock = mocker.MagicMock()
    # lta_rc_mock = mocker.patch("rest_tools.client.RestClient.request", new_callable=AsyncMock)
    mock_zipfile_init = mocker.patch("zipfile.ZipFile.__init__")
    mock_zipfile_init.return_value = None
    mock_zipfile_infolist = mocker.patch("zipfile.ZipFile.infolist")
    mock_zipfile_infolist.return_value = [
        ObjectLiteral(
            filename="9a1cab0a395211eab1cbce3a3da73f88.metadata.json",
            file_size=0,
        ),
        ObjectLiteral(
            filename="scarehouse.tar.bz2",
            file_size=1234567890,
        ),
    ]
    mock_zipfile_extract = mocker.patch("zipfile.ZipFile.extract")
    mock_zipfile_extract.return_value = None
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
    p = Unpacker(config, logger_mock)
    BUNDLE_OBJ = {
        "bundle_path": "/mnt/lfss/jade-lta/bundler_out/9a1cab0a395211eab1cbce3a3da73f88.zip",
        "uuid": "f74db80e-9661-40cc-9f01-8d087af23f56",
        "source": "NERSC",
        "dest": "WIPAC",
        "path": "/full/path/to/file",
        "files": [{"logical_name": "/full/path/to/file/in/data/warehouse.tar.bz2", }],
    }
    with patch("builtins.open", mock_open(read_data="data")) as metadata_mock:
        with pytest.raises(Exception):
            await p._do_work_bundle(mocker.AsyncMock(), BUNDLE_OBJ)
        metadata_mock.assert_called_with(mocker.ANY)


@pytest.mark.asyncio
async def test_unpacker_do_work_bundle_disk_file_size_mismatch_manifest_size(config: TestConfig, mocker: MockerFixture, path_map_mock: MagicMock) -> None:
    """Test that _do_work_bundle does the work of preparing an archive."""
    logger_mock = mocker.MagicMock()
    mock_zipfile_init = mocker.patch("zipfile.ZipFile.__init__")
    mock_zipfile_init.return_value = None
    mock_zipfile_infolist = mocker.patch("zipfile.ZipFile.infolist")
    mock_zipfile_infolist.return_value = [
        ObjectLiteral(
            filename="9a1cab0a395211eab1cbce3a3da73f88.metadata.json",
            file_size=0,
        ),
        ObjectLiteral(
            filename="warehouse.tar.bz2",
            file_size=1234567890,
        ),
    ]
    mock_zipfile_extract = mocker.patch("zipfile.ZipFile.extract")
    mock_zipfile_extract.return_value = None
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
    mock_os_path_getsize = mocker.patch("os.path.getsize")
    mock_os_path_getsize.return_value = 2345678901
    p = Unpacker(config, logger_mock)
    BUNDLE_OBJ = {
        "bundle_path": "/mnt/lfss/jade-lta/bundler_out/9a1cab0a395211eab1cbce3a3da73f88.zip",
        "uuid": "f74db80e-9661-40cc-9f01-8d087af23f56",
        "source": "NERSC",
        "dest": "WIPAC",
        "path": "/full/path/to/file",
        "files": [{"logical_name": "/full/path/to/file/in/data/warehouse.tar.bz2", }],
    }
    with patch("builtins.open", mock_open(read_data="data")) as metadata_mock:
        with pytest.raises(ValueError):
            await p._do_work_bundle(mocker.AsyncMock(), BUNDLE_OBJ)
        metadata_mock.assert_called_with(mocker.ANY)


@pytest.mark.asyncio
async def test_unpacker_do_work_bundle_zipinfo_size_mismatch_disk_file_size(config: TestConfig, mocker: MockerFixture, path_map_mock: MagicMock) -> None:
    """Test that _do_work_bundle does the work of preparing an archive."""
    logger_mock = mocker.MagicMock()
    mock_zipfile_init = mocker.patch("zipfile.ZipFile.__init__")
    mock_zipfile_init.return_value = None
    mock_zipfile_infolist = mocker.patch("zipfile.ZipFile.infolist")
    mock_zipfile_infolist.return_value = [
        ObjectLiteral(
            filename="9a1cab0a395211eab1cbce3a3da73f88.metadata.json",
            file_size=0,
        ),
        ObjectLiteral(
            filename="warehouse.tar.bz2",
            file_size=2345678901,
        ),
    ]
    mock_zipfile_extract = mocker.patch("zipfile.ZipFile.extract")
    mock_zipfile_extract.return_value = None
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
    mock_os_path_getsize = mocker.patch("os.path.getsize")
    mock_os_path_getsize.return_value = 1234567890
    p = Unpacker(config, logger_mock)
    BUNDLE_OBJ = {
        "bundle_path": "/mnt/lfss/jade-lta/bundler_out/9a1cab0a395211eab1cbce3a3da73f88.zip",
        "uuid": "f74db80e-9661-40cc-9f01-8d087af23f56",
        "source": "NERSC",
        "dest": "WIPAC",
        "path": "/full/path/to/file",
        "files": [{"logical_name": "/full/path/to/file/in/data/warehouse.tar.bz2", }],
    }
    with patch("builtins.open", mock_open(read_data="data")) as metadata_mock:
        with pytest.raises(ValueError):
            await p._do_work_bundle(mocker.AsyncMock(), BUNDLE_OBJ)
        metadata_mock.assert_called_with(mocker.ANY)


@pytest.mark.asyncio
async def test_unpacker_do_work_bundle_sha512_checksum_mismatch(config: TestConfig, mocker: MockerFixture, path_map_mock: MagicMock) -> None:
    """Test that _do_work_bundle does the work of preparing an archive."""
    logger_mock = mocker.MagicMock()
    mock_zipfile_init = mocker.patch("zipfile.ZipFile.__init__")
    mock_zipfile_init.return_value = None
    mock_zipfile_infolist = mocker.patch("zipfile.ZipFile.infolist")
    mock_zipfile_infolist.return_value = [
        ObjectLiteral(
            filename="9a1cab0a395211eab1cbce3a3da73f88.metadata.json",
            file_size=0,
        ),
        ObjectLiteral(
            filename="warehouse.tar.bz2",
            file_size=1234567890,
        ),
    ]
    mock_zipfile_extract = mocker.patch("zipfile.ZipFile.extract")
    mock_zipfile_extract.return_value = None
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
    edd_mock = mocker.patch("lta.unpacker.Unpacker._ensure_dest_directory")
    edd_mock.return_value = None
    mock_shutil_move = mocker.patch("shutil.move")
    mock_shutil_move.return_value = None
    mock_lta_checksums = mocker.patch("lta.unpacker.lta_checksums")
    mock_lta_checksums.return_value = {
        "adler32": "89d5efeb",
        "sha512": "deadbeef00002327c179e26be799b06cdaf48bf6efce56fb9d53f758c1b997099831ad05453fdb1ba65be7b35d0b4c5cebfc439efbdf83317ba0e38bf6f42570",
    }
    mock_os_path_getsize = mocker.patch("os.path.getsize")
    mock_os_path_getsize.return_value = 1234567890
    p = Unpacker(config, logger_mock)
    BUNDLE_OBJ = {
        "bundle_path": "/mnt/lfss/jade-lta/bundler_out/9a1cab0a395211eab1cbce3a3da73f88.zip",
        "uuid": "f74db80e-9661-40cc-9f01-8d087af23f56",
        "source": "NERSC",
        "dest": "WIPAC",
        "path": "/full/path/to/file",
        "files": [{"logical_name": "/full/path/to/file/in/data/warehouse.tar.bz2", }],
    }
    with patch("builtins.open", mock_open(read_data="data")) as metadata_mock:
        with pytest.raises(ValueError):
            await p._do_work_bundle(mocker.AsyncMock(), BUNDLE_OBJ)
        metadata_mock.assert_called_with(mocker.ANY)


@pytest.mark.asyncio
async def test_unpacker_do_work_bundle_path_remapping(config: TestConfig, mocker: MockerFixture, path_map_mock: MagicMock) -> None:
    """Test that _do_work_bundle does the work of preparing an archive."""
    logger_mock = mocker.MagicMock()
    lta_rc_mock = mocker.patch("rest_tools.client.RestClient.request", new_callable=AsyncMock)
    mock_zipfile_init = mocker.patch("zipfile.ZipFile.__init__")
    mock_zipfile_init.return_value = None
    mock_zipfile_infolist = mocker.patch("zipfile.ZipFile.infolist")
    mock_zipfile_infolist.return_value = [
        ObjectLiteral(
            filename="9a1cab0a395211eab1cbce3a3da73f88.metadata.json",
            file_size=0,
        ),
        ObjectLiteral(
            filename="PFFilt_PhysicsFiltering_Run00123231_Subrun00000000_00000002.tar.bz2",
            file_size=1234567890,
        ),
    ]
    mock_zipfile_extract = mocker.patch("zipfile.ZipFile.extract")
    mock_zipfile_extract.return_value = None
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
    edd_mock = mocker.patch("lta.unpacker.Unpacker._ensure_dest_directory")
    edd_mock.return_value = None
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
    mock_os_scandir = mocker.patch("os.scandir")
    mock_os_scandir.return_value.__enter__.return_value = []
    altfc_mock = mocker.patch("lta.unpacker.Unpacker._add_location_to_file_catalog", new_callable=AsyncMock)
    altfc_mock.return_value = False
    p = Unpacker(config, logger_mock)
    BUNDLE_OBJ = {
        "bundle_path": "/mnt/lfss/jade-lta/bundler_out/9a1cab0a395211eab1cbce3a3da73f88.zip",
        "uuid": "f74db80e-9661-40cc-9f01-8d087af23f56",
        "source": "NERSC",
        "dest": "WIPAC",
        "path": "/mnt/lfs7/IceCube/2013/filtered/PFFilt/1109",
        "files": [{"logical_name": "/mnt/lfs7/IceCube/2013/filtered/PFFilt/1109/PFFilt_PhysicsFiltering_Run00123231_Subrun00000000_00000002.tar.bz2", }],
    }
    with patch("builtins.open", mock_open(read_data="data")) as metadata_mock:
        await p._do_work_bundle(lta_rc_mock, BUNDLE_OBJ)
        metadata_mock.assert_called_with(mocker.ANY)
        mock_lta_checksums.assert_called_with("/data/exp/IceCube/2013/filtered/PFFilt/1109/PFFilt_PhysicsFiltering_Run00123231_Subrun00000000_00000002.tar.bz2")


def test_unpacker_delete_manifest_metadata_v3(config: TestConfig, mocker: MockerFixture, path_map_mock: MagicMock) -> None:
    """Test that _delete_manifest_metadata will delete metadata of either version."""
    logger_mock = mocker.MagicMock()
    p = Unpacker(config, logger_mock)
    mock_os_remove = mocker.patch("os.remove")
    mock_os_remove.side_effect = [NameError, None]
    p._delete_manifest_metadata("0869ea50-e437-443f-8cdb-31a350f88e57")
    mock_os_remove.assert_called_with("/tmp/lta/testing/unpacker/outbox/0869ea50-e437-443f-8cdb-31a350f88e57.metadata.ndjson")


def test_unpacker_delete_manifest_metadata_unknown(config: TestConfig, mocker: MockerFixture, path_map_mock: MagicMock) -> None:
    """Test that _delete_manifest_metadata will throw on an unknown version."""
    logger_mock = mocker.MagicMock()
    p = Unpacker(config, logger_mock)
    mock_os_remove = mocker.patch("os.remove")
    mock_os_remove.side_effect = [NameError, NameError]
    with pytest.raises(NameError):
        p._delete_manifest_metadata("0869ea50-e437-443f-8cdb-31a350f88e57")
    mock_os_remove.assert_called_with("/tmp/lta/testing/unpacker/outbox/0869ea50-e437-443f-8cdb-31a350f88e57.metadata.ndjson")


def test_unpacker_ensure_dest_directory(config: TestConfig, mocker: MockerFixture, path_map_mock: MagicMock) -> None:
    """Test that _ensure_dest_directory will attempt to ensure a directory exists."""
    logger_mock = mocker.MagicMock()
    p = Unpacker(config, logger_mock)
    mock_mkdir = mocker.patch("pathlib.Path.mkdir")
    p._ensure_dest_directory("/path/of/some/file/that/we/want/to/make/sure/directory/exists/MyFile_000123.tar.bz2")
    mock_mkdir.assert_called_with(parents=True, exist_ok=True)


def test_unpacker_read_manifest_metadata_for_v3(config: TestConfig, mocker: MockerFixture, path_map_mock: MagicMock) -> None:
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


def test_unpacker_read_manifest_metadata_unknown(config: TestConfig, mocker: MockerFixture, path_map_mock: MagicMock) -> None:
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


def test_unpacker_read_manifest_metadata_v2(config: TestConfig, mocker: MockerFixture, path_map_mock: MagicMock) -> None:
    """Test that _read_manifest_metadata_v2 will try to read metadata."""
    logger_mock = mocker.MagicMock()
    p = Unpacker(config, logger_mock)
    with patch("builtins.open", mock_open(read_data='{"some": "object"}')) as metadata_mock:
        assert p._read_manifest_metadata_v2("0869ea50-e437-443f-8cdb-31a350f88e57") == {"some": "object"}
    metadata_mock.assert_called_with(mocker.ANY)


@patch('builtins.open')
def test_unpacker_read_manifest_metadata_v2_no_throw(file_open_mock: MagicMock, config: TestConfig, mocker: MockerFixture, path_map_mock: MagicMock) -> None:
    """Test that _read_manifest_metadata_v2 will not throw when unable to read metadata."""
    logger_mock = mocker.MagicMock()
    file_open_mock.side_effect = NameError
    p = Unpacker(config, logger_mock)
    assert not p._read_manifest_metadata_v2("0869ea50-e437-443f-8cdb-31a350f88e57")


def test_unpacker_read_manifest_metadata_v3(config: TestConfig, mocker: MockerFixture, path_map_mock: MagicMock) -> None:
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
def test_unpacker_read_manifest_metadata_v3_no_throw(file_open_mock: MagicMock, config: TestConfig, mocker: MockerFixture, path_map_mock: MagicMock) -> None:
    """Test that _read_manifest_metadata_v3 will not throw when unable to read metadata."""
    logger_mock = mocker.MagicMock()
    file_open_mock.side_effect = NameError
    p = Unpacker(config, logger_mock)
    assert not p._read_manifest_metadata_v3("0869ea50-e437-443f-8cdb-31a350f88e57")


def test_unpacker_clean_outbox_directory_bail_early(config: TestConfig, mocker: MockerFixture, path_map_mock: MagicMock) -> None:
    """Test that _clean_outbox_directory will bail when configured not to clean."""
    logger_mock = mocker.MagicMock()
    config["CLEAN_OUTBOX"] = "FALSE"
    mock_os_scandir = mocker.patch("os.scandir")
    p = Unpacker(config, logger_mock)
    p._clean_outbox_directory()
    mock_os_scandir.assert_not_called()


def test_unpacker_clean_outbox_directory_empty(config: TestConfig, mocker: MockerFixture, path_map_mock: MagicMock) -> None:
    """Test that _clean_outbox_directory will bail when configured not to clean."""
    logger_mock = mocker.MagicMock()
    mock_os_scandir = mocker.patch("os.scandir")
    mock_os_scandir.return_value.__enter__.return_value = []
    mock_os_remove = mocker.patch("os.remove")
    mock_shutil_rmtree = mocker.patch("shutil.rmtree")
    p = Unpacker(config, logger_mock)
    p._clean_outbox_directory()
    mock_os_remove.assert_not_called()
    mock_shutil_rmtree.assert_not_called()


def test_unpacker_clean_outbox_directory_file(config: TestConfig, mocker: MockerFixture, path_map_mock: MagicMock) -> None:
    """Test that _clean_outbox_directory will bail when configured not to clean."""
    logger_mock = mocker.MagicMock()
    mock_os_scandir = mocker.patch("os.scandir")
    direntry = mocker.MagicMock()
    direntry.is_file.return_value = True
    mock_os_scandir.return_value.__enter__.return_value = [direntry]
    mock_os_remove = mocker.patch("os.remove")
    mock_shutil_rmtree = mocker.patch("shutil.rmtree")
    p = Unpacker(config, logger_mock)
    p._clean_outbox_directory()
    mock_os_remove.assert_called()
    mock_shutil_rmtree.assert_not_called()


def test_unpacker_clean_outbox_directory_directory(config: TestConfig, mocker: MockerFixture, path_map_mock: MagicMock) -> None:
    """Test that _clean_outbox_directory will bail when configured not to clean."""
    logger_mock = mocker.MagicMock()
    mock_os_scandir = mocker.patch("os.scandir")
    direntry = mocker.MagicMock()
    direntry.is_file.return_value = False
    direntry.is_dir.return_value = True
    mock_os_scandir.return_value.__enter__.return_value = [direntry]
    mock_os_remove = mocker.patch("os.remove")
    mock_shutil_rmtree = mocker.patch("shutil.rmtree")
    p = Unpacker(config, logger_mock)
    p._clean_outbox_directory()
    mock_os_remove.assert_not_called()
    mock_shutil_rmtree.assert_called()


def test_unpacker_clean_outbox_directory_unknown(config: TestConfig, mocker: MockerFixture, path_map_mock: MagicMock) -> None:
    """Test that _clean_outbox_directory will bail when configured not to clean."""
    logger_mock = mocker.MagicMock()
    mock_os_scandir = mocker.patch("os.scandir")
    direntry = mocker.MagicMock()
    direntry.is_file.return_value = False
    direntry.is_dir.return_value = False
    mock_os_scandir.return_value.__enter__.return_value = [direntry]
    mock_os_remove = mocker.patch("os.remove")
    mock_shutil_rmtree = mocker.patch("shutil.rmtree")
    p = Unpacker(config, logger_mock)
    p._clean_outbox_directory()
    mock_os_remove.assert_not_called()
    mock_shutil_rmtree.assert_not_called()
