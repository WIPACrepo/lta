# test_bundler.py
"""Unit tests for lta/bundler.py."""

import os
from typing import Dict
from unittest.mock import AsyncMock, call, mock_open, patch
from uuid import uuid1

import pytest
from pytest import MonkeyPatch
from pytest_mock import MockerFixture
from tornado.web import HTTPError

from lta.bundler import Bundler, main_sync

TestConfig = Dict[str, str]


@pytest.fixture(autouse=True)
def clear_prometheus() -> None:
    """Clear Prometheus for each test in this module."""
    from prometheus_client import REGISTRY
    collectors = list(REGISTRY._collector_to_names.keys())
    for collector in collectors:
        REGISTRY.unregister(collector)
    from prometheus_client import gc_collector, platform_collector, process_collector
    process_collector.ProcessCollector()
    platform_collector.PlatformCollector()
    gc_collector.GCCollector()


@pytest.fixture
def config() -> TestConfig:
    """Supply a stock Bundler component configuration."""
    return {
        "BUNDLER_OUTBOX_PATH": "/tmp/lta/testing/bundler/outbox",
        "BUNDLER_WORKBOX_PATH": "/tmp/lta/testing/bundler/workbox",
        "CLIENT_ID": "long-term-archive",
        "CLIENT_SECRET": "hunter2",  # http://bash.org/?244321
        "COMPONENT_NAME": "testing-bundler",
        "DEST_SITE": "NERSC",
        "FILE_CATALOG_CLIENT_ID": "file-catalog-client-id",
        "FILE_CATALOG_CLIENT_SECRET": "file-catalog-client-secret",
        "FILE_CATALOG_REST_URL": "localhost:12346",
        "INPUT_STATUS": "specified",
        "LOG_LEVEL": "DEBUG",
        "LTA_AUTH_OPENID_URL": "localhost:12345",
        "LTA_REST_URL": "localhost:12347",
        "MYSQL_DB": "testing-db",
        "MYSQL_HOST": "just-testing.icecube.wisc.edu",
        "MYSQL_PASSWORD": "hunter2",  # http://bash.org/?244321
        "MYSQL_PORT": "23306",
        "MYSQL_USER": "jade-user",
        "OUTPUT_STATUS": "created",
        "PROMETHEUS_METRICS_PORT": "8080",
        "RUN_ONCE_AND_DIE": "False",
        "RUN_UNTIL_NO_WORK": "False",
        "SOURCE_SITE": "WIPAC",
        "WORK_RETRIES": "3",
        "WORK_SLEEP_DURATION_SECONDS": "60",
        "WORK_TIMEOUT_SECONDS": "30",
    }


def test_constructor_config_missing_values(mocker: MockerFixture) -> None:
    """Fail with a ValueError if the configuration object is missing required configuration variables."""
    config = {
        "PAN_GALACTIC_GARGLE_BLASTER": "Yummy"
    }
    logger_mock = mocker.MagicMock()
    with pytest.raises(ValueError):
        Bundler(config, logger_mock)


def test_constructor_config_poison_values(config: TestConfig, mocker: MockerFixture) -> None:
    """Fail with a ValueError if the configuration object is missing required configuration variables."""
    bundler_config = config.copy()
    del bundler_config["LTA_REST_URL"]
    logger_mock = mocker.MagicMock()
    with pytest.raises(ValueError):
        Bundler(bundler_config, logger_mock)


def test_constructor_config(config: TestConfig, mocker: MockerFixture) -> None:
    """Test that a Bundler can be constructed with a configuration object and a logging object."""
    logger_mock = mocker.MagicMock()
    p = Bundler(config, logger_mock)
    assert p.lta_auth_openid_url == "localhost:12345"
    assert p.lta_rest_url == "localhost:12347"
    assert p.name == "testing-bundler"
    assert p.work_sleep_duration_seconds == 60
    assert p.logger == logger_mock


def test_constructor_config_sleep_type_int(config: TestConfig, mocker: MockerFixture) -> None:
    """Ensure that sleep seconds can also be provided as an integer."""
    logger_mock = mocker.MagicMock()
    p = Bundler(config, logger_mock)
    assert p.lta_auth_openid_url == "localhost:12345"
    assert p.lta_rest_url == "localhost:12347"
    assert p.name == "testing-bundler"
    assert p.work_sleep_duration_seconds == 60
    assert p.logger == logger_mock


def test_constructor_state(config: TestConfig, mocker: MockerFixture) -> None:
    """Verify that the Bundler has a reasonable state when it is first constructed."""
    logger_mock = mocker.MagicMock()
    p = Bundler(config, logger_mock)
    assert p.last_work_begin_timestamp is p.last_work_end_timestamp


def test_do_status(config: TestConfig, mocker: MockerFixture) -> None:
    """Verify that the Bundler has no additional state to offer."""
    logger_mock = mocker.MagicMock()
    p = Bundler(config, logger_mock)
    assert p._do_status() == {}


@pytest.mark.asyncio
async def test_script_main_sync(config: TestConfig,
                                mocker: MockerFixture,
                                monkeypatch: MonkeyPatch) -> None:
    """
    Verify Bundler component behavior when run as a script.

    Test to make sure running the Bundler as a script does the setup work
    that we expect and then launches the bundler service.
    """
    for key in config.keys():
        monkeypatch.setenv(key, config[key])
    mock_run = mocker.patch("asyncio.run")
    mock_main = mocker.patch("lta.bundler.main")
    mock_shs = mocker.patch("lta.bundler.start_http_server")
    main_sync()
    mock_shs.assert_called()
    mock_main.assert_called()
    mock_run.assert_called()
    await mock_run.call_args.args[0]


@pytest.mark.asyncio
async def test_bundler_logs_configuration(mocker: MockerFixture) -> None:
    """Test to make sure the Bundler logs its configuration."""
    logger_mock = mocker.MagicMock()
    bundler_config = {
        "BUNDLER_OUTBOX_PATH": "logme/tmp/lta/testing/bundler/outbox",
        "BUNDLER_WORKBOX_PATH": "logme/tmp/lta/testing/bundler/workbox",
        "CLIENT_ID": "long-term-archive",
        "CLIENT_SECRET": "hunter2",  # http://bash.org/?244321
        "COMPONENT_NAME": "logme-testing-bundler",
        "DEST_SITE": "NERSC",
        "FILE_CATALOG_CLIENT_ID": "file-catalog-client-id",
        "FILE_CATALOG_CLIENT_SECRET": "file-catalog-client-secret",
        "FILE_CATALOG_REST_URL": "http://kVj74wBA1AMTDV8zccn67pGuWJqHZzD7iJQHrUJKA.com/",
        "INPUT_STATUS": "specified",
        "LOG_LEVEL": "DEBUG",
        "LTA_AUTH_OPENID_URL": "localhost:12345",
        "LTA_REST_URL": "logme-http://RmMNHdPhHpH2ZxfaFAC9d2jiIbf5pZiHDqy43rFLQiM.com/",
        "MYSQL_DB": "logme-testing-db",
        "MYSQL_HOST": "logme-just-testing.icecube.wisc.edu",
        "MYSQL_PASSWORD": "logme-hunter2",
        "MYSQL_PORT": "23306",
        "MYSQL_USER": "logme-jade-user",
        "OUTPUT_STATUS": "created",
        "PROMETHEUS_METRICS_PORT": "8080",
        "RUN_ONCE_AND_DIE": "False",
        "RUN_UNTIL_NO_WORK": "False",
        "SOURCE_SITE": "WIPAC",
        "WORK_RETRIES": "5",
        "WORK_SLEEP_DURATION_SECONDS": "70",
        "WORK_TIMEOUT_SECONDS": "90",
    }
    Bundler(bundler_config, logger_mock)
    EXPECTED_LOGGER_CALLS = [
        call("bundler 'logme-testing-bundler' is configured:"),
        call('BUNDLER_OUTBOX_PATH = logme/tmp/lta/testing/bundler/outbox'),
        call('BUNDLER_WORKBOX_PATH = logme/tmp/lta/testing/bundler/workbox'),
        call('CLIENT_ID = long-term-archive'),
        call('CLIENT_SECRET = [秘密]'),
        call('COMPONENT_NAME = logme-testing-bundler'),
        call('DEST_SITE = NERSC'),
        call('FILE_CATALOG_CLIENT_ID = file-catalog-client-id'),
        call('FILE_CATALOG_CLIENT_SECRET = [秘密]'),
        call('FILE_CATALOG_REST_URL = http://kVj74wBA1AMTDV8zccn67pGuWJqHZzD7iJQHrUJKA.com/'),
        call('INPUT_STATUS = specified'),
        call('LOG_LEVEL = DEBUG'),
        call('LTA_AUTH_OPENID_URL = localhost:12345'),
        call('LTA_REST_URL = logme-http://RmMNHdPhHpH2ZxfaFAC9d2jiIbf5pZiHDqy43rFLQiM.com/'),
        call('MYSQL_DB = logme-testing-db'),
        call('MYSQL_HOST = logme-just-testing.icecube.wisc.edu'),
        call('MYSQL_PASSWORD = logme-hunter2'),
        call('MYSQL_PORT = 23306'),
        call('MYSQL_USER = logme-jade-user'),
        call('OUTPUT_STATUS = created'),
        call('PROMETHEUS_METRICS_PORT = 8080'),
        call('RUN_ONCE_AND_DIE = False'),
        call('RUN_UNTIL_NO_WORK = False'),
        call('SOURCE_SITE = WIPAC'),
        call('WORK_RETRIES = 5'),
        call('WORK_SLEEP_DURATION_SECONDS = 70'),
        call('WORK_TIMEOUT_SECONDS = 90'),
    ]
    logger_mock.info.assert_has_calls(EXPECTED_LOGGER_CALLS)


@pytest.mark.asyncio
async def test_bundler_run(config: TestConfig, mocker: MockerFixture) -> None:
    """Test the Bundler does the work the bundler should do."""
    logger_mock = mocker.MagicMock()
    p = Bundler(config, logger_mock)
    p._do_work = AsyncMock()  # type: ignore[method-assign]
    await p.run()
    p._do_work.assert_called()


@pytest.mark.asyncio
async def test_bundler_run_exception(config: TestConfig, mocker: MockerFixture) -> None:
    """Test an error doesn't kill the Bundler."""
    logger_mock = mocker.MagicMock()
    p = Bundler(config, logger_mock)
    p.last_work_end_timestamp = ""
    p._do_work = AsyncMock()  # type: ignore[method-assign]
    p._do_work.side_effect = [Exception("bad thing happen!")]
    await p.run()
    p._do_work.assert_called()
    assert p.last_work_end_timestamp


@pytest.mark.asyncio
async def test_bundler_do_work_pop_exception(config: TestConfig, mocker: MockerFixture) -> None:
    """Test that _do_work raises when the RestClient can't pop."""
    logger_mock = mocker.MagicMock()
    lta_rc_mock = AsyncMock()
    lta_rc_mock.request = AsyncMock()
    lta_rc_mock.request.side_effect = HTTPError(500, "LTA DB on fire. Again.")
    p = Bundler(config, logger_mock)
    with pytest.raises(HTTPError):
        await p._do_work(lta_rc_mock)
    lta_rc_mock.request.assert_called_with("POST", '/Bundles/actions/pop?source=WIPAC&dest=NERSC&status=specified', mocker.ANY)


@pytest.mark.asyncio
async def test_bundler_do_work_no_results(config: TestConfig, mocker: MockerFixture) -> None:
    """Test that _do_work goes on vacation when the LTA DB has no work."""
    logger_mock = mocker.MagicMock()
    lta_rc_mock = mocker.patch("rest_tools.client.RestClient.request", new_callable=AsyncMock)
    claim_mock = mocker.patch("lta.bundler.Bundler._do_work_claim", new_callable=AsyncMock)
    claim_mock.return_value = False
    p = Bundler(config, logger_mock)
    await p._do_work(lta_rc_mock)


@pytest.mark.asyncio
async def test_bundler_do_work_claim_no_results(config: TestConfig, mocker: MockerFixture) -> None:
    """Test that _do_work_claim returns False when the LTA DB has no work."""
    logger_mock = mocker.MagicMock()
    lta_rc_mock = AsyncMock()
    lta_rc_mock.request = AsyncMock()
    lta_rc_mock.request.return_value = {
        "bundle": None
    }
    p = Bundler(config, logger_mock)
    assert not await p._do_work_claim(lta_rc_mock)
    lta_rc_mock.request.assert_called_with("POST", '/Bundles/actions/pop?source=WIPAC&dest=NERSC&status=specified', mocker.ANY)


@pytest.mark.asyncio
async def test_bundler_do_work_yes_results(config: TestConfig, mocker: MockerFixture) -> None:
    """Test that _do_work_claim processes each Bundle that it gets from the LTA DB."""
    BUNDLE_OBJ = {
        "uuid": "f74db80e-9661-40cc-9f01-8d087af23f56"
    }
    logger_mock = mocker.MagicMock()
    lta_rc_mock = AsyncMock()
    lta_rc_mock.request = AsyncMock()
    lta_rc_mock.request.return_value = {
        "bundle": BUNDLE_OBJ
    }
    dwb_mock = mocker.patch("lta.bundler.Bundler._do_work_bundle", new_callable=AsyncMock)
    p = Bundler(config, logger_mock)
    assert await p._do_work_claim(lta_rc_mock)
    lta_rc_mock.request.assert_called_with("POST", '/Bundles/actions/pop?source=WIPAC&dest=NERSC&status=specified', mocker.ANY)
    dwb_mock.assert_called_with(mocker.ANY, mocker.ANY, BUNDLE_OBJ)


@pytest.mark.asyncio
async def test_bundler_do_work_dest_results(config: TestConfig, mocker: MockerFixture) -> None:
    """Test that _do_work_bundle does the work of preparing an archive."""
    BUNDLE_UUID = "f74db80e-9661-40cc-9f01-8d087af23f56"
    FILE_CATALOG_UUID = "a8777703-6e9e-41a2-8776-c924a86d5f0f"
    logger_mock = mocker.MagicMock()
    fc_rc_mock = mocker.patch("rest_tools.client.RestClient.request", new_callable=AsyncMock)
    fc_rc_mock.request.side_effect = [
        {
            "uuid": FILE_CATALOG_UUID,
            "logical_name": "/path/to/some/data/warehouse/file.i3"
        },
        {
            "uuid": FILE_CATALOG_UUID,
            "logical_name": "/path/to/some/data/warehouse/file.i3"
        },
    ]
    lta_rc_mock = mocker.patch("rest_tools.client.RestClient.request", new_callable=AsyncMock)
    lta_rc_mock.request.side_effect = [
        {
            "results": [
                {
                    "uuid": uuid1().hex,
                    "bundle_uuid": BUNDLE_UUID,
                    "file_catalog_uuid": FILE_CATALOG_UUID,
                }
            ]
        },
        {
            "results": []
        },
        {
            "results": [
                {
                    "uuid": uuid1().hex,
                    "bundle_uuid": BUNDLE_UUID,
                    "file_catalog_uuid": FILE_CATALOG_UUID,
                }
            ]
        },
        {
            "results": []
        },
        {
            "uuid": BUNDLE_UUID,
            "source": "WIPAC",
            "dest": "NERSC",
            "file_count": 1,
            "path": "/path/to/some/data",
            "status": "created",
            "reason": "",
            "bundle_path": "/path/to/bundler/outbox",
            "size": 123456,
            "checksum": "abcdef",
            "verified": False,
            "claimed": False,
        },
    ]
    mock_zipfile_init = mocker.patch("zipfile.ZipFile.__init__")
    mock_zipfile_init.return_value = None
    mock_zipfile_write = mocker.patch("zipfile.ZipFile.write")
    mock_zipfile_write.return_value = None
    mock_shutil_move = mocker.patch("shutil.move")
    mock_shutil_move.return_value = None
    mock_lta_checksums = mocker.patch("lta.bundler.lta_checksums")
    mock_lta_checksums.return_value = {
        "adler32": "89d5efeb",
        "sha512": "c919210281b72327c179e26be799b06cdaf48bf6efce56fb9d53f758c1b997099831ad05453fdb1ba65be7b35d0b4c5cebfc439efbdf83317ba0e38bf6f42570",
    }
    mock_os_path_getsize = mocker.patch("os.path.getsize")
    mock_os_path_getsize.return_value = 1048900
    mock_os_remove = mocker.patch("os.remove")
    mock_os_remove.return_value = None
    p = Bundler(config, logger_mock)
    BUNDLE_OBJ = {
        "uuid": BUNDLE_UUID,
        "source": "WIPAC",
        "dest": "NERSC",
        "file_count": 1,
        "path": "/path/to/some/data",
    }
    with patch("builtins.open", mock_open(read_data="data")) as metadata_mock:
        await p._do_work_bundle(fc_rc_mock, lta_rc_mock, BUNDLE_OBJ)
        metadata_mock.assert_called_with(mocker.ANY, mode="w")
    mock_zipfile_write.assert_called_with('/path/to/some/data/warehouse/file.i3', 'warehouse/file.i3')


@pytest.mark.asyncio
async def test_bundler_do_work_bundle_once_and_die(config: TestConfig, mocker: MockerFixture) -> None:
    """Test that _do_work goes on vacation when the LTA DB has no work."""
    once = config.copy()
    once["RUN_ONCE_AND_DIE"] = "True"
    logger_mock = mocker.MagicMock()
    lta_rc_mock = mocker.patch("rest_tools.client.RestClient.request", new_callable=AsyncMock)
    claim_mock = mocker.patch("lta.bundler.Bundler._do_work_claim", new_callable=AsyncMock)
    claim_mock.return_value = False
    sys_exit_mock = mocker.patch("sys.exit")
    p = Bundler(once, logger_mock)
    await p._do_work(lta_rc_mock)
    sys_exit_mock.assert_called()


def test_relpath() -> None:
    """Ensure os.path.relpath gives us the answers we expect."""
    assert os.path.relpath('/data/exp/IceCube/2020/filtered/PFFilt/1028/PFFilt_PhysicsFiltering_Run00134642_Subrun00000000_00000000.tar.bz2', '/data/exp/IceCube/2020/filtered/PFFilt/1028') == "PFFilt_PhysicsFiltering_Run00134642_Subrun00000000_00000000.tar.bz2"
    assert os.path.relpath('/data/exp/IceCube/2013/internal-system/hit-spooling/0403/HS_SNALERT_20130403_061113_ichub01.tar.gz', '/data/exp/IceCube/2013/internal-system/hit-spooling') == "0403/HS_SNALERT_20130403_061113_ichub01.tar.gz"
    assert os.path.relpath('/data/exp/IceCube/2013/internal-system/hit-spooling/1116/HS_SNALERT_20131116_065747_ichub50.tar.gz', '/data/exp/IceCube/2013/internal-system/hit-spooling') == "1116/HS_SNALERT_20131116_065747_ichub50.tar.gz"
