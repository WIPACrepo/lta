# test_locator.py
"""Unit tests for lta/locator.py."""

# fmt:off

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

from math import floor
from secrets import token_hex
from typing import Any, Dict, List, Union
from unittest.mock import AsyncMock, call, MagicMock
from uuid import uuid1

import pytest
from pytest import MonkeyPatch
from pytest_mock import MockerFixture
from tornado.web import HTTPError

from lta.locator import as_lta_record, main_sync, Locator

TestConfig = Dict[str, str]


@pytest.fixture
def config() -> TestConfig:
    """Supply a stock Locator component configuration."""
    return {
        "CLIENT_ID": "long-term-archive",
        "CLIENT_SECRET": "hunter2",  # http://bash.org/?244321
        "COMPONENT_NAME": "testing-locator",
        "DEST_SITE": "WIPAC",
        "FILE_CATALOG_CLIENT_ID": "file-catalog-client-id",
        "FILE_CATALOG_CLIENT_SECRET": "file-catalog-client-secret",
        "FILE_CATALOG_PAGE_SIZE": "1000",
        "FILE_CATALOG_REST_URL": "localhost:12346",
        "INPUT_STATUS": "ethereal",
        "LOG_LEVEL": "DEBUG",
        "LTA_AUTH_OPENID_URL": "localhost:12345",
        "LTA_REST_URL": "localhost:12347",
        "LTA_SITE_CONFIG": "examples/site.json",
        "OUTPUT_STATUS": "located",
        "PROMETHEUS_METRICS_PORT": "8080",
        "RUN_ONCE_AND_DIE": "False",
        "RUN_UNTIL_NO_WORK": "False",
        "SOURCE_SITE": "NERSC",
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
        Locator(config, logger_mock)


def test_constructor_config_poison_values(config: TestConfig, mocker: MockerFixture) -> None:
    """Fail with a ValueError if the configuration object is missing required configuration variables."""
    locator_config = config.copy()
    del locator_config["LTA_REST_URL"]
    logger_mock = mocker.MagicMock()
    with pytest.raises(ValueError):
        Locator(locator_config, logger_mock)


def test_constructor_config(config: TestConfig, mocker: MockerFixture) -> None:
    """Test that a Locator can be constructed with a configuration object and a logging object."""
    logger_mock = mocker.MagicMock()
    p = Locator(config, logger_mock)
    assert p.file_catalog_rest_url == "localhost:12346"
    assert p.lta_rest_url == "localhost:12347"
    assert p.name == "testing-locator"
    assert p.work_sleep_duration_seconds == 60
    assert p.logger == logger_mock


def test_constructor_config_sleep_type_int(config: TestConfig, mocker: MockerFixture) -> None:
    """Ensure that sleep seconds can also be provided as an integer."""
    logger_mock = mocker.MagicMock()
    p = Locator(config, logger_mock)
    assert p.file_catalog_rest_url == "localhost:12346"
    assert p.lta_rest_url == "localhost:12347"
    assert p.name == "testing-locator"
    assert p.work_sleep_duration_seconds == 60
    assert p.logger == logger_mock


def test_do_status(config: TestConfig, mocker: MockerFixture) -> None:
    """Verify that the Locator has no additional state to offer."""
    logger_mock = mocker.MagicMock()
    p = Locator(config, logger_mock)
    assert p._do_status() == {}


@pytest.mark.asyncio
async def test_script_main_sync(config: TestConfig, mocker: MockerFixture, monkeypatch: MonkeyPatch) -> None:
    """
    Verify Locator component behavior when run as a script.

    Test to make sure running the Locator as a script does the setup work
    that we expect and then launches the locator service.
    """
    for key in config.keys():
        monkeypatch.setenv(key, config[key])
    mock_run = mocker.patch("asyncio.run")
    mock_main = mocker.patch("lta.locator.main")
    mock_shs = mocker.patch("lta.locator.start_http_server")
    main_sync()
    mock_shs.assert_called()
    mock_main.assert_called()
    mock_run.assert_called()
    await mock_run.call_args.args[0]


@pytest.mark.asyncio
async def test_locator_logs_configuration(mocker: MockerFixture) -> None:
    """Test to make sure the Locator logs its configuration."""
    logger_mock = mocker.MagicMock()
    locator_config = {
        "CLIENT_ID": "long-term-archive",
        "CLIENT_SECRET": "hunter2",  # http://bash.org/?244321
        "COMPONENT_NAME": "logme-testing-locator",
        "DEST_SITE": "WIPAC",
        "FILE_CATALOG_CLIENT_ID": "file-catalog-client-id",
        "FILE_CATALOG_CLIENT_SECRET": "file-catalog-client-secret",
        "FILE_CATALOG_PAGE_SIZE": "1000",
        "FILE_CATALOG_REST_URL": "logme-http://kVj74wBA1AMTDV8zccn67pGuWJqHZzD7iJQHrUJKA.com/",
        "INPUT_STATUS": "ethereal",
        "LOG_LEVEL": "DEBUG",
        "LTA_AUTH_OPENID_URL": "localhost:12345",
        "LTA_REST_URL": "logme-http://RmMNHdPhHpH2ZxfaFAC9d2jiIbf5pZiHDqy43rFLQiM.com/",
        "LTA_SITE_CONFIG": "examples/site.json",
        "OUTPUT_STATUS": "located",
        "PROMETHEUS_METRICS_PORT": "8080",
        "RUN_ONCE_AND_DIE": "False",
        "RUN_UNTIL_NO_WORK": "False",
        "SOURCE_SITE": "NERSC",
        "WORK_RETRIES": "5",
        "WORK_SLEEP_DURATION_SECONDS": "70",
        "WORK_TIMEOUT_SECONDS": "90",
    }
    Locator(locator_config, logger_mock)
    EXPECTED_LOGGER_CALLS = [
        call("locator 'logme-testing-locator' is configured:"),
        call('CLIENT_ID = long-term-archive'),
        call('CLIENT_SECRET = [秘密]'),
        call('COMPONENT_NAME = logme-testing-locator'),
        call('DEST_SITE = WIPAC'),
        call('FILE_CATALOG_CLIENT_ID = file-catalog-client-id'),
        call('FILE_CATALOG_CLIENT_SECRET = [秘密]'),
        call('FILE_CATALOG_PAGE_SIZE = 1000'),
        call('FILE_CATALOG_REST_URL = logme-http://kVj74wBA1AMTDV8zccn67pGuWJqHZzD7iJQHrUJKA.com/'),
        call('INPUT_STATUS = ethereal'),
        call('LOG_LEVEL = DEBUG'),
        call('LTA_AUTH_OPENID_URL = localhost:12345'),
        call('LTA_REST_URL = logme-http://RmMNHdPhHpH2ZxfaFAC9d2jiIbf5pZiHDqy43rFLQiM.com/'),
        call('LTA_SITE_CONFIG = examples/site.json'),
        call('OUTPUT_STATUS = located'),
        call('PROMETHEUS_METRICS_PORT = 8080'),
        call('RUN_ONCE_AND_DIE = False'),
        call('RUN_UNTIL_NO_WORK = False'),
        call('SOURCE_SITE = NERSC'),
        call('WORK_RETRIES = 5'),
        call('WORK_SLEEP_DURATION_SECONDS = 70'),
        call('WORK_TIMEOUT_SECONDS = 90')
    ]
    logger_mock.info.assert_has_calls(EXPECTED_LOGGER_CALLS)


@pytest.mark.asyncio
async def test_locator_run(config: TestConfig, mocker: MockerFixture) -> None:
    """Test the Locator does the work the locator should do."""
    logger_mock = mocker.MagicMock()
    p = Locator(config, logger_mock)
    p._do_work = AsyncMock()  # type: ignore[method-assign]
    await p.run()
    p._do_work.assert_called()


@pytest.mark.asyncio
async def test_locator_run_exception(config: TestConfig, mocker: MockerFixture) -> None:
    """Test an error doesn't kill the Locator."""
    logger_mock = mocker.MagicMock()
    p = Locator(config, logger_mock)
    p._do_work = AsyncMock()  # type: ignore[method-assign]
    p._do_work.side_effect = [Exception("bad thing happen!")]
    await p.run()
    p._do_work.assert_called()


@pytest.mark.asyncio
async def test_locator_do_work_pop_exception(config: TestConfig, mocker: MockerFixture) -> None:
    """Test that _do_work raises when the RestClient can't pop."""
    logger_mock = mocker.MagicMock()
    lta_rc_mock = AsyncMock()
    lta_rc_mock.request = AsyncMock()
    lta_rc_mock.request.side_effect = HTTPError(500, "LTA DB on fire. Again.")
    p = Locator(config, logger_mock)
    with pytest.raises(HTTPError):
        await p._do_work(lta_rc_mock)
    lta_rc_mock.request.assert_called_with("POST", '/TransferRequests/actions/pop?source=NERSC&dest=WIPAC', {'claimant': f'{p.name}-{p.instance_uuid}'})


@pytest.mark.asyncio
async def test_locator_do_work_no_results(config: TestConfig, mocker: MockerFixture) -> None:
    """Test that _do_work goes on vacation when the LTA DB has no work."""
    logger_mock = mocker.MagicMock()
    dwc_mock = mocker.patch("lta.locator.Locator._do_work_claim", new_callable=AsyncMock)
    dwc_mock.return_value = False
    p = Locator(config, logger_mock)
    await p._do_work(AsyncMock())
    dwc_mock.assert_called()


@pytest.mark.asyncio
async def test_locator_do_work_yes_results(config: TestConfig, mocker: MockerFixture) -> None:
    """Test that _do_work keeps working until the LTA DB has no work."""
    logger_mock = mocker.MagicMock()
    dwc_mock = mocker.patch("lta.locator.Locator._do_work_claim", new_callable=AsyncMock)
    dwc_mock.side_effect = [True, True, False]
    p = Locator(config, logger_mock)
    await p._do_work(AsyncMock())
    dwc_mock.assert_called()


@pytest.mark.asyncio
async def test_locator_do_work_claim_no_result(config: TestConfig, mocker: MockerFixture) -> None:
    """Test that _do_work_claim does not work when the LTA DB has no work."""
    logger_mock = mocker.MagicMock()
    lta_rc_mock = AsyncMock()
    lta_rc_mock.request = AsyncMock()
    lta_rc_mock.request.return_value = {
        "transfer_request": None
    }
    dwtr_mock = mocker.patch("lta.locator.Locator._do_work_transfer_request", new_callable=AsyncMock)
    p = Locator(config, logger_mock)
    await p._do_work_claim(lta_rc_mock)
    lta_rc_mock.request.assert_called_with("POST", '/TransferRequests/actions/pop?source=NERSC&dest=WIPAC', {'claimant': f'{p.name}-{p.instance_uuid}'})
    dwtr_mock.assert_not_called()


@pytest.mark.asyncio
async def test_locator_do_work_claim_yes_result(config: TestConfig, mocker: MockerFixture) -> None:
    """Test that _do_work_claim processes the TransferRequest it gets from the LTA DB."""
    logger_mock = mocker.MagicMock()
    lta_rc_mock = AsyncMock()
    lta_rc_mock.request = AsyncMock()
    lta_rc_mock.request.return_value = {
        "transfer_request": {
            "one": 1,
        },
    }
    dwtr_mock = mocker.patch("lta.locator.Locator._do_work_transfer_request", new_callable=AsyncMock)
    p = Locator(config, logger_mock)
    await p._do_work_claim(lta_rc_mock)
    lta_rc_mock.request.assert_called_with("POST", '/TransferRequests/actions/pop?source=NERSC&dest=WIPAC', {'claimant': f'{p.name}-{p.instance_uuid}'})
    dwtr_mock.assert_called_with(mocker.ANY, {"one": 1})


@pytest.mark.asyncio
async def test_locator_do_work_claim_exception_when_processing(config: TestConfig, mocker: MockerFixture) -> None:
    """Test that _do_work_claim processes the TransferRequest it gets from the LTA DB."""
    logger_mock = mocker.MagicMock()
    lta_rc_mock = AsyncMock()
    lta_rc_mock.request = AsyncMock()
    lta_rc_mock.request.return_value = {
        "transfer_request": {
            "one": 1,
        },
    }
    dwtr_mock = mocker.patch("lta.locator.Locator._do_work_transfer_request", new_callable=AsyncMock)
    dwtr_mock.side_effect = Exception("lta db crashed like launchpad mcquack")
    qtr_mock = mocker.patch("lta.locator.Locator._quarantine_transfer_request", new_callable=AsyncMock)
    p = Locator(config, logger_mock)
    with pytest.raises(Exception):
        await p._do_work_claim(lta_rc_mock)
    lta_rc_mock.request.assert_called_with("POST", '/TransferRequests/actions/pop?source=NERSC&dest=WIPAC', {'claimant': f'{p.name}-{p.instance_uuid}'})
    dwtr_mock.assert_called_with(mocker.ANY, {"one": 1})
    qtr_mock.assert_called_with(mocker.ANY, {"one": 1}, "lta db crashed like launchpad mcquack")


@pytest.mark.asyncio
async def test_locator_do_work_transfer_request_fc_exception(config: TestConfig, mocker: MockerFixture) -> None:
    """Test that _do_work_transfer_request raises an exception if the File Catalog has an error."""
    logger_mock = mocker.MagicMock()
    p = Locator(config, logger_mock)
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
    fc_rc_mock.assert_called()
    assert fc_rc_mock.call_args[0][0] == "GET"
    assert fc_rc_mock.call_args[0][1].startswith('/api/files?query={"locations.archive": {"$eq": true}, "locations.site": {"$eq": "wipac"}, "locations.path": {"$regex": "^/tmp/this/is/just/a/test"}}')


@pytest.mark.asyncio
async def test_locator_do_work_transfer_request_fc_no_results(config: TestConfig, mocker: MockerFixture) -> None:
    """Test that _do_work_transfer_request raises an exception when the LTA DB refuses to create an empty list."""
    QUARANTINE = {'original_status': 'ethereal', 'status': 'quarantined', 'reason': mocker.ANY, 'work_priority_timestamp': mocker.ANY}
    logger_mock = mocker.MagicMock()
    p = Locator(config, logger_mock)
    lta_rc_mock = mocker.MagicMock()
    lta_rc_mock.request = AsyncMock()
    lta_rc_mock.request.return_value = {}
    tr_uuid = uuid1().hex
    tr = {
        "uuid": tr_uuid,
        "status": "ethereal",
        "source": "wipac",
        "dest": "nersc",
        "path": "/tmp/this/is/just/a/test",
    }
    fc_rc_mock = mocker.patch("rest_tools.client.RestClient.request", new_callable=AsyncMock)
    fc_rc_mock.return_value = {
        "files": []
    }
    await p._do_work_transfer_request(lta_rc_mock, tr)
    fc_rc_mock.assert_called()
    assert fc_rc_mock.call_args[0][0] == "GET"
    assert fc_rc_mock.call_args[0][1].startswith('/api/files?query={"locations.archive": {"$eq": true}, "locations.site": {"$eq": "wipac"}, "locations.path": {"$regex": "^/tmp/this/is/just/a/test"}}')
    lta_rc_mock.request.assert_called_with("PATCH", f'/TransferRequests/{tr_uuid}', QUARANTINE)


@pytest.mark.asyncio
async def test_locator_do_work_transfer_request_fc_yes_results(config: TestConfig, mocker: MockerFixture) -> None:
    """Test that _do_work_transfer_request processes each file it gets back from the File Catalog."""
    logger_mock = mocker.MagicMock()
    lta_rc_mock = mocker.MagicMock()
    lta_rc_mock.request = AsyncMock()
    lta_rc_mock.request.return_value = {}
    cb_mock = mocker.patch("lta.locator.Locator._create_bundle", new_callable=AsyncMock)
    tr_uuid = uuid1().hex
    tr = {
        "uuid": tr_uuid,
        "source": "nersc",
        "dest": "wipac",
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
                    "path": "/path/at/nersc/to/8abe369e59a111ea81bb534d1a62b1fe.zip",
                    "site": "nersc",
                    "archive": True,
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
                    "path": "/path/at/nersc/to/8abe369e59a111ea81bb534d1a62b1fe.zip",
                    "site": "nersc",
                    "archive": True,
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
                    "path": "/path/at/nersc/to/8abe369e59a111ea81bb534d1a62b1fe.zip",
                    "site": "nersc",
                    "archive": True,
                }
            ],
            "file_size": 104136149,
            "meta_modify_date": "2019-07-26 01:53:22.591198"
        },
        {
            "_links": {
                "parent": {
                    "href": "/api"
                },
                "self": {
                    "href": "/api/files"
                }
            },
            "files": [],
        },
        {
            "uuid": "8abe369e59a111ea81bb534d1a62b1fe",
            "logical_name": "/path/at/nersc/to/8abe369e59a111ea81bb534d1a62b1fe.zip",
            "checksum": {
                "adler32": "c14e315e",
                "sha512": "e37aa876153180bba8978afc2f4f3dde000f0d15441856e8dce0ca481dfbb7c14e315e592a82ee0b7b6a7f083af5d7e5b557f93eb8a89780bb70060412a9ec5a",
            },
            "locations": [
                {
                    "site": "NERSC",
                    "path": "/path/at/nersc/to/8abe369e59a111ea81bb534d1a62b1fe.zip",
                    "hpss": True,
                    "online": False,
                }
            ],
            "file_size": 1048576,
            "meta_modify_date": "2019-07-26 01:53:22.591198",
            "lta": {
                "bundle_path": "/path/at/nersc/to/8abe369e59a111ea81bb534d1a62b1fe.zip",
                "checksum": {
                    "adler32": "c14e315e",
                    "sha512": "e37aa876153180bba8978afc2f4f3dde000f0d15441856e8dce0ca481dfbb7c14e315e592a82ee0b7b6a7f083af5d7e5b557f93eb8a89780bb70060412a9ec5a",
                },
            },
        }
    ]
    p = Locator(config, logger_mock)
    await p._do_work_transfer_request(lta_rc_mock, tr)
    fc_rc_mock.assert_called_with("GET", '/api/files/8abe369e59a111ea81bb534d1a62b1fe')
    cb_mock.assert_called_with(lta_rc_mock, {
        'type': 'Bundle',
        'status': 'located',
        'claimed': False,
        'verified': False,
        'reason': '',
        'request': tr_uuid,
        'source': 'nersc',
        'dest': 'wipac',
        'path': '/tmp/this/is/just/a/test',
        'size': 1048576,
        'bundle_path': "/path/at/nersc/to/8abe369e59a111ea81bb534d1a62b1fe.zip",
        'checksum': {
            "adler32": "c14e315e",
            "sha512": "e37aa876153180bba8978afc2f4f3dde000f0d15441856e8dce0ca481dfbb7c14e315e592a82ee0b7b6a7f083af5d7e5b557f93eb8a89780bb70060412a9ec5a",
        },
        'catalog': {
            'checksum': {
                'adler32': 'c14e315e',
                'sha512': 'e37aa876153180bba8978afc2f4f3dde000f0d15441856e8dce0ca481dfbb7c14e315e592a82ee0b7b6a7f083af5d7e5b557f93eb8a89780bb70060412a9ec5a',
            },
            'file_size': 1048576,
            'logical_name': '/path/at/nersc/to/8abe369e59a111ea81bb534d1a62b1fe.zip',
            'meta_modify_date': '2019-07-26 01:53:22.591198',
            'uuid': '8abe369e59a111ea81bb534d1a62b1fe'
        }
    })


@pytest.mark.asyncio
async def test_locator_do_work_transfer_request_fc_its_over_9000(config: TestConfig, mocker: MockerFixture) -> None:
    """Test that _do_work_transfer_request processes each file it gets back from the File Catalog."""
    logger_mock = mocker.MagicMock()
    lta_rc_mock = mocker.MagicMock()
    lta_rc_mock.request = AsyncMock()
    lta_rc_mock.request.return_value = {}
    cb_mock = mocker.patch("lta.locator.Locator._create_bundle", new_callable=AsyncMock)
    tr_uuid = uuid1().hex
    tr = {
        "uuid": tr_uuid,
        "source": "nersc",
        "dest": "wipac",
        "path": "/tmp/this/is/just/a/test",
    }
    FILE_CATALOG_LIMIT = int(config["FILE_CATALOG_PAGE_SIZE"])

    def gen_file(i: int) -> Dict[str, str]:
        return {
            "logical_name": f"/data/exp/IceCube/2013/filtered/PFFilt/1109/PFFilt_PhysicsFiltering_Run00123231_Subrun00000000_{i:08}.tar.bz2",
            "uuid": uuid1().hex,
        }

    def gen_record(i: int) -> Dict[str, Union[int, str, Dict[str, str], List[Dict[str, Union[bool, str]]]]]:
        return {
            "logical_name": f"/data/exp/IceCube/2013/filtered/PFFilt/1109/PFFilt_PhysicsFiltering_Run00123231_Subrun00000000_{i:08}.tar.bz2",
            "uuid": uuid1().hex,
            "checksum": {
                "sha512": token_hex(128),
            },
            "locations": [
                {
                    "path": "/path/at/nersc/to/8abe369e59a111ea81bb534d1a62b1fe.zip",
                    "site": "nersc",
                    "archive": True,
                }],
            "file_size": 103166718,
            "meta_modify_date": "2019-07-26 01:53:20.857303"
        }

    FILE_CATALOG_LIMIT_10TH = floor(FILE_CATALOG_LIMIT / 10)
    fc_rc_mock = mocker.patch("rest_tools.client.RestClient.request", new_callable=AsyncMock)
    side_effects: List[Dict[str, Any]] = []
    side_effects.append({
        "_links": {
            "parent": {
                "href": "/api"
            },
            "self": {
                "href": "/api/files"
            }
        },
        "files": [gen_file(i) for i in range(FILE_CATALOG_LIMIT)],
    })
    side_effects.extend([gen_record(i) for i in range(FILE_CATALOG_LIMIT)])
    side_effects.append({
        "_links": {
            "parent": {
                "href": "/api"
            },
            "self": {
                "href": "/api/files"
            }
        },
        "files": [gen_file(i) for i in range(FILE_CATALOG_LIMIT)],
    })
    side_effects.extend([gen_record(i) for i in range(FILE_CATALOG_LIMIT)])
    side_effects.append({
        "_links": {
            "parent": {
                "href": "/api"
            },
            "self": {
                "href": "/api/files"
            }
        },
        "files": [gen_file(i) for i in range(FILE_CATALOG_LIMIT_10TH)],
    })
    side_effects.extend([gen_record(i) for i in range(FILE_CATALOG_LIMIT_10TH)])
    side_effects.append({
        "_links": {
            "parent": {
                "href": "/api"
            },
            "self": {
                "href": "/api/files"
            }
        },
        "files": [],
    })
    side_effects.extend([{
        "uuid": "8abe369e59a111ea81bb534d1a62b1fe",
        "logical_name": "/path/at/nersc/to/8abe369e59a111ea81bb534d1a62b1fe.zip",
        "checksum": {
            "adler32": "c14e315e",
            "sha512": "e37aa876153180bba8978afc2f4f3dde000f0d15441856e8dce0ca481dfbb7c14e315e592a82ee0b7b6a7f083af5d7e5b557f93eb8a89780bb70060412a9ec5a",
        },
        "locations": [
            {
                "site": "NERSC",
                "path": "/path/at/nersc/to/8abe369e59a111ea81bb534d1a62b1fe.zip",
                "hpss": True,
                "online": False,
            }
        ],
        "file_size": 1048576,
        "meta_modify_date": "2019-07-26 01:53:22.591198",
        "lta": {
            "bundle_path": "/path/at/nersc/to/8abe369e59a111ea81bb534d1a62b1fe.zip",
            "checksum": {
                "adler32": "c14e315e",
                "sha512": "e37aa876153180bba8978afc2f4f3dde000f0d15441856e8dce0ca481dfbb7c14e315e592a82ee0b7b6a7f083af5d7e5b557f93eb8a89780bb70060412a9ec5a",
            },
        },
    }])
    fc_rc_mock.side_effect = side_effects
    p = Locator(config, logger_mock)
    await p._do_work_transfer_request(lta_rc_mock, tr)
    fc_rc_mock.assert_called_with("GET", '/api/files/8abe369e59a111ea81bb534d1a62b1fe')
    cb_mock.assert_called_with(lta_rc_mock, {
        'type': 'Bundle',
        'status': 'located',
        'claimed': False,
        'verified': False,
        'reason': '',
        'request': tr_uuid,
        'source': 'nersc',
        'dest': 'wipac',
        'path': '/tmp/this/is/just/a/test',
        'size': 1048576,
        'bundle_path': "/path/at/nersc/to/8abe369e59a111ea81bb534d1a62b1fe.zip",
        'checksum': {
            "adler32": "c14e315e",
            "sha512": "e37aa876153180bba8978afc2f4f3dde000f0d15441856e8dce0ca481dfbb7c14e315e592a82ee0b7b6a7f083af5d7e5b557f93eb8a89780bb70060412a9ec5a",
        },
        'catalog': {
            'checksum': {
                'adler32': 'c14e315e',
                'sha512': 'e37aa876153180bba8978afc2f4f3dde000f0d15441856e8dce0ca481dfbb7c14e315e592a82ee0b7b6a7f083af5d7e5b557f93eb8a89780bb70060412a9ec5a',
            },
            'file_size': 1048576,
            'logical_name': '/path/at/nersc/to/8abe369e59a111ea81bb534d1a62b1fe.zip',
            'meta_modify_date': '2019-07-26 01:53:22.591198',
            'uuid': '8abe369e59a111ea81bb534d1a62b1fe'
        }
    })


@pytest.mark.asyncio
async def test_locator_create_bundle(config: TestConfig, mocker: MockerFixture) -> None:
    """Test that _create_bundle does what it says on the tin."""
    logger_mock = mocker.MagicMock()
    lta_rc_mock = mocker.MagicMock()
    lta_rc_mock.request = AsyncMock()
    lta_rc_mock.request.return_value = {
        "bundles": [uuid1().hex]
    }
    bundle = {
        'type': 'Bundle',
        'status': 'located',
        'request': uuid1().hex,
        'source': 'nersc',
        'dest': 'wipac',
        'path': '/tmp/this/is/just/a/test',
        'catalog': {
            'checksum': {
                'adler32': 'c14e315e',
                'sha512': 'e37aa876153180bba8978afc2f4f3dde000f0d15441856e8dce0ca481dfbb7c14e315e592a82ee0b7b6a7f083af5d7e5b557f93eb8a89780bb70060412a9ec5a',
            },
            'file_size': 1048576,
            'logical_name': '/path/at/nersc/to/8abe369e59a111ea81bb534d1a62b1fe.zip',
            'meta_modify_date': '2019-07-26 01:53:22.591198',
            'uuid': '8abe369e59a111ea81bb534d1a62b1fe'
        }
    }
    p = Locator(config, logger_mock)
    await p._create_bundle(lta_rc_mock, bundle)
    lta_rc_mock.request.assert_called_with("POST", "/Bundles/actions/bulk_create", {
        "bundles": [bundle],
    })


def test_as_lta_record(config: TestConfig, mocker: MockerFixture) -> None:
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

    bundle_record = as_lta_record(catalog_record)

    assert ("_links" not in bundle_record)
    assert bundle_record["checksum"] == {
        "sha512": "e001e7895e9367d20e804eec5cd867ea0758ebed068c52dac9fac55bf2d263695d1e39231b667598edcb16426048f8801341c44c0d9128df67e3cc22599319a0"
    }
    assert bundle_record["file_size"] == 4977182
    assert ("locations" not in bundle_record)
    assert bundle_record["logical_name"] == "/data/exp/IceCube/2018/unbiased/PFDST/1120/ukey_cf9b674a-7620-498a-8d59-a47d39d80245_PFDST_PhysicsFiltering_Run00131763_Subrun00000000_00000398.tar.gz"
    assert bundle_record["meta_modify_date"] == "2020-01-01 12:26:13.440651"
    assert bundle_record["uuid"] == "e6549962-2c91-11ea-9a10-f6a52f4853dd"
