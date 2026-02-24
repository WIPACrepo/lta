# test_picker.py
"""Unit tests for lta/picker.py."""

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

import itertools
from typing import Dict
from unittest.mock import AsyncMock, call, MagicMock
from uuid import uuid1

import pytest
from pytest import MonkeyPatch
from pytest_mock import MockerFixture
from tornado.web import HTTPError

from lta.picker import main_sync, Picker, BUNDLE_SIZE_MAX_FACTOR

TestConfig = Dict[str, str]

FILE_CATALOG_LIMIT = 9000


@pytest.fixture
def config() -> TestConfig:
    """Supply a stock Picker component configuration."""
    return {
        "CLIENT_ID": "long-term-archive",
        "CLIENT_SECRET": "hunter2",  # http://bash.org/?244321
        "COMPONENT_NAME": "testing-picker",
        "DEST_SITE": "NERSC",
        "FILE_CATALOG_CLIENT_ID": "file-catalog-client-id",
        "FILE_CATALOG_CLIENT_SECRET": "file-catalog-client-secret",
        "FILE_CATALOG_PAGE_SIZE": str(FILE_CATALOG_LIMIT),
        "FILE_CATALOG_REST_URL": "localhost:12346",
        "INPUT_STATUS": "ethereal",
        "LOG_LEVEL": "DEBUG",
        "LTA_AUTH_OPENID_URL": "localhost:12345",
        "LTA_REST_URL": "localhost:12347",
        "IDEAL_BUNDLE_SIZE": "107374182400",  # 100 GiB
        "OUTPUT_STATUS": "specified",
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
        Picker(config, logger_mock)


def test_constructor_config_poison_values(config: TestConfig, mocker: MockerFixture) -> None:
    """Fail with a ValueError if the configuration object is missing required configuration variables."""
    picker_config = config.copy()
    del picker_config["LTA_REST_URL"]
    logger_mock = mocker.MagicMock()
    with pytest.raises(ValueError):
        Picker(picker_config, logger_mock)


def test_constructor_config(config: TestConfig, mocker: MockerFixture) -> None:
    """Test that a Picker can be constructed with a configuration object and a logging object."""
    logger_mock = mocker.MagicMock()
    p = Picker(config, logger_mock)
    assert p.file_catalog_rest_url == "localhost:12346"
    assert p.lta_rest_url == "localhost:12347"
    assert p.name == "testing-picker"
    assert p.work_sleep_duration_seconds == 60
    assert p.logger == logger_mock


def test_constructor_config_sleep_type_int(config: TestConfig, mocker: MockerFixture) -> None:
    """Ensure that sleep seconds can also be provided as an integer."""
    logger_mock = mocker.MagicMock()
    p = Picker(config, logger_mock)
    assert p.file_catalog_rest_url == "localhost:12346"
    assert p.lta_rest_url == "localhost:12347"
    assert p.name == "testing-picker"
    assert p.work_sleep_duration_seconds == 60
    assert p.logger == logger_mock


def test_constructor_state(config: TestConfig, mocker: MockerFixture) -> None:
    """Verify that the Picker has a reasonable state when it is first constructed."""
    logger_mock = mocker.MagicMock()
    p = Picker(config, logger_mock)
    assert p.last_work_begin_timestamp is p.last_work_end_timestamp


def test_do_status(config: TestConfig, mocker: MockerFixture) -> None:
    """Verify that the Picker has no additional state to offer."""
    logger_mock = mocker.MagicMock()
    p = Picker(config, logger_mock)
    assert p._do_status() == {}


@pytest.mark.asyncio
async def test_script_main_sync(config: TestConfig, mocker: MockerFixture, monkeypatch: MonkeyPatch) -> None:
    """
    Verify Picker component behavior when run as a script.

    Test to make sure running the Picker as a script does the setup work
    that we expect and then launches the picker service.
    """
    for key in config.keys():
        monkeypatch.setenv(key, config[key])
    mock_run = mocker.patch("asyncio.run")
    mock_main = mocker.patch("lta.picker.main")
    mock_shs = mocker.patch("lta.picker.start_http_server")
    main_sync()
    mock_shs.assert_called()
    mock_main.assert_called()
    mock_run.assert_called()
    await mock_run.call_args.args[0]


@pytest.mark.asyncio
async def test_picker_logs_configuration(mocker: MockerFixture) -> None:
    """Test to make sure the Picker logs its configuration."""
    logger_mock = mocker.MagicMock()
    picker_config = {
        "CLIENT_ID": "long-term-archive",
        "CLIENT_SECRET": "hunter2",  # http://bash.org/?244321
        "COMPONENT_NAME": "logme-testing-picker",
        "DEST_SITE": "NERSC",
        "FILE_CATALOG_CLIENT_ID": "file-catalog-client-id",
        "FILE_CATALOG_CLIENT_SECRET": "file-catalog-client-secret",
        "FILE_CATALOG_PAGE_SIZE": str(FILE_CATALOG_LIMIT),
        "FILE_CATALOG_REST_URL": "logme-http://kVj74wBA1AMTDV8zccn67pGuWJqHZzD7iJQHrUJKA.com/",
        "INPUT_STATUS": "ethereal",
        "LOG_LEVEL": "DEBUG",
        "LTA_AUTH_OPENID_URL": "localhost:12345",
        "LTA_REST_URL": "logme-http://RmMNHdPhHpH2ZxfaFAC9d2jiIbf5pZiHDqy43rFLQiM.com/",
        "IDEAL_BUNDLE_SIZE": "107374182400",  # 100 GiB
        "OUTPUT_STATUS": "specified",
        "PROMETHEUS_METRICS_PORT": "8080",
        "RUN_ONCE_AND_DIE": "False",
        "RUN_UNTIL_NO_WORK": "False",
        "SOURCE_SITE": "WIPAC",
        "WORK_RETRIES": "5",
        "WORK_SLEEP_DURATION_SECONDS": "70",
        "WORK_TIMEOUT_SECONDS": "90",
    }
    Picker(picker_config, logger_mock)
    EXPECTED_LOGGER_CALLS = [
        call("picker 'logme-testing-picker' is configured:"),
        call('CLIENT_ID = long-term-archive'),
        call('CLIENT_SECRET = [秘密]'),
        call('COMPONENT_NAME = logme-testing-picker'),
        call('DEST_SITE = NERSC'),
        call('FILE_CATALOG_CLIENT_ID = file-catalog-client-id'),
        call('FILE_CATALOG_CLIENT_SECRET = [秘密]'),
        call('FILE_CATALOG_PAGE_SIZE = 9000'),
        call('FILE_CATALOG_REST_URL = logme-http://kVj74wBA1AMTDV8zccn67pGuWJqHZzD7iJQHrUJKA.com/'),
        call('INPUT_STATUS = ethereal'),
        call('LOG_LEVEL = DEBUG'),
        call('LTA_AUTH_OPENID_URL = localhost:12345'),
        call('LTA_REST_URL = logme-http://RmMNHdPhHpH2ZxfaFAC9d2jiIbf5pZiHDqy43rFLQiM.com/'),
        call('IDEAL_BUNDLE_SIZE = 107374182400'),
        call('OUTPUT_STATUS = specified'),
        call('PROMETHEUS_METRICS_PORT = 8080'),
        call('RUN_ONCE_AND_DIE = False'),
        call('RUN_UNTIL_NO_WORK = False'),
        call('SOURCE_SITE = WIPAC'),
        call('WORK_RETRIES = 5'),
        call('WORK_SLEEP_DURATION_SECONDS = 70'),
        call('WORK_TIMEOUT_SECONDS = 90')
    ]
    logger_mock.info.assert_has_calls(EXPECTED_LOGGER_CALLS)


@pytest.mark.asyncio
async def test_picker_run(config: TestConfig, mocker: MockerFixture) -> None:
    """Test the Picker does the work the picker should do."""
    logger_mock = mocker.MagicMock()
    p = Picker(config, logger_mock)
    p._do_work = AsyncMock()  # type: ignore[method-assign]
    await p.run()
    p._do_work.assert_called()


@pytest.mark.asyncio
async def test_picker_run_exception(config: TestConfig, mocker: MockerFixture) -> None:
    """Test an error doesn't kill the Picker."""
    logger_mock = mocker.MagicMock()
    p = Picker(config, logger_mock)
    p.last_work_end_timestamp = ""
    p._do_work = AsyncMock()  # type: ignore[method-assign]
    p._do_work.side_effect = [Exception("bad thing happen!")]
    await p.run()
    p._do_work.assert_called()
    assert p.last_work_end_timestamp


@pytest.mark.asyncio
async def test_picker_do_work_pop_exception(config: TestConfig, mocker: MockerFixture) -> None:
    """Test that _do_work raises when the RestClient can't pop."""
    logger_mock = mocker.MagicMock()
    lta_rc_mock = AsyncMock()
    lta_rc_mock.request = AsyncMock()
    lta_rc_mock.request.side_effect = HTTPError(500, "LTA DB on fire. Again.")
    p = Picker(config, logger_mock)
    with pytest.raises(HTTPError):
        await p._do_work(lta_rc_mock)
    lta_rc_mock.request.assert_called_with("POST", '/TransferRequests/actions/pop?source=WIPAC&dest=NERSC', {'claimant': f'{p.name}-{p.instance_uuid}'})


@pytest.mark.asyncio
async def test_picker_do_work_no_results(config: TestConfig, mocker: MockerFixture) -> None:
    """Test that _do_work goes on vacation when the LTA DB has no work."""
    logger_mock = mocker.MagicMock()
    dwc_mock = mocker.patch("lta.picker.Picker._do_work_claim", new_callable=AsyncMock)
    dwc_mock.return_value = False
    p = Picker(config, logger_mock)
    await p._do_work(AsyncMock())
    dwc_mock.assert_called()


@pytest.mark.asyncio
async def test_picker_do_work_yes_results(config: TestConfig, mocker: MockerFixture) -> None:
    """Test that _do_work keeps working until the LTA DB has no work."""
    logger_mock = mocker.MagicMock()
    dwc_mock = mocker.patch("lta.picker.Picker._do_work_claim", new_callable=AsyncMock)
    dwc_mock.side_effect = [True, True, False]
    p = Picker(config, logger_mock)
    await p._do_work(AsyncMock())
    dwc_mock.assert_called()


@pytest.mark.asyncio
async def test_picker_do_work_claim_no_result(config: TestConfig, mocker: MockerFixture) -> None:
    """Test that _do_work_claim does not work when the LTA DB has no work."""
    logger_mock = mocker.MagicMock()
    lta_rc_mock = AsyncMock()
    lta_rc_mock.request = AsyncMock()
    lta_rc_mock.request.return_value = {
        "transfer_request": None
    }
    dwtr_mock = mocker.patch("lta.picker.Picker._do_work_transfer_request", new_callable=AsyncMock)
    p = Picker(config, logger_mock)
    await p._do_work_claim(lta_rc_mock)
    lta_rc_mock.request.assert_called_with("POST", '/TransferRequests/actions/pop?source=WIPAC&dest=NERSC', {'claimant': f'{p.name}-{p.instance_uuid}'})
    dwtr_mock.assert_not_called()


@pytest.mark.asyncio
async def test_picker_do_work_claim_yes_result(config: TestConfig, mocker: MockerFixture) -> None:
    """Test that _do_work_claim processes the TransferRequest it gets from the LTA DB."""
    logger_mock = mocker.MagicMock()
    lta_rc_mock = AsyncMock()
    lta_rc_mock.request = AsyncMock()
    lta_rc_mock.request.return_value = {
        "transfer_request": {
            "one": 1,
        },
    }
    dwtr_mock = mocker.patch("lta.picker.Picker._do_work_transfer_request", new_callable=AsyncMock)
    p = Picker(config, logger_mock)
    await p._do_work_claim(lta_rc_mock)
    lta_rc_mock.request.assert_called_with("POST", '/TransferRequests/actions/pop?source=WIPAC&dest=NERSC', {'claimant': f'{p.name}-{p.instance_uuid}'})
    dwtr_mock.assert_called_with(mocker.ANY, {"one": 1})


@pytest.mark.asyncio
async def test_picker_do_work_transfer_request_fc_exception(config: TestConfig, mocker: MockerFixture) -> None:
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
    fc_rc_mock.assert_called()
    assert fc_rc_mock.call_args[0][0] == "GET"
    assert fc_rc_mock.call_args[0][1].startswith('/api/files?query={"locations.site": {"$eq": "wipac"}, "locations.path": {"$regex": "^/tmp/this/is/just/a/test"}}')


@pytest.mark.asyncio
async def test_picker_do_work_transfer_request_fc_no_results(config: TestConfig, mocker: MockerFixture) -> None:
    """Test that _do_work_transfer_request raises an exception when the LTA DB refuses to create an empty list."""
    QUARANTINE = {'original_status': 'ethereal', 'status': 'quarantined', 'reason': mocker.ANY, 'work_priority_timestamp': mocker.ANY}
    logger_mock = mocker.MagicMock()
    p = Picker(config, logger_mock)
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
    assert fc_rc_mock.call_args[0][1].startswith('/api/files?query={"locations.site": {"$eq": "wipac"}, "locations.path": {"$regex": "^/tmp/this/is/just/a/test"}}')
    lta_rc_mock.request.assert_called_with("PATCH", f'/TransferRequests/{tr_uuid}', QUARANTINE)


@pytest.mark.asyncio
async def test_picker_do_work_transfer_request_fc_yes_results(config: TestConfig, mocker: MockerFixture) -> None:
    """Test that _do_work_transfer_request processes each file it gets back from the File Catalog."""
    logger_mock = mocker.MagicMock()
    lta_rc_mock = mocker.MagicMock()
    lta_rc_mock.request = AsyncMock()
    lta_rc_mock.request.side_effect = [
        {
            "bundles": [uuid1().hex],
            "count": 1,
        },
        {
            "metadata": [
                "58a334e6-642e-475e-b642-e92bf08e96d4",
                "89528506-9950-43dc-a910-f5108a1d25c0",
                "1e4a88c6-247e-4e59-9c89-1a4edafafb1e",
            ],
            "count": 3,
        },
    ]
    tr_uuid = uuid1().hex
    tr = {
        "uuid": tr_uuid,
        "source": "wipac",
        "dest": "nersc",
        "path": "/tmp/this/is/just/a/test",
    }
    fc_rc_mock_request = mocker.patch("rest_tools.client.RestClient.request", new_callable=AsyncMock)
    fc_rc_mock_request.side_effect = [
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
                {
                    "logical_name": "/data/exp/IceCube/2013/filtered/PFFilt/1109/PFFilt_PhysicsFiltering_Run00123231_Subrun00000000_00000000.tar.bz2",
                    "uuid": "58a334e6-642e-475e-b642-e92bf08e96d4",
                    "file_size": 103166718,
                },
                {
                    "logical_name": "/data/exp/IceCube/2013/filtered/PFFilt/1109/PFFilt_PhysicsFiltering_Run00123231_Subrun00000000_00000001.tar.bz2",
                    "uuid": "89528506-9950-43dc-a910-f5108a1d25c0",
                    "file_size": 103064762,
                },
                {
                    "logical_name": "/data/exp/IceCube/2013/filtered/PFFilt/1109/PFFilt_PhysicsFiltering_Run00123231_Subrun00000000_00000002.tar.bz2",
                    "uuid": "1e4a88c6-247e-4e59-9c89-1a4edafafb1e",
                    "file_size": 104136149,
                },
            ],
        },
        # final empty result
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
        }
    ]
    p = Picker(config, logger_mock)
    await p._do_work_transfer_request(lta_rc_mock, tr)
    assert fc_rc_mock_request.call_args_list == [
        call("GET", '/api/files?query={"locations.site": {"$eq": "wipac"}, "locations.path": {"$regex": "^/tmp/this/is/just/a/test"}}&keys=uuid|file_size&limit=9000&start=0'),
        call("GET", '/api/files?query={"locations.site": {"$eq": "wipac"}, "locations.path": {"$regex": "^/tmp/this/is/just/a/test"}}&keys=uuid|file_size&limit=9000&start=3'),
    ]
    assert lta_rc_mock.request.call_args_list == [
        call("POST", '/Bundles/actions/bulk_create', mocker.ANY),
        call("POST", '/Metadata/actions/bulk_create', mocker.ANY),
    ]


########################################################################################


@pytest.mark.asyncio
async def test_1000_picker_get_files_from_file_catalog_fc_its_over_9000(config: TestConfig, mocker: MockerFixture) -> None:
    """Test that _get_files_from_file_catalog() can handle paginated File Catalog results."""
    logger_mock = mocker.MagicMock()
    tr_uuid = uuid1().hex
    tr = {
        "uuid": tr_uuid,
        "source": "wipac",
        "dest": "nersc",
        "path": "/tmp/this/is/just/a/test",
    }

    def gen_file(i: int) -> Dict[str, str|int]:
        return {
            "logical_name": f"/data/exp/IceCube/2013/filtered/PFFilt/1109/PFFilt_PhysicsFiltering_Run00123231_Subrun00000000_{i:08}.tar.bz2",
            "uuid": uuid1().hex,
            "file_size": 103166718,
        }

    fc_rc_mock_request = mocker.patch("rest_tools.client.RestClient.request", new_callable=AsyncMock)
    # these are the three paged queries to find files to bundle
    fc_rc_mock_request.side_effect = [
        {
            "_links": {
                "parent": {
                    "href": "/api"
                },
                "self": {
                    "href": "/api/files"
                }
            },
            "files": [gen_file(i) for i in range(FILE_CATALOG_LIMIT)],
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
            "files": [gen_file(i) for i in range(FILE_CATALOG_LIMIT)],
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
            "files": [gen_file(i) for i in range(1000)],
        },
        # final empty result
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
    ]

    # now we're ready to play Dr. Mario!
    p = Picker(config, logger_mock)
    ret = await p._get_files_from_file_catalog(tr)
    assert fc_rc_mock_request.call_args_list == [
        call("GET", '/api/files?query={"locations.site": {"$eq": "wipac"}, "locations.path": {"$regex": "^/tmp/this/is/just/a/test"}}&keys=uuid|file_size&limit=9000&start=0'),
        call("GET", '/api/files?query={"locations.site": {"$eq": "wipac"}, "locations.path": {"$regex": "^/tmp/this/is/just/a/test"}}&keys=uuid|file_size&limit=9000&start=9000'),
        call("GET", '/api/files?query={"locations.site": {"$eq": "wipac"}, "locations.path": {"$regex": "^/tmp/this/is/just/a/test"}}&keys=uuid|file_size&limit=9000&start=18000'),
        call("GET", '/api/files?query={"locations.site": {"$eq": "wipac"}, "locations.path": {"$regex": "^/tmp/this/is/just/a/test"}}&keys=uuid|file_size&limit=9000&start=19000'),
    ]
    assert len(ret) == FILE_CATALOG_LIMIT + FILE_CATALOG_LIMIT + 1000


########################################################################################

IDEAL_BUNDLE_SIZE = 100


def test_1100_group_catalog_files_evenly__edge_case(config: dict[str, str]) -> None:
    """Empty input yields 1 bin and no files."""
    logger_mock = MagicMock()
    p = Picker(config, logger_mock)
    p.ideal_bundle_size = IDEAL_BUNDLE_SIZE

    bins = p._group_catalog_files_evenly([])

    assert len(bins) == 1
    assert bins == [[]]


def test_1101_group_catalog_files_evenly__edge_case(config: dict[str, str]) -> None:
    """1 file input yields 1 bin."""
    logger_mock = MagicMock()
    p = Picker(config, logger_mock)
    p.ideal_bundle_size = IDEAL_BUNDLE_SIZE

    files = [MagicMock(uuid="u0", file_size=30)]

    bins = p._group_catalog_files_evenly(files)  # type: ignore

    assert len(bins) == 1
    assert bins == [files]


def test_1110_group_catalog_files_evenly_exact_ideal_size(config: dict[str, str]) -> None:
    """Exact ideal size total yields 1 bin."""
    logger_mock = MagicMock()
    p = Picker(config, logger_mock)
    p.ideal_bundle_size = IDEAL_BUNDLE_SIZE

    files = [
        MagicMock(uuid="u0", file_size=60),
        MagicMock(uuid="u1", file_size=40),
    ]

    bins = p._group_catalog_files_evenly(files)  # type: ignore
    assert len(bins) == 1
    assert bins == [files]


@pytest.mark.parametrize(
    ("file_sizes", "expected_bins"),
    [
        # ceil: 1→2 boundary (ideal=100 ⇒ ideal*1.2=120)
        ([20, 20, 20, 20, 20, 20], 1),  # 120: ceil=1, round=1
        ([21, 20, 20, 20, 20, 20], 2),  # 121: ceil=2, round=1

        # ceil: 2→3 boundary
        ([40, 40, 40, 40, 40, 40], 2),  # 240: ceil=2, round=2
        ([41, 40, 40, 40, 40, 40], 3),  # 241: ceil=3, round=2

        # round dominates (then ceil catches up)
        ([60, 60, 60, 60, 60, 60], 4),  # 360: ceil=3, round=4
        ([61, 60, 60, 60, 60, 60], 4),  # 361: ceil=4, round=4

        # round: 3→4 boundary
        ([60, 60, 60, 60, 60, 49], 3),  # 349: ceil=3, round=3
        ([60, 60, 60, 60, 60, 50], 4),  # 350: ceil=3, round=4
    ],
)
def test_1120_group_catalog_files_evenly_boundaries(
    config: dict[str, str],
    file_sizes: list[int],
    expected_bins: int,
) -> None:
    """Test n_bins boundaries for round vs ceil and the max(..., 1) guard."""
    logger_mock = MagicMock()
    p = Picker(config, logger_mock)
    p.ideal_bundle_size = IDEAL_BUNDLE_SIZE

    assert BUNDLE_SIZE_MAX_FACTOR == 1.2
    # ^^^ important b/c our tests parameters depend on this boundary value

    files = []
    for i, sz in enumerate(file_sizes):
        files.append(MagicMock(uuid=f"u{i}", file_size=sz))

    bins = p._group_catalog_files_evenly(files)  # type: ignore
    assert len(bins) == expected_bins

    # does every bin have files?
    assert all(len(b) for b in bins)

    # did every file make it in a bin?
    got = sorted(f.uuid for f in itertools.chain.from_iterable(bins))
    assert got == ["u0", "u1", "u2", "u3", "u4", "u5"]
