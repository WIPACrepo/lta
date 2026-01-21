# test_site_move_verifier.py
"""Unit tests for lta/site_move_verifier.py."""

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

from typing import Dict
from unittest.mock import AsyncMock, call, MagicMock

import pytest
from pytest import MonkeyPatch
from pytest_mock import MockerFixture
from tornado.web import HTTPError

from lta.site_move_verifier import as_nonempty_columns, discard_empty, MYQUOTA_ARGS, parse_myquota
from lta.site_move_verifier import main_sync, SiteMoveVerifier
from .utils import ObjectLiteral

TestConfig = Dict[str, str]


@pytest.fixture
def config() -> TestConfig:
    """Supply a stock SiteMoveVerifier component configuration."""
    return {
        "CLIENT_ID": "long-term-archive",
        "CLIENT_SECRET": "hunter2",  # http://bash.org/?244321
        "COMPONENT_NAME": "testing-site_move_verifier",
        "DEST_ROOT_PATH": "/path/to/rse",
        "DEST_SITE": "NERSC",
        "INPUT_STATUS": "transferring",
        "LOG_LEVEL": "DEBUG",
        "LTA_AUTH_OPENID_URL": "localhost:12345",
        "LTA_REST_URL": "localhost:12347",
        "OUTPUT_STATUS": "taping",
        "PROMETHEUS_METRICS_PORT": "8080",
        "RUN_ONCE_AND_DIE": "False",
        "RUN_UNTIL_NO_WORK": "False",
        "SOURCE_SITE": "WIPAC",
        "USE_FULL_BUNDLE_PATH": "FALSE",
        "WORK_RETRIES": "3",
        "WORK_SLEEP_DURATION_SECONDS": "60",
        "WORK_TIMEOUT_SECONDS": "30",
    }


def test_as_nonempty_columns() -> None:
    """Test that test_as_nonempty_columns does what it says on the tin."""
    assert as_nonempty_columns("FILESYSTEM   SPACE_USED   SPACE_QUOTA   SPACE_PCT   INODE_USED   INODE_QUOTA   INODE_PCT") == ["FILESYSTEM", "SPACE_USED", "SPACE_QUOTA", "SPACE_PCT", "INODE_USED", "INODE_QUOTA", "INODE_PCT"]
    assert as_nonempty_columns("cscratch1    7638.60GiB   51200.00GiB   14.9%       0.00G        0.01G         0.1%") == ["cscratch1", "7638.60GiB", "51200.00GiB", "14.9%", "0.00G", "0.01G", "0.1%"]


def test_discard_empty() -> None:
    """Test that discard_empty does what it says on the tin."""
    assert not discard_empty("")
    assert discard_empty("alice")


def test_parse_myquota() -> None:
    """Test that parse_myquota provides expected output."""
    stdout = """FILESYSTEM   SPACE_USED   SPACE_QUOTA   SPACE_PCT   INODE_USED   INODE_QUOTA   INODE_PCT
home         1.90GiB      40.00GiB      4.7%        44.00        1.00M         0.0%
cscratch1    12.00KiB     20.00TiB      0.0%        3.00         10.00M        0.0%
"""
    assert parse_myquota(stdout) == [
        {
            "FILESYSTEM": "home",
            "SPACE_USED": "1.90GiB",
            "SPACE_QUOTA": "40.00GiB",
            "SPACE_PCT": "4.7%",
            "INODE_USED": "44.00",
            "INODE_QUOTA": "1.00M",
            "INODE_PCT": "0.0%",
        },
        {
            "FILESYSTEM": "cscratch1",
            "SPACE_USED": "12.00KiB",
            "SPACE_QUOTA": "20.00TiB",
            "SPACE_PCT": "0.0%",
            "INODE_USED": "3.00",
            "INODE_QUOTA": "10.00M",
            "INODE_PCT": "0.0%",
        },
    ]


def test_constructor_config(config: TestConfig, mocker: MockerFixture) -> None:
    """Test that a SiteMoveVerifier can be constructed with a configuration object and a logging object."""
    logger_mock = mocker.MagicMock()
    p = SiteMoveVerifier(config, logger_mock)
    assert p.name == "testing-site_move_verifier"
    assert p.dest_root_path == "/path/to/rse"
    assert p.dest_site == "NERSC"
    assert p.lta_auth_openid_url == "localhost:12345"
    assert p.lta_rest_url == "localhost:12347"
    assert p.output_status == "taping"
    assert p.source_site == "WIPAC"
    assert p.work_retries == 3
    assert p.work_sleep_duration_seconds == 60
    assert p.work_timeout_seconds == 30
    assert p.logger == logger_mock


def test_do_status(config: TestConfig, mocker: MockerFixture) -> None:
    """Verify that the SiteMoveVerifier has additional state to offer."""
    logger_mock = mocker.MagicMock()
    run_mock = mocker.patch("lta.site_move_verifier.run", new_callable=MagicMock)
    run_mock.return_value = ObjectLiteral(
        returncode=0,
        args=MYQUOTA_ARGS,
        stdout=b"FILESYSTEM   SPACE_USED   SPACE_QUOTA   SPACE_PCT   INODE_USED   INODE_QUOTA   INODE_PCT\nhome         1.90GiB      40.00GiB      4.7%        44.00        1.00M         0.0%\ncscratch1    12.00KiB     20.00TiB      0.0%        3.00         10.00M        0.0%\n",
        stderr="",
    )
    p = SiteMoveVerifier(config, logger_mock)
    assert p._do_status() == {
        "quota": [
            {
                "FILESYSTEM": "home",
                "SPACE_USED": "1.90GiB",
                "SPACE_QUOTA": "40.00GiB",
                "SPACE_PCT": "4.7%",
                "INODE_USED": "44.00",
                "INODE_QUOTA": "1.00M",
                "INODE_PCT": "0.0%",
            },
            {
                "FILESYSTEM": "cscratch1",
                "SPACE_USED": "12.00KiB",
                "SPACE_QUOTA": "20.00TiB",
                "SPACE_PCT": "0.0%",
                "INODE_USED": "3.00",
                "INODE_QUOTA": "10.00M",
                "INODE_PCT": "0.0%",
            },
        ]
    }


def test_do_status_myquota_fails(config: TestConfig, mocker: MockerFixture) -> None:
    """Verify that the SiteMoveVerifier has no additional state to offer."""
    logger_mock = mocker.MagicMock()
    run_mock = mocker.patch("lta.site_move_verifier.run", new_callable=MagicMock)
    run_mock.return_value = ObjectLiteral(
        returncode=1,
        args=MYQUOTA_ARGS,
        stdout="",
        stderr="nersc file systems burned down; again",
    )
    p = SiteMoveVerifier(config, logger_mock)
    assert p._do_status() == {"quota": []}


@pytest.mark.asyncio
async def test_site_move_verifier_logs_configuration(mocker: MockerFixture) -> None:
    """Test to make sure the SiteMoveVerifier logs its configuration."""
    logger_mock = mocker.MagicMock()
    site_move_verifier_config = {
        "CLIENT_ID": "long-term-archive",
        "CLIENT_SECRET": "hunter2",  # http://bash.org/?244321
        "COMPONENT_NAME": "logme-testing-site_move_verifier",
        "DEST_ROOT_PATH": "/path/to/some/archive/destination",
        "DEST_SITE": "NERSC",
        "INPUT_STATUS": "transferring",
        "LOG_LEVEL": "DEBUG",
        "LTA_AUTH_OPENID_URL": "localhost:12345",
        "LTA_REST_URL": "logme-http://RmMNHdPhHpH2ZxfaFAC9d2jiIbf5pZiHDqy43rFLQiM.com/",
        "OUTPUT_STATUS": "taping",
        "PROMETHEUS_METRICS_PORT": "8080",
        "RUN_ONCE_AND_DIE": "False",
        "RUN_UNTIL_NO_WORK": "False",
        "SOURCE_SITE": "WIPAC",
        "USE_FULL_BUNDLE_PATH": "FALSE",
        "WORK_RETRIES": "5",
        "WORK_SLEEP_DURATION_SECONDS": "70",
        "WORK_TIMEOUT_SECONDS": "90",
    }
    SiteMoveVerifier(site_move_verifier_config, logger_mock)
    EXPECTED_LOGGER_CALLS = [
        call("site_move_verifier 'logme-testing-site_move_verifier' is configured:"),
        call('CLIENT_ID = long-term-archive'),
        call('CLIENT_SECRET = [秘密]'),
        call('COMPONENT_NAME = logme-testing-site_move_verifier'),
        call('DEST_ROOT_PATH = /path/to/some/archive/destination'),
        call('DEST_SITE = NERSC'),
        call('INPUT_STATUS = transferring'),
        call('LOG_LEVEL = DEBUG'),
        call('LTA_AUTH_OPENID_URL = localhost:12345'),
        call('LTA_REST_URL = logme-http://RmMNHdPhHpH2ZxfaFAC9d2jiIbf5pZiHDqy43rFLQiM.com/'),
        call('OUTPUT_STATUS = taping'),
        call('PROMETHEUS_METRICS_PORT = 8080'),
        call('RUN_ONCE_AND_DIE = False'),
        call('RUN_UNTIL_NO_WORK = False'),
        call('SOURCE_SITE = WIPAC'),
        call('USE_FULL_BUNDLE_PATH = FALSE'),
        call('WORK_RETRIES = 5'),
        call('WORK_SLEEP_DURATION_SECONDS = 70'),
        call('WORK_TIMEOUT_SECONDS = 90')
    ]
    logger_mock.info.assert_has_calls(EXPECTED_LOGGER_CALLS)


@pytest.mark.asyncio
async def test_script_main_sync(config: TestConfig, mocker: MockerFixture, monkeypatch: MonkeyPatch) -> None:
    """
    Verify SiteMoveVerifier component behavior when run as a script.

    Test to make sure running the SiteMoveVerifier as a script does the setup work
    that we expect and then launches the site_move_verifier service.
    """
    for key in config.keys():
        monkeypatch.setenv(key, config[key])
    mock_run = mocker.patch("asyncio.run")
    mock_main = mocker.patch("lta.site_move_verifier.main")
    mock_shs = mocker.patch("lta.site_move_verifier.start_http_server")
    main_sync()
    mock_shs.assert_called()
    mock_main.assert_called()
    mock_run.assert_called()
    await mock_run.call_args.args[0]


@pytest.mark.asyncio
async def test_site_move_verifier_run(config: TestConfig, mocker: MockerFixture) -> None:
    """Test the SiteMoveVerifier does the work the site_move_verifier should do."""
    logger_mock = mocker.MagicMock()
    p = SiteMoveVerifier(config, logger_mock)
    p._do_work = AsyncMock()  # type: ignore[method-assign]
    await p.run()
    p._do_work.assert_called()


@pytest.mark.asyncio
async def test_site_move_verifier_run_exception(config: TestConfig, mocker: MockerFixture) -> None:
    """Test an error doesn't kill the SiteMoveVerifier."""
    logger_mock = mocker.MagicMock()
    p = SiteMoveVerifier(config, logger_mock)
    p.last_work_end_timestamp = ""
    p._do_work = AsyncMock()  # type: ignore[method-assign]
    p._do_work.side_effect = [Exception("bad thing happen!")]
    await p.run()
    p._do_work.assert_called()
    assert p.last_work_end_timestamp


@pytest.mark.asyncio
async def test_site_move_verifier_do_work_pop_exception(config: TestConfig, mocker: MockerFixture) -> None:
    """Test that _do_work raises when the RestClient can't pop."""
    logger_mock = mocker.MagicMock()
    lta_rc_mock = AsyncMock()
    lta_rc_mock.request = AsyncMock()
    lta_rc_mock.request.side_effect = HTTPError(500, "LTA DB on fire. Again.")
    p = SiteMoveVerifier(config, logger_mock)
    with pytest.raises(HTTPError):
        await p._do_work(lta_rc_mock)
    lta_rc_mock.request.assert_called_with("POST", '/Bundles/actions/pop?source=WIPAC&dest=NERSC&status=transferring', {'claimant': f'{p.name}-{p.instance_uuid}'})


@pytest.mark.asyncio
async def test_site_move_verifier_do_work_no_results(config: TestConfig, mocker: MockerFixture) -> None:
    """Test that _do_work goes on vacation when the LTA DB has no work."""
    logger_mock = mocker.MagicMock()
    dwc_mock = mocker.patch("lta.site_move_verifier.SiteMoveVerifier._do_work_claim", new_callable=AsyncMock)
    dwc_mock.return_value = False
    p = SiteMoveVerifier(config, logger_mock)
    await p._do_work(AsyncMock())
    dwc_mock.assert_called()


@pytest.mark.asyncio
async def test_site_move_verifier_do_work_yes_results(config: TestConfig, mocker: MockerFixture) -> None:
    """Test that _do_work keeps working until the LTA DB has no work."""
    logger_mock = mocker.MagicMock()
    dwc_mock = mocker.patch("lta.site_move_verifier.SiteMoveVerifier._do_work_claim", new_callable=AsyncMock)
    dwc_mock.side_effect = [True, True, False]
    p = SiteMoveVerifier(config, logger_mock)
    await p._do_work(AsyncMock())
    dwc_mock.assert_called()


@pytest.mark.asyncio
async def test_site_move_verifier_do_work_claim_no_result(config: TestConfig, mocker: MockerFixture) -> None:
    """Test that _do_work_claim does not work when the LTA DB has no work."""
    logger_mock = mocker.MagicMock()
    lta_rc_mock = AsyncMock()
    lta_rc_mock.request = AsyncMock()
    lta_rc_mock.request.return_value = {
        "bundle": None
    }
    vb_mock = mocker.patch("lta.site_move_verifier.SiteMoveVerifier._verify_bundle", new_callable=AsyncMock)
    p = SiteMoveVerifier(config, logger_mock)
    await p._do_work_claim(lta_rc_mock)
    lta_rc_mock.request.assert_called_with("POST", '/Bundles/actions/pop?source=WIPAC&dest=NERSC&status=transferring', {'claimant': f'{p.name}-{p.instance_uuid}'})
    vb_mock.assert_not_called()


@pytest.mark.asyncio
async def test_site_move_verifier_do_work_claim_yes_result(config: TestConfig, mocker: MockerFixture) -> None:
    """Test that _do_work_claim processes the Bundle that it gets from the LTA DB."""
    logger_mock = mocker.MagicMock()
    lta_rc_mock = AsyncMock()
    lta_rc_mock.request = AsyncMock()
    lta_rc_mock.request.return_value = {
        "bundle": {
            "one": 1,
        },
    }
    vb_mock = mocker.patch("lta.site_move_verifier.SiteMoveVerifier._verify_bundle", new_callable=AsyncMock)
    p = SiteMoveVerifier(config, logger_mock)
    assert await p._do_work_claim(lta_rc_mock)
    lta_rc_mock.request.assert_called_with("POST", '/Bundles/actions/pop?source=WIPAC&dest=NERSC&status=transferring', {'claimant': f'{p.name}-{p.instance_uuid}'})
    vb_mock.assert_called_with(mocker.ANY, {"one": 1})


@pytest.mark.asyncio
async def test_site_move_verifier_verify_bundle_bad_checksum(config: TestConfig, mocker: MockerFixture) -> None:
    """Test that _delete_bundle deletes a completed bundle transfer."""
    logger_mock = mocker.MagicMock()
    lta_rc_mock = mocker.patch("rest_tools.client.RestClient", new_callable=AsyncMock)
    isfile_mock = mocker.patch("os.path.isfile")
    isfile_mock.return_value = True
    time_mock = mocker.patch("time.time")
    time_mock.return_value = 1588042614
    getmtime_mock = mocker.patch("os.path.getmtime")
    getmtime_mock.return_value = 1588042614 - 120
    hash_mock = mocker.patch("lta.site_move_verifier.sha512sum")
    hash_mock.return_value = "54321"
    bundle_obj = {
        "uuid": "8286d3ba-fb1b-4923-876d-935bdf7fc99e",
        "dest": "nersc",
        "path": "/data/exp/IceCube/2014/unbiased/PFRaw/1109",
        "transfer_reference": "dataset-nersc|8286d3ba-fb1b-4923-876d-935bdf7fc99e.zip",
        "bundle_path": "/mnt/lfss/lta/scratch/8286d3ba-fb1b-4923-876d-935bdf7fc99e.zip",
        "checksum": {
            "sha512": "12345",
        },
    }
    p = SiteMoveVerifier(config, logger_mock)
    await p._verify_bundle(lta_rc_mock, bundle_obj)
    hash_mock.assert_called_with("/path/to/rse/8286d3ba-fb1b-4923-876d-935bdf7fc99e.zip")
    lta_rc_mock.request.assert_called_with("PATCH", '/Bundles/8286d3ba-fb1b-4923-876d-935bdf7fc99e', {
        "status": "quarantined",
        "reason": mocker.ANY,
        "work_priority_timestamp": mocker.ANY,
    })


@pytest.mark.asyncio
async def test_site_move_verifier_verify_bundle_good_checksum(config: TestConfig, mocker: MockerFixture) -> None:
    """Test that _delete_bundle deletes a completed bundle transfer."""
    logger_mock = mocker.MagicMock()
    lta_rc_mock = mocker.patch("rest_tools.client.RestClient", new_callable=AsyncMock)
    isfile_mock = mocker.patch("os.path.isfile")
    isfile_mock.return_value = True
    time_mock = mocker.patch("time.time")
    time_mock.return_value = 1588042614
    getmtime_mock = mocker.patch("os.path.getmtime")
    getmtime_mock.return_value = 1588042614 - 120
    hash_mock = mocker.patch("lta.site_move_verifier.sha512sum")
    hash_mock.return_value = "12345"
    bundle_obj = {
        "uuid": "8286d3ba-fb1b-4923-876d-935bdf7fc99e",
        "dest": "nersc",
        "path": "/data/exp/IceCube/2014/unbiased/PFRaw/1109",
        "transfer_reference": "dataset-nersc|8286d3ba-fb1b-4923-876d-935bdf7fc99e.zip",
        "bundle_path": "/mnt/lfss/lta/scratch/8286d3ba-fb1b-4923-876d-935bdf7fc99e.zip",
        "checksum": {
            "sha512": "12345",
        },
    }
    p = SiteMoveVerifier(config, logger_mock)
    await p._verify_bundle(lta_rc_mock, bundle_obj)
    hash_mock.assert_called_with("/path/to/rse/8286d3ba-fb1b-4923-876d-935bdf7fc99e.zip")
    lta_rc_mock.request.assert_called_with("PATCH", '/Bundles/8286d3ba-fb1b-4923-876d-935bdf7fc99e', {
        "status": "taping",
        "reason": "",
        "update_timestamp": mocker.ANY,
        "claimed": False,
    })
