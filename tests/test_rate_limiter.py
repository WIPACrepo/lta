# test_rate_limiter.py
"""Unit tests for lta/rate_limiter.py."""

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

from lta.rate_limiter import main_sync, RateLimiter

TestConfig = Dict[str, str]


@pytest.fixture
def config() -> TestConfig:
    """Supply a stock RateLimiter component configuration."""
    return {
        "CLIENT_ID": "long-term-archive",
        "CLIENT_SECRET": "hunter2",  # http://bash.org/?244321
        "COMPONENT_NAME": "testing-rate_limiter",
        "DEST_SITE": "NERSC",
        "INPUT_PATH": "/path/to/icecube/bundler/outbox",
        "INPUT_STATUS": "created",
        "LOG_LEVEL": "DEBUG",
        "LTA_AUTH_OPENID_URL": "localhost:12345",
        "LTA_REST_URL": "localhost:12347",
        "OUTPUT_PATH": "/path/to/icecube/replicator/inbox",
        "OUTPUT_QUOTA": "12094627905536",  # 11 TiB
        "OUTPUT_STATUS": "staged",
        "PROMETHEUS_METRICS_PORT": "8080",
        "RUN_ONCE_AND_DIE": "False",
        "RUN_UNTIL_NO_WORK": "False",
        "SOURCE_SITE": "WIPAC",
        "WORK_RETRIES": "3",
        "WORK_SLEEP_DURATION_SECONDS": "60",
        "WORK_TIMEOUT_SECONDS": "30",
    }


def test_constructor_config(config: TestConfig, mocker: MockerFixture) -> None:
    """Test that a RateLimiter can be constructed with a configuration object and a logging object."""
    logger_mock = mocker.MagicMock()
    p = RateLimiter(config, logger_mock)
    assert p.name == "testing-rate_limiter"
    assert p.dest_site == "NERSC"
    assert p.input_path == "/path/to/icecube/bundler/outbox"
    assert p.input_status == "created"
    assert p.lta_auth_openid_url == "localhost:12345"
    assert p.lta_rest_url == "localhost:12347"
    assert p.output_path == "/path/to/icecube/replicator/inbox"
    assert p.output_quota == 12094627905536
    assert p.output_status == "staged"
    assert not p.run_once_and_die
    assert p.source_site == "WIPAC"
    assert p.work_retries == 3
    assert p.work_sleep_duration_seconds == 60
    assert p.work_timeout_seconds == 30
    assert p.logger == logger_mock


def test_do_status(config: TestConfig, mocker: MockerFixture) -> None:
    """Verify that the RateLimiter has no additional state to offer."""
    logger_mock = mocker.MagicMock()
    p = RateLimiter(config, logger_mock)
    assert p._do_status() == {}


@pytest.mark.asyncio
async def test_rate_limiter_logs_configuration(mocker: MockerFixture) -> None:
    """Test to make sure the RateLimiter logs its configuration."""
    logger_mock = mocker.MagicMock()
    rate_limiter_config = {
        "CLIENT_ID": "long-term-archive",
        "CLIENT_SECRET": "hunter2",  # http://bash.org/?244321
        "COMPONENT_NAME": "logme-testing-rate_limiter",
        "DEST_SITE": "NERSC",
        "INPUT_PATH": "/path/to/icecube/bundler/outbox",
        "INPUT_STATUS": "created",
        "LOG_LEVEL": "DEBUG",
        "LTA_AUTH_OPENID_URL": "localhost:12345",
        "LTA_REST_URL": "logme-http://RmMNHdPhHpH2ZxfaFAC9d2jiIbf5pZiHDqy43rFLQiM.com/",
        "OUTPUT_PATH": "/path/to/icecube/replicator/inbox",
        "OUTPUT_QUOTA": "12094627905536",  # 11 TiB
        "OUTPUT_STATUS": "staged",
        "PROMETHEUS_METRICS_PORT": "8080",
        "RUN_ONCE_AND_DIE": "False",
        "RUN_UNTIL_NO_WORK": "False",
        "SOURCE_SITE": "WIPAC",
        "WORK_RETRIES": "5",
        "WORK_SLEEP_DURATION_SECONDS": "70",
        "WORK_TIMEOUT_SECONDS": "90",
    }
    RateLimiter(rate_limiter_config, logger_mock)
    EXPECTED_LOGGER_CALLS = [
        call("rate_limiter 'logme-testing-rate_limiter' is configured:"),
        call('CLIENT_ID = long-term-archive'),
        call('CLIENT_SECRET = [秘密]'),
        call('COMPONENT_NAME = logme-testing-rate_limiter'),
        call('DEST_SITE = NERSC'),
        call('INPUT_PATH = /path/to/icecube/bundler/outbox'),
        call('INPUT_STATUS = created'),
        call('LOG_LEVEL = DEBUG'),
        call('LTA_AUTH_OPENID_URL = localhost:12345'),
        call('LTA_REST_URL = logme-http://RmMNHdPhHpH2ZxfaFAC9d2jiIbf5pZiHDqy43rFLQiM.com/'),
        call('OUTPUT_PATH = /path/to/icecube/replicator/inbox'),
        call('OUTPUT_QUOTA = 12094627905536'),
        call('OUTPUT_STATUS = staged'),
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
async def test_script_main_sync(config: TestConfig, mocker: MockerFixture, monkeypatch: MonkeyPatch) -> None:
    """
    Verify RateLimiter component behavior when run as a script.

    Test to make sure running the RateLimiter as a script does the setup work
    that we expect and then launches the rate_limiter service.
    """
    for key in config.keys():
        monkeypatch.setenv(key, config[key])
    mock_run = mocker.patch("asyncio.run")
    mock_main = mocker.patch("lta.rate_limiter.main")
    mock_shs = mocker.patch("lta.rate_limiter.start_http_server")
    main_sync()
    mock_shs.assert_called()
    mock_main.assert_called()
    mock_run.assert_called()
    await mock_run.call_args.args[0]


@pytest.mark.asyncio
async def test_rate_limiter_run(config: TestConfig, mocker: MockerFixture) -> None:
    """Test the RateLimiter does the work the rate_limiter should do."""
    logger_mock = mocker.MagicMock()
    p = RateLimiter(config, logger_mock)
    p._do_work = AsyncMock()  # type: ignore[method-assign]
    await p.run()
    p._do_work.assert_called()


@pytest.mark.asyncio
async def test_rate_limiter_run_exception(config: TestConfig, mocker: MockerFixture) -> None:
    """Test an error doesn't kill the RateLimiter."""
    logger_mock = mocker.MagicMock()
    p = RateLimiter(config, logger_mock)
    p.last_work_end_timestamp = ""
    p._do_work = AsyncMock()  # type: ignore[method-assign]
    p._do_work.side_effect = [Exception("bad thing happen!")]
    await p.run()
    p._do_work.assert_called()
    assert p.last_work_end_timestamp


@pytest.mark.asyncio
async def test_rate_limiter_do_work_pop_exception(config: TestConfig, mocker: MockerFixture) -> None:
    """Test that _do_work raises when the RestClient can't pop."""
    logger_mock = mocker.MagicMock()
    lta_rc_mock = AsyncMock()
    lta_rc_mock.request = AsyncMock()
    lta_rc_mock.request.side_effect = HTTPError(500, "LTA DB on fire. Again.")
    p = RateLimiter(config, logger_mock)
    with pytest.raises(HTTPError):
        await p._do_work(lta_rc_mock)
    lta_rc_mock.request.assert_called_with("POST", '/Bundles/actions/pop?source=WIPAC&dest=NERSC&status=created', {'claimant': f'{p.name}-{p.instance_uuid}'})


@pytest.mark.asyncio
async def test_rate_limiter_do_work_no_results(config: TestConfig, mocker: MockerFixture) -> None:
    """Test that _do_work goes on vacation when the LTA DB has no work."""
    logger_mock = mocker.MagicMock()
    dwc_mock = mocker.patch("lta.rate_limiter.RateLimiter._do_work_claim", new_callable=AsyncMock)
    dwc_mock.return_value = False
    p = RateLimiter(config, logger_mock)
    await p._do_work(AsyncMock())
    dwc_mock.assert_called()


@pytest.mark.asyncio
async def test_rate_limiter_do_work_yes_results(config: TestConfig, mocker: MockerFixture) -> None:
    """Test that _do_work keeps working until the LTA DB has no work."""
    logger_mock = mocker.MagicMock()
    dwc_mock = mocker.patch("lta.rate_limiter.RateLimiter._do_work_claim", new_callable=AsyncMock)
    dwc_mock.side_effect = [True, True, False]
    p = RateLimiter(config, logger_mock)
    await p._do_work(AsyncMock())
    dwc_mock.assert_called()


@pytest.mark.asyncio
async def test_rate_limiter_do_work_claim_no_result(config: TestConfig, mocker: MockerFixture) -> None:
    """Test that _do_work_claim does not work when the LTA DB has no work."""
    logger_mock = mocker.MagicMock()
    lta_rc_mock = AsyncMock()
    lta_rc_mock.request = AsyncMock()
    lta_rc_mock.request.return_value = {
        "bundle": None
    }
    sb_mock = mocker.patch("lta.rate_limiter.RateLimiter._stage_bundle", new_callable=AsyncMock)
    p = RateLimiter(config, logger_mock)
    await p._do_work_claim(lta_rc_mock)
    lta_rc_mock.request.assert_called_with("POST", '/Bundles/actions/pop?source=WIPAC&dest=NERSC&status=created', {'claimant': f'{p.name}-{p.instance_uuid}'})
    sb_mock.assert_not_called()


@pytest.mark.asyncio
async def test_rate_limiter_do_work_claim_yes_result(config: TestConfig, mocker: MockerFixture) -> None:
    """Test that _do_work_claim processes the Bundle that it gets from the LTA DB."""
    logger_mock = mocker.MagicMock()
    lta_rc_mock = AsyncMock()
    lta_rc_mock.request = AsyncMock()
    lta_rc_mock.request.return_value = {
        "bundle": {
            "one": 1,
        },
    }
    sb_mock = mocker.patch("lta.rate_limiter.RateLimiter._stage_bundle", new_callable=AsyncMock)
    p = RateLimiter(config, logger_mock)
    assert not await p._do_work_claim(lta_rc_mock)
    lta_rc_mock.request.assert_called_with("POST", '/Bundles/actions/pop?source=WIPAC&dest=NERSC&status=created', {'claimant': f'{p.name}-{p.instance_uuid}'})
    sb_mock.assert_called_with(mocker.ANY, {"one": 1})


@pytest.mark.asyncio
async def test_rate_limiter_stage_bundle_raises(config: TestConfig, mocker: MockerFixture) -> None:
    """Test that _do_work_claim both calls _quarantine_bundle and re-raises when _stage_bundle raises."""
    logger_mock = mocker.MagicMock()
    lta_rc_mock = AsyncMock()
    lta_rc_mock.request = AsyncMock()
    lta_rc_mock.request.return_value = {
        "bundle": {
            "one": 1,
        },
    }
    sb_mock = mocker.patch("lta.rate_limiter.RateLimiter._stage_bundle", new_callable=AsyncMock)
    qb_mock = mocker.patch("lta.rate_limiter.RateLimiter._quarantine_bundle", new_callable=AsyncMock)
    sb_mock.side_effect = Exception("LTA DB unavailable; currently safer at home")
    p = RateLimiter(config, logger_mock)
    with pytest.raises(Exception):
        await p._do_work_claim(lta_rc_mock)
    lta_rc_mock.request.assert_called_with("POST", '/Bundles/actions/pop?source=WIPAC&dest=NERSC&status=created', {'claimant': f'{p.name}-{p.instance_uuid}'})
    sb_mock.assert_called_with(mocker.ANY, {"one": 1})
    qb_mock.assert_called_with(mocker.ANY, {"one": 1}, "LTA DB unavailable; currently safer at home")


@pytest.mark.asyncio
async def test_rate_limiter_stage_bundle(config: TestConfig, mocker: MockerFixture) -> None:
    """Test that _stage_bundle attempts to stage a Bundle."""
    logger_mock = mocker.MagicMock()
    lta_rc_mock = mocker.patch("rest_tools.client.RestClient", new_callable=AsyncMock)
    move_mock = mocker.patch("shutil.move", new_callable=MagicMock)
    gfas_mock = mocker.patch("lta.lta_cmd._get_files_and_size", new_callable=MagicMock)
    gfas_mock.return_value = ([], 0)
    p = RateLimiter(config, logger_mock)
    await p._stage_bundle(lta_rc_mock, {
        "uuid": "c4b345e4-2395-4f9e-b0eb-9cc1c9cdf003",
        "bundle_path": "/icecube/datawarehouse/path/to/c4b345e4-2395-4f9e-b0eb-9cc1c9cdf003.zip",
        "size": 536870912000,
    })
    move_mock.assert_called()
    lta_rc_mock.request.assert_called_with("PATCH", "/Bundles/c4b345e4-2395-4f9e-b0eb-9cc1c9cdf003", mocker.ANY)


@pytest.mark.asyncio
async def test_rate_limiter_stage_bundle_over_quota(config: TestConfig, mocker: MockerFixture) -> None:
    """Test that _stage_bundle attempts to unclaim a Bundle when over quota."""
    logger_mock = mocker.MagicMock()
    lta_rc_mock = mocker.patch("rest_tools.client.RestClient", new_callable=AsyncMock)
    move_mock = mocker.patch("shutil.move", new_callable=MagicMock)
    gfas_mock = mocker.patch("lta.rate_limiter.RateLimiter._get_files_and_size", new_callable=MagicMock)
    gfas_mock.return_value = (["/path/to/one/file.zip"], 11826192449536)
    ub_mock = mocker.patch("lta.rate_limiter.RateLimiter._unclaim_bundle", new_callable=AsyncMock)
    p = RateLimiter(config, logger_mock)
    await p._stage_bundle(lta_rc_mock, {
        "uuid": "c4b345e4-2395-4f9e-b0eb-9cc1c9cdf003",
        "bundle_path": "/icecube/datawarehouse/path/to/c4b345e4-2395-4f9e-b0eb-9cc1c9cdf003.zip",
        "size": 536870912000,
    })
    gfas_mock.assert_called()
    ub_mock.assert_called_with(lta_rc_mock, {
        "uuid": "c4b345e4-2395-4f9e-b0eb-9cc1c9cdf003",
        "bundle_path": "/icecube/datawarehouse/path/to/c4b345e4-2395-4f9e-b0eb-9cc1c9cdf003.zip",
        "size": 536870912000,
    })
    move_mock.assert_not_called()


@pytest.mark.asyncio
async def test_rate_limiter_quarantine_bundle_with_reason(config: TestConfig, mocker: MockerFixture) -> None:
    """Test that _do_work_claim attempts to quarantine a Bundle that fails to get deleted."""
    logger_mock = mocker.MagicMock()
    lta_rc_mock = mocker.patch("rest_tools.client.RestClient", new_callable=AsyncMock)
    p = RateLimiter(config, logger_mock)
    await p._quarantine_bundle(
        lta_rc_mock,
        {"uuid": "c4b345e4-2395-4f9e-b0eb-9cc1c9cdf003", "status": "created"},
        "Rucio caught fire, then we roasted marshmellows."
    )
    lta_rc_mock.request.assert_called_with("PATCH", "/Bundles/c4b345e4-2395-4f9e-b0eb-9cc1c9cdf003", mocker.ANY)


@pytest.mark.asyncio
async def test_rate_limiter_unclaim_bundle(config: TestConfig, mocker: MockerFixture) -> None:
    """Test that _unclaim_bundle attempts to update the LTA DB."""
    logger_mock = mocker.MagicMock()
    lta_rc_mock = mocker.patch("rest_tools.client.RestClient", new_callable=AsyncMock)
    p = RateLimiter(config, logger_mock)
    await p._unclaim_bundle(lta_rc_mock, {"uuid": "c4b345e4-2395-4f9e-b0eb-9cc1c9cdf003"})
    lta_rc_mock.request.assert_called_with("PATCH", "/Bundles/c4b345e4-2395-4f9e-b0eb-9cc1c9cdf003", mocker.ANY)


def test_get_files_and_size_with_ignore_bad_files(config: TestConfig, mocker: MockerFixture) -> None:
    """Test that missing files will not stop the measurement process."""
    BUNDLES_IN_DESTINATION_DIRECTORY = [
        "/path/to/destination/directory/bundle1.zip",
        "/path/to/destination/directory/bundle2.zip",
    ]
    logger_mock = mocker.MagicMock()
    ep_mock = mocker.patch("lta.rate_limiter.RateLimiter._enumerate_path", new_callable=MagicMock)
    ep_mock.return_value = BUNDLES_IN_DESTINATION_DIRECTORY
    gs_mock = mocker.patch("os.path.getsize", new_callable=MagicMock)
    gs_mock.side_effect = [
        Exception("bundle1.zip is sold out!"),
        123_456_789,
    ]
    p = RateLimiter(config, logger_mock)
    assert p._get_files_and_size("/path/to/destination/directory") == (BUNDLES_IN_DESTINATION_DIRECTORY, 123_456_789)
