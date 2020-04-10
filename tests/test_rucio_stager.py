# test_rucio_stager.py
"""Unit tests for lta/rucio_stager.py."""

from unittest.mock import call, MagicMock

import pytest  # type: ignore
from tornado.web import HTTPError  # type: ignore

from lta.rucio_stager import main, RucioStager
from .test_util import AsyncMock

@pytest.fixture
def config():
    """Supply a stock RucioStager component configuration."""
    return {
        "BUNDLER_OUTBOX_PATH": "/path/to/icecube/bundler/outbox",
        "COMPONENT_NAME": "testing-rucio_stager",
        "DEST_QUOTA": "12094627905536",  # 11 TiB
        "DEST_SITE": "NERSC",
        "HEARTBEAT_PATCH_RETRIES": "3",
        "HEARTBEAT_PATCH_TIMEOUT_SECONDS": "30",
        "HEARTBEAT_SLEEP_DURATION_SECONDS": "60",
        "LTA_REST_TOKEN": "fake-lta-rest-token",
        "LTA_REST_URL": "http://RmMNHdPhHpH2ZxfaFAC9d2jiIbf5pZiHDqy43rFLQiM.com/",
        "RUCIO_INBOX_PATH": "/path/to/icecube/rucio/inbox",
        "RUN_ONCE_AND_DIE": "False",
        "SOURCE_SITE": "WIPAC",
        "WORK_RETRIES": "3",
        "WORK_SLEEP_DURATION_SECONDS": "60",
        "WORK_TIMEOUT_SECONDS": "30",
    }

def test_constructor_config(config, mocker):
    """Test that a RucioStager can be constructed with a configuration object and a logging object."""
    logger_mock = mocker.MagicMock()
    p = RucioStager(config, logger_mock)
    assert p.name == "testing-rucio_stager"
    assert p.bundler_outbox_path == "/path/to/icecube/bundler/outbox"
    assert p.dest_quota == 12094627905536
    assert p.dest_site == "NERSC"
    assert p.heartbeat_patch_retries == 3
    assert p.heartbeat_patch_timeout_seconds == 30
    assert p.heartbeat_sleep_duration_seconds == 60
    assert p.lta_rest_token == "fake-lta-rest-token"
    assert p.lta_rest_url == "http://RmMNHdPhHpH2ZxfaFAC9d2jiIbf5pZiHDqy43rFLQiM.com/"
    assert p.rucio_inbox_path == "/path/to/icecube/rucio/inbox"
    assert not p.run_once_and_die
    assert p.source_site == "WIPAC"
    assert p.work_retries == 3
    assert p.work_sleep_duration_seconds == 60
    assert p.work_timeout_seconds == 30
    assert p.logger == logger_mock


def test_do_status(config, mocker):
    """Verify that the RucioStager has no additional state to offer."""
    logger_mock = mocker.MagicMock()
    p = RucioStager(config, logger_mock)
    assert p._do_status() == {}

@pytest.mark.asyncio
async def test_rucio_stager_logs_configuration(mocker):
    """Test to make sure the RucioStager logs its configuration."""
    logger_mock = mocker.MagicMock()
    rucio_stager_config = {
        "BUNDLER_OUTBOX_PATH": "/path/to/icecube/bundler/outbox",
        "COMPONENT_NAME": "logme-testing-rucio_stager",
        "DEST_QUOTA": "12094627905536",  # 11 TiB
        "DEST_SITE": "NERSC",
        "HEARTBEAT_PATCH_RETRIES": "1",
        "HEARTBEAT_PATCH_TIMEOUT_SECONDS": "20",
        "HEARTBEAT_SLEEP_DURATION_SECONDS": "30",
        "LTA_REST_TOKEN": "logme-fake-lta-rest-token",
        "LTA_REST_URL": "logme-http://zjwdm5ggeEgS1tZDZy9l1DOZU53uiSO4Urmyb8xL0.com/",
        "RUCIO_INBOX_PATH": "/path/to/icecube/rucio/inbox",
        "RUN_ONCE_AND_DIE": "False",
        "SOURCE_SITE": "WIPAC",
        "WORK_RETRIES": "5",
        "WORK_SLEEP_DURATION_SECONDS": "70",
        "WORK_TIMEOUT_SECONDS": "90",
    }
    RucioStager(rucio_stager_config, logger_mock)
    EXPECTED_LOGGER_CALLS = [
        call("rucio_stager 'logme-testing-rucio_stager' is configured:"),
        call('BUNDLER_OUTBOX_PATH = /path/to/icecube/bundler/outbox'),
        call('COMPONENT_NAME = logme-testing-rucio_stager'),
        call('DEST_QUOTA = 12094627905536'),
        call('DEST_SITE = NERSC'),
        call('HEARTBEAT_PATCH_RETRIES = 1'),
        call('HEARTBEAT_PATCH_TIMEOUT_SECONDS = 20'),
        call('HEARTBEAT_SLEEP_DURATION_SECONDS = 30'),
        call('LTA_REST_TOKEN = logme-fake-lta-rest-token'),
        call('LTA_REST_URL = logme-http://zjwdm5ggeEgS1tZDZy9l1DOZU53uiSO4Urmyb8xL0.com/'),
        call('RUCIO_INBOX_PATH = /path/to/icecube/rucio/inbox'),
        call('RUN_ONCE_AND_DIE = False'),
        call('SOURCE_SITE = WIPAC'),
        call('WORK_RETRIES = 5'),
        call('WORK_SLEEP_DURATION_SECONDS = 70'),
        call('WORK_TIMEOUT_SECONDS = 90')
    ]
    logger_mock.info.assert_has_calls(EXPECTED_LOGGER_CALLS)

@pytest.mark.asyncio
async def test_script_main(config, mocker, monkeypatch):
    """
    Verify RucioStager component behavior when run as a script.

    Test to make sure running the RucioStager as a script does the setup work
    that we expect and then launches the rucio_stager service.
    """
    for key in config.keys():
        monkeypatch.setenv(key, config[key])
    mock_event_loop = mocker.patch("asyncio.get_event_loop")
    mock_root_logger = mocker.patch("logging.getLogger")
    mock_status_loop = mocker.patch("lta.rucio_stager.status_loop")
    mock_work_loop = mocker.patch("lta.rucio_stager.work_loop")
    main()
    mock_event_loop.assert_called()
    mock_root_logger.assert_called()
    mock_status_loop.assert_called()
    mock_work_loop.assert_called()

@pytest.mark.asyncio
async def test_rucio_stager_run(config, mocker):
    """Test the RucioStager does the work the rucio_stager should do."""
    logger_mock = mocker.MagicMock()
    p = RucioStager(config, logger_mock)
    p._do_work = AsyncMock()
    await p.run()
    p._do_work.assert_called()

@pytest.mark.asyncio
async def test_rucio_stager_run_exception(config, mocker):
    """Test an error doesn't kill the RucioStager."""
    logger_mock = mocker.MagicMock()
    p = RucioStager(config, logger_mock)
    p.last_work_end_timestamp = None
    p._do_work = AsyncMock()
    p._do_work.side_effect = [Exception("bad thing happen!")]
    await p.run()
    p._do_work.assert_called()
    assert p.last_work_end_timestamp

@pytest.mark.asyncio
async def test_rucio_stager_do_work_pop_exception(config, mocker):
    """Test that _do_work raises when the RestClient can't pop."""
    logger_mock = mocker.MagicMock()
    lta_rc_mock = mocker.patch("rest_tools.client.RestClient.request", new_callable=AsyncMock)
    lta_rc_mock.side_effect = HTTPError(500, "LTA DB on fire. Again.")
    p = RucioStager(config, logger_mock)
    with pytest.raises(HTTPError):
        await p._do_work()
    lta_rc_mock.assert_called_with("POST", '/Bundles/actions/pop?dest=NERSC&status=created', {'claimant': f'{p.name}-{p.instance_uuid}'})

@pytest.mark.asyncio
async def test_rucio_stager_do_work_no_results(config, mocker):
    """Test that _do_work goes on vacation when the LTA DB has no work."""
    logger_mock = mocker.MagicMock()
    dwc_mock = mocker.patch("lta.rucio_stager.RucioStager._do_work_claim", new_callable=AsyncMock)
    dwc_mock.return_value = False
    p = RucioStager(config, logger_mock)
    await p._do_work()
    dwc_mock.assert_called()

@pytest.mark.asyncio
async def test_rucio_stager_do_work_yes_results(config, mocker):
    """Test that _do_work keeps working until the LTA DB has no work."""
    logger_mock = mocker.MagicMock()
    dwc_mock = mocker.patch("lta.rucio_stager.RucioStager._do_work_claim", new_callable=AsyncMock)
    dwc_mock.side_effect = [True, True, False]
    p = RucioStager(config, logger_mock)
    await p._do_work()
    dwc_mock.assert_called()

@pytest.mark.asyncio
async def test_rucio_stager_do_work_claim_no_result(config, mocker):
    """Test that _do_work_claim does not work when the LTA DB has no work."""
    logger_mock = mocker.MagicMock()
    lta_rc_mock = mocker.patch("rest_tools.client.RestClient.request", new_callable=AsyncMock)
    lta_rc_mock.return_value = {
        "bundle": None
    }
    sb_mock = mocker.patch("lta.rucio_stager.RucioStager._stage_bundle", new_callable=AsyncMock)
    p = RucioStager(config, logger_mock)
    await p._do_work_claim()
    lta_rc_mock.assert_called_with("POST", '/Bundles/actions/pop?dest=NERSC&status=created', {'claimant': f'{p.name}-{p.instance_uuid}'})
    sb_mock.assert_not_called()

@pytest.mark.asyncio
async def test_rucio_stager_do_work_claim_yes_result(config, mocker):
    """Test that _do_work_claim processes the Bundle that it gets from the LTA DB."""
    logger_mock = mocker.MagicMock()
    lta_rc_mock = mocker.patch("rest_tools.client.RestClient.request", new_callable=AsyncMock)
    lta_rc_mock.return_value = {
        "bundle": {
            "one": 1,
        },
    }
    sb_mock = mocker.patch("lta.rucio_stager.RucioStager._stage_bundle", new_callable=AsyncMock)
    p = RucioStager(config, logger_mock)
    assert not await p._do_work_claim()
    lta_rc_mock.assert_called_with("POST", '/Bundles/actions/pop?dest=NERSC&status=created', {'claimant': f'{p.name}-{p.instance_uuid}'})
    sb_mock.assert_called_with(mocker.ANY, {"one": 1})

@pytest.mark.asyncio
async def test_rucio_stager_stage_bundle_raises(config, mocker):
    """Test that _do_work_claim both calls _quarantine_bundle and re-raises when _stage_bundle raises."""
    logger_mock = mocker.MagicMock()
    lta_rc_mock = mocker.patch("rest_tools.client.RestClient.request", new_callable=AsyncMock)
    lta_rc_mock.return_value = {
        "bundle": {
            "one": 1,
        },
    }
    sb_mock = mocker.patch("lta.rucio_stager.RucioStager._stage_bundle", new_callable=AsyncMock)
    qb_mock = mocker.patch("lta.rucio_stager.RucioStager._quarantine_bundle", new_callable=AsyncMock)
    sb_mock.side_effect = Exception("LTA DB unavailable; currently safer at home")
    p = RucioStager(config, logger_mock)
    with pytest.raises(Exception):
        await p._do_work_claim()
    lta_rc_mock.assert_called_with("POST", '/Bundles/actions/pop?dest=NERSC&status=created', {'claimant': f'{p.name}-{p.instance_uuid}'})
    sb_mock.assert_called_with(mocker.ANY, {"one": 1})
    qb_mock.assert_called_with(mocker.ANY, {"one": 1}, "LTA DB unavailable; currently safer at home")

@pytest.mark.asyncio
async def test_rucio_stager_stage_bundle(config, mocker):
    """Test that _stage_bundle attempts to stage a Bundle."""
    logger_mock = mocker.MagicMock()
    lta_rc_mock = mocker.patch("rest_tools.client.RestClient", new_callable=AsyncMock)
    move_mock = mocker.patch("shutil.move", new_callable=MagicMock)
    gfas_mock = mocker.patch("lta.lta_cmd._get_files_and_size", new_callable=MagicMock)
    gfas_mock.return_value = ([], 0)
    p = RucioStager(config, logger_mock)
    await p._stage_bundle(lta_rc_mock, {
        "uuid": "c4b345e4-2395-4f9e-b0eb-9cc1c9cdf003",
        "bundle_path": "/icecube/datawarehouse/path/to/c4b345e4-2395-4f9e-b0eb-9cc1c9cdf003.zip",
        "size": 536870912000,
    })
    move_mock.assert_called()
    lta_rc_mock.request.assert_called_with("PATCH", "/Bundles/c4b345e4-2395-4f9e-b0eb-9cc1c9cdf003", mocker.ANY)

@pytest.mark.asyncio
async def test_rucio_stager_stage_bundle_over_quota(config, mocker):
    """Test that _stage_bundle attempts to unclaim a Bundle when over quota."""
    logger_mock = mocker.MagicMock()
    lta_rc_mock = mocker.patch("rest_tools.client.RestClient", new_callable=AsyncMock)
    move_mock = mocker.patch("shutil.move", new_callable=MagicMock)
    gfas_mock = mocker.patch("lta.rucio_stager._get_files_and_size", new_callable=MagicMock)
    gfas_mock.return_value = (["/path/to/one/file.zip"], 11826192449536)
    ub_mock = mocker.patch("lta.rucio_stager.RucioStager._unclaim_bundle", new_callable=AsyncMock)
    p = RucioStager(config, logger_mock)
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
async def test_rucio_stager_quarantine_bundle_with_reason(config, mocker):
    """Test that _do_work_claim attempts to quarantine a Bundle that fails to get deleted."""
    logger_mock = mocker.MagicMock()
    lta_rc_mock = mocker.patch("rest_tools.client.RestClient", new_callable=AsyncMock)
    p = RucioStager(config, logger_mock)
    await p._quarantine_bundle(lta_rc_mock, {"uuid": "c4b345e4-2395-4f9e-b0eb-9cc1c9cdf003"}, "Rucio caught fire, then we roasted marshmellows.")
    lta_rc_mock.request.assert_called_with("PATCH", "/Bundles/c4b345e4-2395-4f9e-b0eb-9cc1c9cdf003", mocker.ANY)

@pytest.mark.asyncio
async def test_rucio_stager_unclaim_bundle(config, mocker):
    """Test that _unclaim_bundle attempts to update the LTA DB."""
    logger_mock = mocker.MagicMock()
    lta_rc_mock = mocker.patch("rest_tools.client.RestClient", new_callable=AsyncMock)
    p = RucioStager(config, logger_mock)
    await p._unclaim_bundle(lta_rc_mock, {"uuid": "c4b345e4-2395-4f9e-b0eb-9cc1c9cdf003"})
    lta_rc_mock.request.assert_called_with("PATCH", "/Bundles/c4b345e4-2395-4f9e-b0eb-9cc1c9cdf003", mocker.ANY)
