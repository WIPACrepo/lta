# test_desy_move_verifier.py
"""Unit tests for lta/desy_move_verifier.py."""

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
from unittest.mock import AsyncMock, call

import pytest
from pytest import MonkeyPatch
from pytest_mock import MockerFixture
from tornado.web import HTTPError

from lta.desy_move_verifier import main_sync, DesyMoveVerifier

TestConfig = Dict[str, str]


@pytest.fixture
def config() -> TestConfig:
    """Supply a stock DesyMoveVerifier component configuration."""
    return {
        "CLIENT_ID": "long-term-archive",
        "CLIENT_SECRET": "hunter2",  # http://bash.org/?244321
        "COMPONENT_NAME": "testing-desy_move_verifier",
        "DEST_SITE": "DESY",
        "GRIDFTP_DEST_URL": "gsiftp://icecube.wisc.edu:7654/path/to/nowhere",
        "GRIDFTP_TIMEOUT": "1200",
        "INPUT_STATUS": "transferring",
        "LOG_LEVEL": "DEBUG",
        "LTA_AUTH_OPENID_URL": "localhost:12345",
        "LTA_REST_URL": "localhost:12347",
        "OUTPUT_STATUS": "taping",
        "PROMETHEUS_METRICS_PORT": "8080",
        "RUN_ONCE_AND_DIE": "False",
        "RUN_UNTIL_NO_WORK": "False",
        "SOURCE_SITE": "WIPAC",
        "TRANSFER_CONFIG_PATH": "examples/rucio.json",
        "WORK_RETRIES": "3",
        "WORK_SLEEP_DURATION_SECONDS": "60",
        "WORK_TIMEOUT_SECONDS": "30",
        "WORKBOX_PATH": "/path/to/some/temp/directory",
    }


def test_constructor_config(config: TestConfig, mocker: MockerFixture) -> None:
    """Test that a DesyMoveVerifier can be constructed with a configuration object and a logging object."""
    logger_mock = mocker.MagicMock()
    p = DesyMoveVerifier(config, logger_mock)
    assert p.name == "testing-desy_move_verifier"
    assert p.dest_site == "DESY"
    assert p.gridftp_dest_url == "gsiftp://icecube.wisc.edu:7654/path/to/nowhere"
    assert p.gridftp_timeout == 1200
    assert p.lta_auth_openid_url == "localhost:12345"
    assert p.lta_rest_url == "localhost:12347"
    assert p.output_status == "taping"
    assert p.source_site == "WIPAC"
    assert p.work_retries == 3
    assert p.work_sleep_duration_seconds == 60
    assert p.work_timeout_seconds == 30
    assert p.logger == logger_mock
    assert p.workbox_path == "/path/to/some/temp/directory"


def test_do_status(config: TestConfig, mocker: MockerFixture) -> None:
    """Verify that the DesyMoveVerifier has additional state to offer."""
    logger_mock = mocker.MagicMock()
    p = DesyMoveVerifier(config, logger_mock)
    assert p._do_status() == {}


@pytest.mark.asyncio
async def test_desy_move_verifier_logs_configuration(mocker: MockerFixture) -> None:
    """Test to make sure the DesyMoveVerifier logs its configuration."""
    logger_mock = mocker.MagicMock()
    desy_move_verifier_config = {
        "CLIENT_ID": "long-term-archive",
        "CLIENT_SECRET": "hunter2",  # http://bash.org/?244321
        "COMPONENT_NAME": "logme-testing-desy_move_verifier",
        "DEST_SITE": "DESY",
        "GRIDFTP_DEST_URL": "gsiftp://icecube.wisc.edu:7654/path/to/nowhere",
        "GRIDFTP_TIMEOUT": "1200",
        "INPUT_STATUS": "transferring",
        "LOG_LEVEL": "DEBUG",
        "LTA_AUTH_OPENID_URL": "localhost:12345",
        "LTA_REST_URL": "localhost:12347",
        "OUTPUT_STATUS": "taping",
        "PROMETHEUS_METRICS_PORT": "8080",
        "RUN_ONCE_AND_DIE": "False",
        "RUN_UNTIL_NO_WORK": "False",
        "SOURCE_SITE": "WIPAC",
        "WORK_RETRIES": "5",
        "WORK_SLEEP_DURATION_SECONDS": "70",
        "WORK_TIMEOUT_SECONDS": "90",
        "WORKBOX_PATH": "/path/to/some/temp/directory",
    }
    DesyMoveVerifier(desy_move_verifier_config, logger_mock)
    EXPECTED_LOGGER_CALLS = [
        call("desy_move_verifier 'logme-testing-desy_move_verifier' is configured:"),
        call('CLIENT_ID = long-term-archive'),
        call('CLIENT_SECRET = [秘密]'),
        call('COMPONENT_NAME = logme-testing-desy_move_verifier'),
        call('DEST_SITE = DESY'),
        call('GRIDFTP_DEST_URL = gsiftp://icecube.wisc.edu:7654/path/to/nowhere'),
        call('GRIDFTP_TIMEOUT = 1200'),
        call('INPUT_STATUS = transferring'),
        call('LOG_LEVEL = DEBUG'),
        call('LTA_AUTH_OPENID_URL = localhost:12345'),
        call('LTA_REST_URL = localhost:12347'),
        call('OUTPUT_STATUS = taping'),
        call('PROMETHEUS_METRICS_PORT = 8080'),
        call('RUN_ONCE_AND_DIE = False'),
        call('RUN_UNTIL_NO_WORK = False'),
        call('SOURCE_SITE = WIPAC'),
        call('WORK_RETRIES = 5'),
        call('WORK_SLEEP_DURATION_SECONDS = 70'),
        call('WORK_TIMEOUT_SECONDS = 90'),
        call('WORKBOX_PATH = /path/to/some/temp/directory'),
    ]
    logger_mock.info.assert_has_calls(EXPECTED_LOGGER_CALLS)


@pytest.mark.asyncio
async def test_script_main_sync(config: TestConfig, mocker: MockerFixture, monkeypatch: MonkeyPatch) -> None:
    """
    Verify DesyMoveVerifier component behavior when run as a script.

    Test to make sure running the DesyMoveVerifier as a script does the setup work
    that we expect and then launches the desy_move_verifier service.
    """
    for key in config.keys():
        monkeypatch.setenv(key, config[key])
    mock_run = mocker.patch("asyncio.run")
    mock_main = mocker.patch("lta.desy_move_verifier.main")
    mock_shs = mocker.patch("lta.desy_move_verifier.start_http_server")
    main_sync()
    mock_shs.assert_called()
    mock_main.assert_called()
    mock_run.assert_called()
    await mock_run.call_args.args[0]


@pytest.mark.asyncio
async def test_desy_move_verifier_run(config: TestConfig, mocker: MockerFixture) -> None:
    """Test the DesyMoveVerifier does the work the desy_move_verifier should do."""
    logger_mock = mocker.MagicMock()
    p = DesyMoveVerifier(config, logger_mock)
    p._do_work = AsyncMock()  # type: ignore[method-assign]
    await p.run()
    p._do_work.assert_called()


@pytest.mark.asyncio
async def test_desy_move_verifier_run_exception(config: TestConfig, mocker: MockerFixture) -> None:
    """Test an error doesn't kill the DesyMoveVerifier."""
    logger_mock = mocker.MagicMock()
    p = DesyMoveVerifier(config, logger_mock)
    p.last_work_end_timestamp = ""
    p._do_work = AsyncMock()  # type: ignore[method-assign]
    p._do_work.side_effect = [Exception("bad thing happen!")]
    await p.run()
    p._do_work.assert_called()
    assert p.last_work_end_timestamp


@pytest.mark.asyncio
async def test_desy_move_verifier_do_work_pop_exception(config: TestConfig, mocker: MockerFixture) -> None:
    """Test that _do_work raises when the RestClient can't pop."""
    logger_mock = mocker.MagicMock()
    lta_rc_mock = mocker.patch("rest_tools.client.RestClient.request", new_callable=AsyncMock)
    lta_rc_mock.side_effect = HTTPError(500, "LTA DB on fire. Again.")
    p = DesyMoveVerifier(config, logger_mock)
    with pytest.raises(HTTPError):
        await p._do_work()
    lta_rc_mock.assert_called_with("POST", '/Bundles/actions/pop?source=WIPAC&dest=DESY&status=transferring', {'claimant': f'{p.name}-{p.instance_uuid}'})


@pytest.mark.asyncio
async def test_desy_move_verifier_do_work_no_results(config: TestConfig, mocker: MockerFixture) -> None:
    """Test that _do_work goes on vacation when the LTA DB has no work."""
    logger_mock = mocker.MagicMock()
    dwc_mock = mocker.patch("lta.desy_move_verifier.DesyMoveVerifier._do_work_claim", new_callable=AsyncMock)
    dwc_mock.return_value = False
    p = DesyMoveVerifier(config, logger_mock)
    await p._do_work()
    dwc_mock.assert_called()


@pytest.mark.asyncio
async def test_desy_move_verifier_do_work_yes_results(config: TestConfig, mocker: MockerFixture) -> None:
    """Test that _do_work keeps working until the LTA DB has no work."""
    logger_mock = mocker.MagicMock()
    dwc_mock = mocker.patch("lta.desy_move_verifier.DesyMoveVerifier._do_work_claim", new_callable=AsyncMock)
    dwc_mock.side_effect = [True, True, False]
    p = DesyMoveVerifier(config, logger_mock)
    await p._do_work()
    dwc_mock.assert_called()


@pytest.mark.asyncio
async def test_desy_move_verifier_do_work_claim_no_result(config: TestConfig, mocker: MockerFixture) -> None:
    """Test that _do_work_claim does not work when the LTA DB has no work."""
    logger_mock = mocker.MagicMock()
    lta_rc_mock = mocker.patch("rest_tools.client.RestClient.request", new_callable=AsyncMock)
    lta_rc_mock.return_value = {
        "bundle": None
    }
    vb_mock = mocker.patch("lta.desy_move_verifier.DesyMoveVerifier._verify_bundle", new_callable=AsyncMock)
    p = DesyMoveVerifier(config, logger_mock)
    await p._do_work_claim()
    lta_rc_mock.assert_called_with("POST", '/Bundles/actions/pop?source=WIPAC&dest=DESY&status=transferring', {'claimant': f'{p.name}-{p.instance_uuid}'})
    vb_mock.assert_not_called()


@pytest.mark.asyncio
async def test_desy_move_verifier_do_work_claim_yes_result(config: TestConfig, mocker: MockerFixture) -> None:
    """Test that _do_work_claim processes the Bundle that it gets from the LTA DB."""
    logger_mock = mocker.MagicMock()
    lta_rc_mock = mocker.patch("rest_tools.client.RestClient.request", new_callable=AsyncMock)
    lta_rc_mock.return_value = {
        "bundle": {
            "one": 1,
        },
    }
    vb_mock = mocker.patch("lta.desy_move_verifier.DesyMoveVerifier._verify_bundle", new_callable=AsyncMock)
    p = DesyMoveVerifier(config, logger_mock)
    assert await p._do_work_claim()
    lta_rc_mock.assert_called_with("POST", '/Bundles/actions/pop?source=WIPAC&dest=DESY&status=transferring', {'claimant': f'{p.name}-{p.instance_uuid}'})
    vb_mock.assert_called_with(mocker.ANY, {"one": 1})


@pytest.mark.asyncio
async def test_desy_move_verifier_verify_bundle_finished(config: TestConfig, mocker: MockerFixture) -> None:
    """Test that _delete_bundle deletes a completed bundle transfer."""
    logger_mock = mocker.MagicMock()
    lta_rc_mock = mocker.patch("rest_tools.client.RestClient", new_callable=AsyncMock)
    sgp_mock = mocker.patch("lta.desy_move_verifier.SiteGlobusProxy")
    update_mock = mocker.patch("lta.desy_move_verifier.SiteGlobusProxy.update_proxy")
    grid_mock = mocker.patch("lta.desy_move_verifier.GridFTP.get")
    hash_mock = mocker.patch("lta.desy_move_verifier.sha512sum")
    hash_mock.return_value = "12345"
    remove_mock = mocker.patch("os.remove")
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
    p = DesyMoveVerifier(config, logger_mock)
    await p._verify_bundle(lta_rc_mock, bundle_obj)
    remove_mock.assert_called()
    grid_mock.assert_called()
    update_mock.assert_not_called()
    sgp_mock.assert_called()
    lta_rc_mock.request.assert_called_with("PATCH", '/Bundles/8286d3ba-fb1b-4923-876d-935bdf7fc99e', {
        "status": "taping",
        "reason": "",
        "update_timestamp": mocker.ANY,
        "claimed": False,
    })
