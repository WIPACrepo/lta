# test_desy_mirror_replicator.py
"""Unit tests for lta/desy_mirror_replicator.py."""
import logging
# fmt:off

from typing import Awaitable, Callable, cast, Concatenate, ParamSpec, TypeVar
from unittest.mock import AsyncMock, call, MagicMock

import pytest
from pytest import MonkeyPatch
from pytest_mock import MockerFixture
from tornado.web import HTTPError

from lta.desy_mirror_replicator import main_sync, DesyMirrorReplicator

TestConfig = dict[str, str]


@pytest.fixture
def config() -> TestConfig:
    """Supply a stock DesyMirrorReplicator component configuration."""
    return {
        "CI_TEST": "TRUE",
        "CLIENT_ID": "long-term-archive",
        "CLIENT_SECRET": "hunter2",  # http://bash.org/?244321
        "COMPONENT_NAME": "testing-desy-mirror-replicator",
        "DEST_BASE_PATH": "/some/root/at/desy/icecube/archive",
        "DEST_SITE": "DESY",
        "DEST_URL": "https://localhost:12880/",
        "INPUT_PATH": "/path/to/bundler_todesy",
        "INPUT_STATUS": "staged",
        "LOG_LEVEL": "DEBUG",
        "LTA_AUTH_OPENID_URL": "localhost:12345",
        "LTA_REST_URL": "localhost:12347",
        "MAX_PARALLEL": "100",
        "OUTPUT_STATUS": "transferring",
        "PROMETHEUS_METRICS_PORT": "8080",
        "RUN_ONCE_AND_DIE": "FALSE",
        "RUN_UNTIL_NO_WORK": "FALSE",
        "SOURCE_SITE": "WIPAC",
        "WORK_RETRIES": "3",
        "WORK_SLEEP_DURATION_SECONDS": "60",
        "WORK_TIMEOUT_SECONDS": "30",
    }


def test_bind_setup_curl() -> None:
    """Test that we can bind and call a callback with the bound value."""
    # define a function to create a callback with a bound config parameter
    def bind_callback(config: dict[str, str]) -> Callable[[str], str]:
        # create the callback function using the bound parameter
        def callback(name: str) -> str:
            message = f"{config['message']} {name}!"
            return message
        # return the callback function
        return callback

    # call our binder function, supply the message to be bound, keep the now-bound callback
    cb_func = bind_callback({
        "message": "Hello,"
    })

    # call the callback function and make sure it used the bound parameter and argument correctly
    assert cb_func('Alice') == "Hello, Alice!"


@pytest.mark.asyncio
async def test_object_decorator() -> None:
    """Test that a decorator can use object fields for itself."""
    from functools import wraps

    P = ParamSpec("P")
    R = TypeVar("R", bound="str")
    T = TypeVar("T", bound="MyClass")

    def my_decorator(func: Callable[Concatenate[T, P], Awaitable[R]]) -> Callable[Concatenate[T, P], Awaitable[R]]:
        @wraps(func)
        async def inner(self: T, *args: P.args, **kwargs: P.kwargs) -> str:
            message = "[ "
            message = message + await func(self, *args, **kwargs)
            message = message + " ]"
            return message
        return cast(Callable[Concatenate[T, P], Awaitable[R]], inner)

    class MyClass:
        def __init__(self, name: str):
            self.name = name

        @my_decorator
        async def get_title(self) -> str:
            return self.name

    alice = MyClass("Alice")
    assert alice.name == "Alice"
    assert await alice.get_title() == "[ Alice ]"


def test_constructor_config_missing_values(mocker: MockerFixture) -> None:
    """Fail with a ValueError if the configuration object is missing required configuration variables."""
    config = {
        "PAN_GALACTIC_GARGLE_BLASTER": "Yummy"
    }
    with pytest.raises(ValueError):
        DesyMirrorReplicator(config, logging.getLogger())


def test_constructor_config_poison_values(config: TestConfig, mocker: MockerFixture) -> None:
    """Fail with a ValueError if the configuration object is missing required configuration variables."""
    desy_mirror_replicator_config = config.copy()
    del desy_mirror_replicator_config["LTA_REST_URL"]
    with pytest.raises(ValueError):
        DesyMirrorReplicator(desy_mirror_replicator_config, logging.getLogger())


def test_constructor_config(config: TestConfig, mocker: MockerFixture) -> None:
    """Test that a DesyMirrorReplicator can be constructed with a configuration object and a logging object."""
    p = DesyMirrorReplicator(config, logging.getLogger())
    assert p.lta_rest_url == "localhost:12347"
    assert p.name == "testing-desy-mirror-replicator"
    assert p.work_sleep_duration_seconds == 60
    assert p.logger == logging.getLogger()


def test_constructor_config_sleep_type_int(config: TestConfig, mocker: MockerFixture) -> None:
    """Ensure that sleep seconds can also be provided as an integer."""
    p = DesyMirrorReplicator(config, logging.getLogger())
    assert p.lta_rest_url == "localhost:12347"
    assert p.name == "testing-desy-mirror-replicator"
    assert p.work_sleep_duration_seconds == 60
    assert p.logger == logging.getLogger()


def test_do_status(config: TestConfig, mocker: MockerFixture) -> None:
    """Verify that the DesyMirrorReplicator has no additional state to offer."""
    p = DesyMirrorReplicator(config, logging.getLogger())
    assert p._do_status() == {}


@pytest.mark.asyncio
async def test_script_main_sync(config: TestConfig, mocker: MockerFixture, monkeypatch: MonkeyPatch) -> None:
    """
    Verify DesyMirrorReplicator component behavior when run as a script.

    Test to make sure running the DesyMirrorReplicator as a script does the setup work
    that we expect and then launches the desy_mirror_replicator service.
    """
    for key in config.keys():
        monkeypatch.setenv(key, config[key])
    mock_run = mocker.patch("asyncio.run")
    mock_main = mocker.patch("lta.desy_mirror_replicator.main")
    mock_shs = mocker.patch("lta.desy_mirror_replicator.start_http_server")
    main_sync()
    mock_shs.assert_called()
    mock_main.assert_called()
    mock_run.assert_called()
    await mock_run.call_args.args[0]


@pytest.mark.asyncio
async def test_desy_mirror_replicator_logs_configuration(mocker: MockerFixture) -> None:
    """Test to make sure the DesyMirrorReplicator logs its configuration."""
    logger_mock = mocker.MagicMock(); desy_mirror_replicator_config = {
        "CI_TEST": "TRUE",
        "CLIENT_ID": "long-term-archive",
        "CLIENT_SECRET": "hunter2",  # http://bash.org/?244321
        "COMPONENT_NAME": "logme-testing-desy-mirror-replicator",
        "DEST_BASE_PATH": "/some/root/at/desy/icecube/archive",
        "DEST_SITE": "DESY",
        "DEST_URL": "https://localhost:12880/",
        "INPUT_PATH": "/path/to/bundler_todesy",
        "INPUT_STATUS": "staged",
        "LOG_LEVEL": "DEBUG",
        "LTA_AUTH_OPENID_URL": "localhost:12345",
        "LTA_REST_URL": "logme-http://RmMNHdPhHpH2ZxfaFAC9d2jiIbf5pZiHDqy43rFLQiM.com/",
        "MAX_PARALLEL": "100",
        "OUTPUT_STATUS": "transferring",
        "PROMETHEUS_METRICS_PORT": "8080",
        "RUN_ONCE_AND_DIE": "FALSE",
        "RUN_UNTIL_NO_WORK": "FALSE",
        "SOURCE_SITE": "WIPAC",
        "WORK_RETRIES": "5",
        "WORK_SLEEP_DURATION_SECONDS": "70",
        "WORK_TIMEOUT_SECONDS": "90",
    }
    DesyMirrorReplicator(desy_mirror_replicator_config, logger_mock)
    EXPECTED_LOGGER_CALLS = [
        call("desy_mirror_replicator 'logme-testing-desy-mirror-replicator' is configured:"),
        call('CI_TEST = TRUE'),
        call('CLIENT_ID = long-term-archive'),
        call('CLIENT_SECRET = [秘密]'),
        call('COMPONENT_NAME = logme-testing-desy-mirror-replicator'),
        call('DEST_BASE_PATH = /some/root/at/desy/icecube/archive'),
        call('DEST_SITE = DESY'),
        call('DEST_URL = https://localhost:12880/'),
        call('INPUT_PATH = /path/to/bundler_todesy'),
        call('INPUT_STATUS = staged'),
        call('LOG_LEVEL = DEBUG'),
        call('LTA_AUTH_OPENID_URL = localhost:12345'),
        call('LTA_REST_URL = logme-http://RmMNHdPhHpH2ZxfaFAC9d2jiIbf5pZiHDqy43rFLQiM.com/'),
        call('MAX_PARALLEL = 100'),
        call('OUTPUT_STATUS = transferring'),
        call('PROMETHEUS_METRICS_PORT = 8080'),
        call('RUN_ONCE_AND_DIE = FALSE'),
        call('RUN_UNTIL_NO_WORK = FALSE'),
        call('SOURCE_SITE = WIPAC'),
        call('WORK_RETRIES = 5'),
        call('WORK_SLEEP_DURATION_SECONDS = 70'),
        call('WORK_TIMEOUT_SECONDS = 90')
    ]
    logger_mock.info.assert_has_calls(EXPECTED_LOGGER_CALLS)


@pytest.mark.asyncio
async def test_desy_mirror_replicator_run(config: TestConfig, mocker: MockerFixture) -> None:
    """Test the DesyMirrorReplicator does the work the desy_mirror_replicator should do."""
    p = DesyMirrorReplicator(config, logging.getLogger())
    p._do_work = AsyncMock()  # type: ignore[method-assign]
    await p.run()
    p._do_work.assert_called()


@pytest.mark.asyncio
async def test_desy_mirror_replicator_run_exception(config: TestConfig, mocker: MockerFixture) -> None:
    """Test an error doesn't kill the DesyMirrorReplicator."""
    p = DesyMirrorReplicator(config, logging.getLogger())
    p._do_work = AsyncMock()  # type: ignore[method-assign]
    p._do_work.side_effect = [Exception("bad thing happen!")]
    await p.run()
    p._do_work.assert_called()


@pytest.mark.asyncio
async def test_desy_mirror_replicator_do_work_no_results(config: TestConfig, mocker: MockerFixture) -> None:
    """Test that _do_work goes on vacation when the LTA DB has no work."""
    dwc_mock = mocker.patch("lta.desy_mirror_replicator.DesyMirrorReplicator._do_work_claim", new_callable=AsyncMock)
    dwc_mock.return_value = False
    p = DesyMirrorReplicator(config, logging.getLogger())
    await p._do_work(AsyncMock())
    dwc_mock.assert_called()


@pytest.mark.asyncio
async def test_desy_mirror_replicator_do_work_yes_results(config: TestConfig, mocker: MockerFixture) -> None:
    """Test that _do_work keeps working until the LTA DB has no work."""
    dwc_mock = mocker.patch("lta.desy_mirror_replicator.DesyMirrorReplicator._do_work_claim", new_callable=AsyncMock)
    dwc_mock.side_effect = [True, True, False]
    p = DesyMirrorReplicator(config, logging.getLogger())
    await p._do_work(AsyncMock())
    dwc_mock.assert_called()


@pytest.mark.asyncio
async def test_desy_mirror_replicator_do_work_yes_then_die(config: TestConfig, mocker: MockerFixture) -> None:
    """Test that _do_work keeps working until the LTA DB has no work."""
    # use the standard config, but we'll run once and die
    config_but_die = config.copy()
    config_but_die.update({
        "RUN_ONCE_AND_DIE": "TRUE",
    })
    # run the _do_work loop and see if we call sys.exit() at some point
    dwc_mock = mocker.patch("lta.desy_mirror_replicator.DesyMirrorReplicator._do_work_claim", new_callable=AsyncMock)
    dwc_mock.side_effect = [True, True, False]
    p = DesyMirrorReplicator(config_but_die, logging.getLogger())
    with pytest.raises(SystemExit):
        await p._do_work(AsyncMock())
    dwc_mock.assert_called()


@pytest.mark.asyncio
async def test_desy_mirror_replicator_do_work_pop_exception(config: TestConfig, mocker: MockerFixture) -> None:
    """Test that _do_work raises when the RestClient can't pop."""
    lta_rc_mock = AsyncMock()
    lta_rc_mock.request = AsyncMock()
    lta_rc_mock.request.side_effect = [
        HTTPError(500, "LTA DB on fire. Again.")
    ]
    p = DesyMirrorReplicator(config, logging.getLogger())
    with pytest.raises(HTTPError):
        await p._do_work(lta_rc_mock)
    lta_rc_mock.request.assert_called_with("POST", '/Bundles/actions/pop?source=WIPAC&dest=DESY&status=staged', {'claimant': f'{p.name}-{p.instance_uuid}'})


@pytest.mark.asyncio
async def test_desy_mirror_replicator_do_work_claim_no_result(config: TestConfig, mocker: MockerFixture) -> None:
    """Test that _do_work_claim does not work when the LTA DB has no work."""
    lta_rc_mock = AsyncMock()
    lta_rc_mock.request = AsyncMock()
    lta_rc_mock.request.side_effect = [
        {
            "bundle": None
        }
    ]
    rbtds_mock = mocker.patch("lta.desy_mirror_replicator.DesyMirrorReplicator._replicate_bundle_to_destination_site", new_callable=AsyncMock)
    p = DesyMirrorReplicator(config, logging.getLogger())
    await p._do_work_claim(lta_rc_mock, MagicMock())
    lta_rc_mock.request.assert_called_with("POST", '/Bundles/actions/pop?source=WIPAC&dest=DESY&status=staged', {'claimant': f'{p.name}-{p.instance_uuid}'})
    rbtds_mock.assert_not_called()


@pytest.mark.asyncio
async def test_desy_mirror_replicator_do_work_claim_yes_result(config: TestConfig, mocker: MockerFixture) -> None:
    """Test that _do_work_claim processes the Bundle it gets from the LTA DB."""
    lta_rc_mock = AsyncMock()
    lta_rc_mock.request = AsyncMock()
    lta_rc_mock.request.side_effect = [
        {
            "bundle": {"one": 1, "uuid": "abc123", "type": "Bundle"}
        }
    ]
    rbtds_mock = mocker.patch("lta.desy_mirror_replicator.DesyMirrorReplicator._replicate_bundle_to_destination_site", new_callable=AsyncMock)
    p = DesyMirrorReplicator(config, logging.getLogger())
    await p._do_work_claim(lta_rc_mock, MagicMock())
    lta_rc_mock.request.assert_called_with("POST", '/Bundles/actions/pop?source=WIPAC&dest=DESY&status=staged', {'claimant': f'{p.name}-{p.instance_uuid}'})
    rbtds_mock.assert_called_with(mocker.ANY, {"one": 1, "uuid": "abc123", "type": "Bundle"})


@pytest.mark.asyncio
async def test_desy_mirror_replicator_do_work_claim_write_bundle_raise_exception(config: TestConfig, mocker: MockerFixture) -> None:
    """Test that _do_work_claim will quarantine a bundle if an exception occurs."""
    lta_rc_mock = AsyncMock()
    lta_rc_mock.request = AsyncMock()
    lta_rc_mock.request.side_effect = [
        {
            "bundle": {
                "uuid": "8f03a920-49d6-446b-811e-830e3f7942f5",
                "status": "staged",
                "type": "Bundle",
            },
        },
        {}
    ]
    rbtds_mock = mocker.patch("lta.desy_mirror_replicator.DesyMirrorReplicator._replicate_bundle_to_destination_site", new_callable=AsyncMock)
    exc = Exception("BAD THING HAPPEN!")
    rbtds_mock.side_effect = exc
    p = DesyMirrorReplicator(config, logging.getLogger())
    with pytest.raises(type(exc)) as excinfo:
        await p._do_work_claim(lta_rc_mock, MagicMock())
    assert excinfo.value == exc
    lta_rc_mock.request.assert_called_with("PATCH", '/Bundles/8f03a920-49d6-446b-811e-830e3f7942f5', mocker.ANY)
    rbtds_mock.assert_called_with(
        mocker.ANY,
        {"uuid": "8f03a920-49d6-446b-811e-830e3f7942f5", "status": "staged", "type": "Bundle"}
    )


@pytest.mark.asyncio
async def test_desy_mirror_replicator_replicate_bundle_to_destination_site_raise_exception(config: TestConfig, mocker: MockerFixture) -> None:
    """Test that _replicate_bundle_to_destination_site calls Sync.put_path()."""
    lta_rc_mock = AsyncMock()
    lta_rc_mock.request = AsyncMock()
    lta_rc_mock.request.side_effect = [
        {
            "bundle": {
                "uuid": "398ca1ed-0178-4333-a323-8b9158c3dd88",
                "status": "staged",
                "bundle_path": "/path/on/source/rse/398ca1ed-0178-4333-a323-8b9158c3dd88.zip",
                "path": "/data/exp/IceCube/2019/filtered/PFFilt/1109",
                "type": "Bundle",
            },
        }
    ]
    sync_class_mock = mocker.patch("lta.desy_mirror_replicator.Sync", new_callable=MagicMock)
    sync_class_mock.return_value = AsyncMock()
    sync_class_mock.return_value.put_path = AsyncMock()
    exc = Exception("DESY system admins won lottery; nobody left to fix the problems")
    sync_class_mock.return_value.put_path.side_effect = exc
    p = DesyMirrorReplicator(config, logging.getLogger())
    with pytest.raises(type(exc)) as excinfo:
        await p._do_work_claim(lta_rc_mock, MagicMock())
    assert excinfo.value == exc
    sync_class_mock.return_value.put_path.assert_called_with(
        '/path/on/source/rse/398ca1ed-0178-4333-a323-8b9158c3dd88.zip',
        '/data/exp/IceCube/2019/filtered/PFFilt/1109/398ca1ed-0178-4333-a323-8b9158c3dd88.zip',
        30
    )
    lta_rc_mock.request.assert_called_with("PATCH", '/Bundles/398ca1ed-0178-4333-a323-8b9158c3dd88', mocker.ANY)


@pytest.mark.asyncio
async def test_desy_mirror_replicator_replicate_bundle_to_destination_site(config: TestConfig, mocker: MockerFixture) -> None:
    """Test that _replicate_bundle_to_destination_site calls Sync.put_path()."""
    lta_rc_mock = AsyncMock()
    lta_rc_mock.request = AsyncMock()
    lta_rc_mock.request.side_effect = [
        {
            "bundle": {
                "uuid": "398ca1ed-0178-4333-a323-8b9158c3dd88",
                "status": "staged",
                "bundle_path": "/path/on/source/rse/398ca1ed-0178-4333-a323-8b9158c3dd88.zip",
                "path": "/data/exp/IceCube/2019/filtered/PFFilt/1109",
                "type": "Bundle",
            },
        },
        {},  # response to PATCH /Bundles/<uuid>
    ]
    sync_class_mock = mocker.patch("lta.desy_mirror_replicator.Sync", new_callable=MagicMock)
    sync_class_mock.return_value = AsyncMock()
    sync_class_mock.return_value.put_path = AsyncMock()
    p = DesyMirrorReplicator(config, logging.getLogger())
    await p._do_work_claim(lta_rc_mock, MagicMock())
    sync_class_mock.return_value.put_path.assert_called_with(
        '/path/on/source/rse/398ca1ed-0178-4333-a323-8b9158c3dd88.zip',
        '/data/exp/IceCube/2019/filtered/PFFilt/1109/398ca1ed-0178-4333-a323-8b9158c3dd88.zip',
        30
    )
    lta_rc_mock.request.assert_called_with("PATCH", '/Bundles/398ca1ed-0178-4333-a323-8b9158c3dd88', mocker.ANY)
