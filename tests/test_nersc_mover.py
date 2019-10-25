# test_nersc_mover.py
"""Unit tests for lta/nersc_mover.py."""

from unittest.mock import call, MagicMock

import pytest  # type: ignore
from tornado.web import HTTPError  # type: ignore

from lta.nersc_mover import main, NerscMover
from .test_util import AsyncMock, ObjectLiteral

@pytest.fixture
def config():
    """Supply a stock NerscMover component configuration."""
    return {
        "COMPONENT_NAME": "testing-nersc-mover",
        "HEARTBEAT_PATCH_RETRIES": "3",
        "HEARTBEAT_PATCH_TIMEOUT_SECONDS": "30",
        "HEARTBEAT_SLEEP_DURATION_SECONDS": "60",
        "LTA_REST_TOKEN": "fake-lta-rest-token",
        "LTA_REST_URL": "http://RmMNHdPhHpH2ZxfaFAC9d2jiIbf5pZiHDqy43rFLQiM.com/",
        "RSE_BASE_PATH": "/path/to/rse",
        "TAPE_BASE_PATH": "/path/to/hpss",
        "SOURCE_SITE": "WIPAC",
        "WORK_RETRIES": "3",
        "WORK_SLEEP_DURATION_SECONDS": "60",
        "WORK_TIMEOUT_SECONDS": "30",
    }

def test_constructor_missing_config():
    """Fail with a TypeError if a configuration object isn't provided."""
    with pytest.raises(TypeError):
        NerscMover()


def test_constructor_missing_logging():
    """Fail with a TypeError if a logging object isn't provided."""
    with pytest.raises(TypeError):
        config = {
            "PAN_GALACTIC_GARGLE_BLASTER": "Yummy"
        }
        NerscMover(config)


def test_constructor_config_missing_values(mocker):
    """Fail with a ValueError if the configuration object is missing required configuration variables."""
    config = {
        "PAN_GALACTIC_GARGLE_BLASTER": "Yummy"
    }
    logger_mock = mocker.MagicMock()
    with pytest.raises(ValueError):
        NerscMover(config, logger_mock)


def test_constructor_config_poison_values(config, mocker):
    """Fail with a ValueError if the configuration object is missing required configuration variables."""
    nersc_mover_config = config.copy()
    nersc_mover_config["LTA_REST_URL"] = None
    logger_mock = mocker.MagicMock()
    with pytest.raises(ValueError):
        NerscMover(nersc_mover_config, logger_mock)


def test_constructor_config(config, mocker):
    """Test that a NerscMover can be constructed with a configuration object and a logging object."""
    logger_mock = mocker.MagicMock()
    p = NerscMover(config, logger_mock)
    assert p.heartbeat_sleep_duration_seconds == 60
    assert p.lta_rest_url == "http://RmMNHdPhHpH2ZxfaFAC9d2jiIbf5pZiHDqy43rFLQiM.com/"
    assert p.name == "testing-nersc-mover"
    assert p.work_sleep_duration_seconds == 60
    assert p.logger == logger_mock


def test_constructor_config_sleep_type_int(config, mocker):
    """Ensure that sleep seconds can also be provided as an integer."""
    logger_mock = mocker.MagicMock()
    p = NerscMover(config, logger_mock)
    assert p.heartbeat_sleep_duration_seconds == 60
    assert p.lta_rest_url == "http://RmMNHdPhHpH2ZxfaFAC9d2jiIbf5pZiHDqy43rFLQiM.com/"
    assert p.name == "testing-nersc-mover"
    assert p.work_sleep_duration_seconds == 60
    assert p.logger == logger_mock


def test_constructor_state(config, mocker):
    """Verify that the NerscMover has a reasonable state when it is first constructed."""
    logger_mock = mocker.MagicMock()
    p = NerscMover(config, logger_mock)
    assert p.last_work_begin_timestamp is p.last_work_end_timestamp


def test_do_status(config, mocker):
    """Verify that the NerscMover has no additional state to offer."""
    logger_mock = mocker.MagicMock()
    p = NerscMover(config, logger_mock)
    assert p._do_status() == {}


@pytest.mark.asyncio
async def test_script_main(config, mocker, monkeypatch):
    """
    Verify NerscMover component behavior when run as a script.

    Test to make sure running the NerscMover as a script does the setup work
    that we expect and then launches the nersc_mover service.
    """
    for key in config.keys():
        monkeypatch.setenv(key, config[key])
    mock_event_loop = mocker.patch("asyncio.get_event_loop")
    mock_root_logger = mocker.patch("logging.getLogger")
    mock_status_loop = mocker.patch("lta.nersc_mover.status_loop")
    mock_work_loop = mocker.patch("lta.nersc_mover.work_loop")
    main()
    mock_event_loop.assert_called()
    mock_root_logger.assert_called()
    mock_status_loop.assert_called()
    mock_work_loop.assert_called()


@pytest.mark.asyncio
async def test_nersc_mover_logs_configuration(mocker):
    """Test to make sure the NerscMover logs its configuration."""
    logger_mock = mocker.MagicMock()
    nersc_mover_config = {
        "COMPONENT_NAME": "logme-testing-nersc-mover",
        "HEARTBEAT_PATCH_RETRIES": "1",
        "HEARTBEAT_PATCH_TIMEOUT_SECONDS": "20",
        "HEARTBEAT_SLEEP_DURATION_SECONDS": "30",
        "LTA_REST_TOKEN": "logme-fake-lta-rest-token",
        "LTA_REST_URL": "logme-http://RmMNHdPhHpH2ZxfaFAC9d2jiIbf5pZiHDqy43rFLQiM.com/",
        "RSE_BASE_PATH": "/log/me/path/to/rse",
        "TAPE_BASE_PATH": "/log/me/path/to/hpss",
        "SOURCE_SITE": "NERSC",
        "WORK_RETRIES": "5",
        "WORK_SLEEP_DURATION_SECONDS": "70",
        "WORK_TIMEOUT_SECONDS": "90",
    }
    NerscMover(nersc_mover_config, logger_mock)
    EXPECTED_LOGGER_CALLS = [
        call("nersc_mover 'logme-testing-nersc-mover' is configured:"),
        call('COMPONENT_NAME = logme-testing-nersc-mover'),
        call('HEARTBEAT_PATCH_RETRIES = 1'),
        call('HEARTBEAT_PATCH_TIMEOUT_SECONDS = 20'),
        call('HEARTBEAT_SLEEP_DURATION_SECONDS = 30'),
        call('LTA_REST_TOKEN = logme-fake-lta-rest-token'),
        call('LTA_REST_URL = logme-http://RmMNHdPhHpH2ZxfaFAC9d2jiIbf5pZiHDqy43rFLQiM.com/'),
        call('RSE_BASE_PATH = /log/me/path/to/rse'),
        call('TAPE_BASE_PATH = /log/me/path/to/hpss'),
        call('SOURCE_SITE = NERSC'),
        call('WORK_RETRIES = 5'),
        call('WORK_SLEEP_DURATION_SECONDS = 70'),
        call('WORK_TIMEOUT_SECONDS = 90')
    ]
    logger_mock.info.assert_has_calls(EXPECTED_LOGGER_CALLS)


@pytest.mark.asyncio
async def test_nersc_mover_run(config, mocker):
    """Test the NerscMover does the work the nersc_mover should do."""
    logger_mock = mocker.MagicMock()
    p = NerscMover(config, logger_mock)
    p._do_work = AsyncMock()
    await p.run()
    p._do_work.assert_called()


@pytest.mark.asyncio
async def test_nersc_mover_run_exception(config, mocker):
    """Test an error doesn't kill the NerscMover."""
    logger_mock = mocker.MagicMock()
    p = NerscMover(config, logger_mock)
    p.last_work_end_timestamp = None
    p._do_work = AsyncMock()
    p._do_work.side_effect = [Exception("bad thing happen!")]
    await p.run()
    p._do_work.assert_called()
    assert p.last_work_end_timestamp


@pytest.mark.asyncio
async def test_nersc_mover_do_work_pop_exception(config, mocker):
    """Test that _do_work raises when the RestClient can't pop."""
    logger_mock = mocker.MagicMock()
    lta_rc_mock = mocker.patch("rest_tools.client.RestClient.request", new_callable=AsyncMock)
    lta_rc_mock.side_effect = HTTPError(500, "LTA DB on fire. Again.")
    p = NerscMover(config, logger_mock)
    with pytest.raises(HTTPError):
        await p._do_work()
    lta_rc_mock.assert_called_with("POST", '/Bundles/actions/pop?dest=NERSC&status=taping', {'claimant': f'{p.name}-{p.instance_uuid}'})


@pytest.mark.asyncio
async def test_nersc_mover_do_work_no_results(config, mocker):
    """Test that _do_work goes on vacation when the LTA DB has no work."""
    logger_mock = mocker.MagicMock()
    dwc_mock = mocker.patch("lta.nersc_mover.NerscMover._do_work_claim", new_callable=AsyncMock)
    dwc_mock.return_value = False
    p = NerscMover(config, logger_mock)
    await p._do_work()
    dwc_mock.assert_called()


@pytest.mark.asyncio
async def test_nersc_mover_do_work_yes_results(config, mocker):
    """Test that _do_work keeps working until the LTA DB has no work."""
    logger_mock = mocker.MagicMock()
    dwc_mock = mocker.patch("lta.nersc_mover.NerscMover._do_work_claim", new_callable=AsyncMock)
    dwc_mock.side_effect = [True, True, False]
    p = NerscMover(config, logger_mock)
    await p._do_work()
    dwc_mock.assert_called()


@pytest.mark.asyncio
async def test_nersc_mover_do_work_claim_no_result(config, mocker):
    """Test that _do_work_claim does not work when the LTA DB has no work."""
    logger_mock = mocker.MagicMock()
    lta_rc_mock = mocker.patch("rest_tools.client.RestClient.request", new_callable=AsyncMock)
    lta_rc_mock.return_value = {
        "bundle": None
    }
    wbth_mock = mocker.patch("lta.nersc_mover.NerscMover._write_bundle_to_hpss", new_callable=AsyncMock)
    p = NerscMover(config, logger_mock)
    await p._do_work_claim()
    lta_rc_mock.assert_called_with("POST", '/Bundles/actions/pop?dest=NERSC&status=taping', {'claimant': f'{p.name}-{p.instance_uuid}'})
    wbth_mock.assert_not_called()


@pytest.mark.asyncio
async def test_nersc_mover_do_work_claim_yes_result(config, mocker):
    """Test that _do_work_claim processes the Bundle it gets from the LTA DB."""
    logger_mock = mocker.MagicMock()
    lta_rc_mock = mocker.patch("rest_tools.client.RestClient.request", new_callable=AsyncMock)
    lta_rc_mock.return_value = {
        "bundle": {
            "one": 1,
        },
    }
    wbth_mock = mocker.patch("lta.nersc_mover.NerscMover._write_bundle_to_hpss", new_callable=AsyncMock)
    p = NerscMover(config, logger_mock)
    await p._do_work_claim()
    lta_rc_mock.assert_called_with("POST", '/Bundles/actions/pop?dest=NERSC&status=taping', {'claimant': f'{p.name}-{p.instance_uuid}'})
    wbth_mock.assert_called_with(mocker.ANY, {"one": 1})


@pytest.mark.asyncio
async def test_nersc_mover_write_bundle_to_hpss_mkdir(config, mocker):
    """Test that _write_bundle_to_hpss executes an HSI command to create the destination directory."""
    logger_mock = mocker.MagicMock()
    lta_rc_mock = mocker.patch("rest_tools.client.RestClient.request", new_callable=AsyncMock)
    lta_rc_mock.return_value = {
        "bundle": {
            "uuid": "398ca1ed-0178-4333-a323-8b9158c3dd88",
            "bundle_path": "/path/on/source/rse/398ca1ed-0178-4333-a323-8b9158c3dd88.zip",
            "path": "/data/exp/IceCube/2019/filtered/PFFilt/1109",
        },
    }
    ehc_mock = mocker.patch("lta.nersc_mover.NerscMover._execute_hsi_command", new_callable=AsyncMock)
    ehc_mock.return_value = False
    p = NerscMover(config, logger_mock)
    await p._do_work_claim()
    ehc_mock.assert_called_with(lta_rc_mock, mocker.ANY, ['hsi', 'mkdir', '-p', '/path/to/hpss/data/exp/IceCube/2019/filtered/PFFilt/1109'])
    lta_rc_mock.assert_called_once()


@pytest.mark.asyncio
async def test_nersc_mover_write_bundle_to_hpss_hsi_put(config, mocker):
    """Test that _write_bundle_to_hpss executes an HSI command to write the file to tape."""
    logger_mock = mocker.MagicMock()
    lta_rc_mock = mocker.patch("rest_tools.client.RestClient.request", new_callable=AsyncMock)
    lta_rc_mock.return_value = {
        "bundle": {
            "uuid": "398ca1ed-0178-4333-a323-8b9158c3dd88",
            "bundle_path": "/path/on/source/rse/398ca1ed-0178-4333-a323-8b9158c3dd88.zip",
            "path": "/data/exp/IceCube/2019/filtered/PFFilt/1109",
        },
    }
    ehc_mock = mocker.patch("lta.nersc_mover.NerscMover._execute_hsi_command", new_callable=AsyncMock)
    ehc_mock.side_effect = [True, False]
    p = NerscMover(config, logger_mock)
    await p._do_work_claim()
    ehc_mock.assert_called_with(lta_rc_mock, mocker.ANY, ['hsi', 'put', '-c', 'on', '-H', 'sha512', '/path/to/rse/398ca1ed-0178-4333-a323-8b9158c3dd88.zip', ':', '/path/to/hpss/data/exp/IceCube/2019/filtered/PFFilt/1109/398ca1ed-0178-4333-a323-8b9158c3dd88.zip'])
    lta_rc_mock.assert_called_once()


@pytest.mark.asyncio
async def test_nersc_mover_write_bundle_to_hpss(config, mocker):
    """Test that _write_bundle_to_hpss updates the LTA DB after success."""
    logger_mock = mocker.MagicMock()
    lta_rc_mock = mocker.patch("rest_tools.client.RestClient.request", new_callable=AsyncMock)
    lta_rc_mock.return_value = {
        "bundle": {
            "uuid": "398ca1ed-0178-4333-a323-8b9158c3dd88",
            "bundle_path": "/path/on/source/rse/398ca1ed-0178-4333-a323-8b9158c3dd88.zip",
            "path": "/data/exp/IceCube/2019/filtered/PFFilt/1109",
        },
    }
    ehc_mock = mocker.patch("lta.nersc_mover.NerscMover._execute_hsi_command", new_callable=AsyncMock)
    ehc_mock.side_effect = [True, True]
    p = NerscMover(config, logger_mock)
    await p._do_work_claim()
    ehc_mock.assert_called_with(lta_rc_mock, mocker.ANY, ['hsi', 'put', '-c', 'on', '-H', 'sha512', '/path/to/rse/398ca1ed-0178-4333-a323-8b9158c3dd88.zip', ':', '/path/to/hpss/data/exp/IceCube/2019/filtered/PFFilt/1109/398ca1ed-0178-4333-a323-8b9158c3dd88.zip'])
    lta_rc_mock.assert_called_with("PATCH", '/Bundles/398ca1ed-0178-4333-a323-8b9158c3dd88', mocker.ANY)


@pytest.mark.asyncio
async def test_nersc_mover_execute_hsi_command_failed(config, mocker):
    """Test that _execute_hsi_command will PATCH a bundle to quarantine on failure."""
    logger_mock = mocker.MagicMock()
    lta_rc_mock = mocker.patch("rest_tools.client.RestClient.request", new_callable=AsyncMock)
    lta_rc_mock.return_value = {
        "bundle": {
            "uuid": "398ca1ed-0178-4333-a323-8b9158c3dd88",
            "bundle_path": "/path/on/source/rse/398ca1ed-0178-4333-a323-8b9158c3dd88.zip",
            "path": "/data/exp/IceCube/2019/filtered/PFFilt/1109",
        },
    }
    run_mock = mocker.patch("lta.nersc_mover.run", new_callable=MagicMock)
    run_mock.return_value = ObjectLiteral(
        returncode=1,
        args=['hsi', 'mkdir', '-p', '/path/to/hpss/data/exp/IceCube/2019/filtered/PFFilt/1109'],
        stdout="some text on stdout",
        stderr="some text on stderr",
    )
    p = NerscMover(config, logger_mock)
    await p._do_work_claim()
    lta_rc_mock.assert_called_with("PATCH", '/Bundles/398ca1ed-0178-4333-a323-8b9158c3dd88', mocker.ANY)


@pytest.mark.asyncio
async def test_nersc_mover_execute_hsi_command_success(config, mocker):
    """Test that _execute_hsi_command will PATCH a bundle to quarantine on failure."""
    logger_mock = mocker.MagicMock()
    lta_rc_mock = mocker.patch("rest_tools.client.RestClient.request", new_callable=AsyncMock)
    lta_rc_mock.return_value = {
        "bundle": {
            "uuid": "398ca1ed-0178-4333-a323-8b9158c3dd88",
            "bundle_path": "/path/on/source/rse/398ca1ed-0178-4333-a323-8b9158c3dd88.zip",
            "path": "/data/exp/IceCube/2019/filtered/PFFilt/1109",
        },
    }
    run_mock = mocker.patch("lta.nersc_mover.run", new_callable=MagicMock)
    run_mock.side_effect = [ObjectLiteral(returncode=0), ObjectLiteral(returncode=0)]
    p = NerscMover(config, logger_mock)
    await p._do_work_claim()
    lta_rc_mock.assert_called_with("PATCH", '/Bundles/398ca1ed-0178-4333-a323-8b9158c3dd88', mocker.ANY)


# @pytest.mark.asyncio
# async def test_nersc_mover_do_work_transfer_request_fc_exception(config, mocker):
#     """Test that _write_bundle_to_hpss raises an exception if the File Catalog has an error."""
#     logger_mock = mocker.MagicMock()
#     p = NerscMover(config, logger_mock)
#     lta_rc_mock = MagicMock()
#     tr = {
#         "uuid": uuid1().hex,
#         "source": "WIPAC",
#         "dest": "NERSC",
#         "path": "/tmp/this/is/just/a/test",
#     }
#     fc_rc_mock = mocker.patch("rest_tools.client.RestClient.request", new_callable=AsyncMock)
#     fc_rc_mock.side_effect = HTTPError(500, "LTA DB on fire. Again.")
#     with pytest.raises(HTTPError):
#         await p._do_work_transfer_request(lta_rc_mock, tr)
#     fc_rc_mock.assert_called_with("GET", '/api/files?query={"locations.site": {"$eq": "WIPAC"}, "locations.path": {"$regex": "^/tmp/this/is/just/a/test"}}')
#
#
# @pytest.mark.asyncio
# async def test_nersc_mover_do_work_transfer_request_fc_no_results(config, mocker):
#     """Test that _do_work_transfer_request raises an exception when the LTA DB refuses to create an empty list."""
#     QUARANTINE = {'status': 'quarantined', 'reason': 'File Catalog returned zero files for the TransferRequest'}
#     logger_mock = mocker.MagicMock()
#     p = NerscMover(config, logger_mock)
#     lta_rc_mock = mocker.MagicMock()
#     lta_rc_mock.request = AsyncMock()
#     lta_rc_mock.request.return_value = {}
#     tr_uuid = uuid1().hex
#     tr = {
#         "uuid": tr_uuid,
#         "source": "WIPAC",
#         "dest": "NERSC",
#         "path": "/tmp/this/is/just/a/test",
#     }
#     fc_rc_mock = mocker.patch("rest_tools.client.RestClient.request", new_callable=AsyncMock)
#     fc_rc_mock.return_value = {
#         "files": []
#     }
#     await p._do_work_transfer_request(lta_rc_mock, tr)
#     fc_rc_mock.assert_called_with("GET", '/api/files?query={"locations.site": {"$eq": "WIPAC"}, "locations.path": {"$regex": "^/tmp/this/is/just/a/test"}}')
#     lta_rc_mock.request.assert_called_with("PATCH", f'/TransferRequests/{tr_uuid}', QUARANTINE)
#
#
# @pytest.mark.asyncio
# async def test_nersc_mover_do_work_transfer_request_fc_yes_results(config, mocker):
#     """Test that _do_work_transfer_request processes each file it gets back from the File Catalog."""
#     logger_mock = mocker.MagicMock()
#     lta_rc_mock = mocker.MagicMock()
#     lta_rc_mock.request = AsyncMock()
#     lta_rc_mock.request.return_value = {
#         "files": [uuid1().hex, uuid1().hex, uuid1().hex]
#     }
#     tr_uuid = uuid1().hex
#     tr = {
#         "uuid": tr_uuid,
#         "source": "WIPAC",
#         "dest": "NERSC",
#         "path": "/tmp/this/is/just/a/test",
#     }
#     fc_rc_mock = mocker.patch("rest_tools.client.RestClient.request", new_callable=AsyncMock)
#     fc_rc_mock.side_effect = [
#         {
#             "_links": {
#                 "parent": {
#                     "href": "/api"
#                 },
#                 "self": {
#                     "href": "/api/files"
#                 }
#             },
#             "files": [
#                 {"logical_name": "/data/exp/IceCube/2013/filtered/PFFilt/1109/PFFilt_PhysicsFiltering_Run00123231_Subrun00000000_00000000.tar.bz2",
#                  "uuid": "58a334e6-642e-475e-b642-e92bf08e96d4"},
#                 {"logical_name": "/data/exp/IceCube/2013/filtered/PFFilt/1109/PFFilt_PhysicsFiltering_Run00123231_Subrun00000000_00000001.tar.bz2",
#                  "uuid": "89528506-9950-43dc-a910-f5108a1d25c0"},
#                 {"logical_name": "/data/exp/IceCube/2013/filtered/PFFilt/1109/PFFilt_PhysicsFiltering_Run00123231_Subrun00000000_00000002.tar.bz2",
#                  "uuid": "1e4a88c6-247e-4e59-9c89-1a4edafafb1e"}]
#         },
#         {
#             "logical_name": "/data/exp/IceCube/2013/filtered/PFFilt/1109/PFFilt_PhysicsFiltering_Run00123231_Subrun00000000_00000000.tar.bz2",
#             "uuid": "58a334e6-642e-475e-b642-e92bf08e96d4",
#             "checksum": {
#                 "sha512": "63c25d9bcf7bacc8cdb7ccf0a480403eea111a6b8db2d0c54fef0e39c32fe76f75b3632b3582ef888caeaf8a8aac44fb51d0eb051f67e874f9fe694981649b74"
#             },
#             "locations": [
#                 {
#                     "path": "/data/exp/IceCube/2013/filtered/PFFilt/1109/PFFilt_PhysicsFiltering_Run00123231_Subrun00000000_00000000.tar.bz2",
#                     "site": "WIPAC"
#                 }],
#             "file_size": 103166718,
#             "meta_modify_date": "2019-07-26 01:53:20.857303"
#         },
#         {
#             "logical_name": "/data/exp/IceCube/2013/filtered/PFFilt/1109/PFFilt_PhysicsFiltering_Run00123231_Subrun00000000_00000001.tar.bz2",
#             "uuid": "89528506-9950-43dc-a910-f5108a1d25c0",
#             "checksum": {
#                 "sha512": "5acee016b1f7a367f549d3a861af32a66e5b577753d6d7b8078a30129f6535108042a74b70b1374a9f7506022e6cc64372a8d01500db5d56afb17071cba6da9e"
#             },
#             "locations": [
#                 {
#                     "path": "/data/exp/IceCube/2013/filtered/PFFilt/1109/PFFilt_PhysicsFiltering_Run00123231_Subrun00000000_00000001.tar.bz2",
#                     "site": "WIPAC"
#                 }
#             ],
#             "file_size": 103064762,
#             "meta_modify_date": "2019-07-26 01:53:20.646010"
#         },
#         {
#             "logical_name": "/data/exp/IceCube/2013/filtered/PFFilt/1109/PFFilt_PhysicsFiltering_Run00123231_Subrun00000000_00000002.tar.bz2",
#             "uuid": "1e4a88c6-247e-4e59-9c89-1a4edafafb1e",
#             "checksum": {
#                 "sha512": "ae7c1639aeaacbd69b8540a117e71a6a92b5e4eff0d7802150609daa98d99fd650f8285e26af23f97f441f3047afbce88ad54bb3feb4fe243a429934d0ee4211"
#             },
#             "locations": [
#                 {
#                     "path": "/data/exp/IceCube/2013/filtered/PFFilt/1109/PFFilt_PhysicsFiltering_Run00123231_Subrun00000000_00000002.tar.bz2",
#                     "site": "WIPAC"
#                 }
#             ],
#             "file_size": 104136149,
#             "meta_modify_date": "2019-07-26 01:53:22.591198"
#         }
#     ]
#     p = NerscMover(config, logger_mock)
#     await p._do_work_transfer_request(lta_rc_mock, tr)
#     fc_rc_mock.assert_called_with("GET", '/api/files/1e4a88c6-247e-4e59-9c89-1a4edafafb1e')
#     lta_rc_mock.request.assert_called_with("POST", '/Bundles/actions/bulk_create', mocker.ANY)
