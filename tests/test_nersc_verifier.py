# test_nersc_verifier.py
"""Unit tests for lta/nersc_verifier.py."""

from unittest.mock import call, MagicMock
from uuid import uuid1

import pytest  # type: ignore
from tornado.web import HTTPError  # type: ignore

from lta.nersc_verifier import main, NerscVerifier
from .test_util import AsyncMock, ObjectLiteral

@pytest.fixture
def config():
    """Supply a stock NerscVerifier component configuration."""
    return {
        "COMPONENT_NAME": "testing-nersc_verifier",
        "DEST_SITE": "NERSC",
        "FILE_CATALOG_REST_TOKEN": "fake-file-catalog-token",
        "FILE_CATALOG_REST_URL": "http://kVj74wBA1AMTDV8zccn67pGuWJqHZzD7iJQHrUJKA.com/",
        "HEARTBEAT_PATCH_RETRIES": "3",
        "HEARTBEAT_PATCH_TIMEOUT_SECONDS": "30",
        "HEARTBEAT_SLEEP_DURATION_SECONDS": "60",
        "INPUT_STATUS": "verifying",
        "LTA_REST_TOKEN": "fake-lta-rest-token",
        "LTA_REST_URL": "http://RmMNHdPhHpH2ZxfaFAC9d2jiIbf5pZiHDqy43rFLQiM.com/",
        "OUTPUT_STATUS": "completed",
        "RUN_ONCE_AND_DIE": "False",
        "SOURCE_SITE": "WIPAC",
        "TAPE_BASE_PATH": "/path/to/hpss",
        "WORK_RETRIES": "3",
        "WORK_SLEEP_DURATION_SECONDS": "60",
        "WORK_TIMEOUT_SECONDS": "30",
    }

def test_constructor_config(config, mocker):
    """Test that a NerscVerifier can be constructed with a configuration object and a logging object."""
    logger_mock = mocker.MagicMock()
    p = NerscVerifier(config, logger_mock)
    assert p.name == "testing-nersc_verifier"
    assert p.file_catalog_rest_token == "fake-file-catalog-token"
    assert p.file_catalog_rest_url == "http://kVj74wBA1AMTDV8zccn67pGuWJqHZzD7iJQHrUJKA.com/"
    assert p.heartbeat_patch_retries == 3
    assert p.heartbeat_patch_timeout_seconds == 30
    assert p.heartbeat_sleep_duration_seconds == 60
    assert p.lta_rest_token == "fake-lta-rest-token"
    assert p.lta_rest_url == "http://RmMNHdPhHpH2ZxfaFAC9d2jiIbf5pZiHDqy43rFLQiM.com/"
    assert p.source_site == "WIPAC"
    assert p.tape_base_path == "/path/to/hpss"
    assert p.work_retries == 3
    assert p.work_sleep_duration_seconds == 60
    assert p.work_timeout_seconds == 30
    assert p.logger == logger_mock

def test_do_status(config, mocker):
    """Verify that the NerscVerifier has no additional state to offer."""
    logger_mock = mocker.MagicMock()
    p = NerscVerifier(config, logger_mock)
    assert p._do_status() == {}

@pytest.mark.asyncio
async def test_nersc_verifier_logs_configuration(mocker):
    """Test to make sure the NerscVerifier logs its configuration."""
    logger_mock = mocker.MagicMock()
    nersc_verifier_config = {
        "COMPONENT_NAME": "logme-testing-nersc_verifier",
        "DEST_SITE": "NERSC",
        "FILE_CATALOG_REST_TOKEN": "logme-fake-file-catalog-token",
        "FILE_CATALOG_REST_URL": "logme-http://kVj74wBA1AMTDV8zccn67pGuWJqHZzD7iJQHrUJKA.com/",
        "HEARTBEAT_PATCH_RETRIES": "1",
        "HEARTBEAT_PATCH_TIMEOUT_SECONDS": "20",
        "HEARTBEAT_SLEEP_DURATION_SECONDS": "30",
        "INPUT_STATUS": "verifying",
        "LTA_REST_TOKEN": "logme-fake-lta-rest-token",
        "LTA_REST_URL": "logme-http://RmMNHdPhHpH2ZxfaFAC9d2jiIbf5pZiHDqy43rFLQiM.com/",
        "OUTPUT_STATUS": "completed",
        "RUN_ONCE_AND_DIE": "False",
        "SOURCE_SITE": "WIPAC",
        "TAPE_BASE_PATH": "/logme/path/to/hpss",
        "WORK_RETRIES": "5",
        "WORK_SLEEP_DURATION_SECONDS": "70",
        "WORK_TIMEOUT_SECONDS": "90",
    }
    NerscVerifier(nersc_verifier_config, logger_mock)
    EXPECTED_LOGGER_CALLS = [
        call("nersc_verifier 'logme-testing-nersc_verifier' is configured:"),
        call('COMPONENT_NAME = logme-testing-nersc_verifier'),
        call('DEST_SITE = NERSC'),
        call('FILE_CATALOG_REST_TOKEN = logme-fake-file-catalog-token'),
        call('FILE_CATALOG_REST_URL = logme-http://kVj74wBA1AMTDV8zccn67pGuWJqHZzD7iJQHrUJKA.com/'),
        call('HEARTBEAT_PATCH_RETRIES = 1'),
        call('HEARTBEAT_PATCH_TIMEOUT_SECONDS = 20'),
        call('HEARTBEAT_SLEEP_DURATION_SECONDS = 30'),
        call('INPUT_STATUS = verifying'),
        call('LTA_REST_TOKEN = logme-fake-lta-rest-token'),
        call('LTA_REST_URL = logme-http://RmMNHdPhHpH2ZxfaFAC9d2jiIbf5pZiHDqy43rFLQiM.com/'),
        call('OUTPUT_STATUS = completed'),
        call('RUN_ONCE_AND_DIE = False'),
        call('SOURCE_SITE = WIPAC'),
        call('TAPE_BASE_PATH = /logme/path/to/hpss'),
        call('WORK_RETRIES = 5'),
        call('WORK_SLEEP_DURATION_SECONDS = 70'),
        call('WORK_TIMEOUT_SECONDS = 90')
    ]
    logger_mock.info.assert_has_calls(EXPECTED_LOGGER_CALLS)

@pytest.mark.asyncio
async def test_script_main(config, mocker, monkeypatch):
    """
    Verify NerscVerifier component behavior when run as a script.

    Test to make sure running the NerscVerifier as a script does the setup work
    that we expect and then launches the nersc_verifier service.
    """
    for key in config.keys():
        monkeypatch.setenv(key, config[key])
    mock_event_loop = mocker.patch("asyncio.get_event_loop")
    mock_root_logger = mocker.patch("logging.getLogger")
    mock_status_loop = mocker.patch("lta.nersc_verifier.status_loop")
    mock_work_loop = mocker.patch("lta.nersc_verifier.work_loop")
    main()
    mock_event_loop.assert_called()
    mock_root_logger.assert_called()
    mock_status_loop.assert_called()
    mock_work_loop.assert_called()

@pytest.mark.asyncio
async def test_nersc_verifier_run(config, mocker):
    """Test the NerscVerifier does the work the nersc_verifier should do."""
    logger_mock = mocker.MagicMock()
    p = NerscVerifier(config, logger_mock)
    p._do_work = AsyncMock()
    await p.run()
    p._do_work.assert_called()

@pytest.mark.asyncio
async def test_nersc_verifier_run_exception(config, mocker):
    """Test an error doesn't kill the NerscVerifier."""
    logger_mock = mocker.MagicMock()
    p = NerscVerifier(config, logger_mock)
    p.last_work_end_timestamp = None
    p._do_work = AsyncMock()
    p._do_work.side_effect = [Exception("bad thing happen!")]
    await p.run()
    p._do_work.assert_called()
    assert p.last_work_end_timestamp

@pytest.mark.asyncio
async def test_nersc_verifier_hpss_not_available(config, mocker):
    """Test that a bad returncode on hpss_avail will prevent work."""
    logger_mock = mocker.MagicMock()
    run_mock = mocker.patch("lta.nersc_verifier.run", new_callable=MagicMock)
    run_mock.return_value = ObjectLiteral(
        returncode=1,
        args=["/usr/common/mss/bin/hpss_avail", "archive"],
        stdout="some text on stdout",
        stderr="some text on stderr",
    )
    p = NerscVerifier(config, logger_mock)
    assert not await p._do_work_claim()

@pytest.mark.asyncio
async def test_nersc_verifier_do_work_pop_exception(config, mocker):
    """Test that _do_work raises when the RestClient can't pop."""
    logger_mock = mocker.MagicMock()
    run_mock = mocker.patch("lta.nersc_verifier.run", new_callable=MagicMock)
    run_mock.side_effect = [
        ObjectLiteral(
            returncode=0,
            args=["/usr/common/mss/bin/hpss_avail", "archive"],
            stdout="some text on stdout",
            stderr="some text on stderr",
        ),
        ObjectLiteral(
            returncode=0,
            args=["/usr/bin/which", "hsi"],
            stdout="some text on stdout",
            stderr="some text on stderr",
        ),
    ]
    lta_rc_mock = mocker.patch("rest_tools.client.RestClient.request", new_callable=AsyncMock)
    lta_rc_mock.side_effect = HTTPError(500, "LTA DB on fire. Again.")
    p = NerscVerifier(config, logger_mock)
    with pytest.raises(HTTPError):
        await p._do_work()
    lta_rc_mock.assert_called_with("POST", '/Bundles/actions/pop?source=WIPAC&dest=NERSC&status=verifying', {'claimant': f'{p.name}-{p.instance_uuid}'})

@pytest.mark.asyncio
async def test_nersc_verifier_do_work_no_results(config, mocker):
    """Test that _do_work goes on vacation when the LTA DB has no work."""
    logger_mock = mocker.MagicMock()
    dwc_mock = mocker.patch("lta.nersc_verifier.NerscVerifier._do_work_claim", new_callable=AsyncMock)
    dwc_mock.return_value = False
    p = NerscVerifier(config, logger_mock)
    await p._do_work()
    dwc_mock.assert_called()

@pytest.mark.asyncio
async def test_nersc_verifier_do_work_yes_results(config, mocker):
    """Test that _do_work keeps working until the LTA DB has no work."""
    logger_mock = mocker.MagicMock()
    dwc_mock = mocker.patch("lta.nersc_verifier.NerscVerifier._do_work_claim", new_callable=AsyncMock)
    dwc_mock.side_effect = [True, True, False]
    p = NerscVerifier(config, logger_mock)
    await p._do_work()
    assert dwc_mock.call_count == 3

@pytest.mark.asyncio
async def test_nersc_verifier_do_work_claim_no_result(config, mocker):
    """Test that _do_work_claim does not work when the LTA DB has no work."""
    logger_mock = mocker.MagicMock()
    run_mock = mocker.patch("lta.nersc_verifier.run", new_callable=MagicMock)
    run_mock.side_effect = [
        ObjectLiteral(
            returncode=0,
            args=["/usr/common/mss/bin/hpss_avail", "archive"],
            stdout="some text on stdout",
            stderr="some text on stderr",
        ),
        ObjectLiteral(
            returncode=0,
            args=["/usr/bin/which", "hsi"],
            stdout="some text on stdout",
            stderr="some text on stderr",
        ),
    ]
    lta_rc_mock = mocker.patch("rest_tools.client.RestClient.request", new_callable=AsyncMock)
    lta_rc_mock.return_value = {
        "bundle": None
    }
    vbih_mock = mocker.patch("lta.nersc_verifier.NerscVerifier._verify_bundle_in_hpss", new_callable=AsyncMock)
    p = NerscVerifier(config, logger_mock)
    await p._do_work_claim()
    lta_rc_mock.assert_called_with("POST", '/Bundles/actions/pop?source=WIPAC&dest=NERSC&status=verifying', {'claimant': f'{p.name}-{p.instance_uuid}'})
    vbih_mock.assert_not_called()

@pytest.mark.asyncio
async def test_nersc_verifier_do_work_claim_yes_result(config, mocker):
    """Test that _do_work_claim processes the Bundle that it gets from the LTA DB."""
    logger_mock = mocker.MagicMock()
    run_mock = mocker.patch("lta.nersc_verifier.run", new_callable=MagicMock)
    run_mock.side_effect = [
        ObjectLiteral(
            returncode=0,
            args=["/usr/common/mss/bin/hpss_avail", "archive"],
            stdout="some text on stdout",
            stderr="some text on stderr",
        ),
        ObjectLiteral(
            returncode=0,
            args=["/usr/bin/which", "hsi"],
            stdout="some text on stdout",
            stderr="some text on stderr",
        ),
    ]
    lta_rc_mock = mocker.patch("rest_tools.client.RestClient.request", new_callable=AsyncMock)
    lta_rc_mock.return_value = {
        "bundle": {
            "one": 1,
        },
    }
    vbih_mock = mocker.patch("lta.nersc_verifier.NerscVerifier._verify_bundle_in_hpss", new_callable=AsyncMock)
    vbih_mock.return_value = False
    p = NerscVerifier(config, logger_mock)
    assert await p._do_work_claim()
    lta_rc_mock.assert_called_with("POST", '/Bundles/actions/pop?source=WIPAC&dest=NERSC&status=verifying', {'claimant': f'{p.name}-{p.instance_uuid}'})
    vbih_mock.assert_called_with(mocker.ANY, {"one": 1})

@pytest.mark.asyncio
async def test_nersc_verifier_do_work_claim_yes_result_update_fc_and_lta(config, mocker):
    """Test that _do_work_claim processes the Bundle that it gets from the LTA DB."""
    logger_mock = mocker.MagicMock()
    run_mock = mocker.patch("lta.nersc_verifier.run", new_callable=MagicMock)
    run_mock.side_effect = [
        ObjectLiteral(
            returncode=0,
            args=["/usr/common/mss/bin/hpss_avail", "archive"],
            stdout="some text on stdout",
            stderr="some text on stderr",
        ),
        ObjectLiteral(
            returncode=0,
            args=["/usr/bin/which", "hsi"],
            stdout="some text on stdout",
            stderr="some text on stderr",
        ),
    ]
    lta_rc_mock = mocker.patch("rest_tools.client.RestClient.request", new_callable=AsyncMock)
    lta_rc_mock.return_value = {
        "bundle": {
            "one": 1,
        },
    }
    vbih_mock = mocker.patch("lta.nersc_verifier.NerscVerifier._verify_bundle_in_hpss", new_callable=AsyncMock)
    vbih_mock.return_value = True
    abtfc_mock = mocker.patch("lta.nersc_verifier.NerscVerifier._add_bundle_to_file_catalog", new_callable=AsyncMock)
    ubild_mock = mocker.patch("lta.nersc_verifier.NerscVerifier._update_bundle_in_lta_db", new_callable=AsyncMock)
    p = NerscVerifier(config, logger_mock)
    assert await p._do_work_claim()
    lta_rc_mock.assert_called_with("POST", '/Bundles/actions/pop?source=WIPAC&dest=NERSC&status=verifying', {'claimant': f'{p.name}-{p.instance_uuid}'})
    vbih_mock.assert_called_with(mocker.ANY, {"one": 1})
    abtfc_mock.assert_called_with(mocker.ANY, {"one": 1})
    ubild_mock.assert_called_with(mocker.ANY, {"one": 1})

@pytest.mark.asyncio
async def test_nersc_verifier_do_work_claim_exception_caught(config, mocker):
    """Test that _do_work_claim quarantines a Bundle if it catches an Exception."""
    logger_mock = mocker.MagicMock()
    run_mock = mocker.patch("lta.nersc_verifier.run", new_callable=MagicMock)
    run_mock.side_effect = [
        ObjectLiteral(
            returncode=0,
            args=["/usr/common/mss/bin/hpss_avail", "archive"],
            stdout="some text on stdout",
            stderr="some text on stderr",
        ),
        ObjectLiteral(
            returncode=0,
            args=["/usr/bin/which", "hsi"],
            stdout="some text on stdout",
            stderr="some text on stderr",
        ),
    ]
    lta_rc_mock = mocker.patch("rest_tools.client.RestClient.request", new_callable=AsyncMock)
    lta_rc_mock.side_effect = [
        {
            "bundle": {
                "uuid": "45ae2ad39c664fda86e5981be0976d9c",
                "one": 1,
            },
        },
        {}
    ]
    vbih_mock = mocker.patch("lta.nersc_verifier.NerscVerifier._verify_bundle_in_hpss", new_callable=AsyncMock)
    vbih_mock.side_effect = Exception("Database totally on fire, guys")
    p = NerscVerifier(config, logger_mock)
    assert not await p._do_work_claim()
    lta_rc_mock.assert_called_with("PATCH", '/Bundles/45ae2ad39c664fda86e5981be0976d9c', mocker.ANY)
    vbih_mock.assert_called_with(mocker.ANY, {"uuid": "45ae2ad39c664fda86e5981be0976d9c", "one": 1})

@pytest.mark.asyncio
async def test_nersc_verifier_add_bundle_to_file_catalog(config, mocker):
    """Test that _add_bundle_to_file_catalog adds a record for the bundle and adds its location to constituent files."""
    logger_mock = mocker.MagicMock()
    bundle = {
        "uuid": "7ec8a8f9-fae3-4f25-ae54-c1f66014f5ef",
        "path": "/data/exp/IceCube/2019/filtered/PFFilt/1109",
        "bundle_path": "/path/to/source/rse/7ec8a8f9-fae3-4f25-ae54-c1f66014f5ef.zip",
        "checksum": {
            "sha512": "97de2a6ad728f50a381eb1be6ecf015019887fac27e8bf608334fb72caf8d3f654fdcce68c33b0f0f27de499b84e67b8357cd81ef7bba3cdaa9e23a648f43ad2",
        },
        "size": 12345,
    }
    fc_rc_mock = mocker.patch("rest_tools.client.RestClient.request", new_callable=AsyncMock)
    fc_rc_mock.side_effect = [
        True,  # POST /api/files - create the bundle record
        {  # GET /api/files/UUID - get the file record
            "uuid": "e0d15152-fd73-4e98-9aea-a9e5fdd8618e",
            "logical_name": "/data/exp/IceCube/2019/filtered/PFFilt/1109/file1.tar.gz",
        },
        True,  # POST /api/files/UUID/locations - add the location
        {  # GET /api/files/UUID - get the file record
            "uuid": "e107a8e8-8a86-41d6-9d4d-b6c8bc3797c4",
            "logical_name": "/data/exp/IceCube/2019/filtered/PFFilt/1109/file2.tar.gz",
        },
        True,  # POST /api/files/UUID/locations - add the location
        {  # GET /api/files/UUID - get the file record
            "uuid": "93bcd96e-0110-4064-9a79-b5bdfa3effb4",
            "logical_name": "/data/exp/IceCube/2019/filtered/PFFilt/1109/file3.tar.gz",
        },
        True,  # POST /api/files/UUID/locations - add the location
    ]
    metadata_uuid0 = uuid1().hex
    metadata_uuid1 = uuid1().hex
    metadata_uuid2 = uuid1().hex
    lta_rc_mock = mocker.patch("rest_tools.client.RestClient", new_callable=AsyncMock)
    lta_rc_mock.request.side_effect = [
        {  # GET /Metadata?bundle_uuid={bundle_uuid}&limit={limit}
            "results": [
                {"uuid": metadata_uuid0, "file_catalog_uuid": "e0d15152-fd73-4e98-9aea-a9e5fdd8618e"},
                {"uuid": metadata_uuid1, "file_catalog_uuid": "e107a8e8-8a86-41d6-9d4d-b6c8bc3797c4"},
                {"uuid": metadata_uuid2, "file_catalog_uuid": "93bcd96e-0110-4064-9a79-b5bdfa3effb4"},
            ]
        },
        {  # POST /Metadata/actions/bulk_delete
            "metadata": [metadata_uuid0, metadata_uuid1, metadata_uuid2],
            "count": 3,
        },
        {
            "results": []
        },
    ]
    p = NerscVerifier(config, logger_mock)
    assert await p._add_bundle_to_file_catalog(lta_rc_mock, bundle)
    assert lta_rc_mock.request.call_count == 3
    lta_rc_mock.request.assert_called_with("GET", '/Metadata?bundle_uuid=7ec8a8f9-fae3-4f25-ae54-c1f66014f5ef&limit=1000')
    assert fc_rc_mock.call_count == 7
    fc_rc_mock.assert_called_with("POST", '/api/files/93bcd96e-0110-4064-9a79-b5bdfa3effb4/locations', mocker.ANY)

@pytest.mark.asyncio
async def test_nersc_verifier_add_bundle_to_file_catalog_patch_after_post_error(config, mocker):
    """Test that _add_bundle_to_file_catalog patches the record for the bundle already in the file catalog."""
    logger_mock = mocker.MagicMock()
    bundle = {
        "uuid": "7ec8a8f9-fae3-4f25-ae54-c1f66014f5ef",
        "path": "/data/exp/IceCube/2019/filtered/PFFilt/1109",
        "bundle_path": "/path/to/source/rse/7ec8a8f9-fae3-4f25-ae54-c1f66014f5ef.zip",
        "checksum": {
            "sha512": "97de2a6ad728f50a381eb1be6ecf015019887fac27e8bf608334fb72caf8d3f654fdcce68c33b0f0f27de499b84e67b8357cd81ef7bba3cdaa9e23a648f43ad2",
        },
        "size": 12345,
    }
    fc_rc_mock = mocker.patch("rest_tools.client.RestClient.request", new_callable=AsyncMock)
    fc_rc_mock.side_effect = [
        Exception("409 conflict"),  # POST /api/files - bundle record already exists!!
        True,  # PATCH /api/files/UUID - bundle record gets updated
        {  # GET /api/files/UUID - get the file record
            "uuid": "e0d15152-fd73-4e98-9aea-a9e5fdd8618e",
            "logical_name": "/data/exp/IceCube/2019/filtered/PFFilt/1109/file1.tar.gz",
        },
        True,  # POST /api/files/UUID/locations - add the location
        {  # GET /api/files/UUID - get the file record
            "uuid": "e107a8e8-8a86-41d6-9d4d-b6c8bc3797c4",
            "logical_name": "/data/exp/IceCube/2019/filtered/PFFilt/1109/file2.tar.gz",
        },
        True,  # POST /api/files/UUID/locations - add the location
        {  # GET /api/files/UUID - get the file record
            "uuid": "93bcd96e-0110-4064-9a79-b5bdfa3effb4",
            "logical_name": "/data/exp/IceCube/2019/filtered/PFFilt/1109/file3.tar.gz",
        },
        True,  # POST /api/files/UUID/locations - add the location
    ]
    metadata_uuid0 = uuid1().hex
    metadata_uuid1 = uuid1().hex
    metadata_uuid2 = uuid1().hex
    lta_rc_mock = mocker.patch("rest_tools.client.RestClient", new_callable=AsyncMock)
    lta_rc_mock.request.side_effect = [
        {  # GET /Metadata?bundle_uuid={bundle_uuid}&limit={limit}
            "results": [
                {"uuid": metadata_uuid0, "file_catalog_uuid": "e0d15152-fd73-4e98-9aea-a9e5fdd8618e"},
                {"uuid": metadata_uuid1, "file_catalog_uuid": "e107a8e8-8a86-41d6-9d4d-b6c8bc3797c4"},
                {"uuid": metadata_uuid2, "file_catalog_uuid": "93bcd96e-0110-4064-9a79-b5bdfa3effb4"},
            ]
        },
        {  # POST /Metadata/actions/bulk_delete
            "metadata": [metadata_uuid0, metadata_uuid1, metadata_uuid2],
            "count": 3,
        },
        {
            "results": []
        },
    ]
    p = NerscVerifier(config, logger_mock)
    assert await p._add_bundle_to_file_catalog(lta_rc_mock, bundle)
    assert lta_rc_mock.request.call_count == 3
    lta_rc_mock.request.assert_called_with("GET", '/Metadata?bundle_uuid=7ec8a8f9-fae3-4f25-ae54-c1f66014f5ef&limit=1000')
    assert fc_rc_mock.call_count == 8
    fc_rc_mock.assert_called_with("POST", '/api/files/93bcd96e-0110-4064-9a79-b5bdfa3effb4/locations', mocker.ANY)

@pytest.mark.asyncio
async def test_nersc_verifier_update_bundle_in_lta_db(config, mocker):
    """Test that _update_bundle_in_lta_db updates the bundle as verified in the LTA DB."""
    logger_mock = mocker.MagicMock()
    bundle = {"uuid": "7ec8a8f9-fae3-4f25-ae54-c1f66014f5ef"}
    lta_mock = mocker.MagicMock()
    lta_rc_mock = mocker.patch("rest_tools.client.RestClient.request", new_callable=AsyncMock)
    lta_rc_mock.return_value = True
    lta_mock.request = lta_rc_mock
    p = NerscVerifier(config, logger_mock)
    assert await p._update_bundle_in_lta_db(lta_mock, bundle)
    lta_rc_mock.assert_called_with("PATCH", '/Bundles/7ec8a8f9-fae3-4f25-ae54-c1f66014f5ef', mocker.ANY)

@pytest.mark.asyncio
async def test_nersc_verifier_verify_bundle_in_hpss_success_no_quarantine(config, mocker):
    """Test that _verify_bundle_in_hpss does not quarantine a bundle if the HSI command succeeds."""
    logger_mock = mocker.MagicMock()
    bundle = {
        "uuid": "7ec8a8f9-fae3-4f25-ae54-c1f66014f5ef",
        "path": "/data/exp/IceCube/2019/filtered/PFFilt/1109",
        "bundle_path": "/path/to/source/rse/7ec8a8f9-fae3-4f25-ae54-c1f66014f5ef.zip",
        "checksum": {
            "sha512": "97de2a6ad728f50a381eb1be6ecf015019887fac27e8bf608334fb72caf8d3f654fdcce68c33b0f0f27de499b84e67b8357cd81ef7bba3cdaa9e23a648f43ad2",
        },
    }
    run_mock = mocker.patch("lta.nersc_verifier.run", new_callable=MagicMock)
    run_mock.side_effect = [
        ObjectLiteral(
            returncode=0,
            args=["hsi", "-q", "hashlist", "/home/projects/icecube/data/exp/IceCube/2019/filtered/PFFilt/1109/7ec8a8f9-fae3-4f25-ae54-c1f66014f5ef.zip"],
            stdout=b"97de2a6ad728f50a381eb1be6ecf015019887fac27e8bf608334fb72caf8d3f654fdcce68c33b0f0f27de499b84e67b8357cd81ef7bba3cdaa9e23a648f43ad2 sha512 /home/projects/icecube/data/exp/IceCube/2019/filtered/PFFilt/1109/7ec8a8f9-fae3-4f25-ae54-c1f66014f5ef.zip [hsi]\n",
            stderr=b"",
        ),
        ObjectLiteral(
            returncode=0,
            args=["hsi", "-q", "hashverify", "-A", "/home/projects/icecube/data/exp/IceCube/2019/filtered/PFFilt/1109/7ec8a8f9-fae3-4f25-ae54-c1f66014f5ef.zip"],
            stdout=b"/home/projects/icecube/data/exp/IceCube/2018/unbiased/PFDST/1230/50145c5c-01e1-4727-a9a1-324e5af09a29.zip: (sha512) OK\n",
            stderr=b"",
        ),
    ]
    lta_mock = mocker.MagicMock()
    lta_rc_mock = mocker.patch("rest_tools.client.RestClient.request", new_callable=AsyncMock)
    lta_mock.request = lta_rc_mock
    p = NerscVerifier(config, logger_mock)
    assert await p._verify_bundle_in_hpss(lta_mock, bundle)
    assert run_mock.call_count == 2
    lta_rc_mock.assert_not_called()

@pytest.mark.asyncio
async def test_nersc_verifier_verify_bundle_in_hpss_hsi_failure_quarantine(config, mocker):
    """Test that _verify_bundle_in_hpss quarantines a bundle if the HSI command fails."""
    logger_mock = mocker.MagicMock()
    bundle = {
        "uuid": "7ec8a8f9-fae3-4f25-ae54-c1f66014f5ef",
        "path": "/data/exp/IceCube/2019/filtered/PFFilt/1109",
        "bundle_path": "/path/to/source/rse/7ec8a8f9-fae3-4f25-ae54-c1f66014f5ef.zip",
        "checksum": {
            "sha512": "97de2a6ad728f50a381eb1be6ecf015019887fac27e8bf608334fb72caf8d3f654fdcce68c33b0f0f27de499b84e67b8357cd81ef7bba3cdaa9e23a648f43ad2",
        },
    }
    run_mock = mocker.patch("lta.nersc_verifier.run", new_callable=MagicMock)
    run_mock.side_effect = [
        ObjectLiteral(
            returncode=1,
            args=["hsi", "-q", "hashlist", "/home/projects/icecube/data/exp/IceCube/2019/filtered/PFFilt/1109/7ec8a8f9-fae3-4f25-ae54-c1f66014f5ef.zip"],
            stdout=b"",
            stderr=b"",
        ),
    ]
    lta_mock = mocker.MagicMock()
    lta_rc_mock = mocker.patch("rest_tools.client.RestClient.request", new_callable=AsyncMock)
    lta_mock.request = lta_rc_mock
    p = NerscVerifier(config, logger_mock)
    assert not await p._verify_bundle_in_hpss(lta_mock, bundle)
    assert run_mock.call_count == 1
    lta_rc_mock.assert_called_with('PATCH', '/Bundles/7ec8a8f9-fae3-4f25-ae54-c1f66014f5ef', mocker.ANY)

@pytest.mark.asyncio
async def test_nersc_verifier_verify_bundle_in_hpss_mismatch_checksum_quarantine(config, mocker):
    """Test that _verify_bundle_in_hpss quarantines a bundle if the checksums do not match."""
    logger_mock = mocker.MagicMock()
    bundle = {
        "uuid": "7ec8a8f9-fae3-4f25-ae54-c1f66014f5ef",
        "path": "/data/exp/IceCube/2019/filtered/PFFilt/1109",
        "bundle_path": "/path/to/source/rse/7ec8a8f9-fae3-4f25-ae54-c1f66014f5ef.zip",
        "checksum": {
            "sha512": "97de2a6ad728f50a381eb1be6ecf015019887fac27e8bf608334fb72caf8d3f654fdcce68c33b0f0f27de499b84e67b8357cd81ef7bba3cdaa9e23a648f43ad2",
        },
    }
    run_mock = mocker.patch("lta.nersc_verifier.run", new_callable=MagicMock)
    run_mock.side_effect = [
        ObjectLiteral(
            returncode=0,
            args=["hsi", "-q", "hashlist", "/home/projects/icecube/data/exp/IceCube/2019/filtered/PFFilt/1109/7ec8a8f9-fae3-4f25-ae54-c1f66014f5ef.zip"],
            stdout=b"1693e9d0273e3a2995b917c0e72e6bd2f40ea677f3613b6d57eaa14bd3a285c73e8db8b6e556b886c3929afe324bcc718711f2faddfeb43c3e030d9afe697873 sha512 /home/projects/icecube/data/exp/IceCube/2018/unbiased/PFDST/1230/50145c5c-01e1-4727-a9a1-324e5af09a29.zip [hsi]\n",
            stderr=b"",
        ),
    ]
    lta_mock = mocker.MagicMock()
    lta_rc_mock = mocker.patch("rest_tools.client.RestClient.request", new_callable=AsyncMock)
    lta_mock.request = lta_rc_mock
    p = NerscVerifier(config, logger_mock)
    assert not await p._verify_bundle_in_hpss(lta_mock, bundle)
    assert run_mock.call_count == 1
    lta_rc_mock.assert_called_with('PATCH', '/Bundles/7ec8a8f9-fae3-4f25-ae54-c1f66014f5ef', mocker.ANY)

@pytest.mark.asyncio
async def test_nersc_verifier_verify_bundle_in_hpss_failure_hashverify_quarantine(config, mocker):
    """Test that _verify_bundle_in_hpss does not quarantine a bundle if the HSI command succeeds."""
    logger_mock = mocker.MagicMock()
    bundle = {
        "uuid": "7ec8a8f9-fae3-4f25-ae54-c1f66014f5ef",
        "path": "/data/exp/IceCube/2019/filtered/PFFilt/1109",
        "bundle_path": "/path/to/source/rse/7ec8a8f9-fae3-4f25-ae54-c1f66014f5ef.zip",
        "checksum": {
            "sha512": "97de2a6ad728f50a381eb1be6ecf015019887fac27e8bf608334fb72caf8d3f654fdcce68c33b0f0f27de499b84e67b8357cd81ef7bba3cdaa9e23a648f43ad2",
        },
    }
    run_mock = mocker.patch("lta.nersc_verifier.run", new_callable=MagicMock)
    run_mock.side_effect = [
        ObjectLiteral(
            returncode=0,
            args=["hsi", "-q", "hashlist", "/home/projects/icecube/data/exp/IceCube/2019/filtered/PFFilt/1109/7ec8a8f9-fae3-4f25-ae54-c1f66014f5ef.zip"],
            stdout=b"97de2a6ad728f50a381eb1be6ecf015019887fac27e8bf608334fb72caf8d3f654fdcce68c33b0f0f27de499b84e67b8357cd81ef7bba3cdaa9e23a648f43ad2 sha512 /home/projects/icecube/data/exp/IceCube/2019/filtered/PFFilt/1109/7ec8a8f9-fae3-4f25-ae54-c1f66014f5ef.zip [hsi]\n",
            stderr=b"",
        ),
        ObjectLiteral(
            returncode=1,
            args=["hsi", "-q", "hashverify", "-A", "/home/projects/icecube/data/exp/IceCube/2019/filtered/PFFilt/1109/7ec8a8f9-fae3-4f25-ae54-c1f66014f5ef.zip"],
            stdout=b"",
            stderr=b"",
        ),
    ]
    lta_mock = mocker.MagicMock()
    lta_rc_mock = mocker.patch("rest_tools.client.RestClient.request", new_callable=AsyncMock)
    lta_mock.request = lta_rc_mock
    p = NerscVerifier(config, logger_mock)
    assert not await p._verify_bundle_in_hpss(lta_mock, bundle)
    assert run_mock.call_count == 2
    lta_rc_mock.assert_called_with('PATCH', '/Bundles/7ec8a8f9-fae3-4f25-ae54-c1f66014f5ef', mocker.ANY)

@pytest.mark.asyncio
async def test_nersc_verifier_verify_bundle_in_hpss_hashverify_bad_type_quarantine(config, mocker):
    """Test that _verify_bundle_in_hpss does not quarantine a bundle if the HSI command succeeds."""
    logger_mock = mocker.MagicMock()
    bundle = {
        "uuid": "7ec8a8f9-fae3-4f25-ae54-c1f66014f5ef",
        "path": "/data/exp/IceCube/2019/filtered/PFFilt/1109",
        "bundle_path": "/path/to/source/rse/7ec8a8f9-fae3-4f25-ae54-c1f66014f5ef.zip",
        "checksum": {
            "sha512": "97de2a6ad728f50a381eb1be6ecf015019887fac27e8bf608334fb72caf8d3f654fdcce68c33b0f0f27de499b84e67b8357cd81ef7bba3cdaa9e23a648f43ad2",
        },
    }
    run_mock = mocker.patch("lta.nersc_verifier.run", new_callable=MagicMock)
    run_mock.side_effect = [
        ObjectLiteral(
            returncode=0,
            args=["hsi", "-q", "hashlist", "/home/projects/icecube/data/exp/IceCube/2019/filtered/PFFilt/1109/7ec8a8f9-fae3-4f25-ae54-c1f66014f5ef.zip"],
            stdout=b"97de2a6ad728f50a381eb1be6ecf015019887fac27e8bf608334fb72caf8d3f654fdcce68c33b0f0f27de499b84e67b8357cd81ef7bba3cdaa9e23a648f43ad2 sha512 /home/projects/icecube/data/exp/IceCube/2019/filtered/PFFilt/1109/7ec8a8f9-fae3-4f25-ae54-c1f66014f5ef.zip [hsi]\n",
            stderr=b"",
        ),
        ObjectLiteral(
            returncode=0,
            args=["hsi", "-q", "hashverify", "-A", "/home/projects/icecube/data/exp/IceCube/2019/filtered/PFFilt/1109/7ec8a8f9-fae3-4f25-ae54-c1f66014f5ef.zip"],
            stdout=b"/home/projects/icecube/data/exp/IceCube/2018/unbiased/PFDST/1230/50145c5c-01e1-4727-a9a1-324e5af09a29.zip: (sha256) OK\n",
            stderr=b"",
        ),
    ]
    lta_mock = mocker.MagicMock()
    lta_rc_mock = mocker.patch("rest_tools.client.RestClient.request", new_callable=AsyncMock)
    lta_mock.request = lta_rc_mock
    p = NerscVerifier(config, logger_mock)
    assert not await p._verify_bundle_in_hpss(lta_mock, bundle)
    assert run_mock.call_count == 2
    lta_rc_mock.assert_called_with('PATCH', '/Bundles/7ec8a8f9-fae3-4f25-ae54-c1f66014f5ef', mocker.ANY)

@pytest.mark.asyncio
async def test_nersc_verifier_verify_bundle_in_hpss_hashverify_bad_result_quarantine(config, mocker):
    """Test that _verify_bundle_in_hpss does not quarantine a bundle if the HSI command succeeds."""
    logger_mock = mocker.MagicMock()
    bundle = {
        "uuid": "7ec8a8f9-fae3-4f25-ae54-c1f66014f5ef",
        "path": "/data/exp/IceCube/2019/filtered/PFFilt/1109",
        "bundle_path": "/path/to/source/rse/7ec8a8f9-fae3-4f25-ae54-c1f66014f5ef.zip",
        "checksum": {
            "sha512": "97de2a6ad728f50a381eb1be6ecf015019887fac27e8bf608334fb72caf8d3f654fdcce68c33b0f0f27de499b84e67b8357cd81ef7bba3cdaa9e23a648f43ad2",
        },
    }
    run_mock = mocker.patch("lta.nersc_verifier.run", new_callable=MagicMock)
    run_mock.side_effect = [
        ObjectLiteral(
            returncode=0,
            args=["hsi", "-q", "hashlist", "/home/projects/icecube/data/exp/IceCube/2019/filtered/PFFilt/1109/7ec8a8f9-fae3-4f25-ae54-c1f66014f5ef.zip"],
            stdout=b"97de2a6ad728f50a381eb1be6ecf015019887fac27e8bf608334fb72caf8d3f654fdcce68c33b0f0f27de499b84e67b8357cd81ef7bba3cdaa9e23a648f43ad2 sha512 /home/projects/icecube/data/exp/IceCube/2019/filtered/PFFilt/1109/7ec8a8f9-fae3-4f25-ae54-c1f66014f5ef.zip [hsi]\n",
            stderr=b"",
        ),
        ObjectLiteral(
            returncode=0,
            args=["hsi", "-q", "hashverify", "-A", "/home/projects/icecube/data/exp/IceCube/2019/filtered/PFFilt/1109/7ec8a8f9-fae3-4f25-ae54-c1f66014f5ef.zip"],
            stdout=b"/home/projects/icecube/data/exp/IceCube/2018/unbiased/PFDST/1230/50145c5c-01e1-4727-a9a1-324e5af09a29.zip: (sha256) OK\n",
            stderr=b"",
        ),
    ]
    lta_mock = mocker.MagicMock()
    lta_rc_mock = mocker.patch("rest_tools.client.RestClient.request", new_callable=AsyncMock)
    lta_mock.request = lta_rc_mock
    p = NerscVerifier(config, logger_mock)
    assert not await p._verify_bundle_in_hpss(lta_mock, bundle)
    assert run_mock.call_count == 2
    lta_rc_mock.assert_called_with('PATCH', '/Bundles/7ec8a8f9-fae3-4f25-ae54-c1f66014f5ef', mocker.ANY)
