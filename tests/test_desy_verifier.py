# test_desy_verifier.py
"""Unit tests for lta/desy_verifier.py."""

from typing import Dict
from unittest.mock import AsyncMock, call
from uuid import uuid1

import pytest
from pytest import MonkeyPatch
from pytest_mock import MockerFixture
from tornado.web import HTTPError

from lta.desy_verifier import main, DesyVerifier

TestConfig = Dict[str, str]


@pytest.fixture
def config() -> TestConfig:
    """Supply a stock DesyVerifier component configuration."""
    return {
        "CLIENT_ID": "long-term-archive",
        "CLIENT_SECRET": "hunter2",  # http://bash.org/?244321
        "COMPONENT_NAME": "testing-desy_verifier",
        "DEST_SITE": "DESY",
        "DESY_CRED_PATH": "/path/to/my/gridftp/cert",
        "DESY_GSIFTP": "gsiftp://kVj74wBA1AMTDV8zccn67pGuWJqHZzD7iJQHrUJKA.com:2811/path/to/files/at/desy",
        "FILE_CATALOG_CLIENT_ID": "file-catalog-client-id",
        "FILE_CATALOG_CLIENT_SECRET": "file-catalog-client-secret",
        "FILE_CATALOG_REST_URL": "http://kVj74wBA1AMTDV8zccn67pGuWJqHZzD7iJQHrUJKA.com/",
        "HEARTBEAT_PATCH_RETRIES": "3",
        "HEARTBEAT_PATCH_TIMEOUT_SECONDS": "30",
        "HEARTBEAT_SLEEP_DURATION_SECONDS": "60",
        "INPUT_STATUS": "verifying",
        "LOG_LEVEL": "DEBUG",
        "LTA_AUTH_OPENID_URL": "localhost:12345",
        "LTA_REST_URL": "localhost:12347",
        "OUTPUT_STATUS": "completed",
        "RUN_ONCE_AND_DIE": "False",
        "SOURCE_SITE": "WIPAC",
        "TAPE_BASE_PATH": "/path/to/hpss",
        "WORK_RETRIES": "3",
        "WORK_SLEEP_DURATION_SECONDS": "60",
        "WORK_TIMEOUT_SECONDS": "30",
        "WORKBOX_PATH": "/path/to/wipac/workbox/directory",
    }


def test_constructor_config(config: TestConfig, mocker: MockerFixture) -> None:
    """Test that a DesyVerifier can be constructed with a configuration object and a logging object."""
    logger_mock = mocker.MagicMock()
    p = DesyVerifier(config, logger_mock)
    assert p.name == "testing-desy_verifier"
    assert p.file_catalog_client_id == "file-catalog-client-id"
    assert p.file_catalog_client_secret == "file-catalog-client-secret"
    assert p.file_catalog_rest_url == "http://kVj74wBA1AMTDV8zccn67pGuWJqHZzD7iJQHrUJKA.com/"
    assert p.heartbeat_patch_retries == 3
    assert p.heartbeat_patch_timeout_seconds == 30
    assert p.heartbeat_sleep_duration_seconds == 60
    assert p.lta_auth_openid_url == "localhost:12345"
    assert p.lta_rest_url == "localhost:12347"
    assert p.source_site == "WIPAC"
    assert p.tape_base_path == "/path/to/hpss"
    assert p.work_retries == 3
    assert p.work_sleep_duration_seconds == 60
    assert p.work_timeout_seconds == 30
    assert p.logger == logger_mock


def test_do_status(config: TestConfig, mocker: MockerFixture) -> None:
    """Verify that the DesyVerifier has no additional state to offer."""
    logger_mock = mocker.MagicMock()
    p = DesyVerifier(config, logger_mock)
    assert p._do_status() == {}


@pytest.mark.asyncio
async def test_desy_verifier_logs_configuration(mocker: MockerFixture) -> None:
    """Test to make sure the DesyVerifier logs its configuration."""
    logger_mock = mocker.MagicMock()
    desy_verifier_config = {
        "CLIENT_ID": "long-term-archive",
        "CLIENT_SECRET": "hunter2",  # http://bash.org/?244321
        "COMPONENT_NAME": "logme-testing-desy_verifier",
        "DEST_SITE": "DESY",
        "DESY_CRED_PATH": "/path/to/my/gridftp/cert",
        "DESY_GSIFTP": "gsiftp://kVj74wBA1AMTDV8zccn67pGuWJqHZzD7iJQHrUJKA.com:2811/path/to/files/at/desy",
        "FILE_CATALOG_CLIENT_ID": "file-catalog-client-id",
        "FILE_CATALOG_CLIENT_SECRET": "file-catalog-client-secret",
        "FILE_CATALOG_REST_URL": "logme-http://kVj74wBA1AMTDV8zccn67pGuWJqHZzD7iJQHrUJKA.com/",
        "HEARTBEAT_PATCH_RETRIES": "1",
        "HEARTBEAT_PATCH_TIMEOUT_SECONDS": "20",
        "HEARTBEAT_SLEEP_DURATION_SECONDS": "30",
        "INPUT_STATUS": "verifying",
        "LOG_LEVEL": "DEBUG",
        "LTA_AUTH_OPENID_URL": "localhost:12345",
        "LTA_REST_URL": "localhost:12347",
        "OUTPUT_STATUS": "completed",
        "RUN_ONCE_AND_DIE": "False",
        "SOURCE_SITE": "WIPAC",
        "TAPE_BASE_PATH": "/logme/path/to/hpss",
        "WORK_RETRIES": "5",
        "WORK_SLEEP_DURATION_SECONDS": "70",
        "WORK_TIMEOUT_SECONDS": "90",
        "WORKBOX_PATH": "/path/to/wipac/workbox/directory",
    }
    DesyVerifier(desy_verifier_config, logger_mock)
    EXPECTED_LOGGER_CALLS = [
        call("desy_verifier 'logme-testing-desy_verifier' is configured:"),
        call('CLIENT_ID = long-term-archive'),
        call('CLIENT_SECRET = [秘密]'),
        call('COMPONENT_NAME = logme-testing-desy_verifier'),
        call('DEST_SITE = DESY'),
        call('DESY_CRED_PATH = /path/to/my/gridftp/cert'),
        call('DESY_GSIFTP = gsiftp://kVj74wBA1AMTDV8zccn67pGuWJqHZzD7iJQHrUJKA.com:2811/path/to/files/at/desy'),
        call('FILE_CATALOG_CLIENT_ID = file-catalog-client-id'),
        call('FILE_CATALOG_CLIENT_SECRET = [秘密]'),
        call('FILE_CATALOG_REST_URL = logme-http://kVj74wBA1AMTDV8zccn67pGuWJqHZzD7iJQHrUJKA.com/'),
        call('HEARTBEAT_PATCH_RETRIES = 1'),
        call('HEARTBEAT_PATCH_TIMEOUT_SECONDS = 20'),
        call('HEARTBEAT_SLEEP_DURATION_SECONDS = 30'),
        call('INPUT_STATUS = verifying'),
        call('LOG_LEVEL = DEBUG'),
        call('LTA_AUTH_OPENID_URL = localhost:12345'),
        call('LTA_REST_URL = localhost:12347'),
        call('OUTPUT_STATUS = completed'),
        call('RUN_ONCE_AND_DIE = False'),
        call('SOURCE_SITE = WIPAC'),
        call('TAPE_BASE_PATH = /logme/path/to/hpss'),
        call('WORK_RETRIES = 5'),
        call('WORK_SLEEP_DURATION_SECONDS = 70'),
        call('WORK_TIMEOUT_SECONDS = 90'),
        call('WORKBOX_PATH = /path/to/wipac/workbox/directory'),
    ]
    logger_mock.info.assert_has_calls(EXPECTED_LOGGER_CALLS)


@pytest.mark.asyncio
async def test_script_main(config: TestConfig, mocker: MockerFixture, monkeypatch: MonkeyPatch) -> None:
    """
    Verify DesyVerifier component behavior when run as a script.

    Test to make sure running the DesyVerifier as a script does the setup work
    that we expect and then launches the desy_verifier service.
    """
    for key in config.keys():
        monkeypatch.setenv(key, config[key])
    mock_event_loop = mocker.patch("asyncio.get_event_loop")
    mock_work_loop = mocker.patch("lta.desy_verifier.work_loop")
    main()
    mock_event_loop.assert_called()
    mock_work_loop.assert_called()


@pytest.mark.asyncio
async def test_desy_verifier_run(config: TestConfig, mocker: MockerFixture) -> None:
    """Test the DesyVerifier does the work the desy_verifier should do."""
    logger_mock = mocker.MagicMock()
    p = DesyVerifier(config, logger_mock)
    p._do_work = AsyncMock()  # type: ignore[assignment]
    await p.run()
    p._do_work.assert_called()


@pytest.mark.asyncio
async def test_desy_verifier_run_exception(config: TestConfig, mocker: MockerFixture) -> None:
    """Test an error doesn't kill the DesyVerifier."""
    logger_mock = mocker.MagicMock()
    p = DesyVerifier(config, logger_mock)
    p.last_work_end_timestamp = ""
    p._do_work = AsyncMock()  # type: ignore[assignment]
    p._do_work.side_effect = [Exception("bad thing happen!")]
    await p.run()
    p._do_work.assert_called()
    assert p.last_work_end_timestamp


@pytest.mark.asyncio
async def test_desy_verifier_do_work_pop_exception(config: TestConfig, mocker: MockerFixture) -> None:
    """Test that _do_work raises when the RestClient can't pop."""
    logger_mock = mocker.MagicMock()
    lta_rc_mock = mocker.patch("rest_tools.client.RestClient.request", new_callable=AsyncMock)
    lta_rc_mock.side_effect = HTTPError(500, "LTA DB on fire. Again.")
    p = DesyVerifier(config, logger_mock)
    with pytest.raises(HTTPError):
        await p._do_work()
    lta_rc_mock.assert_called_with("POST", '/Bundles/actions/pop?source=WIPAC&dest=DESY&status=verifying', {'claimant': f'{p.name}-{p.instance_uuid}'})


@pytest.mark.asyncio
async def test_desy_verifier_do_work_no_results(config: TestConfig, mocker: MockerFixture) -> None:
    """Test that _do_work goes on vacation when the LTA DB has no work."""
    logger_mock = mocker.MagicMock()
    dwc_mock = mocker.patch("lta.desy_verifier.DesyVerifier._do_work_claim", new_callable=AsyncMock)
    dwc_mock.return_value = False
    p = DesyVerifier(config, logger_mock)
    await p._do_work()
    dwc_mock.assert_called()


@pytest.mark.asyncio
async def test_desy_verifier_do_work_yes_results(config: TestConfig, mocker: MockerFixture) -> None:
    """Test that _do_work keeps working until the LTA DB has no work."""
    logger_mock = mocker.MagicMock()
    dwc_mock = mocker.patch("lta.desy_verifier.DesyVerifier._do_work_claim", new_callable=AsyncMock)
    dwc_mock.side_effect = [True, True, False]
    p = DesyVerifier(config, logger_mock)
    await p._do_work()
    assert dwc_mock.call_count == 3


@pytest.mark.asyncio
async def test_desy_verifier_do_work_claim_yes_result_update_fc_and_lta(config: TestConfig, mocker: MockerFixture) -> None:
    """Test that _do_work_claim processes the Bundle that it gets from the LTA DB."""
    logger_mock = mocker.MagicMock()
    lta_rc_mock = mocker.patch("rest_tools.client.RestClient.request", new_callable=AsyncMock)
    lta_rc_mock.return_value = {
        "bundle": {
            "one": 1,
        },
    }
    abtfc_mock = mocker.patch("lta.desy_verifier.DesyVerifier._add_bundle_to_file_catalog", new_callable=AsyncMock)
    ubild_mock = mocker.patch("lta.desy_verifier.DesyVerifier._update_bundle_in_lta_db", new_callable=AsyncMock)
    p = DesyVerifier(config, logger_mock)
    assert await p._do_work_claim()
    lta_rc_mock.assert_called_with("POST", '/Bundles/actions/pop?source=WIPAC&dest=DESY&status=verifying', {'claimant': f'{p.name}-{p.instance_uuid}'})
    abtfc_mock.assert_called_with(mocker.ANY, {"one": 1})
    ubild_mock.assert_called_with(mocker.ANY, {"one": 1})


@pytest.mark.asyncio
async def test_desy_verifier_add_bundle_to_file_catalog(config: TestConfig, mocker: MockerFixture) -> None:
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
    p = DesyVerifier(config, logger_mock)
    assert await p._add_bundle_to_file_catalog(lta_rc_mock, bundle)
    assert lta_rc_mock.request.call_count == 3
    lta_rc_mock.request.assert_called_with("GET", '/Metadata?bundle_uuid=7ec8a8f9-fae3-4f25-ae54-c1f66014f5ef&limit=1000')
    assert fc_rc_mock.call_count == 7
    fc_rc_mock.assert_called_with("POST", '/api/files/93bcd96e-0110-4064-9a79-b5bdfa3effb4/locations', mocker.ANY)


@pytest.mark.asyncio
async def test_desy_verifier_add_bundle_to_file_catalog_patch_after_post_error(config: TestConfig, mocker: MockerFixture) -> None:
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
    p = DesyVerifier(config, logger_mock)
    assert await p._add_bundle_to_file_catalog(lta_rc_mock, bundle)
    assert lta_rc_mock.request.call_count == 3
    lta_rc_mock.request.assert_called_with("GET", '/Metadata?bundle_uuid=7ec8a8f9-fae3-4f25-ae54-c1f66014f5ef&limit=1000')
    assert fc_rc_mock.call_count == 8
    fc_rc_mock.assert_called_with("POST", '/api/files/93bcd96e-0110-4064-9a79-b5bdfa3effb4/locations', mocker.ANY)


@pytest.mark.asyncio
async def test_desy_verifier_update_bundle_in_lta_db(config: TestConfig, mocker: MockerFixture) -> None:
    """Test that _update_bundle_in_lta_db updates the bundle as verified in the LTA DB."""
    logger_mock = mocker.MagicMock()
    bundle = {"uuid": "7ec8a8f9-fae3-4f25-ae54-c1f66014f5ef"}
    lta_mock = mocker.MagicMock()
    lta_rc_mock = mocker.patch("rest_tools.client.RestClient.request", new_callable=AsyncMock)
    lta_rc_mock.return_value = True
    lta_mock.request = lta_rc_mock
    p = DesyVerifier(config, logger_mock)
    assert await p._update_bundle_in_lta_db(lta_mock, bundle)
    lta_rc_mock.assert_called_with("PATCH", '/Bundles/7ec8a8f9-fae3-4f25-ae54-c1f66014f5ef', mocker.ANY)
