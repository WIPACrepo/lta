# test_transfer_request_finisher.py
"""Unit tests for lta/transfer_request_finisher.py."""
from uuid import uuid1

# fmt:off

from typing import Dict
from unittest.mock import AsyncMock, call

import pytest
from pytest import MonkeyPatch
from pytest_mock import MockerFixture
from tornado.web import HTTPError

from lta.transfer_request_finisher import main_sync, TransferRequestFinisher

TestConfig = Dict[str, str]


@pytest.fixture
def config() -> TestConfig:
    """Supply a stock TransferRequestFinisher component configuration."""
    return {
        "CLIENT_ID": "long-term-archive",
        "CLIENT_SECRET": "hunter2",  # http://bash.org/?244321
        "COMPONENT_NAME": "testing-transfer_request_finisher",
        "DEST_SITE": "NERSC",
        "INPUT_STATUS": "deleted",
        "LOG_LEVEL": "DEBUG",
        "LTA_AUTH_OPENID_URL": "localhost:12345",
        "LTA_REST_URL": "localhost:12347",
        "OUTPUT_STATUS": "finished",
        "PROMETHEUS_METRICS_PORT": "8080",
        "RUCIO_PASSWORD": "hunter2",
        "RUN_ONCE_AND_DIE": "False",
        "RUN_UNTIL_NO_WORK": "False",
        "SOURCE_SITE": "WIPAC",
        "TRANSFER_CONFIG_PATH": "examples/rucio.json",
        "WORK_RETRIES": "3",
        "WORK_SLEEP_DURATION_SECONDS": "60",
        "WORK_TIMEOUT_SECONDS": "30",
        "FILE_CATALOG_CLIENT_ID": "file-catalog-client-id",
        "FILE_CATALOG_CLIENT_SECRET": "file-catalog-client-secret",
        "FILE_CATALOG_REST_URL": "http://kVj74wBA1AMTDV8zccn67pGuWJqHZzD7iJQHrUJKA.com/",
    }


def test_constructor_config(config: TestConfig, mocker: MockerFixture) -> None:
    """Test that a TransferRequestFinisher can be constructed with a configuration object and a logging object."""
    logger_mock = mocker.MagicMock()
    p = TransferRequestFinisher(config, logger_mock)
    assert p.name == "testing-transfer_request_finisher"
    assert p.lta_auth_openid_url == "localhost:12345"
    assert p.lta_rest_url == "localhost:12347"
    assert p.source_site == "WIPAC"
    assert p.work_retries == 3
    assert p.work_sleep_duration_seconds == 60
    assert p.work_timeout_seconds == 30
    assert p.logger == logger_mock
    assert p.file_catalog_client_id == "file-catalog-client-id"
    assert p.file_catalog_client_secret == "file-catalog-client-secret"
    assert p.file_catalog_rest_url == "http://kVj74wBA1AMTDV8zccn67pGuWJqHZzD7iJQHrUJKA.com/"


def test_do_status(config: TestConfig, mocker: MockerFixture) -> None:
    """Verify that the TransferRequestFinisher has no additional state to offer."""
    logger_mock = mocker.MagicMock()
    p = TransferRequestFinisher(config, logger_mock)
    assert p._do_status() == {}


@pytest.mark.asyncio
async def test_transfer_request_finisher_logs_configuration(mocker: MockerFixture) -> None:
    """Test to make sure the TransferRequestFinisher logs its configuration."""
    logger_mock = mocker.MagicMock()
    transfer_request_finisher_config = {
        "CLIENT_ID": "long-term-archive",
        "CLIENT_SECRET": "hunter2",  # http://bash.org/?244321
        "COMPONENT_NAME": "logme-testing-transfer_request_finisher",
        "DEST_SITE": "NERSC",
        "INPUT_STATUS": "deleted",
        "LOG_LEVEL": "DEBUG",
        "LTA_AUTH_OPENID_URL": "localhost:12345",
        "LTA_REST_URL": "logme-http://zjwdm5ggeEgS1tZDZy9l1DOZU53uiSO4Urmyb8xL0.com/",
        "OUTPUT_STATUS": "finished",
        "PROMETHEUS_METRICS_PORT": "8080",
        "RUCIO_PASSWORD": "hunter3-electric-boogaloo",
        "RUN_ONCE_AND_DIE": "False",
        "RUN_UNTIL_NO_WORK": "False",
        "SOURCE_SITE": "WIPAC",
        "TRANSFER_CONFIG_PATH": "examples/rucio.json",
        "WORK_RETRIES": "5",
        "WORK_SLEEP_DURATION_SECONDS": "70",
        "WORK_TIMEOUT_SECONDS": "90",
        "FILE_CATALOG_CLIENT_ID": "file-catalog-client-id",
        "FILE_CATALOG_CLIENT_SECRET": "file-catalog-client-secret",
        "FILE_CATALOG_REST_URL": "logme-http://kVj74wBA1AMTDV8zccn67pGuWJqHZzD7iJQHrUJKA.com/",
    }
    TransferRequestFinisher(transfer_request_finisher_config, logger_mock)
    EXPECTED_LOGGER_CALLS = [
        call("transfer_request_finisher 'logme-testing-transfer_request_finisher' is configured:"),
        call('CLIENT_ID = long-term-archive'),
        call('CLIENT_SECRET = [秘密]'),
        call('COMPONENT_NAME = logme-testing-transfer_request_finisher'),
        call('DEST_SITE = NERSC'),
        call('INPUT_STATUS = deleted'),
        call('LOG_LEVEL = DEBUG'),
        call('LTA_AUTH_OPENID_URL = localhost:12345'),
        call('LTA_REST_URL = logme-http://zjwdm5ggeEgS1tZDZy9l1DOZU53uiSO4Urmyb8xL0.com/'),
        call('OUTPUT_STATUS = finished'),
        call('PROMETHEUS_METRICS_PORT = 8080'),
        call('RUCIO_PASSWORD = hunter3-electric-boogaloo'),
        call('RUN_ONCE_AND_DIE = False'),
        call('RUN_UNTIL_NO_WORK = False'),
        call('SOURCE_SITE = WIPAC'),
        call('TRANSFER_CONFIG_PATH = examples/rucio.json'),
        call('WORK_RETRIES = 5'),
        call('WORK_SLEEP_DURATION_SECONDS = 70'),
        call('WORK_TIMEOUT_SECONDS = 90'),
        call('FILE_CATALOG_CLIENT_ID = file-catalog-client-id'),
        call('FILE_CATALOG_CLIENT_SECRET = [秘密]'),
        call('FILE_CATALOG_REST_URL = logme-http://kVj74wBA1AMTDV8zccn67pGuWJqHZzD7iJQHrUJKA.com/'),
    ]
    logger_mock.info.assert_has_calls(EXPECTED_LOGGER_CALLS)


@pytest.mark.asyncio
async def test_script_main_sync(config: TestConfig, mocker: MockerFixture, monkeypatch: MonkeyPatch) -> None:
    """
    Verify TransferRequestFinisher component behavior when run as a script.

    Test to make sure running the TransferRequestFinisher as a script does the setup work
    that we expect and then launches the transfer_request_finisher service.
    """
    for key in config.keys():
        monkeypatch.setenv(key, config[key])
    mock_run = mocker.patch("asyncio.run")
    mock_main = mocker.patch("lta.transfer_request_finisher.main")
    mock_shs = mocker.patch("lta.transfer_request_finisher.start_http_server")
    main_sync()
    mock_shs.assert_called()
    mock_main.assert_called()
    mock_run.assert_called()
    await mock_run.call_args.args[0]


@pytest.mark.asyncio
async def test_transfer_request_finisher_run(config: TestConfig, mocker: MockerFixture) -> None:
    """Test the TransferRequestFinisher does the work the transfer_request_finisher should do."""
    logger_mock = mocker.MagicMock()
    p = TransferRequestFinisher(config, logger_mock)
    p._do_work = AsyncMock()  # type: ignore[method-assign]
    await p.run()
    p._do_work.assert_called()


@pytest.mark.asyncio
async def test_transfer_request_finisher_run_exception(config: TestConfig, mocker: MockerFixture) -> None:
    """Test an error doesn't kill the TransferRequestFinisher."""
    logger_mock = mocker.MagicMock()
    p = TransferRequestFinisher(config, logger_mock)
    p._do_work = AsyncMock()  # type: ignore[method-assign]
    p._do_work.side_effect = [Exception("bad thing happen!")]
    await p.run()
    p._do_work.assert_called()


@pytest.mark.asyncio
async def test_transfer_request_finisher_do_work_pop_exception(config: TestConfig, mocker: MockerFixture) -> None:
    """Test that _do_work raises when the RestClient can't pop."""
    logger_mock = mocker.MagicMock()
    lta_rc_mock = AsyncMock()
    lta_rc_mock.request = AsyncMock()
    lta_rc_mock.request.side_effect = HTTPError(500, "LTA DB on fire. Again.")
    p = TransferRequestFinisher(config, logger_mock)
    with pytest.raises(HTTPError):
        await p._do_work(lta_rc_mock)
    lta_rc_mock.request.assert_called_with("POST", '/Bundles/actions/pop?source=WIPAC&dest=NERSC&status=deleted', {'claimant': f'{p.name}-{p.instance_uuid}'})


@pytest.mark.asyncio
async def test_transfer_request_finisher_do_work_no_results(config: TestConfig, mocker: MockerFixture) -> None:
    """Test that _do_work goes on vacation when the LTA DB has no work."""
    logger_mock = mocker.MagicMock()
    dwc_mock = mocker.patch("lta.transfer_request_finisher.TransferRequestFinisher._do_work_claim", new_callable=AsyncMock)
    dwc_mock.return_value = False
    p = TransferRequestFinisher(config, logger_mock)
    await p._do_work(AsyncMock())
    dwc_mock.assert_called()


@pytest.mark.asyncio
async def test_transfer_request_finisher_do_work_yes_results(config: TestConfig, mocker: MockerFixture) -> None:
    """Test that _do_work keeps working until the LTA DB has no work."""
    logger_mock = mocker.MagicMock()
    dwc_mock = mocker.patch("lta.transfer_request_finisher.TransferRequestFinisher._do_work_claim", new_callable=AsyncMock)
    dwc_mock.side_effect = [True, True, False]
    p = TransferRequestFinisher(config, logger_mock)
    await p._do_work(AsyncMock())
    dwc_mock.assert_called()


@pytest.mark.asyncio
async def test_transfer_request_finisher_do_work_claim_no_result(config: TestConfig, mocker: MockerFixture) -> None:
    """Test that _do_work_claim does not work when the LTA DB has no work."""
    logger_mock = mocker.MagicMock()
    lta_rc_mock = AsyncMock()
    lta_rc_mock.request = AsyncMock()
    lta_rc_mock.request.return_value = {
        "bundle": None
    }
    utr_mock = mocker.patch("lta.transfer_request_finisher.TransferRequestFinisher._update_transfer_request", new_callable=AsyncMock)
    p = TransferRequestFinisher(config, logger_mock)
    await p._do_work_claim(lta_rc_mock, MagicMock())
    lta_rc_mock.request.assert_called_with("POST", '/Bundles/actions/pop?source=WIPAC&dest=NERSC&status=deleted', {'claimant': f'{p.name}-{p.instance_uuid}'})
    utr_mock.assert_not_called()


@pytest.mark.asyncio
async def test_transfer_request_finisher_do_work_claim_yes_result(config: TestConfig, mocker: MockerFixture) -> None:
    """Test that _do_work_claim processes the Bundle that it gets from the LTA DB."""
    logger_mock = mocker.MagicMock()
    lta_rc_mock = AsyncMock()
    lta_rc_mock.request = AsyncMock()
    lta_rc_mock.request.return_value = {
        "bundle": {
            "one": 1,
        },
    }
    utr_mock = mocker.patch("lta.transfer_request_finisher.TransferRequestFinisher._update_transfer_request", new_callable=AsyncMock)
    mbf_mock = mocker.patch("lta.transfer_request_finisher.TransferRequestFinisher._migrate_bundle_files_to_file_catalog", new_callable=AsyncMock)
    p = TransferRequestFinisher(config, logger_mock)
    assert not await p._do_work_claim(lta_rc_mock, MagicMock())
    lta_rc_mock.request.assert_called_with("POST", '/Bundles/actions/pop?source=WIPAC&dest=NERSC&status=deleted', {'claimant': f'{p.name}-{p.instance_uuid}'})
    utr_mock.assert_called_with(mocker.ANY, {"one": 1})
    mbf_mock.assert_called_with(mocker.ANY, mocker.ANY, {"one": 1})


@pytest.mark.asyncio
async def test_transfer_request_finisher_update_transfer_request_no(config: TestConfig, mocker: MockerFixture) -> None:
    """Test that _update_transfer_request does not update an incomplete TransferRequest."""
    deleted_bundle = {
        "uuid": "8286d3ba-fb1b-4923-876d-935bdf7fc99e",
        "request": "a8758a77-2a66-46e6-b43d-b4c74d3078a6",
        "status": "deleted",
    }
    transferring_bundle = {
        "uuid": "90a664cc-e3f9-4421-973f-7bc2bc7407d0",
        "request": "a8758a77-2a66-46e6-b43d-b4c74d3078a6",
        "status": "transferring",
    }
    logger_mock = mocker.MagicMock()
    lta_rc_mock = mocker.patch("rest_tools.client.RestClient", new_callable=AsyncMock)
    lta_rc_mock.request.side_effect = [
        {
            "results": [
                "8286d3ba-fb1b-4923-876d-935bdf7fc99e",
                "90a664cc-e3f9-4421-973f-7bc2bc7407d0",
            ],
        },
        deleted_bundle,
        transferring_bundle,
        deleted_bundle,
    ]
    p = TransferRequestFinisher(config, logger_mock)
    await p._update_transfer_request(lta_rc_mock, deleted_bundle)
    lta_rc_mock.request.assert_called_with("PATCH", '/Bundles/8286d3ba-fb1b-4923-876d-935bdf7fc99e', {
        'claimed': False,
        'update_timestamp': mocker.ANY,
        'work_priority_timestamp': mocker.ANY,
    })


@pytest.mark.asyncio
async def test_transfer_request_finisher_update_transfer_request_yes(config: TestConfig, mocker: MockerFixture) -> None:
    """Test that _update_transfer_request does update a complete TransferRequest."""
    deleted_bundle = {
        "uuid": "8286d3ba-fb1b-4923-876d-935bdf7fc99e",
        "request": "a8758a77-2a66-46e6-b43d-b4c74d3078a6",
        "status": "deleted",
    }
    finished_bundle = {
        "uuid": "90a664cc-e3f9-4421-973f-7bc2bc7407d0",
        "request": "a8758a77-2a66-46e6-b43d-b4c74d3078a6",
        "status": "finished",
    }
    transfer_request = {
        "uuid": "a8758a77-2a66-46e6-b43d-b4c74d3078a6",
    }
    logger_mock = mocker.MagicMock()
    lta_rc_mock = mocker.patch("rest_tools.client.RestClient", new_callable=AsyncMock)
    lta_rc_mock.request.side_effect = [
        {
            "results": [
                "8286d3ba-fb1b-4923-876d-935bdf7fc99e",
                "90a664cc-e3f9-4421-973f-7bc2bc7407d0",
            ],
        },
        deleted_bundle,
        finished_bundle,
        transfer_request,
        deleted_bundle,
        finished_bundle,
    ]
    p = TransferRequestFinisher(config, logger_mock)
    await p._update_transfer_request(lta_rc_mock, deleted_bundle)
    lta_rc_mock.request.assert_called_with("PATCH", '/Bundles/90a664cc-e3f9-4421-973f-7bc2bc7407d0', {
        "claimant": mocker.ANY,
        "claimed": False,
        "claim_timestamp": mocker.ANY,
        "status": "finished",
        "reason": "",
        "update_timestamp": mocker.ANY,
    })

@pytest.mark.asyncio
async def test_transfer_request_finisher_migrate_bundle_files_to_file_catalog(config: TestConfig, mocker: MockerFixture) -> None:
    """Test that _migrate_bundle_files_to_file_catalog adds a record for the bundle and adds its location to constituent files."""
    logger_mock = mocker.MagicMock()
    bundle = {
        "uuid": "7ec8a8f9-fae3-4f25-ae54-c1f66014f5ef",
        "path": "/data/exp/IceCube/2019/filtered/PFFilt/1109",
        "bundle_path": "/path/to/source/rse/7ec8a8f9-fae3-4f25-ae54-c1f66014f5ef.zip",
        "checksum": {
            "sha512": "97de2a6ad728f50a381eb1be6ecf015019887fac27e8bf608334fb72caf8d3f654fdcce68c33b0f0f27de499b84e67b8357cd81ef7bba3cdaa9e23a648f43ad2",
        },
        "size": 12345,
        "dest": "MOON",
        "final_dest_location": {
            "path": "/its/now/on-the/moon.tape",
            "foo": "bar",
        }
    }
    fc_rc_mock = mocker.MagicMock()
    fc_rc_mock.request = AsyncMock()
    fc_rc_mock.request.side_effect = [
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
    lta_rc_mock = mocker.MagicMock()
    lta_rc_mock.request = AsyncMock()
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
    p = TransferRequestFinisher(config, logger_mock)
    await p._migrate_bundle_files_to_file_catalog(fc_rc_mock, lta_rc_mock, bundle)
    assert lta_rc_mock.request.call_count == 3
    lta_rc_mock.request.assert_called_with("GET", '/Metadata?bundle_uuid=7ec8a8f9-fae3-4f25-ae54-c1f66014f5ef&limit=1000')
    assert fc_rc_mock.request.call_count == 7
    assert fc_rc_mock.request.await_args_list == [
        # POST /api/files - create the bundle record
        call(
            "POST",
            '/api/files',
            {
                "uuid": "7ec8a8f9-fae3-4f25-ae54-c1f66014f5ef",
                "logical_name": "/its/now/on-the/moon.tape",
                "checksum": {
                    "sha512": "97de2a6ad728f50a381eb1be6ecf015019887fac27e8bf608334fb72caf8d3f654fdcce68c33b0f0f27de499b84e67b8357cd81ef7bba3cdaa9e23a648f43ad2",
                },
                "locations": [
                    {
                        "site": "MOON",
                        "path": "/its/now/on-the/moon.tape",
                        "foo": "bar",
                    }
                ],
                "file_size": 12345,
                "lta": {
                    "date_archived": mocker.ANY,
                },
            },
        ),
        # GET /api/files/UUID - get the file record
        call(
            "GET",
            '/api/files/e0d15152-fd73-4e98-9aea-a9e5fdd8618e',
        ),
        # POST /api/files/UUID/locations - add the location
        call(
            "POST",
            '/api/files/e0d15152-fd73-4e98-9aea-a9e5fdd8618e/locations',
            {
                "locations": [
                    {
                        "site": "MOON",
                        "path": "/its/now/on-the/moon.tape:/data/exp/IceCube/2019/filtered/PFFilt/1109/file1.tar.gz",
                        "archive": True,
                    }
                ]
            },
        ),
        # GET /api/files/UUID - get the file record
        call(
            "GET",
            '/api/files/e107a8e8-8a86-41d6-9d4d-b6c8bc3797c4',
        ),
        # POST /api/files/UUID/locations - add the location
        call(
            "POST",
            '/api/files/e107a8e8-8a86-41d6-9d4d-b6c8bc3797c4/locations',
            {
                "locations": [
                    {
                        "site": "MOON",
                        "path": "/its/now/on-the/moon.tape:/data/exp/IceCube/2019/filtered/PFFilt/1109/file2.tar.gz",
                        "archive": True,
                    }
                ]
            },
        ),
        # GET /api/files/UUID - get the file record
        call(
            "GET",
            '/api/files/93bcd96e-0110-4064-9a79-b5bdfa3effb4',
        ),
        # POST /api/files/UUID/locations - add the location
        call(
            "POST",
            '/api/files/93bcd96e-0110-4064-9a79-b5bdfa3effb4/locations',
            {
                "locations": [
                    {
                        "site": "MOON",
                        "path": "/its/now/on-the/moon.tape:/data/exp/IceCube/2019/filtered/PFFilt/1109/file3.tar.gz",
                        "archive": True,
                    }
                ]
            },
        ),
    ]


@pytest.mark.asyncio
async def test_transfer_request_finisher_migrate_bundle_files_to_file_catalog_patch_after_post_error(config: TestConfig, mocker: MockerFixture) -> None:
    """Test that _migrate_bundle_files_to_file_catalog patches the record for the bundle already in the file catalog."""
    logger_mock = mocker.MagicMock()
    bundle = {
        "uuid": "7ec8a8f9-fae3-4f25-ae54-c1f66014f5ef",
        "path": "/data/exp/IceCube/2019/filtered/PFFilt/1109",
        "bundle_path": "/path/to/source/rse/7ec8a8f9-fae3-4f25-ae54-c1f66014f5ef.zip",
        "checksum": {
            "sha512": "97de2a6ad728f50a381eb1be6ecf015019887fac27e8bf608334fb72caf8d3f654fdcce68c33b0f0f27de499b84e67b8357cd81ef7bba3cdaa9e23a648f43ad2",
        },
        "size": 12345,
        "final_dest_location": {
            "path": "/its/now/on-the/moon.tape",
            "foo": "bar",
        },
        "dest": "MOON",
    }
    fc_rc_mock = mocker.MagicMock()
    fc_rc_mock.request = AsyncMock()
    fc_rc_mock.request.side_effect = [
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
    lta_rc_mock = mocker.MagicMock()
    lta_rc_mock.request = AsyncMock()
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
    p = TransferRequestFinisher(config, logger_mock)
    await p._migrate_bundle_files_to_file_catalog(fc_rc_mock, lta_rc_mock, bundle)
    assert lta_rc_mock.request.call_count == 3
    lta_rc_mock.request.assert_called_with("GET", '/Metadata?bundle_uuid=7ec8a8f9-fae3-4f25-ae54-c1f66014f5ef&limit=1000')
    assert fc_rc_mock.request.call_count == 8
    assert fc_rc_mock.request.await_args_list == [
        # POST /api/files - bundle record already exists!!
        call(
            "POST",
            '/api/files',
            {
                "uuid": "7ec8a8f9-fae3-4f25-ae54-c1f66014f5ef",
                "logical_name": "/its/now/on-the/moon.tape",
                "checksum": {
                    "sha512": "97de2a6ad728f50a381eb1be6ecf015019887fac27e8bf608334fb72caf8d3f654fdcce68c33b0f0f27de499b84e67b8357cd81ef7bba3cdaa9e23a648f43ad2",
                },
                "locations": [
                    {
                        "site": "MOON",
                        "path": "/its/now/on-the/moon.tape",
                        "foo": "bar",
                    }
                ],
                "file_size": 12345,
                "lta": {
                    "date_archived": mocker.ANY,
                },
            },
        ),
        # PATCH /api/files/UUID - bundle record gets updated
        call(
            "PATCH",
            '/api/files/7ec8a8f9-fae3-4f25-ae54-c1f66014f5ef',
            {
                "uuid": "7ec8a8f9-fae3-4f25-ae54-c1f66014f5ef",
                "logical_name": "/its/now/on-the/moon.tape",
                "checksum": {
                    "sha512": "97de2a6ad728f50a381eb1be6ecf015019887fac27e8bf608334fb72caf8d3f654fdcce68c33b0f0f27de499b84e67b8357cd81ef7bba3cdaa9e23a648f43ad2",
                },
                "locations": [
                    {
                        "site": "MOON",
                        "path": "/its/now/on-the/moon.tape",
                        "foo": "bar",
                    }
                ],
                "file_size": 12345,
                "lta": {
                    "date_archived": mocker.ANY,
                },
            },
        ),
        # GET /api/files/UUID - get the file record
        call(
            "GET",
            '/api/files/e0d15152-fd73-4e98-9aea-a9e5fdd8618e',
        ),
        # POST /api/files/UUID/locations - add the location
        call(
            "POST",
            '/api/files/e0d15152-fd73-4e98-9aea-a9e5fdd8618e/locations',
            {
                "locations": [
                    {
                        "site": "MOON",
                        "path": "/its/now/on-the/moon.tape:/data/exp/IceCube/2019/filtered/PFFilt/1109/file1.tar.gz",
                        "archive": True,
                    }
                ]
            },
        ),
        # GET /api/files/UUID - get the file record
        call(
            "GET",
            '/api/files/e107a8e8-8a86-41d6-9d4d-b6c8bc3797c4',
        ),
        # POST /api/files/UUID/locations - add the location
        call(
            "POST",
            '/api/files/e107a8e8-8a86-41d6-9d4d-b6c8bc3797c4/locations',
            {
                "locations": [
                    {
                        "site": "MOON",
                        "path": "/its/now/on-the/moon.tape:/data/exp/IceCube/2019/filtered/PFFilt/1109/file2.tar.gz",
                        "archive": True,
                    }
                ]
            },
        ),
        # GET /api/files/UUID - get the file record
        call(
            "GET",
            '/api/files/93bcd96e-0110-4064-9a79-b5bdfa3effb4',
        ),
        # POST /api/files/UUID/locations - add the location
        call(
            "POST",
            '/api/files/93bcd96e-0110-4064-9a79-b5bdfa3effb4/locations',
            {
                "locations": [
                    {
                        "site": "MOON",
                        "path": "/its/now/on-the/moon.tape:/data/exp/IceCube/2019/filtered/PFFilt/1109/file3.tar.gz",
                        "archive": True,
                    }
                ]
            },
        ),
    ]
