# test_rest_server.py
"""Unit tests for lta/rest_server.py."""

# fmt:off

import asyncio
import logging
import os
import socket
import tracemalloc
from typing import Any, AsyncGenerator, Callable, cast, Dict, List
from unittest.mock import AsyncMock
from urllib.parse import quote_plus

from pymongo import MongoClient
from pymongo.database import Database
import pytest
from pytest import MonkeyPatch
import pytest_asyncio
from pytest_mock import MockerFixture
from rest_tools.client import RestClient
from rest_tools.utils import Auth
from requests.exceptions import HTTPError

from lta.rest_server import boolify, main, start, unique_id

LtaCollection = Database[Dict[str, Any]]
RestClientFactory = Callable[[str, float], RestClient]

tracemalloc.start(1)

ALL_DOCUMENTS: Dict[str, str] = {}
REMOVE_ID = {"_id": False}

CONFIG = {
    "LOG_LEVEL": "DEBUG",
    'LTA_MONGODB_AUTH_USER': '',
    'LTA_MONGODB_AUTH_PASS': '',
    'LTA_MONGODB_DATABASE_NAME': 'lta',
    'LTA_MONGODB_HOST': 'localhost',
    'LTA_MONGODB_PORT': '27017',
    'OTEL_EXPORTER_OTLP_ENDPOINT': 'localhost:4317',
    'PROMETHEUS_METRICS_PORT': '8090',
}
for k in CONFIG:
    if k in os.environ:
        CONFIG[k] = os.environ[k]


@pytest.fixture
def mongo(monkeypatch: MonkeyPatch) -> LtaCollection:
    """Get a reference to a test instance of a MongoDB Database."""
    mongo_user = quote_plus(CONFIG["LTA_MONGODB_AUTH_USER"])
    mongo_pass = quote_plus(CONFIG["LTA_MONGODB_AUTH_PASS"])
    mongo_host = CONFIG["LTA_MONGODB_HOST"]
    mongo_port = int(CONFIG["LTA_MONGODB_PORT"])
    lta_mongodb_url = f"mongodb://{mongo_host}"
    if mongo_user and mongo_pass:
        lta_mongodb_url = f"mongodb://{mongo_user}:{mongo_pass}@{mongo_host}"
    client: MongoClient[Dict[str, Any]] = MongoClient(lta_mongodb_url, port=mongo_port)
    db = client[CONFIG['LTA_MONGODB_DATABASE_NAME']]
    for collection in db.list_collection_names():
        if 'system' not in collection:
            db.drop_collection(collection)
    return db


@pytest.fixture
def port() -> int:
    """Get an ephemeral port number."""
    # https://unix.stackexchange.com/a/132524
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.bind(('', 0))
    addr = s.getsockname()
    ephemeral_port = addr[1]
    s.close()
    return cast(int, ephemeral_port)


@pytest_asyncio.fixture
async def rest(monkeypatch: MonkeyPatch, port: int) -> AsyncGenerator[RestClientFactory, None]:
    """Provide RestClient as a test fixture."""
    # setup_function
    monkeypatch.setenv("CI_TEST", "TRUE")
    monkeypatch.setenv("LTA_MONGODB_DATABASE_NAME", CONFIG['LTA_MONGODB_DATABASE_NAME'])
    monkeypatch.setenv("LTA_REST_PORT", str(port))
    monkeypatch.setenv("LTA_SITE_CONFIG", "examples/site.json")
    monkeypatch.setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "localhost:4317")
    monkeypatch.setenv("PROMETHEUS_METRICS_PORT", "8090")
    s = start(debug=True)

    def client(role: str = "admin", timeout: float = 0.5) -> RestClient:
        # But they were, all of them, deceived, for another Token was made.
        # In the land of PyTest, in the fires of Mount Fixture, the Dark Lord
        # Sauron forged in secret a master Token, to control all others. And
        # into this Token he poured his cruelty, his malice and his will to
        # dominate all life. One Token to rule them all.
        auth = Auth("secret")  # type: ignore[no-untyped-call]
        token_data: Dict[str, Any] = {
            "resource_access": {
                "long-term-archive": {
                    "roles": [role]
                }
            }
        }
        logging.info("setting role to %s", role)
        token = auth.create_token(subject="lta",  # type: ignore[no-untyped-call]
                                  expiration=300,
                                  payload=token_data,
                                  headers=None)
        return RestClient(f'http://localhost:{port}', token=token, timeout=timeout, retries=0)

    try:
        yield client
    # teardown_function
    finally:
        await s.stop()  # type: ignore[no-untyped-call]
        await asyncio.sleep(0.01)


# -----------------------------------------------------------------------------


def test_boolify() -> None:
    """Test the boolify function."""
    assert not boolify("0")
    assert not boolify("F")
    assert not boolify("f")
    assert not boolify("FALSE")
    assert not boolify("false")
    assert not boolify("N")
    assert not boolify("n")
    assert not boolify("NO")
    assert not boolify("no")

    assert boolify("1")
    assert boolify("T")
    assert boolify("t")
    assert boolify("TRUE")
    assert boolify("true")
    assert boolify("Y")
    assert boolify("y")
    assert boolify("YES")
    assert boolify("yes")

    assert not boolify("alice")
    assert not boolify("bob")

# -----------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_server_reachability(rest: RestClientFactory) -> None:
    """Check that we can reach the server."""
    r = rest()  # type: ignore[call-arg]
    ret = await r.request('GET', '/')
    assert ret == {}
    r.close()


@pytest.mark.asyncio
async def test_transfer_request_fail(rest: RestClientFactory) -> None:
    """Check for bad transfer request handling."""
    r = rest()  # type: ignore[call-arg]
    request: Dict[str, Any] = {'dest': ['bar']}
    with pytest.raises(Exception):
        await r.request('POST', '/TransferRequests', request)

    request = {'source': 'foo'}
    with pytest.raises(Exception):
        await r.request('POST', '/TransferRequests', request)

    request = {'source': 'foo', 'dest': 'bar'}
    with pytest.raises(Exception):
        await r.request('POST', '/TransferRequests', request)

    request = {'source': 'foo', 'dest': []}
    with pytest.raises(Exception):
        await r.request('POST', '/TransferRequests', request)

    request = {'source': [], 'dest': 'bar', 'path': 'snafu'}
    with pytest.raises(Exception):
        await r.request('POST', '/TransferRequests', request)

    request = {'source': 'foo', 'dest': [], 'path': 'snafu'}
    with pytest.raises(Exception):
        await r.request('POST', '/TransferRequests', request)

    request = {'source': 'foo', 'dest': 'bar', 'path': []}
    with pytest.raises(Exception):
        await r.request('POST', '/TransferRequests', request)

    request = {'source': "", 'dest': 'bar', 'path': 'snafu'}
    with pytest.raises(Exception):
        await r.request('POST', '/TransferRequests', request)

    request = {'source': 'foo', 'dest': "", 'path': 'snafu'}
    with pytest.raises(Exception):
        await r.request('POST', '/TransferRequests', request)

    request = {'source': 'foo', 'dest': 'bar', 'path': ""}
    with pytest.raises(Exception):
        await r.request('POST', '/TransferRequests', request)
    r.close()


@pytest.mark.asyncio
async def test_transfer_request_crud(mongo: LtaCollection, rest: RestClientFactory) -> None:
    """Check CRUD semantics for transfer requests."""
    r = rest(role="system")  # type: ignore[call-arg]
    request = {'source': 'foo', 'dest': 'bar', 'path': 'snafu'}
    ret = await r.request('POST', '/TransferRequests', request)
    uuid = ret['TransferRequest']
    assert uuid

    ret = await r.request('GET', '/TransferRequests')
    assert len(ret['results']) == 1

    ret = await r.request('GET', f'/TransferRequests/{uuid}')
    for k in request:
        assert request[k] == ret[k]

    request2 = {'bar': 2}
    ret = await r.request('PATCH', f'/TransferRequests/{uuid}', request2)
    assert ret == {}

    with pytest.raises(Exception):
        await r.request('PATCH', '/TransferRequests/foo', request2)

    ret = await r.request('DELETE', f'/TransferRequests/{uuid}')
    assert not ret

    with pytest.raises(Exception):
        await r.request('GET', f'/TransferRequests/{uuid}')

    ret = await r.request('DELETE', f'/TransferRequests/{uuid}')
    assert not ret

    ret = await r.request('GET', '/TransferRequests')
    assert len(ret['results']) == 0
    r.close()


@pytest.mark.asyncio
async def test_transfer_request_pop(rest: RestClientFactory) -> None:
    """Check pop action for transfer requests."""
    r = rest('system')  # type: ignore[call-arg]
    request = {
        'source': 'WIPAC',
        'dest': 'NERSC',
        'path': '/data/exp/foo/bar',
    }
    ret = await r.request('POST', '/TransferRequests', request)
    uuid = ret['TransferRequest']
    assert uuid

    # I'm being a jerk and claiming without naming myself as claimant
    with pytest.raises(Exception):
        await r.request('POST', '/TransferRequests/actions/pop?source=JERK_STORE')

    # I'm at NERSC, and should have no work
    nersc_pop_claimant = {
        'claimant': 'testing-picker-aaaed864-0112-4bcf-a069-bb55c12e291d',
    }
    ret = await r.request('POST', '/TransferRequests/actions/pop?source=NERSC', nersc_pop_claimant)
    assert not ret['transfer_request']

    # I'm the picker at WIPAC, and should have one work item
    wipac_pop_claimant = {
        'claimant': 'testing-picker-3e4da7c3-bb73-4ab3-b6a6-02ceff6501fc',
    }
    ret = await r.request('POST', '/TransferRequests/actions/pop?source=WIPAC', wipac_pop_claimant)
    assert ret['transfer_request']
    for k in request:
        assert request[k] == ret['transfer_request'][k]

    # repeating gets no work
    ret = await r.request('POST', '/TransferRequests/actions/pop?source=WIPAC', wipac_pop_claimant)
    assert not ret['transfer_request']
    r.close()


@pytest.mark.asyncio
async def test_script_main(mocker: MockerFixture) -> None:
    """Ensure that main sets up logging, starts a server, and runs the event loop."""
    mock_basicConfig = mocker.patch("logging.basicConfig")
    mock_start = mocker.patch("lta.rest_server.start")
    mock_start_http_server = mocker.patch("lta.rest_server.start_http_server")
    mock_Event = AsyncMock(spec=asyncio.Event)
    mock_asyncio_event = mocker.patch("asyncio.Event")
    mock_asyncio_event.return_value = mock_Event

    await main()

    mock_Event.wait.assert_called()
    mock_asyncio_event.assert_called()
    mock_start_http_server.assert_called()
    mock_start.assert_called()
    mock_basicConfig.assert_called()


@pytest.mark.asyncio
async def test_bundles_bulk_crud(mongo: LtaCollection, rest: RestClientFactory) -> None:
    """Check CRUD semantics for bundles."""
    r = rest('system')  # type: ignore[call-arg]

    #
    # Create - POST /Bundles/actions/bulk_create
    #
    request = {'bundles': [{"name": "one"}, {"name": "two"}]}
    ret = await r.request('POST', '/Bundles/actions/bulk_create', request)
    assert len(ret["bundles"]) == 2
    assert ret["count"] == 2

    #
    # Read - GET /Bundles
    #
    ret = await r.request('GET', '/Bundles')
    results = ret["results"]
    assert len(results) == 2

    #
    # Update - POST /Bundles/actions/bulk_update
    #
    # request = {'files': results, 'update': {'key': 'value'}}
    results2 = results + [unique_id()]
    request2 = {'bundles': results2, 'update': {'key': 'value'}}
    ret = await r.request('POST', '/Bundles/actions/bulk_update', request2)
    assert ret["count"] == 2
    assert ret["bundles"] == results

    #
    # Read - GET /Bundles/UUID
    #
    for result in results:
        ret = await r.request('GET', f'/Bundles/{result}')
        assert ret["uuid"] == result
        assert ret["name"] in ["one", "two"]
        assert ret["key"] == "value"

    #
    # Delete - POST /Bundles/actions/bulk_delete
    #
    results2 = results + [unique_id()]
    request2 = {'bundles': results2}
    ret = await r.request('POST', '/Bundles/actions/bulk_delete', request2)
    assert ret["count"] == 2
    assert ret["bundles"] == results

    #
    # Read - GET /Bundles
    #
    ret = await r.request('GET', '/Bundles')
    results = ret["results"]
    assert len(results) == 0
    r.close()


@pytest.mark.asyncio
async def test_bundles_actions_bulk_create_errors(rest: RestClientFactory) -> None:
    """Check error conditions for bulk_create."""
    r = rest('system')  # type: ignore[call-arg]

    request: Dict[str, Any] = {}
    with pytest.raises(Exception):
        await r.request('POST', '/Bundles/actions/bulk_create', request)

    request = {'bundles': ''}
    with pytest.raises(Exception):
        await r.request('POST', '/Bundles/actions/bulk_create', request)

    request = {'bundles': []}
    with pytest.raises(Exception):
        await r.request('POST', '/Bundles/actions/bulk_create', request)
    r.close()


@pytest.mark.asyncio
async def test_bundles_actions_bulk_delete_errors(rest: RestClientFactory) -> None:
    """Check error conditions for bulk_delete."""
    r = rest('system')  # type: ignore[call-arg]

    request: Dict[str, Any] = {}
    with pytest.raises(Exception):
        await r.request('POST', '/Bundles/actions/bulk_delete', request)

    request = {'bundles': ''}
    with pytest.raises(Exception):
        await r.request('POST', '/Bundles/actions/bulk_delete', request)

    request = {'bundles': []}
    with pytest.raises(Exception):
        await r.request('POST', '/Bundles/actions/bulk_delete', request)
    r.close()


@pytest.mark.asyncio
async def test_bundles_actions_bulk_update_errors(rest: RestClientFactory) -> None:
    """Check error conditions for bulk_update."""
    r = rest('system')  # type: ignore[call-arg]

    request: Dict[str, Any] = {}
    with pytest.raises(Exception):
        await r.request('POST', '/Bundles/actions/bulk_update', request)

    request = {'update': ''}
    with pytest.raises(Exception):
        await r.request('POST', '/Bundles/actions/bulk_update', request)

    request = {'update': {}}
    with pytest.raises(Exception):
        await r.request('POST', '/Bundles/actions/bulk_update', request)

    request = {'update': {}, 'bundles': ''}
    with pytest.raises(Exception):
        await r.request('POST', '/Bundles/actions/bulk_update', request)

    request = {'update': {}, 'bundles': []}
    with pytest.raises(Exception):
        await r.request('POST', '/Bundles/actions/bulk_update', request)
    r.close()


@pytest.mark.asyncio
async def test_get_bundles_filter(mongo: LtaCollection, rest: RestClientFactory) -> None:
    """Check that GET /Bundles filters properly by query parameters.."""
    r = rest('system')  # type: ignore[call-arg]

    test_data = {
        'bundles': [
            {
                "degenerate": "bundle",
                "has no": "decent keys",
                "or values": "should be deleted",
            },
            {
                "source": "WIPAC:/data/exp/IceCube/2014/15f7a399-fe40-4337-bb7e-d68d2d28ec8e.zip",
                "status": "waiting",
                "verified": False,
            },
            {
                "source": "WIPAC:/tmp/path1/sub1/48091a00-0c97-482f-a716-2e721b8e9662.zip",
                "status": "waiting",
                "verified": False,
            },
            {
                "source": "WIPAC:/tmp/path1/sub1/24814fa8-875b-4bae-b034-ea8885d2aafe.zip",
                "status": "processing",
                "verified": False,
            },
            {
                "source": "WIPAC:/tmp/path1/sub1/cef98a3b-9a24-4fbc-b4e7-ef251367c020.zip",
                "status": "bundled",
                "verified": False,
            },
            {
                "source": "WIPAC:/tmp/path1/sub2/8f141afa-db2f-4337-9a24-560c383887b5.zip",
                "status": "waiting",
                "verified": False,
            },
            {
                "source": "WIPAC:/tmp/path1/sub2/f9153da6-3588-416d-ba59-91526376dc43.zip",
                "status": "processing",
                "verified": True,
            },
            {
                "source": "WIPAC:/tmp/path1/sub2/34b064a7-5ffa-4cb2-9661-6e8b80765e9f.zip",
                "status": "bundled",
                "verified": True,
            },
            {
                "source": "DESY:/tmp/path1/sub2/76e3c5e2-e1a2-42f0-9d59-6546d1cb85e6.zip",
                "status": "waiting",
                "verified": True,
            },
            {
                "source": "DESY:/tmp/path1/sub2/3bcd05f5-ceb8-4eb5-a5db-5f7d55a98ff4.zip",
                "status": "processing",
                "verified": True,
            },
            {
                "source": "DESY:/tmp/path1/sub2/e596e1f7-ddaa-4255-abe3-81a4769bf192.zip",
                "status": "bundled",
                "verified": True,
            },
        ]
    }

    #
    # Create - POST /Bundles/actions/bulk_create
    #
    ret = await r.request('POST', '/Bundles/actions/bulk_create', test_data)
    assert len(ret["bundles"]) == 11
    assert ret["count"] == 11

    #
    # Read - GET /Bundles
    #
    ret = await r.request('GET', '/Bundles')
    results = ret["results"]
    assert len(results) == 11

    ret = await r.request('GET', '/Bundles?location=WIPAC')
    results = ret["results"]
    assert len(results) == 7

    ret = await r.request('GET', '/Bundles?location=DESY')
    results = ret["results"]
    assert len(results) == 3

    ret = await r.request('GET', '/Bundles?location=WIPAC:/tmp/path1')
    results = ret["results"]
    assert len(results) == 6

    ret = await r.request('GET', '/Bundles?status=waiting')
    results = ret["results"]
    assert len(results) == 4

    ret = await r.request('GET', '/Bundles?status=processing')
    results = ret["results"]
    assert len(results) == 3

    ret = await r.request('GET', '/Bundles?status=bundled')
    results = ret["results"]
    assert len(results) == 3

    ret = await r.request('GET', '/Bundles?verified=true')
    results = ret["results"]
    assert len(results) == 5

    ret = await r.request('GET', '/Bundles?verified=false')
    results = ret["results"]
    assert len(results) == 5

    ret = await r.request('GET', '/Bundles?status=waiting&verified=false')
    results = ret["results"]
    assert len(results) == 3

    ret = await r.request('GET', '/Bundles?status=waiting&verified=true')
    results = ret["results"]
    assert len(results) == 1
    r.close()


@pytest.mark.asyncio
async def test_get_bundles_request_filter(mongo: LtaCollection, rest: RestClientFactory) -> None:
    """Check that GET /Bundles filters properly by query parameter request."""
    r = rest('system')  # type: ignore[call-arg]

    test_data = {
        'bundles': [
            {
                "request": "02faec90-9f42-4231-944e-237465c7b988",
                "source": "WIPAC:/data/exp/IceCube/2014/15f7a399-fe40-4337-bb7e-d68d2d28ec8e.zip",
                "status": "waiting",
                "verified": False,
            },
            {
                "request": "baebf071-702f-4ab5-9486-a9dec5420b84",
                "source": "WIPAC:/tmp/path1/sub1/48091a00-0c97-482f-a716-2e721b8e9662.zip",
                "status": "waiting",
                "verified": False,
            },
            {
                "request": "5aba93ec-3c7d-43d7-8fe9-c19e5bc25991",
                "source": "WIPAC:/tmp/path1/sub1/24814fa8-875b-4bae-b034-ea8885d2aafe.zip",
                "status": "processing",
                "verified": False,
            },
            {
                "request": "baebf071-702f-4ab5-9486-a9dec5420b84",
                "source": "WIPAC:/tmp/path1/sub1/cef98a3b-9a24-4fbc-b4e7-ef251367c020.zip",
                "status": "bundled",
                "verified": False,
            },
        ]
    }

    #
    # Create - POST /Bundles/actions/bulk_create
    #
    ret = await r.request('POST', '/Bundles/actions/bulk_create', test_data)
    assert len(ret["bundles"]) == 4
    assert ret["count"] == 4

    #
    # Read - GET /Bundles
    #
    ret = await r.request('GET', '/Bundles')
    results = ret["results"]
    assert len(results) == 4

    ret = await r.request('GET', '/Bundles?request=dd162dad-9880-4ed7-b3c3-f8843d765ac3')
    results = ret["results"]
    assert len(results) == 0

    ret = await r.request('GET', '/Bundles?request=5aba93ec-3c7d-43d7-8fe9-c19e5bc25991')
    results = ret["results"]
    assert len(results) == 1

    ret = await r.request('GET', '/Bundles?request=baebf071-702f-4ab5-9486-a9dec5420b84')
    results = ret["results"]
    assert len(results) == 2
    r.close()


@pytest.mark.asyncio
async def test_get_bundles_uuid_error(rest: RestClientFactory) -> None:
    """Check that GET /Bundles/UUID returns 404 on not found."""
    r = rest('system')  # type: ignore[call-arg]

    with pytest.raises(Exception):
        await r.request('GET', '/Bundles/d4390bcadac74f9dbb49874b444b448d')
    r.close()


@pytest.mark.asyncio
async def test_delete_bundles_uuid(mongo: LtaCollection, rest: RestClientFactory) -> None:
    """Check that DELETE /Bundles/UUID returns 204, exist or not exist."""
    r = rest('system')  # type: ignore[call-arg]

    test_data = {
        'bundles': [
            {
                "source": "WIPAC:/data/exp/IceCube/2014/59aa1e05-84ba-4214-bdfa-a9f42117b3dd.zip",
                "status": "bundled",
                "verified": True,
            },
        ]
    }

    ret = await r.request('POST', '/Bundles/actions/bulk_create', test_data)
    assert len(ret["bundles"]) == 1
    assert ret["count"] == 1

    ret = await r.request('GET', '/Bundles')
    results = ret["results"]
    assert len(results) == 1

    test_uuid = results[0]

    # we delete it when it exists
    ret = await r.request('DELETE', f'/Bundles/{test_uuid}')
    assert not ret

    # we verify that it has been deleted
    ret = await r.request('GET', '/Bundles')
    results = ret["results"]
    assert len(results) == 0

    # we try to delete it again!
    ret = await r.request('DELETE', f'/Bundles/{test_uuid}')
    assert not ret
    r.close()


@pytest.mark.asyncio
async def test_patch_bundles_uuid(mongo: LtaCollection, rest: RestClientFactory) -> None:
    """Check that PATCH /Bundles/UUID does the right thing, every time."""
    r = rest('system')  # type: ignore[call-arg]

    test_data = {
        'bundles': [
            {
                "source": "WIPAC:/data/exp/IceCube/2014/59aa1e05-84ba-4214-bdfa-a9f42117b3dd.zip",
                "status": "bundled",
                "verified": True,
            },
        ]
    }

    ret = await r.request('POST', '/Bundles/actions/bulk_create', test_data)
    assert len(ret["bundles"]) == 1
    assert ret["count"] == 1

    ret = await r.request('GET', '/Bundles')
    results = ret["results"]
    assert len(results) == 1

    test_uuid = results[0]

    # we patch it when it exists
    request = {"key": "value"}
    ret = await r.request('PATCH', f'/Bundles/{test_uuid}', request)
    assert ret["key"] == "value"

    # we try to patch the uuid; error
    with pytest.raises(Exception):
        request = {"key": "value", "uuid": "d4390bca-dac7-4f9d-bb49-874b444b448d"}
        await r.request('PATCH', f'/Bundles/{test_uuid}', request)

    # we try to patch something that doesn't exist; error
    with pytest.raises(Exception):
        request = {"key": "value"}
        await r.request('PATCH', '/Bundles/048c812c780648de8f39a2422e2dcdb0', request)
    r.close()


@pytest.mark.asyncio
async def test_bundles_actions_pop(mongo: LtaCollection, rest: RestClientFactory) -> None:
    """Check pop action for bundles."""
    r = rest('system')  # type: ignore[call-arg]

    test_data = {
        'bundles': [
            {
                "source": "WIPAC",
                "path": "/data/exp/IceCube/2014/15f7a399-fe40-4337-bb7e-d68d2d28ec8e.zip",
                "status": "accessible",
                "verified": False,
            },
            {
                "source": "WIPAC",
                "path": "/tmp/path1/sub1/48091a00-0c97-482f-a716-2e721b8e9662.zip",
                "status": "deletable",
                "verified": False,
            },
            {
                "source": "WIPAC",
                "path": "/tmp/path1/sub1/24814fa8-875b-4bae-b034-ea8885d2aafe.zip",
                "status": "inaccessible",
                "verified": False,
            },
            {
                "source": "WIPAC",
                "path": "/tmp/path1/sub1/cef98a3b-9a24-4fbc-b4e7-ef251367c020.zip",
                "status": "none",
                "verified": False,
            },
            {
                "source": "WIPAC",
                "path": "/tmp/path1/sub2/8f141afa-db2f-4337-9a24-560c383887b5.zip",
                "status": "transferring",
                "verified": False,
            },
            {
                "source": "WIPAC",
                "path": "/tmp/path1/sub2/f9153da6-3588-416d-ba59-91526376dc43.zip",
                "status": "accessible",
                "verified": True,
            },
            {
                "source": "WIPAC",
                "path": "/tmp/path1/sub2/34b064a7-5ffa-4cb2-9661-6e8b80765e9f.zip",
                "status": "deletable",
                "verified": True,
            },
            {
                "source": "DESY",
                "path": "/tmp/path1/sub2/76e3c5e2-e1a2-42f0-9d59-6546d1cb85e6.zip",
                "status": "inaccessible",
                "verified": True,
            },
            {
                "source": "DESY",
                "path": "/tmp/path1/sub2/3bcd05f5-ceb8-4eb5-a5db-5f7d55a98ff4.zip",
                "status": "none",
                "verified": True,
            },
            {
                "source": "DESY",
                "path": "/tmp/path1/sub2/e596e1f7-ddaa-4255-abe3-81a4769bf192.zip",
                "status": "transferring",
                "verified": True,
            },
        ]
    }

    #
    # Create - POST /Bundles/actions/bulk_create
    #
    ret = await r.request('POST', '/Bundles/actions/bulk_create', test_data)
    assert len(ret["bundles"]) == 10
    assert ret["count"] == 10

    # I'm at NERSC, and should have no work
    claimant_body = {
        'claimant': 'testing-picker-aaaed864-0112-4bcf-a069-bb55c12e291d',
    }
    ret = await r.request('POST', '/Bundles/actions/pop?source=NERSC&status=inaccessible', claimant_body)
    assert not ret['bundle']

    # I'm the bundler at WIPAC, and should pop one work item
    ret = await r.request('POST', '/Bundles/actions/pop?source=WIPAC&status=inaccessible', claimant_body)
    assert ret['bundle']
    assert ret['bundle']["path"] == "/tmp/path1/sub1/24814fa8-875b-4bae-b034-ea8885d2aafe.zip"

    # repeating gets no work
    ret = await r.request('POST', '/Bundles/actions/pop?source=WIPAC&status=inaccessible', claimant_body)
    assert not ret['bundle']

    # I'm the bundler at WIPAC, and should pop one work item
    ret = await r.request('POST', '/Bundles/actions/pop?source=WIPAC&status=accessible', claimant_body)
    assert ret['bundle']
    assert ret['bundle']["path"] == "/data/exp/IceCube/2014/15f7a399-fe40-4337-bb7e-d68d2d28ec8e.zip"
    r.close()


@pytest.mark.asyncio
async def test_bundles_actions_pop_errors(mongo: LtaCollection, rest: RestClientFactory) -> None:
    """Check error handlers for pop action for bundles."""
    r = rest('system')  # type: ignore[call-arg]
    request: Dict[str, Any] = {}

    with pytest.raises(Exception):
        await r.request('POST', '/Bundles/actions/pop?source=AREA-51', request)

    with pytest.raises(Exception):
        await r.request('POST', '/Bundles/actions/pop?source=WIPAC', request)

    with pytest.raises(Exception):
        await r.request('POST', '/Bundles/actions/pop?source=WIPAC&status=supercalifragilisticexpialidocious', request)

    with pytest.raises(Exception):
        await r.request('POST', '/Bundles/actions/pop?source=WIPAC&status=none', request)

    with pytest.raises(Exception):
        await r.request('POST', '/Bundles/actions/pop?status=taping', request)
    r.close()


@pytest.mark.asyncio
async def test_bundles_actions_pop_at_destination(mongo: LtaCollection, rest: RestClientFactory) -> None:
    """Check pop action for bundles at destination."""
    r = rest('system')  # type: ignore[call-arg]

    test_data = {
        'bundles': [
            {
                "source": "WIPAC",
                "dest": "NERSC",
                "path": "/data/exp/IceCube/2014/15f7a399-fe40-4337-bb7e-d68d2d28ec8e.zip",
                "status": "taping",
                "verified": False,
            },
        ]
    }

    #
    # Create - POST /Bundles/actions/bulk_create
    #
    ret = await r.request('POST', '/Bundles/actions/bulk_create', test_data)
    assert len(ret["bundles"]) == 1
    assert ret["count"] == 1

    # I'm at destination NERSC, and should have work
    claimant_body = {
        'claimant': 'testing-nersc_mover-aaaed864-0112-4bcf-a069-bb55c12e291d',
    }
    ret = await r.request('POST', '/Bundles/actions/pop?dest=NERSC&status=taping', claimant_body)
    assert ret['bundle']
    assert ret['bundle']["path"] == "/data/exp/IceCube/2014/15f7a399-fe40-4337-bb7e-d68d2d28ec8e.zip"
    r.close()


@pytest.mark.asyncio
async def test_bundles_actions_bulk_create_huge(mongo: LtaCollection, rest: RestClientFactory) -> None:
    """Check pop action for bundles at destination."""
    NUM_FILES_TO_MAKE_IT_HUGE = 16000  # 16000 file entries ~= 12 MB body data

    r = rest(role='system', timeout=10.0)  # type: ignore[call-arg]

    test_data: Dict[str, List[Dict[str, Any]]] = {
        'bundles': [
            {
                "type": "Bundle",
                "status": "specified",
                "request": "55d332222f0311eaa8e78e6b006590ea",
                "source": "WIPAC",
                "dest": "NERSC",
                "path": "/data/exp/IceCube/2018/internal-system/pDAQ-2ndBld/0802",
                "files": []
            },
        ]
    }

    file_spec = {
        "_links": {
            "parent": {
                "href": "/api/files"
            },
            "self": {
                "href": "/api/files/e73aab54-2ead-11ea-a750-f6a52f4853dd"
            }
        },
        "checksum": {
            "sha512": "d8c7ce8c2016816b0c8f543f6b36d4036abee926ccead806fd982b5fff21488f5356b73dcc381093717c629b35e81c0e52e3205abdbb905b8322e8f6919e4987"
        },
        "file_size": 203400520,
        "locations": [
            {
                "site": "WIPAC",
                "path": "/data/exp/IceCube/2018/internal-system/pDAQ-2ndBld/0802/ukey_21a8501c-359a-41b4-95ce-c133e06d39ae_SPS-pDAQ-2ndBld-000_20180802_053901_000000.tar.gz"
            }
        ],
        "logical_name": "/data/exp/IceCube/2018/internal-system/pDAQ-2ndBld/0802/ukey_21a8501c-359a-41b4-95ce-c133e06d39ae_SPS-pDAQ-2ndBld-000_20180802_053901_000000.tar.gz",
        "meta_modify_date": "2020-01-04 04:51:43.182447",
        "uuid": "e73aab54-2ead-11ea-a750-f6a52f4853dd"
    }

    for i in range(1, NUM_FILES_TO_MAKE_IT_HUGE):
        test_data["bundles"][0]["files"].append(file_spec.copy())

    #
    # Create - POST /Bundles/actions/bulk_create
    #
    ret = await r.request('POST', '/Bundles/actions/bulk_create', test_data)
    assert len(ret["bundles"]) == 1
    assert ret["count"] == 1
    r.close()


@pytest.mark.asyncio
async def test_metadata_delete_bundle_uuid(mongo: LtaCollection, rest: RestClientFactory) -> None:
    """Check CRUD semantics for metadata."""
    r = rest('system')  # type: ignore[call-arg]
    bundle_uuid0 = "291afc8d-2a04-4d85-8669-dc8e2c2ab406"
    bundle_uuid1 = "05b7178b-82d0-428c-a0a6-d4add696de62"
    #
    # Create - POST /Metadata/actions/bulk_create
    #
    request = {
        'bundle_uuid': bundle_uuid0,
        'files': ["7b5c1f76-e568-4ae7-94d2-5a31d1d2b081", "125d2a44-a664-4166-bf4a-5d5cf13292d7", "3a92d3d2-2e3e-4184-8d3a-25fb4337fd2f"]
    }
    ret = await r.request('POST', '/Metadata/actions/bulk_create', request)
    assert len(ret["metadata"]) == 3
    assert ret["count"] == 3
    request = {
        'bundle_uuid': bundle_uuid1,
        'files': ["03ccb63e-32cf-4135-85b2-fd06b8c9137f", "c65f2c58-a412-403c-9354-d25d7ae5cdeb", "864f0903-f207-478d-bac2-b437ebc07226"]
    }
    ret = await r.request('POST', '/Metadata/actions/bulk_create', request)
    assert len(ret["metadata"]) == 3
    assert ret["count"] == 3

    #
    # Read - GET /Metadata
    #
    ret = await r.request('GET', f'/Metadata?bundle_uuid={bundle_uuid0}')
    results = ret["results"]
    assert len(results) == 3
    ret = await r.request('GET', f'/Metadata?bundle_uuid={bundle_uuid1}')
    results = ret["results"]
    assert len(results) == 3

    #
    # Delete - DELETE /Metadata?bundle_uuid={uuid}
    #
    ret = await r.request('DELETE', f'/Metadata?bundle_uuid={bundle_uuid0}')
    assert not ret

    #
    # Read - GET /Metadata
    #
    ret = await r.request('GET', f'/Metadata?bundle_uuid={bundle_uuid0}')
    results = ret["results"]
    assert len(results) == 0
    ret = await r.request('GET', f'/Metadata?bundle_uuid={bundle_uuid1}')
    results = ret["results"]
    assert len(results) == 3
    r.close()


@pytest.mark.asyncio
async def test_metadata_single_record(mongo: LtaCollection, rest: RestClientFactory) -> None:
    """Check CRUD semantics for metadata."""
    r = rest('system')  # type: ignore[call-arg]
    bundle_uuid = "291afc8d-2a04-4d85-8669-dc8e2c2ab406"
    #
    # Create - POST /Metadata/actions/bulk_create
    #
    request = {
        'bundle_uuid': bundle_uuid,
        'files': ["7b5c1f76-e568-4ae7-94d2-5a31d1d2b081", "125d2a44-a664-4166-bf4a-5d5cf13292d7", "3a92d3d2-2e3e-4184-8d3a-25fb4337fd2f"]
    }
    ret = await r.request('POST', '/Metadata/actions/bulk_create', request)
    assert len(ret["metadata"]) == 3
    assert ret["count"] == 3
    #
    # Read - GET /Metadata/{uuid}
    #
    metadata_uuid = ret["metadata"][0]
    ret2 = await r.request('GET', f'/Metadata/{metadata_uuid}')
    assert ret2["uuid"] == metadata_uuid
    assert ret2["bundle_uuid"] == bundle_uuid
    assert ret2["file_catalog_uuid"] == "7b5c1f76-e568-4ae7-94d2-5a31d1d2b081"
    #
    # Delete - DELETE /Metadata/{uuid}
    #
    ret3 = await r.request('DELETE', f'/Metadata/{metadata_uuid}')
    assert not ret3
    #
    # Read - GET /Metadata/{uuid}
    #
    with pytest.raises(HTTPError) as e:
        await r.request('GET', f'/Metadata/{metadata_uuid}')
    assert e.value.response.status_code == 404
    assert e.value.response.json()["error"] == "not found"
    r.close()


@pytest.mark.asyncio
async def test_metadata_bulk_crud(mongo: LtaCollection, rest: RestClientFactory) -> None:
    """Check CRUD semantics for metadata."""
    r = rest('system')  # type: ignore[call-arg]
    bundle_uuid = "291afc8d-2a04-4d85-8669-dc8e2c2ab406"
    #
    # Create - POST /Metadata/actions/bulk_create
    #
    request = {
        'bundle_uuid': bundle_uuid,
        'files': ["7b5c1f76-e568-4ae7-94d2-5a31d1d2b081", "125d2a44-a664-4166-bf4a-5d5cf13292d7", "3a92d3d2-2e3e-4184-8d3a-25fb4337fd2f"]
    }
    ret = await r.request('POST', '/Metadata/actions/bulk_create', request)
    assert len(ret["metadata"]) == 3
    assert ret["count"] == 3

    #
    # Read - GET /Metadata
    #
    ret = await r.request('GET', f'/Metadata?bundle_uuid={bundle_uuid}')
    results = ret["results"]
    assert len(results) == 3

    #
    # Delete - POST /Metadata/actions/bulk_delete
    #
    uuids = [unique_id()]
    for result in results:
        uuids.append(result["uuid"])
    request2 = {'metadata': uuids}
    ret = await r.request('POST', '/Metadata/actions/bulk_delete', request2)
    assert ret["count"] == 3
    assert ret["metadata"] == uuids

    #
    # Read - GET /Metadata
    #
    ret = await r.request('GET', '/Metadata')
    results = ret["results"]
    assert len(results) == 0
    r.close()


@pytest.mark.asyncio
async def test_metadata_actions_bulk_create_errors(rest: RestClientFactory) -> None:
    """Check error conditions for bulk_create."""
    r = rest('system')  # type: ignore[call-arg]

    request: Dict[str, Any] = {}
    with pytest.raises(HTTPError) as e:
        await r.request('POST', '/Metadata/actions/bulk_create', request)
    assert e.value.response.status_code == 400
    assert "bundle_uuid" in e.value.response.json()["error"]

    request = {'bundle_uuid': '', 'files': ["foo"]}
    with pytest.raises(HTTPError) as e:
        await r.request('POST', '/Metadata/actions/bulk_create', request)
    assert e.value.response.status_code == 400
    assert "bundle_uuid" in e.value.response.json()["error"]

    request = {'bundle_uuid': "992ae5e1-017c-4a95-b552-bd385020ec27"}
    with pytest.raises(HTTPError) as e:
        await r.request('POST', '/Metadata/actions/bulk_create', request)
    assert e.value.response.status_code == 400
    assert "files" in e.value.response.json()["error"]

    request = {'bundle_uuid': "992ae5e1-017c-4a95-b552-bd385020ec27", "files": []}
    with pytest.raises(HTTPError) as e:
        await r.request('POST', '/Metadata/actions/bulk_create', request)
    assert e.value.response.status_code == 400
    assert "files" in e.value.response.json()["error"]
    r.close()


@pytest.mark.asyncio
async def test_metadata_actions_bulk_delete_errors(rest: RestClientFactory) -> None:
    """Check error conditions for bulk_delete."""
    r = rest('system')  # type: ignore[call-arg]

    request: Dict[str, Any] = {}
    with pytest.raises(HTTPError) as e:
        await r.request('POST', '/Metadata/actions/bulk_delete', request)
    assert e.value.response.status_code == 400
    assert "metadata" in e.value.response.json()["error"]

    request = {'metadata': ''}
    with pytest.raises(HTTPError) as e:
        await r.request('POST', '/Metadata/actions/bulk_delete', request)
    assert e.value.response.status_code == 400
    assert "metadata" in e.value.response.json()["error"]

    request = {'metadata': []}
    with pytest.raises(HTTPError) as e:
        await r.request('POST', '/Metadata/actions/bulk_delete', request)
    assert e.value.response.status_code == 400
    assert "metadata" in e.value.response.json()["error"]
    r.close()


@pytest.mark.asyncio
async def test_metadata_delete_errors(rest: RestClientFactory) -> None:
    """Check error conditions for DELETE /Metadata."""
    r = rest('system')  # type: ignore[call-arg]

    with pytest.raises(HTTPError) as e:
        await r.request('DELETE', '/Metadata')
    assert e.value.response.status_code == 400
    assert "bundle_uuid" in e.value.response.json()["error"]
    r.close()


@pytest.mark.asyncio
async def test_metadata_results_comprehension(rest: RestClientFactory) -> None:
    """Check that our comprehension works."""
    r = rest('system')  # type: ignore[call-arg]
    bundle_uuid = "291afc8d-2a04-4d85-8669-dc8e2c2ab406"
    #
    # Create - POST /Metadata/actions/bulk_create
    #
    request = {
        'bundle_uuid': bundle_uuid,
        'files': ["7b5c1f76-e568-4ae7-94d2-5a31d1d2b081", "125d2a44-a664-4166-bf4a-5d5cf13292d7", "3a92d3d2-2e3e-4184-8d3a-25fb4337fd2f"]
    }
    ret = await r.request('POST', '/Metadata/actions/bulk_create', request)
    assert len(ret["metadata"]) == 3
    assert ret["count"] == 3

    #
    # Read - GET /Metadata
    #
    ret = await r.request('GET', f'/Metadata?bundle_uuid={bundle_uuid}')
    results = ret["results"]
    assert len(results) == 3

    #
    # Obtain the Metadata UUIDs with a comprehension
    #
    uuids = [x['uuid'] for x in results]
    assert len(uuids) == 3
    count = 0
    for result in results:
        assert uuids[count] == result['uuid']
        count = count + 1
    r.close()
