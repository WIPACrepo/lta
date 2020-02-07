# test_rest_server.py
"""Unit tests for lta/rest_server.py."""

import asyncio
from datetime import datetime, timedelta
import os
import socket
from typing import Dict
from urllib.parse import quote_plus

from pymongo import MongoClient  # type: ignore
from pymongo.database import Database  # type: ignore
import pytest  # type: ignore
import requests  # type: ignore
from rest_tools.client import RestClient  # type: ignore

from lta.rest_server import boolify, CheckClaims, main, start, unique_id

ALL_DOCUMENTS: Dict[str, str] = {}
MONGODB_NAME = "lta-unit-tests"
REMOVE_ID = {"_id": False}

CONFIG = {
    'AUTH_SECRET': 'secret',
    'LTA_MONGODB_AUTH_USER': '',
    'LTA_MONGODB_AUTH_PASS': '',
    'LTA_MONGODB_DATABASE_NAME': MONGODB_NAME,
    'LTA_MONGODB_HOST': 'localhost',
    'LTA_MONGODB_PORT': '27017',
    'TOKEN_SERVICE': 'http://localhost:8888',
}
for k in CONFIG:
    if k in os.environ:
        CONFIG[k] = os.environ[k]

@pytest.fixture
def mongo(monkeypatch) -> Database:
    """Get a reference to a test instance of a MongoDB Database."""
    mongo_user = quote_plus(CONFIG["LTA_MONGODB_AUTH_USER"])
    mongo_pass = quote_plus(CONFIG["LTA_MONGODB_AUTH_PASS"])
    mongo_host = CONFIG["LTA_MONGODB_HOST"]
    mongo_port = int(CONFIG["LTA_MONGODB_PORT"])
    lta_mongodb_url = f"mongodb://{mongo_host}"
    if mongo_user and mongo_pass:
        lta_mongodb_url = f"mongodb://{mongo_user}:{mongo_pass}@{mongo_host}"
    client = MongoClient(lta_mongodb_url, port=mongo_port)
    db = client[MONGODB_NAME]
    for collection in db.list_collection_names():
        db.drop_collection(collection)
    return db

@pytest.fixture
def port():
    """Get an ephemeral port number."""
    # https://unix.stackexchange.com/a/132524
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.bind(('', 0))
    addr = s.getsockname()
    ephemeral_port = addr[1]
    s.close()
    return ephemeral_port

@pytest.fixture
async def rest(monkeypatch, port):
    """Provide RestClient as a test fixture."""
    monkeypatch.setenv("LTA_AUTH_ALGORITHM", "HS512")
    monkeypatch.setenv("LTA_AUTH_ISSUER", CONFIG['TOKEN_SERVICE'])
    monkeypatch.setenv("LTA_AUTH_SECRET", CONFIG['AUTH_SECRET'])
    monkeypatch.setenv("LTA_MONGODB_DATABASE_NAME", MONGODB_NAME)
    monkeypatch.setenv("LTA_REST_PORT", str(port))
    monkeypatch.setenv("LTA_SITE_CONFIG", "examples/site.json")
    s = start(debug=True)

    def client(role='admin', timeout=0.25):
        if CONFIG['TOKEN_SERVICE']:
            r = requests.get(CONFIG['TOKEN_SERVICE']+'/token',
                             params={'scope': f'lta:{role}'})
            r.raise_for_status()
            t = r.json()['access']
        else:
            raise Exception('testing token service not defined')
        print(t)
        return RestClient(f'http://localhost:{port}', token=t, timeout=timeout, retries=0)

    yield client
    s.stop()
    await asyncio.sleep(0.01)

# -----------------------------------------------------------------------------

def test_boolify():
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

    assert not boolify(None)
    assert not boolify(12345)
    assert not boolify(6.2831853071)
    assert not boolify("alice")
    assert not boolify("bob")
    assert not boolify({})
    assert not boolify([])

def test_check_claims_old_age():
    """Verify that CheckClaims can determine old age for claims."""
    cc = CheckClaims()
    cutoff = cc.old_age()
    assert isinstance(cutoff, str)

# -----------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_server_reachability(rest):
    """Check that we can reach the server."""
    r = rest()
    ret = await r.request('GET', '/')
    assert ret == {}

@pytest.mark.asyncio
async def test_server_bad_auth(rest):
    """Check for bad auth role."""
    r = rest('')
    with pytest.raises(Exception):
        await r.request('GET', '/TransferRequests')

@pytest.mark.asyncio
async def test_transfer_request_fail(rest):
    """Check for bad transfer request handling."""
    r = rest()
    request = {'dest': ['bar']}
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

@pytest.mark.asyncio
async def test_transfer_request_crud(mongo, rest):
    """Check CRUD semantics for transfer requests."""
    r = rest(role="system")
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
        await r.request('PATCH', f'/TransferRequests/foo', request2)

    ret = await r.request('DELETE', f'/TransferRequests/{uuid}')
    assert not ret

    with pytest.raises(Exception):
        await r.request('GET', f'/TransferRequests/{uuid}')

    ret = await r.request('DELETE', f'/TransferRequests/{uuid}')
    assert not ret

    ret = await r.request('GET', '/TransferRequests')
    assert len(ret['results']) == 0

@pytest.mark.asyncio
async def test_transfer_request_pop(rest):
    """Check pop action for transfer requests."""
    r = rest('system')
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

@pytest.mark.asyncio
async def test_status(mongo, rest):
    """Check for status handling."""
    r = rest('system')
    ret = await r.request('GET', '/status')
    assert ret['health'] == 'OK'

    request = {'1.1': {'timestamp': datetime.utcnow().isoformat(), 'foo': 'bar'}}
    await r.request('PATCH', '/status/1', request)

    ret = await r.request('GET', '/status')
    assert ret['health'] == 'OK'
    assert ret['1'] == 'OK'

    ret = await r.request('GET', '/status/1')
    assert ret == request

    with pytest.raises(Exception):
        await r.request('GET', '/status/2')

    request2 = {'1.2': {'timestamp': datetime.utcnow().isoformat(), 'baz': 2}}
    await r.request('PATCH', '/status/1', request2)
    request_all = dict(request)
    request_all.update(request2)
    ret = await r.request('GET', '/status/1')
    assert ret == request_all

    request = {'2.1': {'timestamp': (datetime.utcnow() - timedelta(hours=1)).isoformat(), 'foo': 'bar'}}
    await r.request('PATCH', '/status/2', request)
    ret = await r.request('GET', '/status')
    assert ret['health'] == 'WARN'
    assert ret['1'] == 'OK'
    assert ret['2'] == 'WARN'

@pytest.mark.asyncio
async def test_script_main(mocker):
    """Ensure that main sets up logging, starts a server, and runs the event loop."""
    mock_root_logger = mocker.patch("logging.basicConfig")
    mock_rest_server = mocker.patch("lta.rest_server.start")
    mock_event_loop = mocker.patch("asyncio.get_event_loop")
    main()
    mock_root_logger.assert_called()
    mock_rest_server.assert_called()
    mock_event_loop.assert_called()

@pytest.mark.asyncio
async def test_bundles_bulk_crud(mongo, rest):
    """Check CRUD semantics for bundles."""
    r = rest('system')

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

@pytest.mark.asyncio
async def test_bundles_actions_bulk_create_errors(rest):
    """Check error conditions for bulk_create."""
    r = rest('system')

    request = {}
    with pytest.raises(Exception):
        await r.request('POST', '/Bundles/actions/bulk_create', request)

    request = {'bundles': ''}
    with pytest.raises(Exception):
        await r.request('POST', '/Bundles/actions/bulk_create', request)

    request = {'bundles': []}
    with pytest.raises(Exception):
        await r.request('POST', '/Bundles/actions/bulk_create', request)

@pytest.mark.asyncio
async def test_bundles_actions_bulk_delete_errors(rest):
    """Check error conditions for bulk_delete."""
    r = rest('system')

    request = {}
    with pytest.raises(Exception):
        await r.request('POST', '/Bundles/actions/bulk_delete', request)

    request = {'bundles': ''}
    with pytest.raises(Exception):
        await r.request('POST', '/Bundles/actions/bulk_delete', request)

    request = {'bundles': []}
    with pytest.raises(Exception):
        await r.request('POST', '/Bundles/actions/bulk_delete', request)

@pytest.mark.asyncio
async def test_bundles_actions_bulk_update_errors(rest):
    """Check error conditions for bulk_update."""
    r = rest('system')

    request = {}
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

@pytest.mark.asyncio
async def test_get_bundles_filter(mongo, rest):
    """Check that GET /Bundles filters properly by query parameters.."""
    r = rest('system')

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

@pytest.mark.asyncio
async def test_get_bundles_request_filter(mongo, rest):
    """Check that GET /Bundles filters properly by query parameter request."""
    r = rest('system')

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

@pytest.mark.asyncio
async def test_get_bundles_uuid_error(rest):
    """Check that GET /Bundles/UUID returns 404 on not found."""
    r = rest('system')

    with pytest.raises(Exception):
        await r.request('GET', '/Bundles/d4390bcadac74f9dbb49874b444b448d')

@pytest.mark.asyncio
async def test_delete_bundles_uuid(mongo, rest):
    """Check that DELETE /Bundles/UUID returns 204, exist or not exist."""
    r = rest('system')

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

@pytest.mark.asyncio
async def test_patch_bundles_uuid(mongo, rest):
    """Check that PATCH /Bundles/UUID does the right thing, every time."""
    r = rest('system')

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
        await r.request('PATCH', f'/Bundles/048c812c780648de8f39a2422e2dcdb0', request)

@pytest.mark.asyncio
async def test_bundles_actions_pop(mongo, rest):
    """Check pop action for bundles."""
    r = rest('system')

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

@pytest.mark.asyncio
async def test_bundles_actions_pop_errors(mongo, rest):
    """Check error handlers for pop action for bundles."""
    r = rest('system')
    request = {}

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

@pytest.mark.asyncio
async def test_bundles_actions_pop_at_destination(mongo, rest):
    """Check pop action for bundles at destination."""
    r = rest('system')

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

@pytest.mark.asyncio
async def test_bundles_actions_bulk_create_huge(mongo, rest):
    """Check pop action for bundles at destination."""
    NUM_FILES_TO_MAKE_IT_HUGE = 16000  # 16000 file entries ~= 12 MB body data

    r = rest(role='system', timeout=10.0)

    test_data = {
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

@pytest.mark.asyncio
async def test_status_component_count(mongo, rest):
    """Verify that GET /status/{component}/count works."""
    r = rest('system')

    request = {'picker1': {'timestamp': datetime.utcnow().isoformat(), 'foo': 'bar'}}
    await r.request('PATCH', '/status/picker', request)

    response = await r.request("GET", "/status/picker/count")
    assert response["component"] == "picker"
    assert response["count"] == 1

    request = {'picker2': {'timestamp': datetime.utcnow().isoformat(), 'foo': 'bar'}}
    await r.request('PATCH', '/status/picker', request)

    request = {'picker3': {'timestamp': datetime.utcnow().isoformat(), 'foo': 'bar'}}
    await r.request('PATCH', '/status/picker', request)

    response = await r.request("GET", "/status/picker/count")
    assert response["component"] == "picker"
    assert response["count"] == 3

    request = {'picker4': {'timestamp': (datetime.utcnow() - timedelta(minutes=15)).isoformat(), 'foo': 'bar'}}
    await r.request('PATCH', '/status/picker', request)

    request = {'picker5': {'timestamp': (datetime.utcnow() - timedelta(minutes=15)).isoformat(), 'foo': 'bar'}}
    await r.request('PATCH', '/status/picker', request)

    response = await r.request("GET", "/status/picker/count")
    assert response["component"] == "picker"
    assert response["count"] == 3
