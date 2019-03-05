# test_rest_server.py
"""Unit tests for lta/rest_server.py."""

import asyncio
from datetime import datetime, timedelta
from math import floor
import os
import pytest  # type: ignore
from random import random
import socket
from typing import Dict

from lta.rest_server import main, start, unique_id
from pymongo import MongoClient  # type: ignore
from pymongo.database import Database  # type: ignore
from rest_tools.client import RestClient  # type: ignore
from rest_tools.server import Auth  # type: ignore

ALL_DOCUMENTS: Dict[str, str] = {}
REMOVE_ID = {"_id": False}

class ObjectLiteral:
    """
    ObjectLiteral transforms named arguments into object attributes.

    This is useful for creating object literals to be used as return
    values from mocked API calls.

    Source: https://stackoverflow.com/a/3335732
    """

    def __init__(self, **kwds):
        """Add attributes to ourself with the provided named arguments."""
        self.__dict__.update(kwds)

@pytest.fixture
def mongo(monkeypatch) -> Database:
    """Get the Database that corresponds to the LTA database."""
    monkeypatch.setenv("LTA_MONGODB_URL", "mongodb://localhost:27017/")
    client = MongoClient(os.environ["LTA_MONGODB_URL"])
    db = client['lta']
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
    monkeypatch.setenv("LTA_REST_PORT", str(port))
    monkeypatch.setenv("LTA_AUTH_SECRET", "secret")
    monkeypatch.setenv("LTA_AUTH_ISSUER", "lta")
    monkeypatch.setenv("LTA_AUTH_ALGORITHM", "HS512")
    s = start(debug=True)
    a = Auth('secret', issuer='lta', algorithm='HS512')

    def client(role='admin', timeout=0.1):
        t = a.create_token('foo', payload={'long-term-archive': {'role': role}})
        return RestClient(f'http://localhost:{port}', token=t, timeout=timeout, retries=0)

    yield client
    s.stop()
    await asyncio.sleep(0.01)

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

@pytest.mark.asyncio
async def test_transfer_request_crud(mongo, rest):
    """Check CRUD semantics for transfer requests."""
    mongo.TransferRequests.delete_many(ALL_DOCUMENTS)

    r = rest()
    request = {'source': 'foo', 'dest': ['bar']}
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
    assert ret is None

    with pytest.raises(Exception):
        await r.request('GET', f'/TransferRequests/{uuid}')

    ret = await r.request('DELETE', f'/TransferRequests/{uuid}')
    assert ret is None

    ret = await r.request('GET', '/TransferRequests')
    assert len(ret['results']) == 0

@pytest.mark.asyncio
async def test_transfer_request_pop(rest):
    """Check pop action for transfer requests."""
    r = rest('system')
    request = {
        'source': 'WIPAC:/data/exp/foo/bar',
        'dest': ['NERSC:/tape/icecube/foo/bar'],
    }
    ret = await r.request('POST', '/TransferRequests', request)
    uuid = ret['TransferRequest']
    assert uuid

    # I'm at NERSC, and should have no work
    ret = await r.request('POST', '/TransferRequests/actions/pop?source=NERSC')
    assert ret['results'] == []

    # I'm the picker at WIPAC, and should have one work item
    ret = await r.request('POST', '/TransferRequests/actions/pop?source=WIPAC')
    assert len(ret['results']) == 1
    for k in request:
        assert request[k] == ret['results'][0][k]

    # repeating gets no work
    ret = await r.request('POST', '/TransferRequests/actions/pop?source=WIPAC')
    assert ret['results'] == []

    # test limit
    await r.request('POST', '/TransferRequests', request)
    await r.request('POST', '/TransferRequests', request)
    ret = await r.request('POST', '/TransferRequests/actions/pop?source=WIPAC&limit=1')
    assert len(ret['results']) == 1

    # test non-int limit
    with pytest.raises(Exception):
        await r.request('POST', '/TransferRequests/actions/pop?source=WIPAC&limit=foo')

@pytest.mark.asyncio
async def test_status(mongo, rest):
    """Check for status handling."""
    mongo.Status.delete_many(ALL_DOCUMENTS)

    r = rest('system')
    ret = await r.request('GET', '/status')
    assert ret['health'] == 'OK'

    request = {'1.1': {'t': datetime.utcnow().isoformat(), 'foo': 'bar'}}
    await r.request('PATCH', '/status/1', request)

    ret = await r.request('GET', '/status')
    assert ret['health'] == 'OK'
    assert ret['1'] == 'OK'

    ret = await r.request('GET', '/status/1')
    assert ret == request

    with pytest.raises(Exception):
        await r.request('GET', '/status/2')

    request2 = {'1.2': {'t': datetime.utcnow().isoformat(), 'baz': 2}}
    await r.request('PATCH', '/status/1', request2)
    request_all = dict(request)
    request_all.update(request2)
    ret = await r.request('GET', '/status/1')
    assert ret == request_all

    request = {'2.1': {'t': (datetime.utcnow() - timedelta(hours=1)).isoformat(), 'foo': 'bar'}}
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
async def test_files_bulk_crud(mongo, rest):
    """Check CRUD semantics for files."""
    mongo.Files.delete_many(ALL_DOCUMENTS)
    r = rest('system')

    #
    # Create - POST /Files/actions/bulk_create
    #
    request = {'files': [{"name": "one"}, {"name": "two"}]}
    ret = await r.request('POST', '/Files/actions/bulk_create', request)
    assert len(ret["files"]) == 2
    assert ret["count"] == 2

    #
    # Read - GET /Files
    #
    ret = await r.request('GET', '/Files')
    results = ret["results"]
    assert len(results) == 2

    #
    # Update - POST /Files/actions/bulk_update
    #
    # request = {'files': results, 'update': {'key': 'value'}}
    results2 = results + [unique_id()]
    request2 = {'files': results2, 'update': {'key': 'value'}}
    ret = await r.request('POST', '/Files/actions/bulk_update', request2)
    assert ret["count"] == 2
    assert ret["files"] == results

    #
    # Read - GET /Files/UUID
    #
    for result in results:
        ret = await r.request('GET', f'/Files/{result}')
        assert ret["uuid"] == result
        assert ret["name"] in ["one", "two"]
        assert ret["key"] == "value"

    #
    # Delete - POST /Files/actions/bulk_delete
    #
    results2 = results + [unique_id()]
    request2 = {'files': results2}
    ret = await r.request('POST', '/Files/actions/bulk_delete', request2)
    assert ret["count"] == 2
    assert ret["files"] == results

    #
    # Read - GET /Files
    #
    ret = await r.request('GET', '/Files')
    results = ret["results"]
    assert len(results) == 0

@pytest.mark.asyncio
async def test_bulk_create_errors(rest):
    """Check error conditions for bulk_create."""
    r = rest('system')

    request = {}
    with pytest.raises(Exception):
        await r.request('POST', '/Files/actions/bulk_create', request)

    request = {'files': ''}
    with pytest.raises(Exception):
        await r.request('POST', '/Files/actions/bulk_create', request)

    request = {'files': []}
    with pytest.raises(Exception):
        await r.request('POST', '/Files/actions/bulk_create', request)

@pytest.mark.asyncio
async def test_bulk_delete_errors(rest):
    """Check error conditions for bulk_delete."""
    r = rest('system')

    request = {}
    with pytest.raises(Exception):
        await r.request('POST', '/Files/actions/bulk_delete', request)

    request = {'files': ''}
    with pytest.raises(Exception):
        await r.request('POST', '/Files/actions/bulk_delete', request)

    request = {'files': []}
    with pytest.raises(Exception):
        await r.request('POST', '/Files/actions/bulk_delete', request)

@pytest.mark.asyncio
async def test_bulk_update_errors(rest):
    """Check error conditions for bulk_update."""
    r = rest('system')

    request = {}
    with pytest.raises(Exception):
        await r.request('POST', '/Files/actions/bulk_update', request)

    request = {'update': ''}
    with pytest.raises(Exception):
        await r.request('POST', '/Files/actions/bulk_update', request)

    request = {'update': {}}
    with pytest.raises(Exception):
        await r.request('POST', '/Files/actions/bulk_update', request)

    request = {'update': {}, 'files': ''}
    with pytest.raises(Exception):
        await r.request('POST', '/Files/actions/bulk_update', request)

    request = {'update': {}, 'files': []}
    with pytest.raises(Exception):
        await r.request('POST', '/Files/actions/bulk_update', request)

@pytest.mark.asyncio
async def test_get_files_filter(mongo, rest):
    """Check that GET /Files filters properly by query parameters.."""
    mongo.Files.delete_many(ALL_DOCUMENTS)
    r = rest('system')

    test_data = {
        'files': [
            {
                "degenerate": "file",
                "has no": "decent keys",
                "or values": "should be deleted",
            },
            {
                "source": "WIPAC:/data/exp/IceCube/2014/fileZ.txt",
                "request": "9852fc1a28d111e9ad4600e18cdcf45b",
                "bundle": None,
                "status": "waiting"
            },
            {
                "source": "WIPAC:/tmp/path1/sub1/file1.txt",
                "request": "7a56893d28c011e98a2754e1ade80899",
                "bundle": None,
                "status": "waiting"
            },
            {
                "source": "WIPAC:/tmp/path1/sub1/file2.txt",
                "request": "7a56893d28c011e98a2754e1ade80899",
                "bundle": "be8316c128c011e98a2754e1ade80899",
                "status": "processing"
            },
            {
                "source": "WIPAC:/tmp/path1/sub1/file3.txt",
                "request": "7a56893d28c011e98a2754e1ade80899",
                "bundle": "be8316c128c011e98a2754e1ade80899",
                "status": "bundled"
            },
            {
                "source": "WIPAC:/tmp/path1/sub2/file1.txt",
                "request": "7a56893d28c011e98a2754e1ade80899",
                "bundle": None,
                "status": "waiting"
            },
            {
                "source": "WIPAC:/tmp/path1/sub2/file2.txt",
                "request": "7a56893d28c011e98a2754e1ade80899",
                "bundle": "be8316c128c011e98a2754e1ade80899",
                "status": "processing"
            },
            {
                "source": "WIPAC:/tmp/path1/sub2/file3.txt",
                "request": "7a56893d28c011e98a2754e1ade80899",
                "bundle": "be8316c128c011e98a2754e1ade80899",
                "status": "bundled"
            },
            {
                "source": "DESY:/tmp/path1/sub2/file1.txt",
                "request": "0cc4b62428d211e9ad4600e18cdcf45b",
                "bundle": None,
                "status": "waiting"
            },
            {
                "source": "DESY:/tmp/path1/sub2/file2.txt",
                "request": "0cc4b62428d211e9ad4600e18cdcf45b",
                "bundle": "2f09e51a28d211e9ad4600e18cdcf45b",
                "status": "processing"
            },
            {
                "source": "DESY:/tmp/path1/sub2/file3.txt",
                "request": "0cc4b62428d211e9ad4600e18cdcf45b",
                "bundle": "2f09e51a28d211e9ad4600e18cdcf45b",
                "status": "bundled"
            },
        ]
    }

    #
    # Create - POST /Files/actions/bulk_create
    #
    ret = await r.request('POST', '/Files/actions/bulk_create', test_data)
    assert len(ret["files"]) == 11
    assert ret["count"] == 11

    #
    # Read - GET /Files
    #
    ret = await r.request('GET', '/Files')
    results = ret["results"]
    assert len(results) == 11

    ret = await r.request('GET', '/Files?location=WIPAC')
    results = ret["results"]
    assert len(results) == 7

    ret = await r.request('GET', '/Files?location=DESY')
    results = ret["results"]
    assert len(results) == 3

    ret = await r.request('GET', '/Files?location=WIPAC:/tmp/path1')
    results = ret["results"]
    assert len(results) == 6

    ret = await r.request('GET', '/Files?transfer_request_uuid=0cc4b62428d211e9ad4600e18cdcf45b')
    results = ret["results"]
    assert len(results) == 3

    ret = await r.request('GET', '/Files?transfer_request_uuid=d974285228d311e9ad4600e18cdcf45b')
    results = ret["results"]
    assert len(results) == 0

    ret = await r.request('GET', '/Files?bundle_uuid=2f09e51a28d211e9ad4600e18cdcf45b')
    results = ret["results"]
    assert len(results) == 2

    ret = await r.request('GET', '/Files?bundle_uuid=df7624ca28d411e9ad4600e18cdcf45b')
    results = ret["results"]
    assert len(results) == 0

    # bulk_create squashes the provided status
    ret = await r.request('GET', '/Files?status=waiting')
    results = ret["results"]
    assert len(results) == 11

    ret = await r.request('GET', '/Files?location=WIPAC:/tmp/path1/sub1&bundle_uuid=be8316c128c011e98a2754e1ade80899')
    results = ret["results"]
    assert len(results) == 2

    ret = await r.request('GET', '/Files?location=WIPAC:/tmp/path1/sub1&bundle_uuid=be8316c128c011e98a2754e1ade80899&status=processing')
    results = ret["results"]
    assert len(results) == 0

@pytest.mark.asyncio
async def test_get_files_uuid_error(rest):
    """Check that GET /Files/UUID returns 404 on not found."""
    r = rest('system')

    with pytest.raises(Exception):
        await r.request('GET', '/Files/d4390bcadac74f9dbb49874b444b448d')

@pytest.mark.asyncio
async def test_delete_files_uuid(mongo, rest):
    """Check that DELETE /Files/UUID returns 204, exist or not exist."""
    mongo.Files.delete_many(ALL_DOCUMENTS)
    r = rest('system')

    test_data = {
        'files': [
            {
                "source": "WIPAC:/data/exp/IceCube/2014/fileZ.txt",
                "request": "9852fc1a28d111e9ad4600e18cdcf45b",
                "bundle": None,
            },
        ]
    }

    ret = await r.request('POST', '/Files/actions/bulk_create', test_data)
    assert len(ret["files"]) == 1
    assert ret["count"] == 1

    ret = await r.request('GET', '/Files')
    results = ret["results"]
    assert len(results) == 1

    test_uuid = results[0]

    # we delete it when it exists
    ret = await r.request('DELETE', f'/Files/{test_uuid}')
    assert ret is None

    # we verify that it has been deleted
    ret = await r.request('GET', '/Files')
    results = ret["results"]
    assert len(results) == 0

    # we try to delete it again!
    ret = await r.request('DELETE', f'/Files/{test_uuid}')
    assert ret is None

@pytest.mark.asyncio
async def test_patch_files_uuid(mongo, rest):
    """Check that PATCH /Files/UUID does the right thing, every time."""
    mongo.Files.delete_many(ALL_DOCUMENTS)
    r = rest('system')

    test_data = {
        'files': [
            {
                "source": "WIPAC:/data/exp/IceCube/2014/fileZ.txt",
                "request": "9852fc1a28d111e9ad4600e18cdcf45b",
                "bundle": None,
            },
        ]
    }

    ret = await r.request('POST', '/Files/actions/bulk_create', test_data)
    assert len(ret["files"]) == 1
    assert ret["count"] == 1

    ret = await r.request('GET', '/Files')
    results = ret["results"]
    assert len(results) == 1

    test_uuid = results[0]

    # we patch it when it exists
    request = {"key": "value"}
    ret = await r.request('PATCH', f'/Files/{test_uuid}', request)
    assert ret["key"] == "value"

    # we try to patch the uuid; error
    with pytest.raises(Exception):
        request = {"key": "value", "uuid": "d4390bca-dac7-4f9d-bb49-874b444b448d"}
        await r.request('PATCH', f'/Files/{test_uuid}', request)

    # we try to patch something that doesn't exist; error
    with pytest.raises(Exception):
        request = {"key": "value"}
        await r.request('PATCH', f'/Files/048c812c780648de8f39a2422e2dcdb0', request)

@pytest.mark.asyncio
async def test_files_actions_pop(mongo, rest):
    """Check pop action for files."""
    mongo.Files.delete_many(ALL_DOCUMENTS)
    r = rest('system', timeout=1.0)
    request = {"bundler": "node12345-bundler"}

    # test missing source
    with pytest.raises(Exception):
        await r.request('POST', '/Files/actions/pop', request)

    # test obnoxious source
    with pytest.raises(Exception):
        await r.request('POST', '/Files/actions/pop?source=lol1hackurstuff!!eleven!11!!!', request)

    # test missing destination
    with pytest.raises(Exception):
        await r.request('POST', '/Files/actions/pop?source=WIPAC', request)

    # test obnoxious destination
    with pytest.raises(Exception):
        await r.request('POST', '/Files/actions/pop?source=WIPAC&dest=lol1hackurstuff!!eleven!11!!!', request)

    # test obnoxious limit
    with pytest.raises(Exception):
        await r.request('POST', '/Files/actions/pop?source=WIPAC&dest=NERSC&limit=NO_LIMITS', request)

    # test obnoxious skip
    with pytest.raises(Exception):
        await r.request('POST', '/Files/actions/pop?source=WIPAC&dest=NERSC&skip=SKIP_SKIP', request)

    # test nothing to bundle
    response = await r.request('POST', '/Files/actions/pop?source=WIPAC&dest=NERSC', request)
    results = response["results"]
    assert len(results) == 0

    # test not enough to bundle
    test_data = {
        'files': [
            {
                "source": "WIPAC:/data/exp/IceCube/2013/filtered/PFFilt/1109",
                "dest": "NERSC:/data/exp/IceCube/2013/filtered/PFFilt/1109",
                "request": "9852fc1a28d111e9ad4600e18cdcf45b",
                "catalog": {
                    "logical_name": "/data/exp/IceCube/2013/filtered/PFFilt/1109/PFFilt_PhysicsFiltering_Run00123231_Subrun00000000_00000015.tar.bz2",
                    "uuid": "6fa8312a-e3b3-4220-a254-c66dc5a68361",
                    "checksum": {
                        "sha512": "4e209fdb9e6545c5ad26e7a119c0da44893424c65abe63e7225f33687c91df7edf95cbc7f935cab58d9ae213765764df340701b961f69ef252c69757db75c0e4"
                    },
                    "locations": [
                        {"path": "/data/exp/IceCube/2013/filtered/PFFilt/1109/PFFilt_PhysicsFiltering_Run00123231_Subrun00000000_00000015.tar.bz2", "site": "WIPAC"}
                    ],
                    "file_size": 104319759,
                    "meta_modify_date": "2018-10-30 17:28:22.914497",
                    "final_analysis_sample": {
                        "collection_tag": "bae45fdd-8e26-47a2-92cc-75b96c105c64"
                    }
                }
            },
        ]
    }
    ret = await r.request('POST', '/Files/actions/bulk_create', test_data)
    assert len(ret["files"]) == 1
    assert ret["count"] == 1
    # request it
    response = await r.request('POST', '/Files/actions/pop?source=WIPAC&dest=NERSC', request)
    results = response["results"]
    assert len(results) == 0

    # test not enough to bundle, but we're going to do it anyway
    response = await r.request('POST', '/Files/actions/pop?source=WIPAC&dest=NERSC&force=true', request)
    results = response["results"]
    assert len(results) == 1

    # test more than enough to bundle
    test_data = {"files": []}
    for i in range(0, 100):
        test_data["files"].append({
            "source": "NERSC:/data/exp/IceCube/2013/filtered/PFFilt/1109",
            "dest": "WIPAC:/data/exp/IceCube/2013/filtered/PFFilt/1109",
            "catalog": {
                "file_size": floor(100000000 + random()*6000000),
            }
        })
    ret = await r.request('POST', '/Files/actions/bulk_create', test_data)
    assert len(ret["files"]) == 100
    assert ret["count"] == 100
    # request it
    response = await r.request('POST', '/Files/actions/pop?source=NERSC&dest=WIPAC', request)
    results = response["results"]
    assert len(results) > 7
    assert len(results) < 10
    total_size = 0
    for res_file in results:
        total_size += res_file["catalog"]["file_size"]
    assert total_size < 1000000000

@pytest.mark.asyncio
async def test_bundles_bulk_crud(mongo, rest):
    """Check CRUD semantics for bundles."""
    mongo.Bundles.delete_many(ALL_DOCUMENTS)
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
async def test_get_files_filter(mongo, rest):
    """Check that GET /Bundles filters properly by query parameters.."""
    mongo.Bundles.delete_many(ALL_DOCUMENTS)
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
async def test_get_bundles_uuid_error(rest):
    """Check that GET /Bundles/UUID returns 404 on not found."""
    r = rest('system')

    with pytest.raises(Exception):
        await r.request('GET', '/Bundles/d4390bcadac74f9dbb49874b444b448d')

@pytest.mark.asyncio
async def test_delete_bundles_uuid(mongo, rest):
    """Check that DELETE /Bundles/UUID returns 204, exist or not exist."""
    mongo.Bundles.delete_many(ALL_DOCUMENTS)
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
    assert ret is None

    # we verify that it has been deleted
    ret = await r.request('GET', '/Bundles')
    results = ret["results"]
    assert len(results) == 0

    # we try to delete it again!
    ret = await r.request('DELETE', f'/Bundles/{test_uuid}')
    assert ret is None

@pytest.mark.asyncio
async def test_patch_bundles_uuid(mongo, rest):
    """Check that PATCH /Bundles/UUID does the right thing, every time."""
    mongo.Bundles.delete_many(ALL_DOCUMENTS)
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
    mongo.Bundles.delete_many(ALL_DOCUMENTS)
    r = rest('system')

    test_data = {
        'bundles': [
            {
                "location": "WIPAC:/data/exp/IceCube/2014/15f7a399-fe40-4337-bb7e-d68d2d28ec8e.zip",
                "status": "accessible",
                "verified": False,
            },
            {
                "location": "WIPAC:/tmp/path1/sub1/48091a00-0c97-482f-a716-2e721b8e9662.zip",
                "status": "deletable",
                "verified": False,
            },
            {
                "location": "WIPAC:/tmp/path1/sub1/24814fa8-875b-4bae-b034-ea8885d2aafe.zip",
                "status": "inaccessible",
                "verified": False,
            },
            {
                "location": "WIPAC:/tmp/path1/sub1/cef98a3b-9a24-4fbc-b4e7-ef251367c020.zip",
                "status": "none",
                "verified": False,
            },
            {
                "location": "WIPAC:/tmp/path1/sub2/8f141afa-db2f-4337-9a24-560c383887b5.zip",
                "status": "transferring",
                "verified": False,
            },
            {
                "location": "WIPAC:/tmp/path1/sub2/f9153da6-3588-416d-ba59-91526376dc43.zip",
                "status": "accessible",
                "verified": True,
            },
            {
                "location": "WIPAC:/tmp/path1/sub2/34b064a7-5ffa-4cb2-9661-6e8b80765e9f.zip",
                "status": "deletable",
                "verified": True,
            },
            {
                "location": "DESY:/tmp/path1/sub2/76e3c5e2-e1a2-42f0-9d59-6546d1cb85e6.zip",
                "status": "inaccessible",
                "verified": True,
            },
            {
                "location": "DESY:/tmp/path1/sub2/3bcd05f5-ceb8-4eb5-a5db-5f7d55a98ff4.zip",
                "status": "none",
                "verified": True,
            },
            {
                "location": "DESY:/tmp/path1/sub2/e596e1f7-ddaa-4255-abe3-81a4769bf192.zip",
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
    ret = await r.request('POST', '/Bundles/actions/pop?site=NERSC&status=inaccessible')
    assert ret['results'] == []

    # I'm the bundler at WIPAC, and should pop one work item
    ret = await r.request('POST', '/Bundles/actions/pop?site=WIPAC&status=inaccessible')
    assert len(ret['results']) == 1
    assert ret['results'][0]["location"] == "WIPAC:/tmp/path1/sub1/24814fa8-875b-4bae-b034-ea8885d2aafe.zip"

    # repeating gets no work
    ret = await r.request('POST', '/Bundles/actions/pop?site=WIPAC&status=inaccessible')
    assert ret['results'] == []

    # I'm the bundler at WIPAC, and should pop one work item
    ret = await r.request('POST', '/Bundles/actions/pop?site=WIPAC&status=accessible')
    assert len(ret['results']) == 1
    assert ret['results'][0]["location"] == "WIPAC:/data/exp/IceCube/2014/15f7a399-fe40-4337-bb7e-d68d2d28ec8e.zip"

@pytest.mark.asyncio
async def test_bundles_actions_pop_errors(mongo, rest):
    """Check error handlers for pop action for bundles."""
    mongo.Bundles.delete_many(ALL_DOCUMENTS)
    r = rest('system')
    request = {}

    with pytest.raises(Exception):
        await r.request('POST', '/Bundles/actions/pop?site=AREA-51', request)

    with pytest.raises(Exception):
        await r.request('POST', '/Bundles/actions/pop?site=WIPAC', request)

    with pytest.raises(Exception):
        await r.request('POST', '/Bundles/actions/pop?site=WIPAC&status=supercalifragilisticexpialidocious', request)

    with pytest.raises(Exception):
        await r.request('POST', '/Bundles/actions/pop?site=WIPAC&status=none&limit=unlimited!!!', request)
