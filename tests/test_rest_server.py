# test_rest_server.py
"""Unit tests for lta/rest_server.py."""

import asyncio
from datetime import datetime, timedelta
import pytest  # type: ignore
import socket

from lta.rest_server import main, start
from rest_tools.client import RestClient  # type: ignore
from rest_tools.server import Auth  # type: ignore

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

    def client(role='admin'):
        t = a.create_token('foo', payload={'long-term-archive': {'role': role}})
        return RestClient(f'http://localhost:{port}', token=t, timeout=0.1, retries=0)

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
async def test_transfer_request_crud(rest):
    """Check CRUD semantics for transfer requests."""
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
async def test_status(rest):
    """Check for status handling."""
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
async def test_files_bulk_crud(rest):
    """Check CRUD semantics for files."""
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
    request = {'files': results, 'update': {'key': 'value'}}
    ret = await r.request('POST', '/Files/actions/bulk_update', request)
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
    request = {'files': results}
    ret = await r.request('POST', '/Files/actions/bulk_delete', request)
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
async def test_get_files_filter(rest):
    """Check that GET /Files filters properly by query parameters.."""
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
async def test_delete_files_uuid(rest):
    """Check that DELETE /Files/UUID returns 204, exist or not exist."""
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
async def test_patch_files_uuid(rest):
    """Check that PATCH /Files/UUID does the right thing, every time."""
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
