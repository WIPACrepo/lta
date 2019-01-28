import asyncio
from datetime import datetime, timedelta
from json.decoder import JSONDecodeError
import pytest
from tornado.web import HTTPError
from unittest.mock import MagicMock

from lta.rest_server import FilesActionsBulkCreateHandler, main, start
from rest_tools.client import RestClient
from rest_tools.server import Auth

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
async def fabch():
    """Create a properly authenticated FilesActionsBulkCreateHandler."""
    fabch = FilesActionsBulkCreateHandler()
    fabch._finished = False
    fabch._headers_written = False
    fabch._transforms = []
    fabch.application = MagicMock()
    fabch.auth_data = {
        "long-term-archive": {
            "role": "system"
        }
    }
    fabch.send_error = MagicMock()
    return fabch

@pytest.fixture
async def rest(monkeypatch):
    monkeypatch.setenv("LTA_REST_PORT", "8080")
    monkeypatch.setenv("LTA_AUTH_SECRET", "secret")
    monkeypatch.setenv("LTA_AUTH_ISSUER", "lta")
    monkeypatch.setenv("LTA_AUTH_ALGORITHM", "HS512")
    s = start(debug=True)
    a = Auth('secret', issuer='lta', algorithm='HS512')

    def client(role='admin'):
        t = a.create_token('foo', payload={'long-term-archive': {'role': role}})
        return RestClient('http://localhost:8080', token=t, timeout=0.1, retries=0)

    yield client
    s.stop()
    await asyncio.sleep(0.01)

@pytest.mark.asyncio
async def test_server_reachability(rest):
    """
    Check that we can reach the server.
    """
    r = rest()
    ret = await r.request('GET', '/')
    assert ret == {}

@pytest.mark.asyncio
async def test_server_bad_auth(rest):
    """
    Check for bad auth role
    """
    r = rest('')
    with pytest.raises(Exception):
        await r.request('GET', '/TransferRequests')

@pytest.mark.asyncio
async def test_transfer_request_fail(rest):
    """
    Check for bad transfer request handling
    """
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
    """
    Check CRUD semantics for transfer requests.
    """
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
    """
    Check pop action for transfer requests.
    """
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
    """
    Check for status handling
    """
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
async def test_FilesActionsBulkCreateHandler_no_body(mocker, fabch):
    """Send an error if not provided with a POST body in the request."""
    mock_auth = mocker.patch("rest_tools.server.RestHandler.get_current_user")
    fabch.request = ObjectLiteral(
        # look ma, no "body"!
        connection=MagicMock(),
        method="POST"
    )
    result = await fabch.post()
    fabch.send_error.assert_called_with(500, reason='Error in FilesActionsBulkCreateHandler')

@pytest.mark.asyncio
async def test_FilesActionsBulkCreateHandler_empty_body(mocker,fabch):
    """Send an error if provided with an empty POST body in the request."""
    mock_auth = mocker.patch("rest_tools.server.RestHandler.get_current_user")
    fabch.request = ObjectLiteral(
        body="",
        connection=MagicMock(),
        method="POST"
    )
    result = await fabch.post()
    fabch.send_error.assert_called_with(500, reason='Error in FilesActionsBulkCreateHandler')

@pytest.mark.asyncio
async def test_FilesActionsBulkCreateHandler_body_empty_obj(mocker,fabch):
    """Throw an HTTPError if the POST body does not contain a 'files' field."""
    mock_auth = mocker.patch("rest_tools.server.RestHandler.get_current_user")
    fabch.request = ObjectLiteral(
        body="{}",
        connection=MagicMock(),
        method="POST"
    )
    with pytest.raises(HTTPError):
        await fabch.post()

@pytest.mark.asyncio
async def test_FilesActionsBulkCreateHandler_body_files_not_list(mocker,fabch):
    """Throw an HTTPError if the POST body contains a non-array 'files' field."""
    mock_auth = mocker.patch("rest_tools.server.RestHandler.get_current_user")
    fabch.request = ObjectLiteral(
        body='''{
            "files": "are like, your opinion, man"
        }''',
        connection=MagicMock(),
        method="POST"
    )
    with pytest.raises(HTTPError):
        await fabch.post()

@pytest.mark.asyncio
async def test_FilesActionsBulkCreateHandler_body_files_empty_list(mocker,fabch):
    """Throw an HTTPError if the POST body contains an empty array 'files' field."""
    mock_auth = mocker.patch("rest_tools.server.RestHandler.get_current_user")
    fabch.request = ObjectLiteral(
        body='''{
            "files": []
        }''',
        connection=MagicMock(),
        method="POST"
    )
    with pytest.raises(HTTPError):
        await fabch.post()

@pytest.mark.asyncio
async def test_FilesActionsBulkCreateHandler_body_files(mocker,fabch):
    """Process bulk_create 'files' provided in the POST body."""
    mock_auth = mocker.patch("rest_tools.server.RestHandler.get_current_user")
    fabch.db = {
        "Files": {}
    }
    fabch.request = ObjectLiteral(
        body='''{
            "files": [
                {
                    "what we've got here is": "failure to communicate",
                    "some men": "you just can't reach"
                },
                {
                    "which is the way": "he wants it",
                    "well, he gets it": "I don’t like it any more than you men."
                }
            ]
        }''',
        connection=MagicMock(),
        method="POST"
    )
    fabch.set_status = MagicMock()
    fabch.write = MagicMock()
    result = await fabch.post()
    fabch.set_status.assert_called_with(201)
    fabch.write.assert_called_with({
        "files": [ mocker.ANY, mocker.ANY ]
    })
