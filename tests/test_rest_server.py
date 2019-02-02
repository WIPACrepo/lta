# test_rest_server.py
"""Unit tests for lta/rest_server.py."""

import asyncio
from datetime import datetime, timedelta
import pytest  # type: ignore
from tornado.web import HTTPError  # type: ignore
from unittest.mock import MagicMock

from lta.rest_server import FilesActionsBulkCreateHandler
from lta.rest_server import FilesActionsBulkDeleteHandler
from lta.rest_server import FilesActionsBulkUpdateHandler
from lta.rest_server import FilesHandler
from lta.rest_server import FilesSingleHandler
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
async def fabdh():
    """Create a properly authenticated FilesActionsBulkDeleteHandler."""
    fabdh = FilesActionsBulkDeleteHandler()
    fabdh._finished = False
    fabdh._headers_written = False
    fabdh._transforms = []
    fabdh.application = MagicMock()
    fabdh.auth_data = {
        "long-term-archive": {
            "role": "system"
        }
    }
    fabdh.send_error = MagicMock()
    return fabdh

@pytest.fixture
async def fabuh():
    """Create a properly authenticated FilesActionsBulkUpdateHandler."""
    fabuh = FilesActionsBulkUpdateHandler()
    fabuh._finished = False
    fabuh._headers_written = False
    fabuh._transforms = []
    fabuh.application = MagicMock()
    fabuh.auth_data = {
        "long-term-archive": {
            "role": "system"
        }
    }
    fabuh.send_error = MagicMock()
    return fabuh

@pytest.fixture
async def files_handler():
    """Create a properly authenticated FilesHandler."""
    fh = FilesHandler()
    fh._finished = False
    fh._headers_written = False
    fh._transforms = []
    fh.application = MagicMock()
    fh.auth_data = {
        "long-term-archive": {
            "role": "system"
        }
    }
    fh.send_error = MagicMock()
    return fh

@pytest.fixture
async def files_single():
    """Create a properly authenticated FilesSingleHandler."""
    fsh = FilesSingleHandler()
    fsh._finished = False
    fsh._headers_written = False
    fsh._transforms = []
    fsh.application = MagicMock()
    fsh.auth_data = {
        "long-term-archive": {
            "role": "system"
        }
    }
    fsh.send_error = MagicMock()
    return fsh

@pytest.fixture
async def rest(monkeypatch):
    """Provide RestClient as a test fixture."""
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
    with pytest.raises(Exception):
        await r.request('DELETE', f'/TransferRequests/{uuid}')

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
async def test_FilesActionsBulkCreateHandler_no_body(mocker, fabch):
    """Send an error if not provided with a POST body in the request."""
    mocker.patch("rest_tools.server.RestHandler.get_current_user")
    fabch.request = ObjectLiteral(
        # look ma, no "body"!
        connection=MagicMock(),
        method="POST"
    )
    await fabch.post()
    fabch.send_error.assert_called_with(500, reason='Error in FilesActionsBulkCreateHandler')

@pytest.mark.asyncio
async def test_FilesActionsBulkCreateHandler_empty_body(mocker, fabch):
    """Send an error if provided with an empty POST body in the request."""
    mocker.patch("rest_tools.server.RestHandler.get_current_user")
    fabch.request = ObjectLiteral(
        body="",
        connection=MagicMock(),
        method="POST"
    )
    await fabch.post()
    fabch.send_error.assert_called_with(500, reason='Error in FilesActionsBulkCreateHandler')

@pytest.mark.asyncio
async def test_FilesActionsBulkCreateHandler_body_empty_obj(mocker, fabch):
    """Throw an HTTPError if the POST body does not contain a 'files' field."""
    mocker.patch("rest_tools.server.RestHandler.get_current_user")
    fabch.request = ObjectLiteral(
        body="{}",
        connection=MagicMock(),
        method="POST"
    )
    with pytest.raises(HTTPError):
        await fabch.post()

@pytest.mark.asyncio
async def test_FilesActionsBulkCreateHandler_body_files_not_list(mocker, fabch):
    """Throw an HTTPError if the POST body contains a non-array 'files' field."""
    mocker.patch("rest_tools.server.RestHandler.get_current_user")
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
async def test_FilesActionsBulkCreateHandler_body_files_empty_list(mocker, fabch):
    """Throw an HTTPError if the POST body contains an empty array 'files' field."""
    mocker.patch("rest_tools.server.RestHandler.get_current_user")
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
async def test_FilesActionsBulkCreateHandler_body_files(mocker, fabch):
    """Process bulk_create 'files' provided in the POST body."""
    mocker.patch("rest_tools.server.RestHandler.get_current_user")
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
    await fabch.post()
    fabch.set_status.assert_called_with(201)
    fabch.write.assert_called_with({
        "files": [mocker.ANY, mocker.ANY],
        "count": 2
    })

@pytest.mark.asyncio
async def test_FilesActionsBulkDeleteHandler_no_body(mocker, fabdh):
    """Send an error if not provided with a POST body in the request."""
    mocker.patch("rest_tools.server.RestHandler.get_current_user")
    fabdh.request = ObjectLiteral(
        # look ma, no "body"!
        connection=MagicMock(),
        method="POST"
    )
    await fabdh.post()
    fabdh.send_error.assert_called_with(500, reason='Error in FilesActionsBulkDeleteHandler')

@pytest.mark.asyncio
async def test_FilesActionsBulkDeleteHandler_body_empty_obj(mocker, fabdh):
    """Throw an HTTPError if the POST body does not contain a 'files' field."""
    mocker.patch("rest_tools.server.RestHandler.get_current_user")
    fabdh.request = ObjectLiteral(
        body="{}",
        connection=MagicMock(),
        method="POST"
    )
    with pytest.raises(HTTPError):
        await fabdh.post()

@pytest.mark.asyncio
async def test_FilesActionsBulkDeleteHandler_body_files_not_list(mocker, fabdh):
    """Throw an HTTPError if the POST body contains a non-array 'files' field."""
    mocker.patch("rest_tools.server.RestHandler.get_current_user")
    fabdh.request = ObjectLiteral(
        body='''{
            "files": "are like, your opinion, man"
        }''',
        connection=MagicMock(),
        method="POST"
    )
    with pytest.raises(HTTPError):
        await fabdh.post()

@pytest.mark.asyncio
async def test_FilesActionsBulkDeleteHandler_body_files_empty_list(mocker, fabdh):
    """Throw an HTTPError if the POST body contains an empty array 'files' field."""
    mocker.patch("rest_tools.server.RestHandler.get_current_user")
    fabdh.request = ObjectLiteral(
        body='''{
            "files": []
        }''',
        connection=MagicMock(),
        method="POST"
    )
    with pytest.raises(HTTPError):
        await fabdh.post()

@pytest.mark.asyncio
async def test_FilesActionsBulkDeleteHandler_body_files(mocker, fabdh):
    """Process bulk_delete 'files' provided in the POST body."""
    mocker.patch("rest_tools.server.RestHandler.get_current_user")
    fabdh.db = {
        "Files": {
            "1ab46629-6ccf-4158-b62c-b894f8e57283": {},
            "c1fdaf5d-5475-4d44-9743-ed525bf9aee9": {},
            "29184358-9570-4bdc-9ff6-c183a7d14ca2": {},
            "113fb8da-7659-449b-9ff2-e7539b8d14a4": {},
            "57541be1-f434-46de-a66b-9ad2a3418c40": {},
            "d929a90c-0c09-441d-875d-b637e136697d": {},
        }
    }
    fabdh.request = ObjectLiteral(
        body='''{
            "files": [
                "29184358-9570-4bdc-9ff6-c183a7d14ca2",
                "113fb8da-7659-449b-9ff2-e7539b8d14a4",
                "395c8954-326e-4b46-80ea-3ef6ef034732"
            ]
        }''',
        connection=MagicMock(),
        method="POST"
    )
    fabdh.set_status = MagicMock()
    fabdh.write = MagicMock()
    await fabdh.post()
    fabdh.set_status.assert_called_with(200)
    fabdh.write.assert_called_with({
        "files": ["29184358-9570-4bdc-9ff6-c183a7d14ca2", "113fb8da-7659-449b-9ff2-e7539b8d14a4"],
        "count": 2
    })

@pytest.mark.asyncio
async def test_FilesActionsBulkUpdateHandler_no_body(mocker, fabuh):
    """Send an error if not provided with a POST body in the request."""
    mocker.patch("rest_tools.server.RestHandler.get_current_user")
    fabuh.request = ObjectLiteral(
        # look ma, no "body"!
        connection=MagicMock(),
        method="POST"
    )
    await fabuh.post()
    fabuh.send_error.assert_called_with(500, reason='Error in FilesActionsBulkUpdateHandler')

@pytest.mark.asyncio
async def test_FilesActionsBulkUpdateHandler_body_empty_obj(mocker, fabuh):
    """Throw an HTTPError if the POST body does not contain a 'files' field."""
    mocker.patch("rest_tools.server.RestHandler.get_current_user")
    fabuh.request = ObjectLiteral(
        body="{}",
        connection=MagicMock(),
        method="POST"
    )
    with pytest.raises(HTTPError):
        await fabuh.post()

@pytest.mark.asyncio
async def test_FilesActionsBulkUpdateHandler_body_update_not_dict(mocker, fabuh):
    """Throw an HTTPError if the POST body contains a non-array 'files' field."""
    mocker.patch("rest_tools.server.RestHandler.get_current_user")
    fabuh.request = ObjectLiteral(
        body='''{
            "update": true
        }''',
        connection=MagicMock(),
        method="POST"
    )
    with pytest.raises(HTTPError):
        await fabuh.post()

@pytest.mark.asyncio
async def test_FilesActionsBulkUpdateHandler_body_files_missing(mocker, fabuh):
    """Throw an HTTPError if the POST body contains a non-array 'files' field."""
    mocker.patch("rest_tools.server.RestHandler.get_current_user")
    fabuh.request = ObjectLiteral(
        body='''{
            "update": {
                "key": "value"
            }
        }''',
        connection=MagicMock(),
        method="POST"
    )
    with pytest.raises(HTTPError):
        await fabuh.post()

@pytest.mark.asyncio
async def test_FilesActionsBulkUpdateHandler_body_files_not_list(mocker, fabuh):
    """Throw an HTTPError if the POST body contains a non-array 'files' field."""
    mocker.patch("rest_tools.server.RestHandler.get_current_user")
    fabuh.request = ObjectLiteral(
        body='''{
            "update": {
                "key": "value"
            },
            "files": "are like, your opinion, man"
        }''',
        connection=MagicMock(),
        method="POST"
    )
    with pytest.raises(HTTPError):
        await fabuh.post()

@pytest.mark.asyncio
async def test_FilesActionsBulkUpdateHandler_body_files_empty_list(mocker, fabuh):
    """Throw an HTTPError if the POST body contains an empty array 'files' field."""
    mocker.patch("rest_tools.server.RestHandler.get_current_user")
    fabuh.request = ObjectLiteral(
        body='''{
            "update": {
                "key": "value"
            },
            "files": []
        }''',
        connection=MagicMock(),
        method="POST"
    )
    with pytest.raises(HTTPError):
        await fabuh.post()

@pytest.mark.asyncio
async def test_FilesActionsBulkUpdateHandler_body_files(mocker, fabuh):
    """Process bulk_delete 'files' provided in the POST body."""
    mocker.patch("rest_tools.server.RestHandler.get_current_user")
    fabuh.db = {
        "Files": {
            "1ab46629-6ccf-4158-b62c-b894f8e57283": {},
            "c1fdaf5d-5475-4d44-9743-ed525bf9aee9": {},
            "29184358-9570-4bdc-9ff6-c183a7d14ca2": {},
            "113fb8da-7659-449b-9ff2-e7539b8d14a4": {},
            "57541be1-f434-46de-a66b-9ad2a3418c40": {},
            "d929a90c-0c09-441d-875d-b637e136697d": {},
        }
    }
    fabuh.request = ObjectLiteral(
        body='''{
            "update": {
                "key": "value"
            },
            "files": [
                "29184358-9570-4bdc-9ff6-c183a7d14ca2",
                "113fb8da-7659-449b-9ff2-e7539b8d14a4",
                "395c8954-326e-4b46-80ea-3ef6ef034732"
            ]
        }''',
        connection=MagicMock(),
        method="POST"
    )
    fabuh.set_status = MagicMock()
    fabuh.write = MagicMock()
    await fabuh.post()
    fabuh.set_status.assert_called_with(200)
    fabuh.write.assert_called_with({
        "files": ["29184358-9570-4bdc-9ff6-c183a7d14ca2", "113fb8da-7659-449b-9ff2-e7539b8d14a4"],
        "count": 2
    })

@pytest.mark.asyncio
async def test_FilesHandler_GET_location(mocker, files_handler):
    """Attempt to GET /Files?location=WIPAC."""
    mocker.patch("rest_tools.server.RestHandler.get_current_user")
    files_handler.db = {
        "Files": {
            "1ab46629-6ccf-4158-b62c-b894f8e57283": {
                "source": "NERSC:/tmp/path"
            },
            "c1fdaf5d-5475-4d44-9743-ed525bf9aee9": {
                "source": "NERSC:/tmp/path"
            },
            "29184358-9570-4bdc-9ff6-c183a7d14ca2": {
                "source": "WIPAC:/tmp/path"
            },
            "113fb8da-7659-449b-9ff2-e7539b8d14a4": {
                "source": "WIPAC:/tmp/path"
            },
            "57541be1-f434-46de-a66b-9ad2a3418c40": {
                "source": "NERSC:/tmp/path"
            },
            "d929a90c-0c09-441d-875d-b637e136697d": {
                "source": "NERSC:/tmp/path"
            },
        }
    }
    files_handler.request = ObjectLiteral(
        connection=MagicMock(),
        method="GET"
    )
    files_handler.get_query_argument = MagicMock()
    files_handler.get_query_argument.side_effect = ["WIPAC", None, None, None]
    files_handler.set_status = MagicMock()
    files_handler.write = MagicMock()
    await files_handler.get()
    files_handler.get_query_argument.assert_called_with("status", default=None)
    files_handler.set_status.assert_called_with(200)
    files_handler.write.assert_called_with({
        "results": ["29184358-9570-4bdc-9ff6-c183a7d14ca2", "113fb8da-7659-449b-9ff2-e7539b8d14a4"]
    })

@pytest.mark.asyncio
async def test_FilesHandler_GET_transfer_request_uuid(mocker, files_handler):
    """Attempt to GET /Files?transfer_request_uuid=UUID."""
    mocker.patch("rest_tools.server.RestHandler.get_current_user")
    files_handler.db = {
        "Files": {
            "1ab46629-6ccf-4158-b62c-b894f8e57283": {
                "request": "112b6be8-b926-465f-999d-c310ce76a19a"
            },
            "c1fdaf5d-5475-4d44-9743-ed525bf9aee9": {
                "request": "ce2d2b9e-8f1f-49cc-8509-4bbe824836f2"
            },
            "29184358-9570-4bdc-9ff6-c183a7d14ca2": {
                "request": "2d17ad72-58ff-4fa0-8b10-fca0e7faf092"
            },
            "113fb8da-7659-449b-9ff2-e7539b8d14a4": {
                "request": "2d17ad72-58ff-4fa0-8b10-fca0e7faf092"
            },
            "57541be1-f434-46de-a66b-9ad2a3418c40": {
                "request": "b9bad034-b402-4c42-a719-e692aa379289"
            },
            "d929a90c-0c09-441d-875d-b637e136697d": {
                "request": "53b2b478-a7b5-4ce2-aec0-52158ad755dc"
            },
        }
    }
    files_handler.request = ObjectLiteral(
        connection=MagicMock(),
        method="GET"
    )
    files_handler.get_query_argument = MagicMock()
    files_handler.get_query_argument.side_effect = [None, "2d17ad72-58ff-4fa0-8b10-fca0e7faf092", None, None]
    files_handler.set_status = MagicMock()
    files_handler.write = MagicMock()
    await files_handler.get()
    files_handler.get_query_argument.assert_called_with("status", default=None)
    files_handler.set_status.assert_called_with(200)
    files_handler.write.assert_called_with({
        "results": ["29184358-9570-4bdc-9ff6-c183a7d14ca2", "113fb8da-7659-449b-9ff2-e7539b8d14a4"]
    })

@pytest.mark.asyncio
async def test_FilesHandler_GET_bundle_uuid(mocker, files_handler):
    """Attempt to GET /Files?bundle_uuid=UUID."""
    mocker.patch("rest_tools.server.RestHandler.get_current_user")
    files_handler.db = {
        "Files": {
            "1ab46629-6ccf-4158-b62c-b894f8e57283": {
                "bundle": "112b6be8-b926-465f-999d-c310ce76a19a"
            },
            "c1fdaf5d-5475-4d44-9743-ed525bf9aee9": {
                "bundle": "ce2d2b9e-8f1f-49cc-8509-4bbe824836f2"
            },
            "29184358-9570-4bdc-9ff6-c183a7d14ca2": {
                "bundle": "2d17ad72-58ff-4fa0-8b10-fca0e7faf092"
            },
            "113fb8da-7659-449b-9ff2-e7539b8d14a4": {
                "bundle": "2d17ad72-58ff-4fa0-8b10-fca0e7faf092"
            },
            "57541be1-f434-46de-a66b-9ad2a3418c40": {
                "bundle": "b9bad034-b402-4c42-a719-e692aa379289"
            },
            "d929a90c-0c09-441d-875d-b637e136697d": {
                "bundle": "53b2b478-a7b5-4ce2-aec0-52158ad755dc"
            },
        }
    }
    files_handler.request = ObjectLiteral(
        connection=MagicMock(),
        method="GET"
    )
    files_handler.get_query_argument = MagicMock()
    files_handler.get_query_argument.side_effect = [None, None, "2d17ad72-58ff-4fa0-8b10-fca0e7faf092", None]
    files_handler.set_status = MagicMock()
    files_handler.write = MagicMock()
    await files_handler.get()
    files_handler.get_query_argument.assert_called_with("status", default=None)
    files_handler.set_status.assert_called_with(200)
    files_handler.write.assert_called_with({
        "results": ["29184358-9570-4bdc-9ff6-c183a7d14ca2", "113fb8da-7659-449b-9ff2-e7539b8d14a4"]
    })

@pytest.mark.asyncio
async def test_FilesHandler_GET_status(mocker, files_handler):
    """Attempt to GET /Files?status=waiting."""
    mocker.patch("rest_tools.server.RestHandler.get_current_user")
    files_handler.db = {
        "Files": {
            "1ab46629-6ccf-4158-b62c-b894f8e57283": {
                "status": "processing"
            },
            "c1fdaf5d-5475-4d44-9743-ed525bf9aee9": {
                "status": "processing"
            },
            "29184358-9570-4bdc-9ff6-c183a7d14ca2": {
                "status": "waiting"
            },
            "113fb8da-7659-449b-9ff2-e7539b8d14a4": {
                "status": "waiting"
            },
            "57541be1-f434-46de-a66b-9ad2a3418c40": {
                "status": "processing"
            },
            "d929a90c-0c09-441d-875d-b637e136697d": {
                "status": "processing"
            },
        }
    }
    files_handler.request = ObjectLiteral(
        connection=MagicMock(),
        method="GET"
    )
    files_handler.get_query_argument = MagicMock()
    files_handler.get_query_argument.side_effect = [None, None, None, "waiting"]
    files_handler.set_status = MagicMock()
    files_handler.write = MagicMock()
    await files_handler.get()
    files_handler.get_query_argument.assert_called_with("status", default=None)
    files_handler.set_status.assert_called_with(200)
    files_handler.write.assert_called_with({
        "results": ["29184358-9570-4bdc-9ff6-c183a7d14ca2", "113fb8da-7659-449b-9ff2-e7539b8d14a4"]
    })

@pytest.mark.asyncio
async def test_FilesHandler_GET(mocker, files_handler):
    """Attempt to GET /Files."""
    mocker.patch("rest_tools.server.RestHandler.get_current_user")
    files_handler.db = {
        "Files": {
            "1ab46629-6ccf-4158-b62c-b894f8e57283": {},
            "c1fdaf5d-5475-4d44-9743-ed525bf9aee9": {},
            "29184358-9570-4bdc-9ff6-c183a7d14ca2": {},
            "113fb8da-7659-449b-9ff2-e7539b8d14a4": {},
            "57541be1-f434-46de-a66b-9ad2a3418c40": {},
            "d929a90c-0c09-441d-875d-b637e136697d": {},
        }
    }
    files_handler.request = ObjectLiteral(
        connection=MagicMock(),
        method="GET"
    )
    files_handler.get_query_argument = MagicMock()
    files_handler.get_query_argument.side_effect = [None, None, None, None]
    files_handler.set_status = MagicMock()
    files_handler.write = MagicMock()
    await files_handler.get()
    files_handler.get_query_argument.assert_called_with("status", default=None)
    files_handler.set_status.assert_called_with(200)
    files_handler.write.assert_called_with({
        "results": [
            "1ab46629-6ccf-4158-b62c-b894f8e57283",
            "c1fdaf5d-5475-4d44-9743-ed525bf9aee9",
            "29184358-9570-4bdc-9ff6-c183a7d14ca2",
            "113fb8da-7659-449b-9ff2-e7539b8d14a4",
            "57541be1-f434-46de-a66b-9ad2a3418c40",
            "d929a90c-0c09-441d-875d-b637e136697d"
        ]
    })

@pytest.mark.asyncio
async def test_FilesSingleHandler_GET_404(mocker, files_single):
    """Attempt to GET /Files/UUID and fail."""
    mocker.patch("rest_tools.server.RestHandler.get_current_user")
    files_single.db = {
        "Files": {
            "1ab46629-6ccf-4158-b62c-b894f8e57283": {},
            "c1fdaf5d-5475-4d44-9743-ed525bf9aee9": {},
            "29184358-9570-4bdc-9ff6-c183a7d14ca2": {},
            "113fb8da-7659-449b-9ff2-e7539b8d14a4": {},
            "57541be1-f434-46de-a66b-9ad2a3418c40": {},
            "d929a90c-0c09-441d-875d-b637e136697d": {},
        }
    }
    files_single.request = ObjectLiteral(
        connection=MagicMock(),
        method="GET"
    )
    files_single.set_status = MagicMock()
    files_single.write = MagicMock()
    with pytest.raises(HTTPError):
        await files_single.get("46cf2a88-5c5e-4016-a0ee-bdbd7cc9f390")

@pytest.mark.asyncio
async def test_FilesSingleHandler_GET_200(mocker, files_single):
    """Attempt to GET /Files/UUID and succeed."""
    mocker.patch("rest_tools.server.RestHandler.get_current_user")
    files_single.db = {
        "Files": {
            "1ab46629-6ccf-4158-b62c-b894f8e57283": {},
            "c1fdaf5d-5475-4d44-9743-ed525bf9aee9": {},
            "29184358-9570-4bdc-9ff6-c183a7d14ca2": {
                "key": "value",
            },
            "113fb8da-7659-449b-9ff2-e7539b8d14a4": {},
            "57541be1-f434-46de-a66b-9ad2a3418c40": {},
            "d929a90c-0c09-441d-875d-b637e136697d": {},
        }
    }
    files_single.request = ObjectLiteral(
        connection=MagicMock(),
        method="GET"
    )
    files_single.set_status = MagicMock()
    files_single.write = MagicMock()
    await files_single.get("29184358-9570-4bdc-9ff6-c183a7d14ca2")
    files_single.set_status.assert_called_with(200)
    files_single.write.assert_called_with({"key": "value"})

@pytest.mark.asyncio
async def test_FilesSingleHandler_PATCH_404(mocker, files_single):
    """Attempt to PATCH /Files/UUID and fail."""
    mocker.patch("rest_tools.server.RestHandler.get_current_user")
    files_single.db = {
        "Files": {
            "1ab46629-6ccf-4158-b62c-b894f8e57283": {},
            "c1fdaf5d-5475-4d44-9743-ed525bf9aee9": {},
            "29184358-9570-4bdc-9ff6-c183a7d14ca2": {},
            "113fb8da-7659-449b-9ff2-e7539b8d14a4": {},
            "57541be1-f434-46de-a66b-9ad2a3418c40": {},
            "d929a90c-0c09-441d-875d-b637e136697d": {},
        }
    }
    files_single.request = ObjectLiteral(
        connection=MagicMock(),
        method="PATCH"
    )
    files_single.set_status = MagicMock()
    files_single.write = MagicMock()
    with pytest.raises(HTTPError):
        await files_single.patch("46cf2a88-5c5e-4016-a0ee-bdbd7cc9f390")

@pytest.mark.asyncio
async def test_FilesSingleHandler_PATCH_400(mocker, files_single):
    """Attempt to PATCH /Files/UUID and fail."""
    mocker.patch("rest_tools.server.RestHandler.get_current_user")
    files_single.db = {
        "Files": {
            "1ab46629-6ccf-4158-b62c-b894f8e57283": {},
            "c1fdaf5d-5475-4d44-9743-ed525bf9aee9": {},
            "29184358-9570-4bdc-9ff6-c183a7d14ca2": {
                "key": "value",
            },
            "113fb8da-7659-449b-9ff2-e7539b8d14a4": {},
            "57541be1-f434-46de-a66b-9ad2a3418c40": {},
            "d929a90c-0c09-441d-875d-b637e136697d": {},
        }
    }
    files_single.request = ObjectLiteral(
        body='''{
            "uuid": "113fb8da-7659-449b-9ff2-e7539b8d14a4"
        }''',
        connection=MagicMock(),
        method="PATCH"
    )
    files_single.set_status = MagicMock()
    files_single.write = MagicMock()
    with pytest.raises(HTTPError):
        await files_single.patch("29184358-9570-4bdc-9ff6-c183a7d14ca2")

@pytest.mark.asyncio
async def test_FilesSingleHandler_PATCH_200(mocker, files_single):
    """Attempt to PATCH /Files/UUID and succeed."""
    mocker.patch("rest_tools.server.RestHandler.get_current_user")
    files_single.db = {
        "Files": {
            "1ab46629-6ccf-4158-b62c-b894f8e57283": {},
            "c1fdaf5d-5475-4d44-9743-ed525bf9aee9": {},
            "29184358-9570-4bdc-9ff6-c183a7d14ca2": {
                "key": "value",
            },
            "113fb8da-7659-449b-9ff2-e7539b8d14a4": {},
            "57541be1-f434-46de-a66b-9ad2a3418c40": {},
            "d929a90c-0c09-441d-875d-b637e136697d": {},
        }
    }
    files_single.request = ObjectLiteral(
        body='''{
            "key": "value2"
        }''',
        connection=MagicMock(),
        method="GET"
    )
    files_single.set_status = MagicMock()
    files_single.write = MagicMock()
    await files_single.patch("29184358-9570-4bdc-9ff6-c183a7d14ca2")
    files_single.set_status.assert_called_with(200)
    files_single.write.assert_called_with({"key": "value2"})

@pytest.mark.asyncio
async def test_FilesSingleHandler_DELETE_404(mocker, files_single):
    """Attempt to DELETE /Files/UUID and fail."""
    mocker.patch("rest_tools.server.RestHandler.get_current_user")
    files_single.db = {
        "Files": {
            "1ab46629-6ccf-4158-b62c-b894f8e57283": {},
            "c1fdaf5d-5475-4d44-9743-ed525bf9aee9": {},
            "29184358-9570-4bdc-9ff6-c183a7d14ca2": {},
            "113fb8da-7659-449b-9ff2-e7539b8d14a4": {},
            "57541be1-f434-46de-a66b-9ad2a3418c40": {},
            "d929a90c-0c09-441d-875d-b637e136697d": {},
        }
    }
    files_single.request = ObjectLiteral(
        connection=MagicMock(),
        method="DELETE"
    )
    files_single.set_status = MagicMock()
    files_single.write = MagicMock()
    result = await files_single.delete("46cf2a88-5c5e-4016-a0ee-bdbd7cc9f390")
    assert result is None
    files_single.set_status.assert_called_with(404)

@pytest.mark.asyncio
async def test_FilesSingleHandler_DELETE_204(mocker, files_single):
    """Attempt to DELETE /Files/UUID and succeed."""
    mocker.patch("rest_tools.server.RestHandler.get_current_user")
    files_single.db = {
        "Files": {
            "1ab46629-6ccf-4158-b62c-b894f8e57283": {},
            "c1fdaf5d-5475-4d44-9743-ed525bf9aee9": {},
            "29184358-9570-4bdc-9ff6-c183a7d14ca2": {},
            "113fb8da-7659-449b-9ff2-e7539b8d14a4": {},
            "57541be1-f434-46de-a66b-9ad2a3418c40": {},
            "d929a90c-0c09-441d-875d-b637e136697d": {},
        }
    }
    files_single.request = ObjectLiteral(
        connection=MagicMock(),
        method="DELETE"
    )
    files_single.set_status = MagicMock()
    files_single.write = MagicMock()
    result = await files_single.delete("29184358-9570-4bdc-9ff6-c183a7d14ca2")
    assert result is None
    files_single.set_status.assert_called_with(204)
