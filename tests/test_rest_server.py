import asyncio
import pytest
from datetime import datetime, timedelta

from lta.rest_server import start
from rest_tools.client import RestClient
from rest_tools.server import Auth

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
