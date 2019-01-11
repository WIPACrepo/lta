import pytest

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

@pytest.mark.asyncio
async def test_server_reachability(rest):
    """
    Check that we can reach the server.
    """
    r = rest()
    ret = await r.request('GET', '/')
    assert ret == {}


@pytest.mark.asyncio
async def test_transfer_request_crud(rest):
    """
    Check CRUD semantics for transfer requests.
    """
    r = rest()
    request = {'foo': 'bar'}
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

    ret = await r.request('DELETE', f'/TransferRequests/{uuid}')
    assert ret is None

    with pytest.raises(Exception):
        await r.request('GET', f'/TransferRequests/{uuid}')
    ret = await r.request('DELETE', f'/TransferRequests/{uuid}')
    assert ret is None

    ret = await r.request('GET', '/TransferRequests')
    assert len(ret['results']) == 0
