import asyncio
import pytest

from lta.rest_server import start
from rest_tools.client import RestClient

@pytest.fixture
async def rest(monkeypatch):
    monkeypatch.setenv("LTA_REST_PORT", "8080")
    s = start(debug=True)
    def client(token=''):
        return RestClient('http://localhost:8080', token=token,
                          timeout=0.1, backoff=False)
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
