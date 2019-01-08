
import pytest

from lta.rest_server import start
from rest_tools.client import AsyncSession

def test_server_startup():
    """
    Check that there are no errors during server startup.
    """
    s = start()
    s.stop()

async def test_server_reachability(monkeypatch):
    """
    Check that we can reach the server.
    """
    monkeypatch.setenv("LTA_REST_PORT", "8080")
    s = start()
    r = AsyncSession()
    req = await r.get('http://localhost:8080/')
    req.raise_for_status()
    s.stop()
