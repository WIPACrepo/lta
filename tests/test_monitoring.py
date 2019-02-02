# test_monitoring.py
"""Unit tests for lta/monitoring.py."""

import asyncio
import pytest  # type: ignore
from unittest.mock import MagicMock

import requests

import lta.monitoring


class AsyncMock(MagicMock):
    """
    AsyncMock is the async version of a MagicMock.

    We use this class in place of MagicMock when we want to mock
    asynchronous callables.

    Source: https://stackoverflow.com/a/32498408
    """

    async def __call__(self, *args, **kwargs):
        """Allow MagicMock to work its magic too."""
        return super(AsyncMock, self).__call__(*args, **kwargs)

@pytest.fixture
def monitor(monkeypatch, mocker):
    """Provide a RestClient fixture."""
    monkeypatch.setenv("LTA_REST_URL", "foo")
    monkeypatch.setenv("LTA_REST_TOKEN", "bar")

    rc = mocker.patch('lta.monitoring.RestClient')
    rc.return_value.request = AsyncMock()
    return rc.return_value.request

@pytest.mark.asyncio
async def test_base_init(monitor):
    """Check basic properties of monitoring, without specifics."""
    monitor.return_value = {'health': 'OK'}

    m = lta.monitoring.Monitor('foo', 'bar')
    ret = await m.get_from_rest()
    assert ret == monitor.return_value
    assert monitor.called

    with pytest.raises(NotImplementedError):
        await m.run()

@pytest.mark.asyncio
async def test_base_run(monitor):
    """Check the run() method of the base class."""
    m = lta.monitoring.Monitor('foo', 'bar')
    m.do = AsyncMock(side_effect=m.stop)

    m.interval = 0.1
    await m.run()
    assert m.running is False

@pytest.mark.asyncio
async def test_prometheus(monitor):
    """Check the prometheus class."""
    m = lta.monitoring.PrometheusMonitor(port=8888, lta_rest_url='foo', lta_rest_token='bar')
    monitor.return_value = {'health': 'OK'}
    await m.do()

    r = requests.request('GET', 'http://localhost:8888')
    for line in r.text.split('\n'):
        if line.startswith('health') and line.split('{', 1)[0] == 'health':
            if 'health="OK"' in line:
                assert line.split()[-1] == '1.0'
            elif 'health="WARN"' in line:
                assert line.split()[-1] == '0.0'
            elif 'health="ERROR"' in line:
                assert line.split()[-1] == '0.0'

def test_main(monitor, monkeypatch, mocker):
    """Check the `main` function."""
    monkeypatch.setenv("ENABLE_PROMETHEUS", "true")
    monkeypatch.setenv("PROMETHEUS_MONITORING_INTERVAL", "1")
    monkeypatch.setenv("PROMETHEUS_PORT", "23456")

    monitor.side_effect = [{'health': 'OK'}, Exception()]

    loop = asyncio.get_event_loop()
    loop.call_later(0.2, loop.stop)

    lta.monitoring.main()

    r = requests.request('GET', 'http://localhost:23456')
    for line in r.text.split('\n'):
        if line.startswith('health') and line.split('{', 1)[0] == 'health':
            if 'health="OK"' in line:
                assert line.split()[-1] == '1.0'
            elif 'health="WARN"' in line:
                assert line.split()[-1] == '0.0'
            elif 'health="ERROR"' in line:
                assert line.split()[-1] == '0.0'
