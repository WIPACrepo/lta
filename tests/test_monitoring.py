# test_monitoring.py
"""Unit tests for lta/monitoring.py."""

import asyncio
import socket
from unittest.mock import AsyncMock

import pytest  # type: ignore
import requests

import lta.monitoring


@pytest.fixture
def monitor(monkeypatch, mocker):
    """Provide a RestClient fixture."""
    monkeypatch.setenv("LTA_REST_URL", "foo")
    monkeypatch.setenv("LTA_REST_TOKEN", "bar")

    rc = mocker.patch('lta.monitoring.RestClient')
    rc.return_value.request = AsyncMock()
    return rc.return_value.request

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
async def test_prometheus(monitor, port):
    """Check the prometheus class."""
    m = lta.monitoring.PrometheusMonitor(port=port, lta_rest_url='foo', lta_rest_token='bar')
    monitor.return_value = {'health': 'OK'}
    await m.do()

    r = requests.request('GET', f'http://localhost:{port}')
    for line in r.text.split('\n'):
        if line.startswith('health') and line.split('{', 1)[0] == 'health':
            if 'health="OK"' in line:
                assert line.split()[-1] == '1.0'
            elif 'health="WARN"' in line:
                assert line.split()[-1] == '0.0'
            elif 'health="ERROR"' in line:
                assert line.split()[-1] == '0.0'

def test_main(monitor, monkeypatch, mocker, port):
    """Check the `main` function."""
    monkeypatch.setenv("ENABLE_PROMETHEUS", "true")
    monkeypatch.setenv("PROMETHEUS_MONITORING_INTERVAL", "1")
    monkeypatch.setenv("PROMETHEUS_PORT", str(port))

    monitor.side_effect = [{'health': 'OK'}, Exception()]

    loop = asyncio.get_event_loop()
    loop.call_later(0.2, loop.stop)

    lta.monitoring.main()

    r = requests.request('GET', f'http://localhost:{port}')
    for line in r.text.split('\n'):
        if line.startswith('health') and line.split('{', 1)[0] == 'health':
            if 'health="OK"' in line:
                assert line.split()[-1] == '1.0'
            elif 'health="WARN"' in line:
                assert line.split()[-1] == '0.0'
            elif 'health="ERROR"' in line:
                assert line.split()[-1] == '0.0'
