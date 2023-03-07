# test_monitoring.py
"""Unit tests for lta/monitoring.py."""

import asyncio
import socket
from typing import cast
from unittest.mock import AsyncMock

import pytest
from pytest import MonkeyPatch
from pytest_mock import MockerFixture
import requests

import lta.monitoring


@pytest.fixture
def monitor(mocker: MockerFixture, monkeypatch: MonkeyPatch) -> AsyncMock:
    """Provide a RestClient fixture."""
    monkeypatch.setenv("LTA_REST_URL", "foo")
    monkeypatch.setenv("LTA_REST_TOKEN", "bar")

    rc = mocker.patch('lta.monitoring.RestClient')
    rc.return_value.request = AsyncMock()
    return rc.return_value.request


@pytest.fixture
def port() -> int:
    """Get an ephemeral port number."""
    # https://unix.stackexchange.com/a/132524
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.bind(('', 0))
    addr = s.getsockname()
    ephemeral_port = addr[1]
    s.close()
    return cast(int, ephemeral_port)


@pytest.mark.asyncio
async def test_base_init(monitor: AsyncMock) -> None:
    """Check basic properties of monitoring, without specifics."""
    monitor.return_value = {'health': 'OK'}

    m = lta.monitoring.Monitor('foo', 'bar')
    ret = await m.get_from_rest()
    assert ret == monitor.return_value
    assert monitor.called

    with pytest.raises(NotImplementedError):
        await m.run()


@pytest.mark.asyncio
async def test_base_run(monitor: AsyncMock) -> None:
    """Check the run() method of the base class."""
    m = lta.monitoring.Monitor('foo', 'bar')
    m.do = AsyncMock(side_effect=m.stop)  # type: ignore[assignment]

    m.interval = 0.1
    await m.run()
    assert m.running is False


@pytest.mark.asyncio
async def test_prometheus(monitor: AsyncMock, port: int) -> None:
    """Check the prometheus class."""
    m = lta.monitoring.PrometheusMonitor(port=str(port), lta_rest_url='foo', lta_rest_token='bar')
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


def test_main(mocker: MockerFixture,
              monitor: AsyncMock,
              monkeypatch: MonkeyPatch,
              port: int) -> None:
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
