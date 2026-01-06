# test_rest_server.py
"""Unit tests for lta/rest_server.py."""

# fmt:off

import asyncio
import logging
import os
import socket
import tracemalloc
from typing import Any, AsyncGenerator, Callable, cast, Dict, List
from unittest.mock import AsyncMock
from urllib.parse import quote_plus

import prometheus_client
from pymongo import MongoClient
from pymongo.database import Database
import pytest
from pytest import MonkeyPatch
import pytest_asyncio
from pytest_mock import MockerFixture
from rest_tools.client import RestClient
from rest_tools.utils import Auth
from requests.exceptions import HTTPError
from wipac_dev_tools import strtobool

from lta.rest_server import main, start, unique_id


LtaCollection = Database[Dict[str, Any]]
RestClientFactory = Callable[[str, float], RestClient]

REQ_TOTAL = "lta_requests_total"
RESP_TOTAL = "lta_responses_total"

tracemalloc.start(1)

ALL_DOCUMENTS: Dict[str, str] = {}
REMOVE_ID = {"_id": False}

CONFIG = {
    "LOG_LEVEL": "INFO",
    'LTA_MONGODB_AUTH_USER': '',
    'LTA_MONGODB_AUTH_PASS': '',
    'LTA_MONGODB_DATABASE_NAME': 'lta',
    'LTA_MONGODB_HOST': 'localhost',
    'LTA_MONGODB_PORT': '27017',
    'OTEL_EXPORTER_OTLP_ENDPOINT': 'localhost:4317',
    'PROMETHEUS_METRICS_PORT': '8090',
}
for k in CONFIG:
    if k in os.environ:
        CONFIG[k] = os.environ[k]

logging.getLogger().setLevel(CONFIG['LOG_LEVEL'])


@pytest.fixture
def mongo(monkeypatch: MonkeyPatch) -> LtaCollection:
    """Get a reference to a test instance of a MongoDB Database."""
    mongo_user = quote_plus(CONFIG["LTA_MONGODB_AUTH_USER"])
    mongo_pass = quote_plus(CONFIG["LTA_MONGODB_AUTH_PASS"])
    mongo_host = CONFIG["LTA_MONGODB_HOST"]
    mongo_port = int(CONFIG["LTA_MONGODB_PORT"])
    lta_mongodb_url = f"mongodb://{mongo_host}"
    if mongo_user and mongo_pass:
        lta_mongodb_url = f"mongodb://{mongo_user}:{mongo_pass}@{mongo_host}"
    client: MongoClient[Dict[str, Any]] = MongoClient(lta_mongodb_url, port=mongo_port)
    db = client[CONFIG['LTA_MONGODB_DATABASE_NAME']]
    for collection in db.list_collection_names():
        if 'system' not in collection:
            db.drop_collection(collection)
    return db


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


def _reset_prometheus_registry() -> None:
    """Remove all collectors from the default Prometheus registry (test-only)."""
    # These attributes are internal to prometheus_client.
    for collector in list(prometheus_client.REGISTRY._collector_to_names.keys()):  # type: ignore[attr-defined]
        prometheus_client.REGISTRY.unregister(collector)


@pytest_asyncio.fixture
async def rest(monkeypatch: MonkeyPatch, port: int) -> AsyncGenerator[RestClientFactory, None]:
    """Provide RestClient as a test fixture."""
    _reset_prometheus_registry()

    # setup_function
    monkeypatch.setenv("CI_TEST", "TRUE")
    monkeypatch.setenv("LTA_MONGODB_DATABASE_NAME", CONFIG['LTA_MONGODB_DATABASE_NAME'])
    monkeypatch.setenv("LTA_REST_PORT", str(port))
    monkeypatch.setenv("LTA_SITE_CONFIG", "examples/site.json")
    monkeypatch.setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "localhost:4317")
    monkeypatch.setenv("PROMETHEUS_METRICS_PORT", "8090")
    s = start(debug=True)

    _clients: list[RestClient] = []

    def client(role: str, timeout: float = 0.5) -> RestClient:
        # But they were, all of them, deceived, for another Token was made.
        # In the land of PyTest, in the fires of Mount Fixture, the Dark Lord
        # Sauron forged in secret a master Token, to control all others. And
        # into this Token he poured his cruelty, his malice and his will to
        # dominate all life. One Token to rule them all.
        auth = Auth("secret")  # type: ignore[no-untyped-call]
        token_data: Dict[str, Any] = {
            "resource_access": {
                "long-term-archive": {
                    "roles": [role]
                }
            }
        }
        logging.info("setting role to %s", role)
        token = auth.create_token(subject="lta",  # type: ignore[no-untyped-call]
                                  expiration=300,
                                  payload=token_data,
                                  headers=None)
        rc = RestClient(f'http://localhost:{port}', token=token, timeout=timeout, retries=0)
        _clients.append(rc)
        return rc

    try:
        yield client
    # teardown_function
    finally:
        for r in _clients:
            r.close()
        await s.stop()  # type: ignore[no-untyped-call]
        await asyncio.sleep(0.01)


# -----------------------------------------------------------------------------
# 000s - Helpers / pure utils
# -----------------------------------------------------------------------------


def test_000_strtobool() -> None:
    """Test the strtobool function."""
    assert not strtobool("0")
    assert not strtobool("F")
    assert not strtobool("f")
    assert not strtobool("FALSE")
    assert not strtobool("false")
    assert not strtobool("N")
    assert not strtobool("n")
    assert not strtobool("NO")
    assert not strtobool("no")

    assert strtobool("1")
    assert strtobool("T")
    assert strtobool("t")
    assert strtobool("TRUE")
    assert strtobool("true")
    assert strtobool("Y")
    assert strtobool("y")
    assert strtobool("YES")
    assert strtobool("yes")

    with pytest.raises(ValueError):
        assert not strtobool("alice")
    with pytest.raises(ValueError):
        assert not strtobool("bob")


# -----------------------------------------------------------------------------
# 100s - Server lifecycle / reachability
# -----------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_100_server_reachability(rest: RestClientFactory) -> None:
    """Check that we can reach the server."""
    get_prom = prometheus_client.REGISTRY.get_sample_value  # alias here in case registry mutates

    r = rest("system")  # type: ignore[call-arg]
    # request: GET
    ret = await r.request('GET', '/')
    assert ret == {}

    assert get_prom(REQ_TOTAL, {"method": "GET", "route": "/"}) == 1.0
    assert get_prom(RESP_TOTAL, {"method": "GET", "response": "200", "route": "/"}) == 1.0


# -----------------------------------------------------------------------------
# 200s - TransferRequests endpoints
# -----------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_200_transfer_request_fail(rest: RestClientFactory) -> None:
    """Check for bad transfer request handling."""
    get_prom = prometheus_client.REGISTRY.get_sample_value  # alias here in case registry mutates

    r = rest("system")  # type: ignore[call-arg]

    # request: POST
    request: Dict[str, Any] = {'dest': ['bar']}
    with pytest.raises(HTTPError, match=r"missing source field") as exc:
        await r.request('POST', '/TransferRequests', request)
    assert exc.value.response.status_code == 400  # type: ignore[union-attr]
    assert get_prom(REQ_TOTAL, {"method": "POST", "route": "/TransferRequests"}) == 1.0
    assert get_prom(RESP_TOTAL, {"method": "POST", "response": "400", "route": "/TransferRequests"}) == 1.0

    # request: POST
    request = {'source': 'foo'}
    with pytest.raises(HTTPError, match=r"missing dest field") as exc:
        await r.request('POST', '/TransferRequests', request)
    assert exc.value.response.status_code == 400  # type: ignore[union-attr]
    assert get_prom(REQ_TOTAL, {"method": "POST", "route": "/TransferRequests"}) == 2.0
    assert get_prom(RESP_TOTAL, {"method": "POST", "response": "400", "route": "/TransferRequests"}) == 2.0

    # request: POST
    request = {'source': 'foo', 'dest': 'bar'}
    with pytest.raises(HTTPError, match=r"missing path field") as exc:
        await r.request('POST', '/TransferRequests', request)
    assert exc.value.response.status_code == 400  # type: ignore[union-attr]
    assert get_prom(REQ_TOTAL, {"method": "POST", "route": "/TransferRequests"}) == 3.0
    assert get_prom(RESP_TOTAL, {"method": "POST", "response": "400", "route": "/TransferRequests"}) == 3.0

    # request: POST
    request = {'source': 'foo', 'dest': []}
    with pytest.raises(HTTPError, match=r"missing path field") as exc:
        await r.request('POST', '/TransferRequests', request)
    assert exc.value.response.status_code == 400  # type: ignore[union-attr]
    assert get_prom(REQ_TOTAL, {"method": "POST", "route": "/TransferRequests"}) == 4.0
    assert get_prom(RESP_TOTAL, {"method": "POST", "response": "400", "route": "/TransferRequests"}) == 4.0

    # request: POST
    request = {'source': [], 'dest': 'bar', 'path': 'snafu'}
    with pytest.raises(HTTPError, match=r"source field is not a string") as exc:
        await r.request('POST', '/TransferRequests', request)
    assert exc.value.response.status_code == 400  # type: ignore[union-attr]
    assert get_prom(REQ_TOTAL, {"method": "POST", "route": "/TransferRequests"}) == 5.0
    assert get_prom(RESP_TOTAL, {"method": "POST", "response": "400", "route": "/TransferRequests"}) == 5.0

    # request: POST
    request = {'source': 'foo', 'dest': [], 'path': 'snafu'}
    with pytest.raises(HTTPError, match=r"dest field is not a string") as exc:
        await r.request('POST', '/TransferRequests', request)
    assert exc.value.response.status_code == 400  # type: ignore[union-attr]
    assert get_prom(REQ_TOTAL, {"method": "POST", "route": "/TransferRequests"}) == 6.0
    assert get_prom(RESP_TOTAL, {"method": "POST", "response": "400", "route": "/TransferRequests"}) == 6.0

    # request: POST
    request = {'source': 'foo', 'dest': 'bar', 'path': []}
    with pytest.raises(HTTPError, match=r"path field is not a string") as exc:
        await r.request('POST', '/TransferRequests', request)
    assert exc.value.response.status_code == 400  # type: ignore[union-attr]
    assert get_prom(REQ_TOTAL, {"method": "POST", "route": "/TransferRequests"}) == 7.0
    assert get_prom(RESP_TOTAL, {"method": "POST", "response": "400", "route": "/TransferRequests"}) == 7.0

    # request: POST
    request = {'source': "", 'dest': 'bar', 'path': 'snafu'}
    with pytest.raises(HTTPError, match=r"source field is empty") as exc:
        await r.request('POST', '/TransferRequests', request)
    assert exc.value.response.status_code == 400  # type: ignore[union-attr]
    assert get_prom(REQ_TOTAL, {"method": "POST", "route": "/TransferRequests"}) == 8.0
    assert get_prom(RESP_TOTAL, {"method": "POST", "response": "400", "route": "/TransferRequests"}) == 8.0

    # request: POST
    request = {'source': 'foo', 'dest': "", 'path': 'snafu'}
    with pytest.raises(HTTPError, match=r"dest field is empty") as exc:
        await r.request('POST', '/TransferRequests', request)
    assert exc.value.response.status_code == 400  # type: ignore[union-attr]
    assert get_prom(REQ_TOTAL, {"method": "POST", "route": "/TransferRequests"}) == 9.0
    assert get_prom(RESP_TOTAL, {"method": "POST", "response": "400", "route": "/TransferRequests"}) == 9.0

    # request: POST
    request = {'source': 'foo', 'dest': 'bar', 'path': ""}
    with pytest.raises(HTTPError, match=r"path field is empty") as exc:
        await r.request('POST', '/TransferRequests', request)
    assert exc.value.response.status_code == 400  # type: ignore[union-attr]
    assert get_prom(REQ_TOTAL, {"method": "POST", "route": "/TransferRequests"}) == 10.0
    assert get_prom(RESP_TOTAL, {"method": "POST", "response": "400", "route": "/TransferRequests"}) == 10.0


@pytest.mark.asyncio
async def test_210_transfer_request_crud(mongo: LtaCollection, rest: RestClientFactory) -> None:
    """Check CRUD semantics for transfer requests."""
    get_prom = prometheus_client.REGISTRY.get_sample_value  # alias here in case registry mutates

    r = rest(role="system")  # type: ignore[call-arg]

    # request: POST
    request = {'source': 'foo', 'dest': 'bar', 'path': 'snafu'}
    ret = await r.request('POST', '/TransferRequests', request)
    uuid = ret['TransferRequest']
    assert uuid
    assert get_prom(REQ_TOTAL, {"method": "POST", "route": "/TransferRequests"}) == 1.0
    assert get_prom(RESP_TOTAL, {"method": "POST", "response": "201", "route": "/TransferRequests"}) == 1.0

    # request: GET
    ret = await r.request('GET', '/TransferRequests')
    assert len(ret['results']) == 1
    assert get_prom(REQ_TOTAL, {"method": "GET", "route": "/TransferRequests"}) == 1.0
    assert get_prom(RESP_TOTAL, {"method": "GET", "response": "200", "route": "/TransferRequests"}) == 1.0

    # request: GET
    ret = await r.request('GET', f'/TransferRequests/{uuid}')
    for k in request:
        assert request[k] == ret[k]
    assert get_prom(REQ_TOTAL, {"method": "GET", "route": "/TransferRequests/{request_id}"}) == 1.0
    assert get_prom(RESP_TOTAL, {"method": "GET", "response": "200", "route": "/TransferRequests/{request_id}"}) == 1.0

    # request: PATCH
    request2 = {'bar': 2}
    ret = await r.request('PATCH', f'/TransferRequests/{uuid}', request2)
    assert ret == {}
    assert get_prom(REQ_TOTAL, {"method": "PATCH", "route": "/TransferRequests/{request_id}"}) == 1.0
    assert get_prom(RESP_TOTAL, {"method": "PATCH", "response": "200", "route": "/TransferRequests/{request_id}"}) == 1.0

    # request: PATCH
    with pytest.raises(HTTPError, match=r"not found") as exc:
        await r.request('PATCH', '/TransferRequests/foo', request2)
    assert exc.value.response.status_code == 404  # type: ignore[union-attr]
    assert get_prom(REQ_TOTAL, {"method": "PATCH", "route": "/TransferRequests/{request_id}"}) == 2.0
    assert get_prom(RESP_TOTAL, {"method": "PATCH", "response": "404", "route": "/TransferRequests/{request_id}"}) == 1.0

    # request: DELETE
    ret = await r.request('DELETE', f'/TransferRequests/{uuid}')
    assert not ret
    assert get_prom(REQ_TOTAL, {"method": "DELETE", "route": "/TransferRequests/{request_id}"}) == 1.0
    assert get_prom(RESP_TOTAL, {"method": "DELETE", "response": "204", "route": "/TransferRequests/{request_id}"}) == 1.0

    # request: GET
    with pytest.raises(HTTPError, match=r"not found") as exc:
        await r.request('GET', f'/TransferRequests/{uuid}')
    assert exc.value.response.status_code == 404  # type: ignore[union-attr]
    assert get_prom(REQ_TOTAL, {"method": "GET", "route": "/TransferRequests/{request_id}"}) == 2.0
    assert get_prom(RESP_TOTAL, {"method": "GET", "response": "404", "route": "/TransferRequests/{request_id}"}) == 1.0

    # request: DELETE
    ret = await r.request('DELETE', f'/TransferRequests/{uuid}')
    assert not ret
    assert get_prom(REQ_TOTAL, {"method": "DELETE", "route": "/TransferRequests/{request_id}"}) == 2.0
    assert get_prom(RESP_TOTAL, {"method": "DELETE", "response": "204", "route": "/TransferRequests/{request_id}"}) == 2.0

    # request: GET
    ret = await r.request('GET', '/TransferRequests')
    assert len(ret['results']) == 0
    assert get_prom(REQ_TOTAL, {"method": "GET", "route": "/TransferRequests"}) == 2.0
    assert get_prom(RESP_TOTAL, {"method": "GET", "response": "200", "route": "/TransferRequests"}) == 2.0


@pytest.mark.asyncio
async def test_220_transfer_request_pop(rest: RestClientFactory) -> None:
    """Check pop action for transfer requests."""
    get_prom = prometheus_client.REGISTRY.get_sample_value  # alias here in case registry mutates

    r = rest('system')  # type: ignore[call-arg]

    # request: POST
    request = {
        'source': 'WIPAC',
        'dest': 'NERSC',
        'path': '/data/exp/foo/bar',
    }
    ret = await r.request('POST', '/TransferRequests', request)
    uuid = ret['TransferRequest']
    assert uuid
    assert get_prom(REQ_TOTAL, {"method": "POST", "route": "/TransferRequests"}) == 1.0
    assert get_prom(RESP_TOTAL, {"method": "POST", "response": "201", "route": "/TransferRequests"}) == 1.0

    # request: POST
    # I'm being a jerk and claiming without naming myself as claimant
    with pytest.raises(HTTPError, match=r"missing claimant field") as exc:
        await r.request('POST', '/TransferRequests/actions/pop?source=JERK_STORE')
    assert exc.value.response.status_code == 400  # type: ignore[union-attr]
    assert get_prom(REQ_TOTAL, {"method": "POST", "route": "/TransferRequests/actions/pop"}) == 1.0
    assert get_prom(RESP_TOTAL, {"method": "POST", "response": "400", "route": "/TransferRequests/actions/pop"}) == 1.0

    # request: POST
    # I'm at NERSC, and should have no work
    nersc_pop_claimant = {
        'claimant': 'testing-picker-aaaed864-0112-4bcf-a069-bb55c12e291d',
    }
    ret = await r.request('POST', '/TransferRequests/actions/pop?source=NERSC', nersc_pop_claimant)
    assert not ret['transfer_request']
    assert get_prom(REQ_TOTAL, {"method": "POST", "route": "/TransferRequests/actions/pop"}) == 2.0
    assert get_prom(RESP_TOTAL, {"method": "POST", "response": "200", "route": "/TransferRequests/actions/pop"}) == 1.0

    # request: POST
    # I'm the picker at WIPAC, and should have one work item
    wipac_pop_claimant = {
        'claimant': 'testing-picker-3e4da7c3-bb73-4ab3-b6a6-02ceff6501fc',
    }
    ret = await r.request('POST', '/TransferRequests/actions/pop?source=WIPAC', wipac_pop_claimant)
    assert ret['transfer_request']
    for k in request:
        assert request[k] == ret['transfer_request'][k]
    assert get_prom(REQ_TOTAL, {"method": "POST", "route": "/TransferRequests/actions/pop"}) == 3.0
    assert get_prom(RESP_TOTAL, {"method": "POST", "response": "200", "route": "/TransferRequests/actions/pop"}) == 2.0

    # request: POST
    # repeating gets no work
    ret = await r.request('POST', '/TransferRequests/actions/pop?source=WIPAC', wipac_pop_claimant)
    assert not ret['transfer_request']
    assert get_prom(REQ_TOTAL, {"method": "POST", "route": "/TransferRequests/actions/pop"}) == 4.0
    assert get_prom(RESP_TOTAL, {"method": "POST", "response": "200", "route": "/TransferRequests/actions/pop"}) == 3.0


# -----------------------------------------------------------------------------
# 300s - Script main
# -----------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_300_script_main(mocker: MockerFixture) -> None:
    """Ensure that main sets up logging, starts a server, and runs the event loop."""
    mock_basicConfig = mocker.patch("logging.basicConfig")
    mock_start = mocker.patch("lta.rest_server.start")
    mock_start_http_server = mocker.patch("lta.rest_server.start_http_server")
    mock_Event = AsyncMock(spec=asyncio.Event)
    mock_asyncio_event = mocker.patch("asyncio.Event")
    mock_asyncio_event.return_value = mock_Event

    await main()

    mock_Event.wait.assert_called()
    mock_asyncio_event.assert_called()
    mock_start_http_server.assert_called()
    mock_start.assert_called()
    mock_basicConfig.assert_called()


# -----------------------------------------------------------------------------
# 400s - Bundles endpoints
# -----------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_400_bundles_bulk_crud(mongo: LtaCollection, rest: RestClientFactory) -> None:
    """Check CRUD semantics for bundles."""
    get_prom = prometheus_client.REGISTRY.get_sample_value  # alias here in case registry mutates

    r = rest('system')  # type: ignore[call-arg]

    #
    # Create - POST /Bundles/actions/bulk_create
    #
    request = {'bundles': [{"name": "one"}, {"name": "two"}]}
    # request: POST
    ret = await r.request('POST', '/Bundles/actions/bulk_create', request)
    assert len(ret["bundles"]) == 2
    assert ret["count"] == 2
    assert get_prom(REQ_TOTAL, {"method": "POST", "route": "/Bundles/actions/bulk_create"}) == 1.0
    assert get_prom(RESP_TOTAL, {"method": "POST", "response": "201", "route": "/Bundles/actions/bulk_create"}) == 1.0

    #
    # Read - GET /Bundles
    #
    # request: GET
    ret = await r.request('GET', '/Bundles')
    results = ret["results"]
    assert len(results) == 2
    assert get_prom(REQ_TOTAL, {"method": "GET", "route": "/Bundles"}) == 1.0
    assert get_prom(RESP_TOTAL, {"method": "GET", "response": "200", "route": "/Bundles"}) == 1.0

    #
    # Update - POST /Bundles/actions/bulk_update
    #
    results2 = results + [unique_id()]
    request2 = {'bundles': results2, 'update': {'key': 'value'}}
    # request: POST
    ret = await r.request('POST', '/Bundles/actions/bulk_update', request2)
    assert ret["count"] == 2
    assert ret["bundles"] == results
    assert get_prom(REQ_TOTAL, {"method": "POST", "route": "/Bundles/actions/bulk_update"}) == 1.0
    assert get_prom(RESP_TOTAL, {"method": "POST", "response": "200", "route": "/Bundles/actions/bulk_update"}) == 1.0

    #
    # Read - GET /Bundles/UUID
    #
    for idx, result in enumerate(results, start=1):
        # request: GET
        ret = await r.request('GET', f'/Bundles/{result}')
        assert ret["uuid"] == result
        assert ret["name"] in ["one", "two"]
        assert ret["key"] == "value"

        assert get_prom(REQ_TOTAL, {"method": "GET", "route": "/Bundles/{bundle_id}"}) == float(idx)
        assert get_prom(RESP_TOTAL, {"method": "GET", "response": "200", "route": "/Bundles/{bundle_id}"}) == float(idx)

    #
    # Delete - POST /Bundles/actions/bulk_delete
    #
    results2 = results + [unique_id()]
    request2 = {'bundles': results2}
    # request: POST
    ret = await r.request('POST', '/Bundles/actions/bulk_delete', request2)
    assert ret["count"] == 2
    assert ret["bundles"] == results
    assert get_prom(REQ_TOTAL, {"method": "POST", "route": "/Bundles/actions/bulk_delete"}) == 1.0
    assert get_prom(RESP_TOTAL, {"method": "POST", "response": "200", "route": "/Bundles/actions/bulk_delete"}) == 1.0

    #
    # Read - GET /Bundles
    #
    # request: GET
    ret = await r.request('GET', '/Bundles')
    results = ret["results"]
    assert len(results) == 0
    assert get_prom(REQ_TOTAL, {"method": "GET", "route": "/Bundles"}) == 2.0
    assert get_prom(RESP_TOTAL, {"method": "GET", "response": "200", "route": "/Bundles"}) == 2.0


@pytest.mark.asyncio
async def test_410_bundles_actions_bulk_create_errors(rest: RestClientFactory) -> None:
    """Check error conditions for bulk_create."""
    get_prom = prometheus_client.REGISTRY.get_sample_value  # alias here in case registry mutates

    r = rest('system')  # type: ignore[call-arg]

    # request: POST
    request: Dict[str, Any] = {}
    with pytest.raises(HTTPError, match=r"missing bundles field") as exc:
        await r.request('POST', '/Bundles/actions/bulk_create', request)
    assert exc.value.response.status_code == 400  # type: ignore[union-attr]
    assert get_prom(REQ_TOTAL, {"method": "POST", "route": "/Bundles/actions/bulk_create"}) == 1.0
    assert get_prom(RESP_TOTAL, {"method": "POST", "response": "400", "route": "/Bundles/actions/bulk_create"}) == 1.0

    # request: POST
    request = {'bundles': ''}
    with pytest.raises(HTTPError, match=r"bundles field is not a list") as exc:
        await r.request('POST', '/Bundles/actions/bulk_create', request)
    assert exc.value.response.status_code == 400  # type: ignore[union-attr]
    assert get_prom(REQ_TOTAL, {"method": "POST", "route": "/Bundles/actions/bulk_create"}) == 2.0
    assert get_prom(RESP_TOTAL, {"method": "POST", "response": "400", "route": "/Bundles/actions/bulk_create"}) == 2.0

    # request: POST
    request = {'bundles': []}
    with pytest.raises(HTTPError, match=r"bundles field is empty") as exc:
        await r.request('POST', '/Bundles/actions/bulk_create', request)
    assert exc.value.response.status_code == 400  # type: ignore[union-attr]
    assert get_prom(REQ_TOTAL, {"method": "POST", "route": "/Bundles/actions/bulk_create"}) == 3.0
    assert get_prom(RESP_TOTAL, {"method": "POST", "response": "400", "route": "/Bundles/actions/bulk_create"}) == 3.0


@pytest.mark.asyncio
async def test_420_bundles_actions_bulk_delete_errors(rest: RestClientFactory) -> None:
    """Check error conditions for bulk_delete."""
    get_prom = prometheus_client.REGISTRY.get_sample_value  # alias here in case registry mutates

    r = rest('system')  # type: ignore[call-arg]

    # request: POST
    request: Dict[str, Any] = {}
    with pytest.raises(HTTPError, match=r"missing bundles field") as exc:
        await r.request('POST', '/Bundles/actions/bulk_delete', request)
    assert exc.value.response.status_code == 400  # type: ignore[union-attr]
    assert get_prom(REQ_TOTAL, {"method": "POST", "route": "/Bundles/actions/bulk_delete"}) == 1.0
    assert get_prom(RESP_TOTAL, {"method": "POST", "response": "400", "route": "/Bundles/actions/bulk_delete"}) == 1.0

    # request: POST
    request = {'bundles': ''}
    with pytest.raises(HTTPError, match=r"bundles field is not a list") as exc:
        await r.request('POST', '/Bundles/actions/bulk_delete', request)
    assert exc.value.response.status_code == 400  # type: ignore[union-attr]
    assert get_prom(REQ_TOTAL, {"method": "POST", "route": "/Bundles/actions/bulk_delete"}) == 2.0
    assert get_prom(RESP_TOTAL, {"method": "POST", "response": "400", "route": "/Bundles/actions/bulk_delete"}) == 2.0

    # request: POST
    request = {'bundles': []}
    with pytest.raises(HTTPError, match=r"bundles field is empty") as exc:
        await r.request('POST', '/Bundles/actions/bulk_delete', request)
    assert exc.value.response.status_code == 400  # type: ignore[union-attr]
    assert get_prom(REQ_TOTAL, {"method": "POST", "route": "/Bundles/actions/bulk_delete"}) == 3.0
    assert get_prom(RESP_TOTAL, {"method": "POST", "response": "400", "route": "/Bundles/actions/bulk_delete"}) == 3.0


@pytest.mark.asyncio
async def test_430_bundles_actions_bulk_update_errors(rest: RestClientFactory) -> None:
    """Check error conditions for bulk_update."""
    get_prom = prometheus_client.REGISTRY.get_sample_value  # alias here in case registry mutates

    r = rest('system')  # type: ignore[call-arg]

    # request: POST
    request: Dict[str, Any] = {}
    with pytest.raises(HTTPError, match=r"missing update field") as exc:
        await r.request('POST', '/Bundles/actions/bulk_update', request)
    assert exc.value.response.status_code == 400  # type: ignore[union-attr]
    assert get_prom(REQ_TOTAL, {"method": "POST", "route": "/Bundles/actions/bulk_update"}) == 1.0
    assert get_prom(RESP_TOTAL, {"method": "POST", "response": "400", "route": "/Bundles/actions/bulk_update"}) == 1.0

    # request: POST
    request = {'update': ''}
    with pytest.raises(HTTPError, match=r"update field is not an object") as exc:
        await r.request('POST', '/Bundles/actions/bulk_update', request)
    assert exc.value.response.status_code == 400  # type: ignore[union-attr]
    assert get_prom(REQ_TOTAL, {"method": "POST", "route": "/Bundles/actions/bulk_update"}) == 2.0
    assert get_prom(RESP_TOTAL, {"method": "POST", "response": "400", "route": "/Bundles/actions/bulk_update"}) == 2.0

    # request: POST
    request = {'update': {}}
    with pytest.raises(HTTPError, match=r"missing bundles field") as exc:
        await r.request('POST', '/Bundles/actions/bulk_update', request)
    assert exc.value.response.status_code == 400  # type: ignore[union-attr]
    assert get_prom(REQ_TOTAL, {"method": "POST", "route": "/Bundles/actions/bulk_update"}) == 3.0
    assert get_prom(RESP_TOTAL, {"method": "POST", "response": "400", "route": "/Bundles/actions/bulk_update"}) == 3.0

    # request: POST
    request = {'update': {}, 'bundles': ''}
    with pytest.raises(HTTPError, match=r"bundles field is not a list") as exc:
        await r.request('POST', '/Bundles/actions/bulk_update', request)
    assert exc.value.response.status_code == 400  # type: ignore[union-attr]
    assert get_prom(REQ_TOTAL, {"method": "POST", "route": "/Bundles/actions/bulk_update"}) == 4.0
    assert get_prom(RESP_TOTAL, {"method": "POST", "response": "400", "route": "/Bundles/actions/bulk_update"}) == 4.0

    # request: POST
    request = {'update': {}, 'bundles': []}
    with pytest.raises(HTTPError, match=r"bundles field is empty") as exc:
        await r.request('POST', '/Bundles/actions/bulk_update', request)
    assert exc.value.response.status_code == 400  # type: ignore[union-attr]
    assert get_prom(REQ_TOTAL, {"method": "POST", "route": "/Bundles/actions/bulk_update"}) == 5.0
    assert get_prom(RESP_TOTAL, {"method": "POST", "response": "400", "route": "/Bundles/actions/bulk_update"}) == 5.0


@pytest.mark.asyncio
async def test_440_get_bundles_filter(mongo: LtaCollection, rest: RestClientFactory) -> None:
    """Check that GET /Bundles filters properly by query parameters.."""
    get_prom = prometheus_client.REGISTRY.get_sample_value  # alias here in case registry mutates

    r = rest('system')  # type: ignore[call-arg]

    test_data = {
        'bundles': [
            {
                "degenerate": "bundle",
                "has no": "decent keys",
                "or values": "should be deleted",
            },
            {
                "source": "WIPAC:/data/exp/IceCube/2014/15f7a399-fe40-4337-bb7e-d68d2d28ec8e.zip",
                "status": "waiting",
                "verified": False,
            },
            {
                "source": "WIPAC:/tmp/path1/sub1/48091a00-0c97-482f-a716-2e721b8e9662.zip",
                "status": "waiting",
                "verified": False,
            },
            {
                "source": "WIPAC:/tmp/path1/sub1/24814fa8-875b-4bae-b034-ea8885d2aafe.zip",
                "status": "processing",
                "verified": False,
            },
            {
                "source": "WIPAC:/tmp/path1/sub1/cef98a3b-9a24-4fbc-b4e7-ef251367c020.zip",
                "status": "bundled",
                "verified": False,
            },
            {
                "source": "WIPAC:/tmp/path1/sub2/8f141afa-db2f-4337-9a24-560c383887b5.zip",
                "status": "waiting",
                "verified": False,
            },
            {
                "source": "WIPAC:/tmp/path1/sub2/f9153da6-3588-416d-ba59-91526376dc43.zip",
                "status": "processing",
                "verified": True,
            },
            {
                "source": "WIPAC:/tmp/path1/sub2/34b064a7-5ffa-4cb2-9661-6e8b80765e9f.zip",
                "status": "bundled",
                "verified": True,
            },
            {
                "source": "DESY:/tmp/path1/sub2/76e3c5e2-e1a2-42f0-9d59-6546d1cb85e6.zip",
                "status": "waiting",
                "verified": True,
            },
            {
                "source": "DESY:/tmp/path1/sub2/3bcd05f5-ceb8-4eb5-a5db-5f7d55a98ff4.zip",
                "status": "processing",
                "verified": True,
            },
            {
                "source": "DESY:/tmp/path1/sub2/e596e1f7-ddaa-4255-abe3-81a4769bf192.zip",
                "status": "bundled",
                "verified": True,
            },
        ]
    }

    #
    # Create - POST /Bundles/actions/bulk_create
    #
    # request: POST
    ret = await r.request('POST', '/Bundles/actions/bulk_create', test_data)
    assert len(ret["bundles"]) == 11
    assert ret["count"] == 11
    assert get_prom(REQ_TOTAL, {"method": "POST", "route": "/Bundles/actions/bulk_create"}) == 1.0
    assert get_prom(RESP_TOTAL, {"method": "POST", "response": "201", "route": "/Bundles/actions/bulk_create"}) == 1.0

    #
    # Read - GET /Bundles (all variants are still route=/Bundles in metrics)
    #
    # request: GET
    ret = await r.request('GET', '/Bundles')
    results = ret["results"]
    assert len(results) == 11
    assert get_prom(REQ_TOTAL, {"method": "GET", "route": "/Bundles"}) == 1.0
    assert get_prom(RESP_TOTAL, {"method": "GET", "response": "200", "route": "/Bundles"}) == 1.0

    # request: GET
    ret = await r.request('GET', '/Bundles?location=WIPAC')
    results = ret["results"]
    assert len(results) == 7
    assert get_prom(REQ_TOTAL, {"method": "GET", "route": "/Bundles"}) == 2.0
    assert get_prom(RESP_TOTAL, {"method": "GET", "response": "200", "route": "/Bundles"}) == 2.0

    # request: GET
    ret = await r.request('GET', '/Bundles?location=DESY')
    results = ret["results"]
    assert len(results) == 3
    assert get_prom(REQ_TOTAL, {"method": "GET", "route": "/Bundles"}) == 3.0
    assert get_prom(RESP_TOTAL, {"method": "GET", "response": "200", "route": "/Bundles"}) == 3.0

    # request: GET
    ret = await r.request('GET', '/Bundles?location=WIPAC:/tmp/path1')
    results = ret["results"]
    assert len(results) == 6
    assert get_prom(REQ_TOTAL, {"method": "GET", "route": "/Bundles"}) == 4.0
    assert get_prom(RESP_TOTAL, {"method": "GET", "response": "200", "route": "/Bundles"}) == 4.0

    # request: GET
    ret = await r.request('GET', '/Bundles?status=waiting')
    results = ret["results"]
    assert len(results) == 4
    assert get_prom(REQ_TOTAL, {"method": "GET", "route": "/Bundles"}) == 5.0
    assert get_prom(RESP_TOTAL, {"method": "GET", "response": "200", "route": "/Bundles"}) == 5.0

    # request: GET
    ret = await r.request('GET', '/Bundles?status=processing')
    results = ret["results"]
    assert len(results) == 3
    assert get_prom(REQ_TOTAL, {"method": "GET", "route": "/Bundles"}) == 6.0
    assert get_prom(RESP_TOTAL, {"method": "GET", "response": "200", "route": "/Bundles"}) == 6.0

    # request: GET
    ret = await r.request('GET', '/Bundles?status=bundled')
    results = ret["results"]
    assert len(results) == 3
    assert get_prom(REQ_TOTAL, {"method": "GET", "route": "/Bundles"}) == 7.0
    assert get_prom(RESP_TOTAL, {"method": "GET", "response": "200", "route": "/Bundles"}) == 7.0

    # request: GET
    ret = await r.request('GET', '/Bundles?verified=true')
    results = ret["results"]
    assert len(results) == 5
    assert get_prom(REQ_TOTAL, {"method": "GET", "route": "/Bundles"}) == 8.0
    assert get_prom(RESP_TOTAL, {"method": "GET", "response": "200", "route": "/Bundles"}) == 8.0

    # request: GET
    ret = await r.request('GET', '/Bundles?verified=false')
    results = ret["results"]
    assert len(results) == 5
    assert get_prom(REQ_TOTAL, {"method": "GET", "route": "/Bundles"}) == 9.0
    assert get_prom(RESP_TOTAL, {"method": "GET", "response": "200", "route": "/Bundles"}) == 9.0

    # request: GET
    ret = await r.request('GET', '/Bundles?status=waiting&verified=false')
    results = ret["results"]
    assert len(results) == 3
    assert get_prom(REQ_TOTAL, {"method": "GET", "route": "/Bundles"}) == 10.0
    assert get_prom(RESP_TOTAL, {"method": "GET", "response": "200", "route": "/Bundles"}) == 10.0

    # request: GET
    ret = await r.request('GET', '/Bundles?status=waiting&verified=true')
    results = ret["results"]
    assert len(results) == 1
    assert get_prom(REQ_TOTAL, {"method": "GET", "route": "/Bundles"}) == 11.0
    assert get_prom(RESP_TOTAL, {"method": "GET", "response": "200", "route": "/Bundles"}) == 11.0


@pytest.mark.asyncio
async def test_450_get_bundles_request_filter(mongo: LtaCollection, rest: RestClientFactory) -> None:
    """Check that GET /Bundles filters properly by query parameter request."""
    get_prom = prometheus_client.REGISTRY.get_sample_value  # alias here in case registry mutates

    r = rest('system')  # type: ignore[call-arg]

    test_data = {
        'bundles': [
            {
                "request": "02faec90-9f42-4231-944e-237465c7b988",
                "source": "WIPAC:/data/exp/IceCube/2014/15f7a399-fe40-4337-bb7e-d68d2d28ec8e.zip",
                "status": "waiting",
                "verified": False,
            },
            {
                "request": "baebf071-702f-4ab5-9486-a9dec5420b84",
                "source": "WIPAC:/tmp/path1/sub1/48091a00-0c97-482f-a716-2e721b8e9662.zip",
                "status": "waiting",
                "verified": False,
            },
            {
                "request": "5aba93ec-3c7d-43d7-8fe9-c19e5bc25991",
                "source": "WIPAC:/tmp/path1/sub1/24814fa8-875b-4bae-b034-ea8885d2aafe.zip",
                "status": "processing",
                "verified": False,
            },
            {
                "request": "baebf071-702f-4ab5-9486-a9dec5420b84",
                "source": "WIPAC:/tmp/path1/sub1/cef98a3b-9a24-4fbc-b4e7-ef251367c020.zip",
                "status": "bundled",
                "verified": False,
            },
        ]
    }

    #
    # Create - POST /Bundles/actions/bulk_create
    #
    # request: POST
    ret = await r.request('POST', '/Bundles/actions/bulk_create', test_data)
    assert len(ret["bundles"]) == 4
    assert ret["count"] == 4
    assert get_prom(REQ_TOTAL, {"method": "POST", "route": "/Bundles/actions/bulk_create"}) == 1.0
    assert get_prom(RESP_TOTAL, {"method": "POST", "response": "201", "route": "/Bundles/actions/bulk_create"}) == 1.0

    #
    # Read - GET /Bundles (all variants are still route=/Bundles in metrics)
    #
    # request: GET
    ret = await r.request('GET', '/Bundles')
    results = ret["results"]
    assert len(results) == 4
    assert get_prom(REQ_TOTAL, {"method": "GET", "route": "/Bundles"}) == 1.0
    assert get_prom(RESP_TOTAL, {"method": "GET", "response": "200", "route": "/Bundles"}) == 1.0

    # request: GET
    ret = await r.request('GET', '/Bundles?request=dd162dad-9880-4ed7-b3c3-f8843d765ac3')
    results = ret["results"]
    assert len(results) == 0
    assert get_prom(REQ_TOTAL, {"method": "GET", "route": "/Bundles"}) == 2.0
    assert get_prom(RESP_TOTAL, {"method": "GET", "response": "200", "route": "/Bundles"}) == 2.0

    # request: GET
    ret = await r.request('GET', '/Bundles?request=5aba93ec-3c7d-43d7-8fe9-c19e5bc25991')
    results = ret["results"]
    assert len(results) == 1
    assert get_prom(REQ_TOTAL, {"method": "GET", "route": "/Bundles"}) == 3.0
    assert get_prom(RESP_TOTAL, {"method": "GET", "response": "200", "route": "/Bundles"}) == 3.0

    # request: GET
    ret = await r.request('GET', '/Bundles?request=baebf071-702f-4ab5-9486-a9dec5420b84')
    results = ret["results"]
    assert len(results) == 2
    assert get_prom(REQ_TOTAL, {"method": "GET", "route": "/Bundles"}) == 4.0
    assert get_prom(RESP_TOTAL, {"method": "GET", "response": "200", "route": "/Bundles"}) == 4.0


@pytest.mark.asyncio
async def test_460_get_bundles_uuid_error(rest: RestClientFactory) -> None:
    """Check that GET /Bundles/UUID returns 404 on not found."""
    get_prom = prometheus_client.REGISTRY.get_sample_value  # alias here in case registry mutates

    r = rest('system')  # type: ignore[call-arg]

    # request: GET
    with pytest.raises(HTTPError, match=r"not found") as exc:
        await r.request('GET', '/Bundles/d4390bcadac74f9dbb49874b444b448d')
    assert exc.value.response.status_code == 404  # type: ignore[union-attr]
    assert get_prom(REQ_TOTAL, {"method": "GET", "route": "/Bundles/{bundle_id}"}) == 1.0
    assert get_prom(RESP_TOTAL, {"method": "GET", "response": "404", "route": "/Bundles/{bundle_id}"}) == 1.0


@pytest.mark.asyncio
async def test_470_delete_bundles_uuid(mongo: LtaCollection, rest: RestClientFactory) -> None:
    """Check that DELETE /Bundles/UUID returns 204, exist or not exist."""
    get_prom = prometheus_client.REGISTRY.get_sample_value  # alias here in case registry mutates

    r = rest('system')  # type: ignore[call-arg]

    test_data = {
        'bundles': [
            {
                "source": "WIPAC:/data/exp/IceCube/2014/59aa1e05-84ba-4214-bdfa-a9f42117b3dd.zip",
                "status": "bundled",
                "verified": True,
            },
        ]
    }

    # request: POST
    ret = await r.request('POST', '/Bundles/actions/bulk_create', test_data)
    assert len(ret["bundles"]) == 1
    assert ret["count"] == 1
    assert get_prom(REQ_TOTAL, {"method": "POST", "route": "/Bundles/actions/bulk_create"}) == 1.0
    assert get_prom(RESP_TOTAL, {"method": "POST", "response": "201", "route": "/Bundles/actions/bulk_create"}) == 1.0

    # request: GET
    ret = await r.request('GET', '/Bundles')
    results = ret["results"]
    assert len(results) == 1
    assert get_prom(REQ_TOTAL, {"method": "GET", "route": "/Bundles"}) == 1.0
    assert get_prom(RESP_TOTAL, {"method": "GET", "response": "200", "route": "/Bundles"}) == 1.0

    test_uuid = results[0]

    # request: DELETE
    # we delete it when it exists
    ret = await r.request('DELETE', f'/Bundles/{test_uuid}')
    assert not ret
    assert get_prom(REQ_TOTAL, {"method": "DELETE", "route": "/Bundles/{bundle_id}"}) == 1.0
    assert get_prom(RESP_TOTAL, {"method": "DELETE", "response": "204", "route": "/Bundles/{bundle_id}"}) == 1.0

    # request: GET
    # we verify that it has been deleted
    ret = await r.request('GET', '/Bundles')
    results = ret["results"]
    assert len(results) == 0
    assert get_prom(REQ_TOTAL, {"method": "GET", "route": "/Bundles"}) == 2.0
    assert get_prom(RESP_TOTAL, {"method": "GET", "response": "200", "route": "/Bundles"}) == 2.0

    # request: DELETE
    # we try to delete it again!
    ret = await r.request('DELETE', f'/Bundles/{test_uuid}')
    assert not ret
    assert get_prom(REQ_TOTAL, {"method": "DELETE", "route": "/Bundles/{bundle_id}"}) == 2.0
    assert get_prom(RESP_TOTAL, {"method": "DELETE", "response": "204", "route": "/Bundles/{bundle_id}"}) == 2.0


@pytest.mark.asyncio
async def test_480_patch_bundles_uuid(mongo: LtaCollection, rest: RestClientFactory) -> None:
    """Check that PATCH /Bundles/UUID does the right thing, every time."""
    get_prom = prometheus_client.REGISTRY.get_sample_value  # alias here in case registry mutates

    r = rest('system')  # type: ignore[call-arg]

    test_data = {
        'bundles': [
            {
                "source": "WIPAC:/data/exp/IceCube/2014/59aa1e05-84ba-4214-bdfa-a9f42117b3dd.zip",
                "status": "bundled",
                "verified": True,
            },
        ]
    }

    # request: POST
    ret = await r.request('POST', '/Bundles/actions/bulk_create', test_data)
    assert len(ret["bundles"]) == 1
    assert ret["count"] == 1
    assert get_prom(REQ_TOTAL, {"method": "POST", "route": "/Bundles/actions/bulk_create"}) == 1.0
    assert get_prom(RESP_TOTAL, {"method": "POST", "response": "201", "route": "/Bundles/actions/bulk_create"}) == 1.0

    # request: GET
    ret = await r.request('GET', '/Bundles')
    results = ret["results"]
    assert len(results) == 1
    assert get_prom(REQ_TOTAL, {"method": "GET", "route": "/Bundles"}) == 1.0
    assert get_prom(RESP_TOTAL, {"method": "GET", "response": "200", "route": "/Bundles"}) == 1.0

    test_uuid = results[0]

    # request: PATCH
    # we patch it when it exists
    request = {"key": "value"}
    ret = await r.request('PATCH', f'/Bundles/{test_uuid}', request)
    assert ret["key"] == "value"
    assert get_prom(REQ_TOTAL, {"method": "PATCH", "route": "/Bundles/{bundle_id}"}) == 1.0
    assert get_prom(RESP_TOTAL, {"method": "PATCH", "response": "200", "route": "/Bundles/{bundle_id}"}) == 1.0

    # request: PATCH
    # we try to patch the uuid; error
    request = {"key": "value", "uuid": "d4390bca-dac7-4f9d-bb49-874b444b448d"}
    with pytest.raises(HTTPError, match=r"bad request") as exc:
        await r.request('PATCH', f'/Bundles/{test_uuid}', request)
    assert exc.value.response.status_code == 400  # type: ignore[union-attr]
    assert get_prom(REQ_TOTAL, {"method": "PATCH", "route": "/Bundles/{bundle_id}"}) == 2.0
    assert get_prom(RESP_TOTAL, {"method": "PATCH", "response": "400", "route": "/Bundles/{bundle_id}"}) == 1.0

    # request: PATCH
    # we try to patch something that doesn't exist; error
    request = {"key": "value"}
    with pytest.raises(HTTPError, match=r"not found") as exc:
        await r.request('PATCH', '/Bundles/048c812c780648de8f39a2422e2dcdb0', request)
    assert exc.value.response.status_code == 404  # type: ignore[union-attr]
    assert get_prom(REQ_TOTAL, {"method": "PATCH", "route": "/Bundles/{bundle_id}"}) == 3.0
    assert get_prom(RESP_TOTAL, {"method": "PATCH", "response": "404", "route": "/Bundles/{bundle_id}"}) == 1.0


@pytest.mark.asyncio
async def test_490_bundles_actions_pop(mongo: LtaCollection, rest: RestClientFactory) -> None:
    """Check pop action for bundles."""
    get_prom = prometheus_client.REGISTRY.get_sample_value  # alias here in case registry mutates

    r = rest('system')  # type: ignore[call-arg]

    test_data = {
        'bundles': [
            {
                "source": "WIPAC",
                "path": "/data/exp/IceCube/2014/15f7a399-fe40-4337-bb7e-d68d2d28ec8e.zip",
                "status": "accessible",
                "verified": False,
            },
            {
                "source": "WIPAC",
                "path": "/tmp/path1/sub1/48091a00-0c97-482f-a716-2e721b8e9662.zip",
                "status": "deletable",
                "verified": False,
            },
            {
                "source": "WIPAC",
                "path": "/tmp/path1/sub1/24814fa8-875b-4bae-b034-ea8885d2aafe.zip",
                "status": "inaccessible",
                "verified": False,
            },
            {
                "source": "WIPAC",
                "path": "/tmp/path1/sub1/cef98a3b-9a24-4fbc-b4e7-ef251367c020.zip",
                "status": "none",
                "verified": False,
            },
            {
                "source": "WIPAC",
                "path": "/tmp/path1/sub2/8f141afa-db2f-4337-9a24-560c383887b5.zip",
                "status": "transferring",
                "verified": False,
            },
            {
                "source": "WIPAC",
                "path": "/tmp/path1/sub2/f9153da6-3588-416d-ba59-91526376dc43.zip",
                "status": "accessible",
                "verified": True,
            },
            {
                "source": "WIPAC",
                "path": "/tmp/path1/sub2/34b064a7-5ffa-4cb2-9661-6e8b80765e9f.zip",
                "status": "deletable",
                "verified": True,
            },
            {
                "source": "DESY",
                "path": "/tmp/path1/sub2/76e3c5e2-e1a2-42f0-9d59-6546d1cb85e6.zip",
                "status": "inaccessible",
                "verified": True,
            },
            {
                "source": "DESY",
                "path": "/tmp/path1/sub2/3bcd05f5-ceb8-4eb5-a5db-5f7d55a98ff4.zip",
                "status": "none",
                "verified": True,
            },
            {
                "source": "DESY",
                "path": "/tmp/path1/sub2/e596e1f7-ddaa-4255-abe3-81a4769bf192.zip",
                "status": "transferring",
                "verified": True,
            },
        ]
    }

    #
    # Create - POST /Bundles/actions/bulk_create
    #
    # request: POST
    ret = await r.request('POST', '/Bundles/actions/bulk_create', test_data)
    assert len(ret["bundles"]) == 10
    assert ret["count"] == 10
    assert get_prom(REQ_TOTAL, {"method": "POST", "route": "/Bundles/actions/bulk_create"}) == 1.0
    assert get_prom(RESP_TOTAL, {"method": "POST", "response": "201", "route": "/Bundles/actions/bulk_create"}) == 1.0

    # I'm at NERSC, and should have no work
    claimant_body = {
        'claimant': 'testing-picker-aaaed864-0112-4bcf-a069-bb55c12e291d',
    }
    # request: POST
    ret = await r.request('POST', '/Bundles/actions/pop?source=NERSC&status=inaccessible', claimant_body)
    assert not ret['bundle']
    assert get_prom(REQ_TOTAL, {"method": "POST", "route": "/Bundles/actions/pop"}) == 1.0
    assert get_prom(RESP_TOTAL, {"method": "POST", "response": "200", "route": "/Bundles/actions/pop"}) == 1.0

    # request: POST
    # I'm the bundler at WIPAC, and should pop one work item
    ret = await r.request('POST', '/Bundles/actions/pop?source=WIPAC&status=inaccessible', claimant_body)
    assert ret['bundle']
    assert ret['bundle']["path"] == "/tmp/path1/sub1/24814fa8-875b-4bae-b034-ea8885d2aafe.zip"
    assert get_prom(REQ_TOTAL, {"method": "POST", "route": "/Bundles/actions/pop"}) == 2.0
    assert get_prom(RESP_TOTAL, {"method": "POST", "response": "200", "route": "/Bundles/actions/pop"}) == 2.0

    # request: POST
    # repeating gets no work
    ret = await r.request('POST', '/Bundles/actions/pop?source=WIPAC&status=inaccessible', claimant_body)
    assert not ret['bundle']
    assert get_prom(REQ_TOTAL, {"method": "POST", "route": "/Bundles/actions/pop"}) == 3.0
    assert get_prom(RESP_TOTAL, {"method": "POST", "response": "200", "route": "/Bundles/actions/pop"}) == 3.0

    # request: POST
    # I'm the bundler at WIPAC, and should pop one work item
    ret = await r.request('POST', '/Bundles/actions/pop?source=WIPAC&status=accessible', claimant_body)
    assert ret['bundle']
    assert ret['bundle']["path"] == "/data/exp/IceCube/2014/15f7a399-fe40-4337-bb7e-d68d2d28ec8e.zip"
    assert get_prom(REQ_TOTAL, {"method": "POST", "route": "/Bundles/actions/pop"}) == 4.0
    assert get_prom(RESP_TOTAL, {"method": "POST", "response": "200", "route": "/Bundles/actions/pop"}) == 4.0


# -----------------------------------------------------------------------------
# 500s - Bundles actions error cases and variants
# -----------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_500_bundles_actions_pop_errors(mongo: LtaCollection, rest: RestClientFactory) -> None:
    """Check error handlers for pop action for bundles."""
    get_prom = prometheus_client.REGISTRY.get_sample_value  # alias here in case registry mutates

    r = rest('system')  # type: ignore[call-arg]

    # request: POST
    # Missing required query arg: status (raised before handler logic)
    with pytest.raises(HTTPError, match=r"Bad Request for url") as exc:
        await r.request('POST', '/Bundles/actions/pop?source=WIPAC', {"claimant": "x"})
    assert exc.value.response.status_code == 400  # type: ignore[union-attr]
    assert get_prom(REQ_TOTAL, {"method": "POST", "route": "/Bundles/actions/pop"}) == 1.0
    # NOTE: no response metric

    # request: POST
    # Missing both dest and source (but status is present, so we reach handler logic)
    with pytest.raises(HTTPError, match=r"missing source and dest fields") as exc:
        await r.request('POST', '/Bundles/actions/pop?status=taping', {"claimant": "x"})
    assert exc.value.response.status_code == 400  # type: ignore[union-attr]
    assert get_prom(REQ_TOTAL, {"method": "POST", "route": "/Bundles/actions/pop"}) == 2.0
    await asyncio.sleep(1.0)  # wait so on_finish() can be called post-response (where label is inc'd)
    assert get_prom(RESP_TOTAL, {"method": "POST", "response": "400", "route": "/Bundles/actions/pop"}) == 1.0

    # request: POST
    # Missing claimant (but other required pieces present)
    with pytest.raises(HTTPError, match=r"missing claimant field") as exc:
        await r.request('POST', '/Bundles/actions/pop?source=WIPAC&status=inaccessible', {})
    assert exc.value.response.status_code == 400  # type: ignore[union-attr]
    assert get_prom(REQ_TOTAL, {"method": "POST", "route": "/Bundles/actions/pop"}) == 3.0
    assert get_prom(RESP_TOTAL, {"method": "POST", "response": "400", "route": "/Bundles/actions/pop"}) == 2.0


@pytest.mark.asyncio
async def test_510_bundles_actions_pop_at_destination(mongo: LtaCollection, rest: RestClientFactory) -> None:
    """Check pop action for bundles at destination."""
    get_prom = prometheus_client.REGISTRY.get_sample_value  # alias here in case registry mutates

    r = rest('system')  # type: ignore[call-arg]

    test_data = {
        'bundles': [
            {
                "source": "WIPAC",
                "dest": "NERSC",
                "path": "/data/exp/IceCube/2014/15f7a399-fe40-4337-bb7e-d68d2d28ec8e.zip",
                "status": "taping",
                "verified": False,
            },
        ]
    }

    #
    # Create - POST /Bundles/actions/bulk_create
    #
    # request: POST
    ret = await r.request('POST', '/Bundles/actions/bulk_create', test_data)
    assert len(ret["bundles"]) == 1
    assert ret["count"] == 1
    assert get_prom(REQ_TOTAL, {"method": "POST", "route": "/Bundles/actions/bulk_create"}) == 1.0
    assert get_prom(RESP_TOTAL, {"method": "POST", "response": "201", "route": "/Bundles/actions/bulk_create"}) == 1.0

    # I'm at destination NERSC, and should have work
    claimant_body = {
        'claimant': 'testing-nersc_mover-aaaed864-0112-4bcf-a069-bb55c12e291d',
    }
    # request: POST
    ret = await r.request('POST', '/Bundles/actions/pop?dest=NERSC&status=taping', claimant_body)
    assert ret['bundle']
    assert ret['bundle']["path"] == "/data/exp/IceCube/2014/15f7a399-fe40-4337-bb7e-d68d2d28ec8e.zip"
    assert get_prom(REQ_TOTAL, {"method": "POST", "route": "/Bundles/actions/pop"}) == 1.0
    assert get_prom(RESP_TOTAL, {"method": "POST", "response": "200", "route": "/Bundles/actions/pop"}) == 1.0


@pytest.mark.asyncio
async def test_520_bundles_actions_bulk_create_huge(mongo: LtaCollection, rest: RestClientFactory) -> None:
    """Check pop action for bundles at destination."""
    get_prom = prometheus_client.REGISTRY.get_sample_value  # alias here in case registry mutates

    NUM_FILES_TO_MAKE_IT_HUGE = 16000  # 16000 file entries ~= 12 MB body data

    r = rest(role='system', timeout=10.0)  # type: ignore[call-arg]

    test_data: Dict[str, List[Dict[str, Any]]] = {
        'bundles': [
            {
                "type": "Bundle",
                "status": "specified",
                "request": "55d332222f0311eaa8e78e6b006590ea",
                "source": "WIPAC",
                "dest": "NERSC",
                "path": "/data/exp/IceCube/2018/internal-system/pDAQ-2ndBld/0802",
                "files": []
            },
        ]
    }

    file_spec = {
        "_links": {
            "parent": {
                "href": "/api/files"
            },
            "self": {
                "href": "/api/files/e73aab54-2ead-11ea-a750-f6a52f4853dd"
            }
        },
        "checksum": {
            "sha512": "d8c7ce8c2016816b0c8f543f6b36d4036abee926ccead806fd982b5fff21488f5356b73dcc381093717c629b35e81c0e52e3205abdbb905b8322e8f6919e4987"
        },
        "file_size": 203400520,
        "locations": [
            {
                "site": "WIPAC",
                "path": "/data/exp/IceCube/2018/internal-system/pDAQ-2ndBld/0802/ukey_21a8501c-359a-41b4-95ce-c133e06d39ae_SPS-pDAQ-2ndBld-000_20180802_053901_000000.tar.gz"
            }
        ],
        "logical_name": "/data/exp/IceCube/2018/internal-system/pDAQ-2ndBld/0802/ukey_21a8501c-359a-41b4-95ce-c133e06d39ae_SPS-pDAQ-2ndBld-000_20180802_053901_000000.tar.gz",
        "meta_modify_date": "2020-01-04 04:51:43.182447",
        "uuid": "e73aab54-2ead-11ea-a750-f6a52f4853dd"
    }

    for i in range(1, NUM_FILES_TO_MAKE_IT_HUGE):
        test_data["bundles"][0]["files"].append(file_spec.copy())

    #
    # Create - POST /Bundles/actions/bulk_create
    #
    # request: POST
    ret = await r.request('POST', '/Bundles/actions/bulk_create', test_data)
    assert len(ret["bundles"]) == 1
    assert ret["count"] == 1
    assert get_prom(REQ_TOTAL, {"method": "POST", "route": "/Bundles/actions/bulk_create"}) == 1.0
    assert get_prom(RESP_TOTAL, {"method": "POST", "response": "201", "route": "/Bundles/actions/bulk_create"}) == 1.0


# -----------------------------------------------------------------------------
# 600s - Metadata endpoints
# -----------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_600_metadata_delete_bundle_uuid(mongo: LtaCollection, rest: RestClientFactory) -> None:
    """Check CRUD semantics for metadata."""
    get_prom = prometheus_client.REGISTRY.get_sample_value  # alias here in case registry mutates

    r = rest('system')  # type: ignore[call-arg]
    bundle_uuid0 = "291afc8d-2a04-4d85-8669-dc8e2c2ab406"
    bundle_uuid1 = "05b7178b-82d0-428c-a0a6-d4add696de62"
    #
    # Create - POST /Metadata/actions/bulk_create
    #
    # request: POST
    request = {
        'bundle_uuid': bundle_uuid0,
        'files': ["7b5c1f76-e568-4ae7-94d2-5a31d1d2b081", "125d2a44-a664-4166-bf4a-5d5cf13292d7", "3a92d3d2-2e3e-4184-8d3a-25fb4337fd2f"]
    }
    ret = await r.request('POST', '/Metadata/actions/bulk_create', request)
    assert len(ret["metadata"]) == 3
    assert ret["count"] == 3
    assert get_prom(REQ_TOTAL, {"method": "POST", "route": "/Metadata/actions/bulk_create"}) == 1.0
    assert get_prom(RESP_TOTAL, {"method": "POST", "response": "201", "route": "/Metadata/actions/bulk_create"}) == 1.0

    # request: POST
    request = {
        'bundle_uuid': bundle_uuid1,
        'files': ["03ccb63e-32cf-4135-85b2-fd06b8c9137f", "c65f2c58-a412-403c-9354-d25d7ae5cdeb", "864f0903-f207-478d-bac2-b437ebc07226"]
    }
    ret = await r.request('POST', '/Metadata/actions/bulk_create', request)
    assert len(ret["metadata"]) == 3
    assert ret["count"] == 3
    assert get_prom(REQ_TOTAL, {"method": "POST", "route": "/Metadata/actions/bulk_create"}) == 2.0
    assert get_prom(RESP_TOTAL, {"method": "POST", "response": "201", "route": "/Metadata/actions/bulk_create"}) == 2.0

    #
    # Read - GET /Metadata
    #
    # request: GET
    ret = await r.request('GET', f'/Metadata?bundle_uuid={bundle_uuid0}')
    results = ret["results"]
    assert len(results) == 3
    assert get_prom(REQ_TOTAL, {"method": "GET", "route": "/Metadata"}) == 1.0
    assert get_prom(RESP_TOTAL, {"method": "GET", "response": "200", "route": "/Metadata"}) == 1.0

    # request: GET
    ret = await r.request('GET', f'/Metadata?bundle_uuid={bundle_uuid1}')
    results = ret["results"]
    assert len(results) == 3
    assert get_prom(REQ_TOTAL, {"method": "GET", "route": "/Metadata"}) == 2.0
    assert get_prom(RESP_TOTAL, {"method": "GET", "response": "200", "route": "/Metadata"}) == 2.0

    #
    # Delete - DELETE /Metadata?bundle_uuid={uuid}
    #
    # request: DELETE
    ret = await r.request('DELETE', f'/Metadata?bundle_uuid={bundle_uuid0}')
    assert not ret
    assert get_prom(REQ_TOTAL, {"method": "DELETE", "route": "/Metadata?bundle_uuid={uuid}"}) == 1.0
    assert get_prom(RESP_TOTAL, {"method": "DELETE", "response": "204", "route": "/Metadata?bundle_uuid={uuid}"}) == 1.0

    #
    # Read - GET /Metadata
    #
    # request: GET
    ret = await r.request('GET', f'/Metadata?bundle_uuid={bundle_uuid0}')
    results = ret["results"]
    assert len(results) == 0
    assert get_prom(REQ_TOTAL, {"method": "GET", "route": "/Metadata"}) == 3.0
    assert get_prom(RESP_TOTAL, {"method": "GET", "response": "200", "route": "/Metadata"}) == 3.0

    # request: GET
    ret = await r.request('GET', f'/Metadata?bundle_uuid={bundle_uuid1}')
    results = ret["results"]
    assert len(results) == 3
    assert get_prom(REQ_TOTAL, {"method": "GET", "route": "/Metadata"}) == 4.0
    assert get_prom(RESP_TOTAL, {"method": "GET", "response": "200", "route": "/Metadata"}) == 4.0


@pytest.mark.asyncio
async def test_610_metadata_single_record(mongo: LtaCollection, rest: RestClientFactory) -> None:
    """Check CRUD semantics for metadata."""
    get_prom = prometheus_client.REGISTRY.get_sample_value  # alias here in case registry mutates

    r = rest('system')  # type: ignore[call-arg]
    bundle_uuid = "291afc8d-2a04-4d85-8669-dc8e2c2ab406"
    #
    # Create - POST /Metadata/actions/bulk_create
    #
    # request: POST
    request = {
        'bundle_uuid': bundle_uuid,
        'files': ["7b5c1f76-e568-4ae7-94d2-5a31d1d2b081", "125d2a44-a664-4166-bf4a-5d5cf13292d7", "3a92d3d2-2e3e-4184-8d3a-25fb4337fd2f"]
    }
    ret = await r.request('POST', '/Metadata/actions/bulk_create', request)
    assert len(ret["metadata"]) == 3
    assert ret["count"] == 3
    assert get_prom(REQ_TOTAL, {"method": "POST", "route": "/Metadata/actions/bulk_create"}) == 1.0
    assert get_prom(RESP_TOTAL, {"method": "POST", "response": "201", "route": "/Metadata/actions/bulk_create"}) == 1.0

    #
    # Read - GET /Metadata/{uuid}
    #
    metadata_uuid = ret["metadata"][0]
    # request: GET
    ret2 = await r.request('GET', f'/Metadata/{metadata_uuid}')
    assert ret2["uuid"] == metadata_uuid
    assert ret2["bundle_uuid"] == bundle_uuid
    assert ret2["file_catalog_uuid"] == "7b5c1f76-e568-4ae7-94d2-5a31d1d2b081"
    assert get_prom(REQ_TOTAL, {"method": "GET", "route": "/Metadata/{metadata_id}"}) == 1.0
    assert get_prom(RESP_TOTAL, {"method": "GET", "response": "200", "route": "/Metadata/{metadata_id}"}) == 1.0

    #
    # Delete - DELETE /Metadata/{uuid}
    #
    # request: DELETE
    ret3 = await r.request('DELETE', f'/Metadata/{metadata_uuid}')
    assert not ret3
    assert get_prom(REQ_TOTAL, {"method": "DELETE", "route": "/Metadata/{metadata_id}"}) == 1.0
    assert get_prom(RESP_TOTAL, {"method": "DELETE", "response": "204", "route": "/Metadata/{metadata_id}"}) == 1.0

    #
    # Read - GET /Metadata/{uuid}
    #
    # request: GET
    with pytest.raises(HTTPError, match=r"not found") as exc:
        await r.request('GET', f'/Metadata/{metadata_uuid}')
    assert exc.value.response.status_code == 404  # type: ignore[union-attr]
    assert get_prom(REQ_TOTAL, {"method": "GET", "route": "/Metadata/{metadata_id}"}) == 2.0
    assert get_prom(RESP_TOTAL, {"method": "GET", "response": "404", "route": "/Metadata/{metadata_id}"}) == 1.0


@pytest.mark.asyncio
async def test_620_metadata_bulk_crud(mongo: LtaCollection, rest: RestClientFactory) -> None:
    """Check CRUD semantics for metadata."""
    get_prom = prometheus_client.REGISTRY.get_sample_value  # alias here in case registry mutates

    r = rest('system')  # type: ignore[call-arg]
    bundle_uuid = "291afc8d-2a04-4d85-8669-dc8e2c2ab406"
    #
    # Create - POST /Metadata/actions/bulk_create
    #
    # request: POST
    request = {
        'bundle_uuid': bundle_uuid,
        'files': ["7b5c1f76-e568-4ae7-94d2-5a31d1d2b081", "125d2a44-a664-4166-bf4a-5d5cf13292d7", "3a92d3d2-2e3e-4184-8d3a-25fb4337fd2f"]
    }
    ret = await r.request('POST', '/Metadata/actions/bulk_create', request)
    assert len(ret["metadata"]) == 3
    assert ret["count"] == 3
    assert get_prom(REQ_TOTAL, {"method": "POST", "route": "/Metadata/actions/bulk_create"}) == 1.0
    assert get_prom(RESP_TOTAL, {"method": "POST", "response": "201", "route": "/Metadata/actions/bulk_create"}) == 1.0

    #
    # Read - GET /Metadata
    #
    # request: GET
    ret = await r.request('GET', f'/Metadata?bundle_uuid={bundle_uuid}')
    results = ret["results"]
    assert len(results) == 3
    assert get_prom(REQ_TOTAL, {"method": "GET", "route": "/Metadata"}) == 1.0
    assert get_prom(RESP_TOTAL, {"method": "GET", "response": "200", "route": "/Metadata"}) == 1.0

    #
    # Delete - POST /Metadata/actions/bulk_delete
    #
    uuids = [unique_id()]
    for result in results:
        uuids.append(result["uuid"])
    request2 = {'metadata': uuids}
    # request: POST
    ret = await r.request('POST', '/Metadata/actions/bulk_delete', request2)
    assert ret["count"] == 3
    assert ret["metadata"] == uuids
    assert get_prom(REQ_TOTAL, {"method": "POST", "route": "/Metadata/actions/bulk_delete"}) == 1.0
    assert get_prom(RESP_TOTAL, {"method": "POST", "response": "200", "route": "/Metadata/actions/bulk_delete"}) == 1.0

    #
    # Read - GET /Metadata
    #
    # request: GET
    ret = await r.request('GET', '/Metadata')
    results = ret["results"]
    assert len(results) == 0
    assert get_prom(REQ_TOTAL, {"method": "GET", "route": "/Metadata"}) == 2.0
    assert get_prom(RESP_TOTAL, {"method": "GET", "response": "200", "route": "/Metadata"}) == 2.0


@pytest.mark.asyncio
async def test_630_metadata_actions_bulk_create_errors(rest: RestClientFactory) -> None:
    """Check error conditions for bulk_create."""
    get_prom = prometheus_client.REGISTRY.get_sample_value  # alias here in case registry mutates

    r = rest('system')  # type: ignore[call-arg]

    # request: POST
    request: Dict[str, Any] = {}
    with pytest.raises(HTTPError, match=r"bundle_uuid") as exc:
        await r.request('POST', '/Metadata/actions/bulk_create', request)
    assert exc.value.response.status_code == 400  # type: ignore[union-attr]
    # NOTE: response_counter is not incremented on these early validation errors in server code
    assert get_prom(REQ_TOTAL, {"method": "POST", "route": "/Metadata/actions/bulk_create"}) == 1.0

    # request: POST
    request = {'bundle_uuid': '', 'files': ["foo"]}
    with pytest.raises(HTTPError, match=r"bundle_uuid must not be empty") as exc:
        await r.request('POST', '/Metadata/actions/bulk_create', request)
    assert exc.value.response.status_code == 400  # type: ignore[union-attr]
    assert get_prom(REQ_TOTAL, {"method": "POST", "route": "/Metadata/actions/bulk_create"}) == 2.0

    # request: POST
    request = {'bundle_uuid': "992ae5e1-017c-4a95-b552-bd385020ec27"}
    with pytest.raises(HTTPError, match=r"files") as exc:
        await r.request('POST', '/Metadata/actions/bulk_create', request)
    assert exc.value.response.status_code == 400  # type: ignore[union-attr]
    assert get_prom(REQ_TOTAL, {"method": "POST", "route": "/Metadata/actions/bulk_create"}) == 3.0

    # request: POST
    request = {'bundle_uuid': "992ae5e1-017c-4a95-b552-bd385020ec27", "files": []}
    with pytest.raises(HTTPError, match=r"files must not be empty") as exc:
        await r.request('POST', '/Metadata/actions/bulk_create', request)
    assert exc.value.response.status_code == 400  # type: ignore[union-attr]
    assert get_prom(REQ_TOTAL, {"method": "POST", "route": "/Metadata/actions/bulk_create"}) == 4.0


@pytest.mark.asyncio
async def test_640_metadata_actions_bulk_delete_errors(rest: RestClientFactory) -> None:
    """Check error conditions for bulk_delete."""
    get_prom = prometheus_client.REGISTRY.get_sample_value  # alias here in case registry mutates

    r = rest('system')  # type: ignore[call-arg]

    # request: POST
    request: Dict[str, Any] = {}
    with pytest.raises(HTTPError, match=r"metadata") as exc:
        await r.request('POST', '/Metadata/actions/bulk_delete', request)
    assert exc.value.response.status_code == 400  # type: ignore[union-attr]
    assert get_prom(REQ_TOTAL, {"method": "POST", "route": "/Metadata/actions/bulk_delete"}) == 1.0

    # request: POST
    request = {'metadata': ''}
    with pytest.raises(HTTPError, match=r"metadata") as exc:
        await r.request('POST', '/Metadata/actions/bulk_delete', request)
    assert exc.value.response.status_code == 400  # type: ignore[union-attr]
    assert get_prom(REQ_TOTAL, {"method": "POST", "route": "/Metadata/actions/bulk_delete"}) == 2.0

    # request: POST
    request = {'metadata': []}
    with pytest.raises(HTTPError, match=r"metadata must not be empty") as exc:
        await r.request('POST', '/Metadata/actions/bulk_delete', request)
    assert exc.value.response.status_code == 400  # type: ignore[union-attr]
    assert get_prom(REQ_TOTAL, {"method": "POST", "route": "/Metadata/actions/bulk_delete"}) == 3.0


@pytest.mark.asyncio
async def test_650_metadata_delete_errors(rest: RestClientFactory) -> None:
    """Check error conditions for DELETE /Metadata."""
    get_prom = prometheus_client.REGISTRY.get_sample_value  # alias here in case registry mutates

    r = rest('system')  # type: ignore[call-arg]

    # request: DELETE
    with pytest.raises(HTTPError, match=r"bundle_uuid") as exc:
        await r.request('DELETE', '/Metadata')
    assert exc.value.response.status_code == 400  # type: ignore[union-attr]

    # NOTE: response_counter is not incremented on this error path in server code
    assert get_prom(REQ_TOTAL, {"method": "DELETE", "route": "/Metadata?bundle_uuid={uuid}"}) == 1.0


@pytest.mark.asyncio
async def test_660_metadata_results_comprehension(rest: RestClientFactory) -> None:
    """Check that our comprehension works."""
    get_prom = prometheus_client.REGISTRY.get_sample_value  # alias here in case registry mutates

    r = rest('system')  # type: ignore[call-arg]
    bundle_uuid = "291afc8d-2a04-4d85-8669-dc8e2c2ab406"
    #
    # Create - POST /Metadata/actions/bulk_create
    #
    # request: POST
    request = {
        'bundle_uuid': bundle_uuid,
        'files': ["7b5c1f76-e568-4ae7-94d2-5a31d1d2b081", "125d2a44-a664-4166-bf4a-5d5cf13292d7", "3a92d3d2-2e3e-4184-8d3a-25fb4337fd2f"]
    }
    ret = await r.request('POST', '/Metadata/actions/bulk_create', request)
    assert len(ret["metadata"]) == 3
    assert ret["count"] == 3
    assert get_prom(REQ_TOTAL, {"method": "POST", "route": "/Metadata/actions/bulk_create"}) == 1.0
    assert get_prom(RESP_TOTAL, {"method": "POST", "response": "201", "route": "/Metadata/actions/bulk_create"}) == 1.0

    #
    # Read - GET /Metadata
    #
    # request: GET
    ret = await r.request('GET', f'/Metadata?bundle_uuid={bundle_uuid}')
    results = ret["results"]
    assert len(results) == 3
    assert get_prom(REQ_TOTAL, {"method": "GET", "route": "/Metadata"}) == 1.0
    assert get_prom(RESP_TOTAL, {"method": "GET", "response": "200", "route": "/Metadata"}) == 1.0

    #
    # Obtain the Metadata UUIDs with a comprehension
    #
    uuids = [x['uuid'] for x in results]
    assert len(uuids) == 3
    count = 0
    for result in results:
        assert uuids[count] == result['uuid']
        count = count + 1
