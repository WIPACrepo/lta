# test_transfer_sync.py
"""Unit tests for lta/transfer/sync.py."""

import asyncio
from asyncio import Task
import os
from pathlib import Path
from tempfile import NamedTemporaryFile
from unittest.mock import AsyncMock, MagicMock

import pycurl
import pytest
from pytest_mock import MockerFixture
from tornado.httpclient import HTTPError

from lta.transfer.sync import (
    _as_task,
    bind_setup_curl,
    _decode_if_necessary,
    convert_checksum_from_dcache,
    sha512sum,
    connection_semaphore,
    DirObject,
    ParallelAsync,
    Sync,
)
from .test_util import ObjectLiteral

TestConfig = dict[str, str]


@pytest.fixture
def config() -> TestConfig:
    """Supply a stock DesyMirrorReplicator component configuration."""
    return {
        "CI_TEST": "TRUE",
        "CLIENT_ID": "long-term-archive",
        "CLIENT_SECRET": "hunter2",  # http://bash.org/?244321
        "COMPONENT_NAME": "testing-desy-mirror-replicator",
        "DEST_BASE_PATH": "/some/root/at/desy/icecube/archive",
        "DEST_SITE": "DESY",
        "DEST_URL": "https://localhost:12880/",
        "INPUT_PATH": "/path/to/bundler_todesy",
        "INPUT_STATUS": "staged",
        "LOG_LEVEL": "DEBUG",
        "LTA_AUTH_OPENID_URL": "localhost:12345",
        "LTA_REST_URL": "localhost:12347",
        "MAX_PARALLEL": "100",
        "OUTPUT_STATUS": "transferring",
        "PROMETHEUS_METRICS_PORT": "8080",
        "RUN_ONCE_AND_DIE": "FALSE",
        "RUN_UNTIL_NO_WORK": "FALSE",
        "SOURCE_SITE": "WIPAC",
        "WIPACTEL_EXPORT_STDOUT": "FALSE",
        "WORK_RETRIES": "3",
        "WORK_SLEEP_DURATION_SECONDS": "60",
        "WORK_TIMEOUT_SECONDS": "30",
    }


def test_always_succeed() -> None:
    """Canary test to verify test framework is operating properly."""
    assert True


def test_bind_setup_curl() -> None:
    """Test that we get a setup_curl function bound to a configuration."""
    setup_curl1 = bind_setup_curl({
        "LOG_LEVEL": "NOT_DEBUG"
    })
    curl_mock = MagicMock()
    setup_curl1(curl_mock)
    curl_mock.setopt.assert_called_with(pycurl.CAPATH, '/etc/grid-security/certificates')

    setup_curl2 = bind_setup_curl({
        "LOG_LEVEL": "DEBUG"
    })
    curl_mock = MagicMock()
    setup_curl2(curl_mock)
    curl_mock.setopt.assert_called_with(pycurl.VERBOSE, True)


def test_decode_if_necessary() -> None:
    """Test that _decode_if_necessary decodes if necessary."""
    assert _decode_if_necessary(None) is None
    assert _decode_if_necessary("string") == "string"
    assert f"{bytes('string', encoding='utf-8')}" == "b'string'"  # type: ignore [str-bytes-safe]
    assert _decode_if_necessary(bytes("string", encoding="utf-8")) == "string"
    with pytest.raises(TypeError):
        _decode_if_necessary(50)  # type: ignore [arg-type]


def test_convert_checksum_from_dcache() -> None:
    """Test that convert_checksum_from_dcache will base64 decode checksums."""
    assert convert_checksum_from_dcache("ArC5mMA4l7VdpUYIodMR9w==") == "02b0b998c03897b55da54608a1d311f7"
    assert convert_checksum_from_dcache("sha-512=FoNbvqRdMmCxsyybg0KPt1oj5/3Ds5NYqlxGnXU4Hc9tJU8Qo1dWd1Klc6ZDNhI3hgLAxaX5o72CbeLleVcoEw==") == "16835bbea45d3260b1b32c9b83428fb75a23e7fdc3b39358aa5c469d75381dcf6d254f10a357567752a573a6433612378602c0c5a5f9a3bd826de2e579572813"


def test_sha512sum_tempfile() -> None:
    """Test that sha512sum hashes a temporary file correctly."""
    with NamedTemporaryFile(mode="wb", delete=False) as temp:
        temp.write(bytearray("The quick brown fox jumps over the lazy dog\n", "utf8"))
        temp.close()
    hashsum = sha512sum(Path(temp.name))
    assert hashsum == "a12ac6bdd854ac30c5cc5b576e1ee2c060c0d8c2bec8797423d7119aa2b962f7f30ce2e39879cbff0109c8f0a3fd9389a369daae45df7d7b286d7d98272dc5b1"
    os.remove(temp.name)


@pytest.mark.asyncio
async def test_connection_semaphore_limits_concurrency() -> None:
    """Test that the connection_semaphore decorator limits concurrency of async class methods."""
    class TestClass(ParallelAsync):
        def __init__(self, max_concurrent: int):
            self._semaphore = asyncio.Semaphore(max_concurrent)
            self.started = 0
            self.running = 0
            self.max_observed = 0

        @connection_semaphore
        async def do_work(self, delay: float) -> str:
            # Increment counters
            self.started += 1
            self.running += 1
            # Record peak concurrency
            if self.running > self.max_observed:
                self.max_observed = self.running
            # Simulate work
            await asyncio.sleep(delay)
            # Decrement running count
            self.running -= 1
            return "done"

    instance = TestClass(max_concurrent=2)

    # Launch 5 tasks in parallel
    tasks: list[Task[str]] = [
        asyncio.create_task(_as_task(instance.do_work(0.1)))
        for _ in range(5)
    ]

    # Wait for all to finish
    results = await asyncio.gather(*tasks)

    # Assertions
    assert all(r == "done" for r in results)
    assert instance.started == 5
    # Here's the key: no more than 2 concurrent executions
    assert instance.max_observed == 2


def test_sync_constructor_missing() -> None:
    """Test the constructor of Sync."""
    with pytest.raises(KeyError):
        Sync({})

    with pytest.raises(KeyError):
        Sync({
            # "DEST_URL": "",
            "LTA_AUTH_OPENID_URL": "",
            "CLIENT_ID": "",
            "CLIENT_SECRET": "",
            "WORK_TIMEOUT_SECONDS": "30",
            "WORK_RETRIES": "3",
            "MAX_PARALLEL": "10",
        })

    with pytest.raises(KeyError):
        Sync({
            "DEST_URL": "",
            # "LTA_AUTH_OPENID_URL": "",
            "CLIENT_ID": "",
            "CLIENT_SECRET": "",
            "WORK_TIMEOUT_SECONDS": "30",
            "WORK_RETRIES": "3",
            "MAX_PARALLEL": "10",
        })

    with pytest.raises(KeyError):
        Sync({
            "DEST_URL": "",
            "LTA_AUTH_OPENID_URL": "",
            # "CLIENT_ID": "",
            "CLIENT_SECRET": "",
            "WORK_TIMEOUT_SECONDS": "30",
            "WORK_RETRIES": "3",
            "MAX_PARALLEL": "10",
        })

    with pytest.raises(KeyError):
        Sync({
            "DEST_URL": "",
            "LTA_AUTH_OPENID_URL": "",
            "CLIENT_ID": "",
            # "CLIENT_SECRET": "",
            "WORK_TIMEOUT_SECONDS": "30",
            "WORK_RETRIES": "3",
            "MAX_PARALLEL": "10",
        })

    with pytest.raises(KeyError):
        Sync({
            "DEST_URL": "",
            "LTA_AUTH_OPENID_URL": "",
            "CLIENT_ID": "",
            "CLIENT_SECRET": "",
            # "WORK_TIMEOUT_SECONDS": "30",
            "WORK_RETRIES": "3",
            "MAX_PARALLEL": "10",
        })

    with pytest.raises(KeyError):
        Sync({
            "DEST_URL": "",
            "LTA_AUTH_OPENID_URL": "",
            "CLIENT_ID": "",
            "CLIENT_SECRET": "",
            "WORK_TIMEOUT_SECONDS": "30",
            # "WORK_RETRIES": "3",
            "MAX_PARALLEL": "10",
        })

    with pytest.raises(KeyError):
        Sync({
            "DEST_URL": "",
            "LTA_AUTH_OPENID_URL": "",
            "CLIENT_ID": "",
            "CLIENT_SECRET": "",
            "WORK_TIMEOUT_SECONDS": "30",
            "WORK_RETRIES": "3",
            # "MAX_PARALLEL": "10",
        })

    sync = Sync({
        "DEST_URL": "",
        "LTA_AUTH_OPENID_URL": "",
        "CLIENT_ID": "",
        "CLIENT_SECRET": "",
        "WORK_TIMEOUT_SECONDS": "30",
        "WORK_RETRIES": "3",
        "MAX_PARALLEL": "10",
    })
    assert sync.config is not None
    assert sync.rc is not None
    assert sync.http_client is not None
    assert sync._semaphore is not None


@pytest.mark.asyncio
async def test_sync_run(config: TestConfig) -> None:
    """Test that the Sync.run() method raises an NotImplementedError exception."""
    sync = Sync(config)
    with pytest.raises(NotImplementedError):
        await sync.run()


@pytest.mark.asyncio
async def test_sync_get_children(config: TestConfig, mocker: MockerFixture) -> None:
    """Test that the Sync.get_children() method can parse a long response."""
    XML_RESPONSE = """
        <?xml version="1.0" encoding="utf-8"?>
        <d:multistatus
            xmlns:d="DAV:"
            xmlns:ns1="http://srm.lbl.gov/StorageResourceManager"
            xmlns:ns2="http://www.dcache.org/2013/webdav">
        <d:response>
            <d:href>/base/</d:href>
            <d:propstat>
            <d:prop>
                <d:iscollection>TRUE</d:iscollection>
            </d:prop>
            <d:status>HTTP/1.1 200 OK</d:status>
            </d:propstat>
        </d:response>
        <d:response>
            <d:href>/base/file1.txt</d:href>
            <d:propstat>
            <d:prop>
                <d:iscollection>FALSE</d:iscollection>
                <d:getcontentlength>12345</d:getcontentlength>
                <ns2:Checksums>adler32=deadbeef;sha1=abc123</ns2:Checksums>
                <ns1:FileLocality>NEARLINE_AND_ONLINE</ns1:FileLocality>
            </d:prop>
            <d:status>HTTP/1.1 200 OK</d:status>
            </d:propstat>
        </d:response>
        <d:response>
            <d:href>/base/dir2/</d:href>
            <d:propstat>
            <d:prop>
                <d:iscollection>TRUE</d:iscollection>
            </d:prop>
            <d:status>HTTP/1.1 200 OK</d:status>
            </d:propstat>
        </d:response>
        <d:response>
            <d:href>/base/file2.txt</d:href>
            <d:propstat>
            <d:prop>
                <d:iscollection>FALSE</d:iscollection>
                <d:getcontentlength>67890</d:getcontentlength>
                <ns1:FileLocality>NEARLINE</ns1:FileLocality>
            </d:prop>
            <d:status>HTTP/1.1 200 OK</d:status>
            </d:propstat>
        </d:response>
        </d:multistatus>
    """
    rc_mock = MagicMock()
    rc_mock.access_token = "Ash nazg durbatulûk, ash nazg gimbatul, ash nazg thrakatulûk, agh burzum-ishi krimpatul"
    ret_mock = MagicMock()
    ret_mock.body = bytes(XML_RESPONSE.strip(), "utf-8")
    hc_mock = AsyncMock()
    hc_mock.fetch.return_value = ret_mock
    sync = Sync(config)
    sync.rc = rc_mock
    sync.http_client = hc_mock

    await sync.get_children("/fake/path/does/not/really/exist")

    hc_mock.fetch.assert_called_with(mocker.ANY)
    rc_mock._get_token.assert_called()


@pytest.mark.asyncio
async def test_sync_get_children_whole_lotta_props(config: TestConfig, mocker: MockerFixture) -> None:
    """Test that the Sync.get_children() can parse out a lot of properties."""
    XML_RESPONSE = """
        <?xml version="1.0" encoding="utf-8"?>
        <d:multistatus
            xmlns:d="DAV:"
            xmlns:ns1="http://srm.lbl.gov/StorageResourceManager"
            xmlns:ns2="http://www.dcache.org/2013/webdav"
            xmlns:qwerty="http://www.dcache.org/2025/qwerty">
        <d:response>
            <d:href>/whole/lotta/props.txt</d:href>
            <d:propstat>
            <d:prop>
                <d:iscollection>FALSE</d:iscollection>
                <d:getcontentlength>23456</d:getcontentlength>
                <ns2:Checksums>sha-512=FoNbvqRdMmCxsyybg0KPt1oj5/3Ds5NYqlxGnXU4Hc9tJU8Qo1dWd1Klc6ZDNhI3hgLAxaX5o72CbeLleVcoEw==</ns2:Checksums>
                <ns1:FileLocality>ONLINE</ns1:FileLocality>
                <qwerty:favoritepokemon>Meowth</qwerty:favoritepokemon>
                <qwerty:onthecloud>FALSE</qwerty:onthecloud>
            </d:prop>
            <d:status>HTTP/1.1 200 OK</d:status>
            </d:propstat>
        </d:response>
        </d:multistatus>
    """
    rc_mock = MagicMock()
    rc_mock.access_token = "Ash nazg durbatulûk, ash nazg gimbatul, ash nazg thrakatulûk, agh burzum-ishi krimpatul"
    ret_mock = MagicMock()
    ret_mock.body = bytes(XML_RESPONSE.strip(), "utf-8")
    hc_mock = AsyncMock()
    hc_mock.fetch.return_value = ret_mock
    sync = Sync(config)
    sync.rc = rc_mock
    sync.http_client = hc_mock

    children = await sync.get_children("/fake/path/does/not/really/exist")
    assert children == {
        'props.txt': {
            'name': 'props.txt',
            'type': DirObject.File,
            'size': 23456,
            'checksums': {
                'sha-512': '16835bbea45d3260b1b32c9b83428fb75a23e7fdc3b39358aa5c469d75381dcf6d254f10a357567752a573a6433612378602c0c5a5f9a3bd826de2e579572813',
            },
            'tape': False,
        }
    }

    hc_mock.fetch.assert_called_with(mocker.ANY)
    rc_mock._get_token.assert_called()


@pytest.mark.asyncio
async def test_sync_get_file(config: TestConfig, mocker: MockerFixture) -> None:
    """Test that the Sync.get_file() method would download a file."""
    rc_mock = MagicMock()
    rc_mock.access_token = "Ash nazg durbatulûk, ash nazg gimbatul, ash nazg thrakatulûk, agh burzum-ishi krimpatul"
    hc_mock = AsyncMock()
    sync = Sync(config)
    sync.rc = rc_mock
    sync.http_client = hc_mock
    with NamedTemporaryFile(mode="wb", delete=True) as temp:
        await sync.get_file(temp.name)
    hc_mock.fetch.assert_called_with(mocker.ANY)
    rc_mock._get_token.assert_called()


@pytest.mark.asyncio
async def test_sync_rmfile(config: TestConfig, mocker: MockerFixture) -> None:
    """Test that the Sync.rmfile() method would delete a file."""
    rc_mock = MagicMock()
    rc_mock.access_token = "Ash nazg durbatulûk, ash nazg gimbatul, ash nazg thrakatulûk, agh burzum-ishi krimpatul"
    hc_mock = AsyncMock()
    sync = Sync(config)
    sync.rc = rc_mock
    sync.http_client = hc_mock
    await sync.rmfile("/fake/path/does/not/really/exist")
    hc_mock.fetch.assert_called_with(mocker.ANY)
    rc_mock._get_token.assert_called()


@pytest.mark.asyncio
async def test_sync_rmtree_does_not_exist(config: TestConfig, mocker: MockerFixture) -> None:
    """Test that the Sync.rmtree() method ignores things that don't exist."""
    rc_mock = MagicMock()
    rc_mock.access_token = "Ash nazg durbatulûk, ash nazg gimbatul, ash nazg thrakatulûk, agh burzum-ishi krimpatul"
    hc_mock = AsyncMock()
    sync = Sync(config)
    sync.rc = rc_mock
    sync.http_client = hc_mock
    gc_mock = mocker.patch("lta.transfer.sync.Sync.get_children")
    gc_mock.return_value = {}

    await sync.rmtree(Path("/fake/path/does/not/zomg/a/tree"))

    hc_mock.fetch.assert_not_called()
    rc_mock._get_token.assert_not_called()


@pytest.mark.asyncio
async def test_sync_rmtree_delete_files(config: TestConfig, mocker: MockerFixture) -> None:
    """Test that the Sync.rmtree() deletes the file it names."""
    rc_mock = MagicMock()
    rc_mock.access_token = "Ash nazg durbatulûk, ash nazg gimbatul, ash nazg thrakatulûk, agh burzum-ishi krimpatul"
    hc_mock = AsyncMock()
    sync = Sync(config)
    sync.rc = rc_mock
    sync.http_client = hc_mock
    gc_mock = mocker.patch("lta.transfer.sync.Sync.get_children")
    gc_mock.return_value = {
        "file.txt": {
            "type": DirObject.File
        }
    }
    rm_mock = mocker.patch("lta.transfer.sync.Sync.rmfile")

    await sync.rmtree(Path("/fake/path/to/actually/a/file.txt"))

    rm_mock.assert_called_with("/fake/path/to/actually/a/file.txt")
    hc_mock.fetch.assert_not_called()
    rc_mock._get_token.assert_not_called()


@pytest.mark.asyncio
async def test_sync_rmtree_recurse_directory(config: TestConfig, mocker: MockerFixture) -> None:
    """Test that the Sync.rmtree() method recursively rmtree()s subdirectories."""
    rc_mock = MagicMock()
    rc_mock.access_token = "Ash nazg durbatulûk, ash nazg gimbatul, ash nazg thrakatulûk, agh burzum-ishi krimpatul"
    hc_mock = AsyncMock()
    sync = Sync(config)
    sync.rc = rc_mock
    sync.http_client = hc_mock
    gc_mock = mocker.patch("lta.transfer.sync.Sync.get_children")
    gc_mock.side_effect = [
        {
            "dir": {
                "type": DirObject.Directory,
            }
        },
        {
            "a.txt": {
                "name": "a.txt",
                "type": DirObject.File,
            },
            "dir_b": {
                "name": "dir_b",
                "type": DirObject.Directory,
            }
        },
        Exception("Nope"),
    ]
    rm_mock = mocker.patch("lta.transfer.sync.Sync.rmfile")
    tg_mock = mocker.patch("asyncio.TaskGroup.create_task")

    await sync.rmtree(Path("/fake/path/to/a/dir"))

    tg_mock.assert_called()
    rm_mock.assert_called()
    hc_mock.fetch.assert_not_called()
    rc_mock._get_token.assert_not_called()


@pytest.mark.asyncio
async def test_sync_mkdir(config: TestConfig, mocker: MockerFixture) -> None:
    """Test that the Sync.mkdir() method would create a directory."""
    rc_mock = MagicMock()
    rc_mock.access_token = "Ash nazg durbatulûk, ash nazg gimbatul, ash nazg thrakatulûk, agh burzum-ishi krimpatul"
    hc_mock = AsyncMock()
    sync = Sync(config)
    sync.rc = rc_mock
    sync.http_client = hc_mock
    await sync.mkdir("/fake/path/does/not/really/exist")
    hc_mock.fetch.assert_called_with(mocker.ANY)
    rc_mock._get_token.assert_called()


@pytest.mark.asyncio
async def test_sync_put_file_bad_checksum(config: TestConfig, mocker: MockerFixture) -> None:
    """Test that the Sync.put_file() method throws a RuntimeError on a mismatched checksum."""
    rc_mock = MagicMock()
    rc_mock.access_token = "Ash nazg durbatulûk, ash nazg gimbatul, ash nazg thrakatulûk, agh burzum-ishi krimpatul"
    hc_mock = AsyncMock()
    sync = Sync(config)
    sync.rc = rc_mock
    sync.http_client = hc_mock

    ret_mock = ObjectLiteral(
        headers={
            "Digest": "sha-512=FoNbvqRdMmCxsyybg0KPt1oj5/3Ds5NYqlxGnXU4Hc9tJU8Qo1dWd1Klc6ZDNhI3hgLAxaX5o72CbeLleVcoEw==",
        }
    )
    hc_mock.fetch.return_value = ret_mock

    with pytest.raises(RuntimeError):
        with NamedTemporaryFile(mode="rb", delete=True) as temp:
            await sync.put_file(temp.name)

    hc_mock.fetch.assert_called_with(mocker.ANY)
    rc_mock._get_token.assert_called()


@pytest.mark.asyncio
async def test_sync_put_file_readback_checksum_move(config: TestConfig, mocker: MockerFixture) -> None:
    """Test that the Sync.put_file() method will calculate a readback-checksum and move the file."""
    rc_mock = MagicMock()
    rc_mock.access_token = "Ash nazg durbatulûk, ash nazg gimbatul, ash nazg thrakatulûk, agh burzum-ishi krimpatul"
    hc_mock = AsyncMock()
    sync = Sync(config)
    sync.rc = rc_mock
    sync.http_client = hc_mock

    ret_mock = ObjectLiteral(
        headers={}
    )
    hc_mock.fetch.return_value = ret_mock

    with NamedTemporaryFile(mode="rb", delete=True) as temp:
        await sync.put_file(temp.name)

    hc_mock.fetch.assert_called_with(mocker.ANY)
    rc_mock._get_token.assert_called()


def test_sync_get_local_children_not_found(config: TestConfig, mocker: MockerFixture) -> None:
    """Test that the Sync.get_local_children() method will raise an error while the directory doesn't exist."""
    sync = Sync(config)

    with pytest.raises(FileNotFoundError):
        sync.get_local_children(Path("/fake/path/does/not/really/exist"))


def test_sync_get_local_children_empty(config: TestConfig, mocker: MockerFixture) -> None:
    """Test that the Sync.get_local_children() method will get local children."""
    sync = Sync(config)
    iterdir_mock = mocker.patch("lta.transfer.sync.Path.iterdir")

    local_children = sync.get_local_children(Path("/fake/path/does/not/really/exist"))
    assert local_children == {}

    iterdir_mock.assert_called()


def test_sync_get_local_children_dir_and_file(config: TestConfig, mocker: MockerFixture) -> None:
    """Test that the Sync.get_local_children() method will get local children."""
    sync = Sync(config)

    local1 = MagicMock()
    local1.name = "some_dir"
    local2 = MagicMock()

    stat_mock = MagicMock()
    stat_mock.st_size = 12345

    local2.name = "some_file.txt"
    local2.is_dir.return_value = False
    local2.stat.return_value = stat_mock

    iterdir_mock = mocker.patch("lta.transfer.sync.Path.iterdir")
    iterdir_mock.return_value = [local1, local2]

    local_children = sync.get_local_children(Path("/fake/path/does/not/really/exist"))
    assert local_children == {
        "some_dir": {
            "name": "some_dir",
            "type": DirObject.Directory,
        },
        "some_file.txt": {
            "name": "some_file.txt",
            "type": DirObject.File,
            "size": 12345,
        },
    }

    iterdir_mock.assert_called()


def test_sync_get_local_children_skip_run_directories(config: TestConfig, mocker: MockerFixture) -> None:
    """Test that the Sync.get_local_children() method will get local children."""
    sync = Sync(config)

    run_dir = MagicMock()
    run_dir.name = "Run_1122334455"

    iterdir_mock = mocker.patch("lta.transfer.sync.Path.iterdir")
    iterdir_mock.return_value = [run_dir]

    local_children = sync.get_local_children(Path("/fake/path/does/not/really/exist"))
    assert local_children == {}

    iterdir_mock.assert_called()


@pytest.mark.asyncio
async def test_sync_sync_dir_empty(config: TestConfig, mocker: MockerFixture) -> None:
    """Test that the Sync.sync_dir() will not really synchronize an empty directory."""
    rc_mock = MagicMock()
    rc_mock.access_token = "Ash nazg durbatulûk, ash nazg gimbatul, ash nazg thrakatulûk, agh burzum-ishi krimpatul"
    hc_mock = AsyncMock()
    sync = Sync(config)
    sync.rc = rc_mock
    sync.http_client = hc_mock

    gc_mock = mocker.patch("lta.transfer.sync.Sync.get_children")
    glc_mock = mocker.patch("lta.transfer.sync.Sync.get_local_children")

    await sync.sync_dir(Path("/fake/path/to/sync"))

    gc_mock.assert_called()
    glc_mock.assert_called()

    hc_mock.fetch.assert_called_with(mocker.ANY)
    rc_mock._get_token.assert_called()


@pytest.mark.asyncio
async def test_sync_sync_dir_remove_failed_uploads(config: TestConfig, mocker: MockerFixture) -> None:
    """Test that the Sync.sync_dir() will attempt to remove failed uploads."""
    rc_mock = MagicMock()
    rc_mock.access_token = "Ash nazg durbatulûk, ash nazg gimbatul, ash nazg thrakatulûk, agh burzum-ishi krimpatul"
    hc_mock = AsyncMock()
    sync = Sync(config)
    sync.rc = rc_mock
    sync.http_client = hc_mock

    gc_mock = mocker.patch("lta.transfer.sync.Sync.get_children")
    gc_mock.side_effect = [
        {
            "sync": {
                "name": "sync",
                "type": DirObject.Directory,
            }
        },
        {
            "_upload_SomeFile.txt": {
                "name": "_upload_SomeFile.txt",
                "type": DirObject.File,
                "size": 12345,
            }
        },
    ]
    glc_mock = mocker.patch("lta.transfer.sync.Sync.get_local_children")
    tg_mock = mocker.patch("asyncio.TaskGroup.create_task")
    rmf_mock = mocker.patch("lta.transfer.sync.Sync.rmfile")

    await sync.sync_dir(Path("/fake/path/to/sync"))

    rmf_mock.assert_called()
    tg_mock.assert_called()
    gc_mock.assert_called()
    glc_mock.assert_called()

    hc_mock.fetch.assert_not_called()
    rc_mock._get_token.assert_not_called()


@pytest.mark.asyncio
async def test_sync_sync_dir_fix_directory_to_file(config: TestConfig, mocker: MockerFixture) -> None:
    """Test that the Sync.sync_dir() will fix a remote directory by replacement with a local file."""
    rc_mock = MagicMock()
    rc_mock.access_token = "Ash nazg durbatulûk, ash nazg gimbatul, ash nazg thrakatulûk, agh burzum-ishi krimpatul"
    hc_mock = AsyncMock()

    gc_mock = mocker.patch("lta.transfer.sync.Sync.get_children")
    gc_mock.side_effect = [
        {
            "sync": {
                "name": "sync",
                "type": DirObject.Directory,
            }
        },
        {
            "actually_a_file.txt": {
                "name": "actually_a_file.txt",
                "type": DirObject.Directory,
            }
        },
        Exception("Nope"),
    ]
    glc_mock = mocker.patch("lta.transfer.sync.Sync.get_local_children")
    glc_mock.side_effect = [
        {
            "actually_a_file.txt": {
                "name": "actually_a_file.txt",
                "type": DirObject.File,
                "size": 23456,
            }
        },
        Exception("Nope"),
    ]
    pf_mock = mocker.patch("lta.transfer.sync.Sync.put_file")
    rf_mock = mocker.patch("lta.transfer.sync.Sync.rmfile")
    rt_mock = mocker.patch("lta.transfer.sync.Sync.rmtree")
    stat_mock = mocker.patch("lta.transfer.sync.Path.stat")

    sync = Sync(config)
    sync.rc = rc_mock
    sync.http_client = hc_mock
    await sync.sync_dir(Path("/fake/path/to/sync"))

    stat_mock.assert_not_called()
    rt_mock.assert_called()
    rf_mock.assert_not_called()
    pf_mock.assert_called()
    glc_mock.assert_called()
    gc_mock.assert_called()

    hc_mock.fetch.assert_not_called()
    rc_mock._get_token.assert_not_called()


@pytest.mark.asyncio
async def test_sync_sync_dir_verify_existing_file(config: TestConfig, mocker: MockerFixture) -> None:
    """Test that the Sync.sync_dir() will verify an existing file of the same size."""
    rc_mock = MagicMock()
    rc_mock.access_token = "Ash nazg durbatulûk, ash nazg gimbatul, ash nazg thrakatulûk, agh burzum-ishi krimpatul"
    hc_mock = AsyncMock()

    gc_mock = mocker.patch("lta.transfer.sync.Sync.get_children")
    gc_mock.side_effect = [
        {
            "sync": {
                "name": "sync",
                "type": DirObject.Directory,
            }
        },
        {
            "already_here.txt": {
                "name": "already_here.txt",
                "type": DirObject.File,
                "size": 4294967295,
            }
        },
        Exception("Nope"),
    ]
    glc_mock = mocker.patch("lta.transfer.sync.Sync.get_local_children")
    glc_mock.side_effect = [
        {
            "already_here.txt": {
                "name": "already_here.txt",
                "type": DirObject.File,
                "size": 4294967295,
            }
        },
        Exception("Nope"),
    ]
    pf_mock = mocker.patch("lta.transfer.sync.Sync.put_file")
    rf_mock = mocker.patch("lta.transfer.sync.Sync.rmfile")
    rt_mock = mocker.patch("lta.transfer.sync.Sync.rmtree")
    stat_mock = mocker.patch("lta.transfer.sync.Path.stat")

    sync = Sync(config)
    sync.rc = rc_mock
    sync.http_client = hc_mock
    await sync.sync_dir(Path("/fake/path/to/sync"))

    stat_mock.assert_not_called()
    rt_mock.assert_not_called()
    rf_mock.assert_not_called()
    pf_mock.assert_not_called()
    glc_mock.assert_called()
    gc_mock.assert_called()

    hc_mock.fetch.assert_not_called()
    rc_mock._get_token.assert_not_called()


@pytest.mark.asyncio
async def test_sync_sync_dir_will_sync_missing_dir(config: TestConfig, mocker: MockerFixture) -> None:
    """Test that the Sync.sync_dir() will synchronize a local directory to remote."""
    rc_mock = MagicMock()
    rc_mock.access_token = "Ash nazg durbatulûk, ash nazg gimbatul, ash nazg thrakatulûk, agh burzum-ishi krimpatul"
    hc_mock = AsyncMock()

    gc_mock = mocker.patch("lta.transfer.sync.Sync.get_children")
    gc_mock.side_effect = [
        {
            "sync": {
                "name": "sync",
                "type": DirObject.Directory,
            }
        },
        {
            "some_other_dir": {
                "name": "some_other_dir",
                "type": DirObject.Directory,
            }
        },
        Exception("Nope"),
    ]
    glc_mock = mocker.patch("lta.transfer.sync.Sync.get_local_children")
    glc_mock.side_effect = [
        {
            "some_other_dir": {
                "name": "some_other_dir",
                "type": DirObject.Directory,
            },
            "sync_me_plz": {
                "name": "sync_me_plz",
                "type": DirObject.Directory,
            }
        },
        Exception("Nope"),
    ]
    pf_mock = mocker.patch("lta.transfer.sync.Sync.put_file")
    rf_mock = mocker.patch("lta.transfer.sync.Sync.rmfile")
    rt_mock = mocker.patch("lta.transfer.sync.Sync.rmtree")
    stat_mock = mocker.patch("lta.transfer.sync.Path.stat")

    sync = Sync(config)
    sync.rc = rc_mock
    sync.http_client = hc_mock

    # what's that in the unit test?
    # it's a bird!
    # it's a plane!
    # no, it's Recursive Mock Man!
    real_sync_dir = sync.sync_dir
    with mocker.patch.object(sync, "sync_dir"):
        await real_sync_dir(Path("/fake/path/to/sync"))

    stat_mock.assert_not_called()
    rt_mock.assert_not_called()
    rf_mock.assert_not_called()
    pf_mock.assert_not_called()
    glc_mock.assert_called()
    gc_mock.assert_called()

    hc_mock.fetch.assert_not_called()
    rc_mock._get_token.assert_not_called()


@pytest.mark.asyncio
async def test_sync_mkdir_p_already_exist(config: TestConfig, mocker: MockerFixture) -> None:
    """Test that the Sync.mkdir_p() will not create anything if the directory already exists."""
    rc_mock = MagicMock()
    rc_mock.access_token = "Ash nazg durbatulûk, ash nazg gimbatul, ash nazg thrakatulûk, agh burzum-ishi krimpatul"
    hc_mock = AsyncMock()
    sync = Sync(config)
    sync.rc = rc_mock
    sync.http_client = hc_mock

    await sync.mkdir_p("/fake/path/to/create/with/parents")

    hc_mock.fetch.assert_called_with(mocker.ANY)
    rc_mock._get_token.assert_called()


@pytest.mark.asyncio
async def test_sync_mkdir_p_base_path_missing(config: TestConfig, mocker: MockerFixture) -> None:
    """Test that the Sync.mkdir_p() will raise an exception if the base path doesn't exist on remote."""
    rc_mock = MagicMock()
    rc_mock.access_token = "Ash nazg durbatulûk, ash nazg gimbatul, ash nazg thrakatulûk, agh burzum-ishi krimpatul"
    hc_mock = AsyncMock()
    sync = Sync(config)
    sync.rc = rc_mock
    sync.http_client = hc_mock

    hc_mock.fetch.side_effect = [HTTPError(404)] * 20

    with pytest.raises(Exception):
        await sync.mkdir_p("/fake/path/to/create/with/parents")

    hc_mock.fetch.assert_called_with(mocker.ANY)
    rc_mock._get_token.assert_called()


@pytest.mark.asyncio
async def test_sync_mkdir_p_server_on_fire(config: TestConfig, mocker: MockerFixture) -> None:
    """Test that the Sync.mkdir_p() will raise an exception if remote server is on fire."""
    rc_mock = MagicMock()
    rc_mock.access_token = "Ash nazg durbatulûk, ash nazg gimbatul, ash nazg thrakatulûk, agh burzum-ishi krimpatul"
    hc_mock = AsyncMock()
    sync = Sync(config)
    sync.rc = rc_mock
    sync.http_client = hc_mock

    hc_mock.fetch.side_effect = HTTPError(500)

    with pytest.raises(Exception):
        await sync.mkdir_p("/fake/path/to/create/with/parents")

    hc_mock.fetch.assert_called_with(mocker.ANY)
    rc_mock._get_token.assert_called()


@pytest.mark.asyncio
async def test_sync_mkdir_p_need_two(config: TestConfig, mocker: MockerFixture) -> None:
    """Test that the Sync.mkdir_p() will create a missing parent and dir."""
    rc_mock = MagicMock()
    rc_mock.access_token = "Ash nazg durbatulûk, ash nazg gimbatul, ash nazg thrakatulûk, agh burzum-ishi krimpatul"
    hc_mock = AsyncMock()
    sync = Sync(config)
    sync.rc = rc_mock
    sync.http_client = hc_mock

    hc_mock.fetch.side_effect = [
        # checking
        HTTPError(404),  # /fake/path/to/create/with/parents  not found
        HTTPError(405),  # /fake/path/to/create/with  not allowed
        {},              # /fake/path/to/create  found
        # creating
        HTTPError(409),  # /fake/path/to/create/with  conflict
        {},              # /fake/path/to/create/with/parents  created
    ]

    await sync.mkdir_p("/fake/path/to/create/with/parents")

    hc_mock.fetch.assert_called_with(mocker.ANY)
    rc_mock._get_token.assert_called()


@pytest.mark.asyncio
async def test_sync_mkdir_p_need_two_but_fire(config: TestConfig, mocker: MockerFixture) -> None:
    """Test that the Sync.mkdir_p() will create a missing parent and dir."""
    rc_mock = MagicMock()
    rc_mock.access_token = "Ash nazg durbatulûk, ash nazg gimbatul, ash nazg thrakatulûk, agh burzum-ishi krimpatul"
    hc_mock = AsyncMock()
    sync = Sync(config)
    sync.rc = rc_mock
    sync.http_client = hc_mock

    hc_mock.fetch.side_effect = [
        # checking
        HTTPError(404),  # /fake/path/to/create/with/parents  not found
        HTTPError(405),  # /fake/path/to/create/with  not allowed
        {},              # /fake/path/to/create  found
        # creating
        HTTPError(409),  # /fake/path/to/create/with  conflict
        HTTPError(500),  # /fake/path/to/create/with/parents  fire started
    ]

    with pytest.raises(Exception):
        await sync.mkdir_p("/fake/path/to/create/with/parents")

    hc_mock.fetch.assert_called_with(mocker.ANY)
    rc_mock._get_token.assert_called()


@pytest.mark.asyncio
async def test_sync_put_file_src_dest_bad_checksum(config: TestConfig, mocker: MockerFixture) -> None:
    """Test that the Sync.put_file_src_dest() method throws a RuntimeError on a mismatched checksum."""
    rc_mock = MagicMock()
    rc_mock.access_token = "Ash nazg durbatulûk, ash nazg gimbatul, ash nazg thrakatulûk, agh burzum-ishi krimpatul"
    hc_mock = AsyncMock()
    sync = Sync(config)
    sync.rc = rc_mock
    sync.http_client = hc_mock

    ret_mock = ObjectLiteral(
        headers={
            "Digest": "sha-512=FoNbvqRdMmCxsyybg0KPt1oj5/3Ds5NYqlxGnXU4Hc9tJU8Qo1dWd1Klc6ZDNhI3hgLAxaX5o72CbeLleVcoEw==",
        }
    )
    hc_mock.fetch.return_value = ret_mock

    stat_mock = mocker.patch("os.stat")
    stat_mock.return_value = ObjectLiteral(
        st_size=12345
    )

    with pytest.raises(RuntimeError):
        with NamedTemporaryFile(mode="rb", delete=True) as temp:
            await sync.put_file_src_dest(temp.name, "/fake/temp/files/go/here/temp.txt")

    hc_mock.fetch.assert_called_with(mocker.ANY)
    rc_mock._get_token.assert_called()


@pytest.mark.asyncio
async def test_sync_put_file_src_dest_readback_checksum_move(config: TestConfig, mocker: MockerFixture) -> None:
    """Test that the Sync.put_file_src_dest() method will calculate a readback-checksum and move the file."""
    rc_mock = MagicMock()
    rc_mock.access_token = "Ash nazg durbatulûk, ash nazg gimbatul, ash nazg thrakatulûk, agh burzum-ishi krimpatul"
    hc_mock = AsyncMock()
    sync = Sync(config)
    sync.rc = rc_mock
    sync.http_client = hc_mock

    ret_mock = ObjectLiteral(
        headers={}
    )
    hc_mock.fetch.return_value = ret_mock

    stat_mock = mocker.patch("os.stat")
    stat_mock.return_value = ObjectLiteral(
        st_size=12345
    )

    with NamedTemporaryFile(mode="rb", delete=True) as temp:
        await sync.put_file_src_dest(temp.name, "/fake/temp/files/go/here/temp.txt")

    hc_mock.fetch.assert_called_with(mocker.ANY)
    rc_mock._get_token.assert_called()


@pytest.mark.asyncio
async def test_sync_put_path(config: TestConfig, mocker: MockerFixture) -> None:
    """Test that the Sync.put_puth() method will upload a file to the remote."""
    rc_mock = MagicMock()
    rc_mock.access_token = "Ash nazg durbatulûk, ash nazg gimbatul, ash nazg thrakatulûk, agh burzum-ishi krimpatul"
    hc_mock = AsyncMock()
    sync = Sync(config)
    sync.rc = rc_mock
    sync.http_client = hc_mock

    mdp_mock = mocker.patch("lta.transfer.sync.Sync.mkdir_p")
    pfsd_mock = mocker.patch("lta.transfer.sync.Sync.put_file_src_dest")

    await sync.put_path("9c0ccadf-6d21-4dae-aba5-38750f0d22ec.zip", "/fake/data/exp/IceCube/2050/unbiased/PFRaw/1109/9c0ccadf-6d21-4dae-aba5-38750f0d22ec.zip")

    mdp_mock.assert_called_with("/fake/data/exp/IceCube/2050/unbiased/PFRaw/1109", 30)
    pfsd_mock.assert_called_with(
        "9c0ccadf-6d21-4dae-aba5-38750f0d22ec.zip",
        "/fake/data/exp/IceCube/2050/unbiased/PFRaw/1109/9c0ccadf-6d21-4dae-aba5-38750f0d22ec.zip",
        1200
    )

    hc_mock.fetch.assert_not_called()
    rc_mock._get_token.assert_not_called()
