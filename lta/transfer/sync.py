# sync.py
"""Transfer implementation using WebDAV to copy files to DESY."""

import asyncio
import base64
import hashlib
import logging
import xml.etree.ElementTree as ET
from collections.abc import Awaitable, Callable, Coroutine
from enum import Enum
from functools import wraps
from pathlib import Path
from typing import (
    Any,
    BinaryIO,
    Concatenate,
    ParamSpec,
    TypeAlias,
    TypeVar,
    cast,
)
from xml.etree.ElementTree import Element

import anyio
import pycurl
from rest_tools.client import ClientCredentialsAuth
from tornado.httpclient import AsyncHTTPClient, HTTPError, HTTPRequest
from tornado.simple_httpclient import SimpleAsyncHTTPClient

LOG = logging.getLogger(__name__)

BodyWriter: TypeAlias = Callable[[bytes], None]
AsyncBodyWriter: TypeAlias = Callable[[bytes], Awaitable[None]]
BodyProducer: TypeAlias = Callable[
    [BodyWriter],
    asyncio.Future[None],
]
DataDict = dict[str, Any]

P = ParamSpec("P")
R = TypeVar("R")
T = TypeVar("T", bound="ParallelAsync")
TaskReturn = TypeVar("TaskReturn")

XMLNS = {
    'd': 'DAV:',
    'ns1': 'http://srm.lbl.gov/StorageResourceManager',
    'ns2': 'http://www.dcache.org/2013/webdav',
}

NUM_BYTES_LARGE_FILE = 2_000_000_000
NUM_TRIVIAL_PROPS = 5


class DirObject(Enum):
    Directory = 1
    File = 2


class InvalidDecodedValueTypeError(TypeError):
    """Raised when a value is not str, bytes, or None."""

    def __init__(self, value: object) -> None:
        self.value = value
        self.actual_type = type(value)

        super().__init__(
            f"Expected str, bytes, or None; got {self.actual_type.__name__}"
        )


class MkdirBasePathDoesNotExistError(Exception):
    """
    Raised when remote mkdir -p discovers that the base remote
    directory does not exist.
    """

    def __init__(
        self,
        dest_base: Path,
    ) -> None:
        super().__init__(
            f"Base path {dest_base!r} does not exist on remote."
        )


class MkdirDirectoryCreationError(Exception):
    """
    Raised when remote mkdir -p is unable to create a directory at remote.
    """

    def __init__(
        self,
        current: Path,
        http_error: HTTPError,
    ) -> None:
        super().__init__(
            f"Error creating directory {current!r}: {http_error!r}"
        )


class MkdirPathDiscoveryError(Exception):
    """
    Raised when remote mkdir -p has an unexpected error
    searching for an existing parent path.
    """

    def __init__(
        self,
        candidate: Path,
        http_error: HTTPError,
    ) -> None:
        super().__init__(
            f"Unexpected error checking {candidate!r}: {http_error!r}"
        )


class UploadChecksumMismatchError(RuntimeError):
    """Raised when an uploaded file fails checksum verification."""

    def __init__(
        self,
        path: str,
        expected_checksum: str,
        received_checksum: str | None,
    ) -> None:
        super().__init__(
            f"Checksum mismatch for {path!r}: "
            f"expected {expected_checksum}, received {received_checksum}"
        )


def _as_task(task: Awaitable[TaskReturn]) -> Coroutine[Any, Any, TaskReturn]:
    """Cast a typical async call to one of the types expected by `asyncio.TaskGroup`."""
    return cast(Coroutine[Any, Any, TaskReturn], task)


def bind_setup_curl(config: dict[str, str]) -> Callable[[pycurl.Curl], None]:
    def setup_curl(c: pycurl.Curl) -> None:
        c.setopt(pycurl.CAPATH, '/etc/grid-security/certificates')
        if config["LOG_LEVEL"].lower() == 'debug':
            c.setopt(pycurl.VERBOSE, True)
    return setup_curl


def convert_checksum_from_dcache(checksum: str) -> str:
    """DCache returns a binary checksum, but we want the hex digest"""
    checksum = checksum.removeprefix('sha-512=')
    return base64.b64decode(checksum).hex()


def _decode_if_necessary(value: str | bytes | None) -> str | None:
    if value is None:
        return None
    if isinstance(value, str):
        return value
    if isinstance(value, bytes):
        return value.decode("utf-8")
    raise InvalidDecodedValueTypeError(value)


def _get_file_size(path: str) -> int:
    """Return the size of a file in bytes."""
    return Path(path).stat(follow_symlinks=True).st_size


def make_file_body_producer(
    path: str | anyio.Path,
) -> BodyProducer:
    async def produce_body(write: BodyWriter) -> None:
        # Tornado types this callback as returning None, but its documented
        # and actual interface returns a Future for flow control.
        async_write = cast(AsyncBodyWriter, write)

        async with await anyio.open_file(path, "rb") as file:
            while chunk := await file.read(1024 * 1024):
                await async_write(chunk)

    def body_producer(write: BodyWriter) -> asyncio.Future[None]:
        return asyncio.create_task(produce_body(write))

    return body_producer


def _open_binary_file(path: str) -> BinaryIO:
    """Open a file for binary reading."""
    return open(path, "rb")


def sha512sum(filename: Path, blocksize: int = 1024 * 1024 * 2) -> str:
    """
    Compute the SHA512 hash of the data in the specified file.
    2MB block size seems optimal on our ceph system.
    """
    h = hashlib.sha512()
    b = bytearray(blocksize)
    mv = memoryview(b)
    with open(filename, 'rb', buffering=0) as f:
        for n in iter(lambda: f.readinto(mv), 0):
            h.update(mv[:n])
    return h.hexdigest()


async def sha512sum_async(path: str | anyio.Path) -> str:
    """Calculate a file's SHA-512 checksum asynchronously."""
    hasher = hashlib.sha512()

    async with await anyio.open_file(path, "rb") as file:
        while chunk := await file.read(1024 * 1024):
            hasher.update(chunk)

    return hasher.hexdigest()


class ParallelAsync:
    def __init__(self, max_parallel: int):
        self._semaphore = asyncio.Semaphore(max_parallel)


def connection_semaphore(func: Callable[Concatenate[T, P], Awaitable[R]]) -> Callable[Concatenate[T, P], Awaitable[R]]:
    @wraps(func)
    async def inner(self: T, *args: P.args, **kwargs: P.kwargs) -> R:
        async with self._semaphore:
            return await func(self, *args, **kwargs)
    return cast(Callable[Concatenate[T, P], Awaitable[R]], inner)


class Sync(ParallelAsync):
    """
    Sync is a transfer implementation using WebDAV to copy files to DESY.

    Original code by David Schultz; gently adapted for LTA by Patrick Meade.
    """
    def __init__(self, config: dict[str, str]):
        super().__init__(int(config["MAX_PARALLEL"]))

        self.config = config

        self.rc = ClientCredentialsAuth(
            address=config["DEST_URL"],
            token_url=config["LTA_AUTH_OPENID_URL"],
            client_id=config["CLIENT_ID"],
            client_secret=config["CLIENT_SECRET"],
            timeout=int(config["WORK_TIMEOUT_SECONDS"]),
            retries=int(config["WORK_RETRIES"]),
        )

        AsyncHTTPClient.configure("tornado.curl_httpclient.CurlAsyncHTTPClient")
        self.http_client = AsyncHTTPClient(max_clients=100, defaults={
            'allow_nonstandard_methods': True,
            'connect_timeout': 0,
            'prepare_curl_callback': bind_setup_curl(self.config),
        })

    async def run(self) -> None:
        # await self.sync_dir(Path(ENV.SRC_DIRECTORY))
        raise NotImplementedError("Directory sync is not used for LTA; please call `await sync.put_path(src_path, dest_path)` instead")

    @connection_semaphore
    async def get_children(self, path_str: str) -> DataDict:
        fullpath = Path(self.config["DEST_BASE_PATH"]) / path_str.lstrip('/')
        self.rc._get_token()
        token = _decode_if_necessary(self.rc.access_token)
        headers = {
            'Authorization': f'bearer {token}',
            'Depth': '1',
        }
        body = b'<?xml version="1.0"?><propfind xmlns="DAV:"><allprop/></propfind>'
        req = HTTPRequest(
            method='PROPFIND',
            url=f'{self.config["DEST_URL"]}{fullpath}',
            headers=headers,
            body=body,
        )
        ret = await self.http_client.fetch(req)

        content = ret.body.decode('utf-8')
        LOG.debug(content)
        root = ET.fromstring(content)
        children = self._process_children(fullpath, root)
        return children

    def _process_children(self, fullpath: Path, root: Element) -> DataDict:
        """Process the children into a data dictionary."""
        children: DataDict = {}
        for e in root.findall('.//d:response', XMLNS):
            # sometimes e.find() returns None
            href = e.find('./d:href', XMLNS)
            if href is None or href.text is None:
                continue
            # href.text is a str, so wrap it up in a Path object
            path = Path(href.text)
            if path != fullpath:
                data = {'name': path.name, 'type': DirObject.Directory}
                proplist = e.findall('./d:propstat/d:prop', XMLNS)
                for props in proplist:
                    if len(props) > NUM_TRIVIAL_PROPS:
                        break
                else:
                    props = None
                if props:
                    data.update(self._process_props(props))
                children[path.name] = data
        return children

    def _process_props(self, props: Element) -> DataDict:
        """Process the properties into a data dictionary."""
        data: DataDict = {}
        isdir = props.find('./d:iscollection', XMLNS)
        if isdir is not None and isdir.text == 'FALSE':
            data['type'] = DirObject.File
            size = props.find('./d:getcontentlength', XMLNS)
            if size is not None and size.text is not None:
                data['size'] = int(size.text)
            checksums = props.find('./ns2:Checksums', XMLNS)
            if checksums is not None and checksums.text:
                data['checksums'] = {
                    c.split('=', 1)[0]: convert_checksum_from_dcache(c.split('=', 1)[1])
                    for c in checksums.text.split(';')
                }
            locality = props.find('./ns1:FileLocality', XMLNS)
            if locality is not None and locality.text is not None:
                data['tape'] = 'ONLINE' not in locality.text
        return data

    # @connection_semaphore
    # async def get_file(self, path: str, request_timeout: int = 1200) -> None:
    #     fullpath = Path(self.config["DEST_BASE_PATH"]) / path.lstrip('/')
    #     self.rc._get_token()
    #     token = _decode_if_necessary(self.rc.access_token)
    #     headers = {
    #         'Authorization': f'bearer {token}',
    #     }
    #     with open(path, 'wb') as f:
    #         def write_callback(data: bytes) -> None:
    #             f.write(data)

    #         req = HTTPRequest(
    #             method='GET',
    #             url=f'{self.config["DEST_URL"]}{fullpath}',
    #             headers=headers,
    #             request_timeout=request_timeout,
    #             streaming_callback=write_callback,
    #         )
    #         await self.http_client.fetch(req)
    @connection_semaphore
    async def get_file(self, path: str, request_timeout: int = 1200) -> None:
        remote_path = Path(self.config["DEST_BASE_PATH"]) / path.lstrip("/")
        destination = Path(path)

        self.rc._get_token()
        token = _decode_if_necessary(self.rc.access_token)

        headers = {
            "Authorization": f"bearer {token}",
        }

        chunks: asyncio.Queue[bytes | None] = asyncio.Queue()

        async def write_file() -> None:
            async with await anyio.open_file(destination, "wb") as file:
                while (chunk := await chunks.get()) is not None:
                    await file.write(chunk)

        writer_task = asyncio.create_task(write_file())

        def write_callback(data: bytes) -> None:
            # propagate a previous file-writing failure instead of continuing
            # to download data that can no longer be saved.
            if writer_task.done():
                writer_task.result()

            chunks.put_nowait(data)

        request = HTTPRequest(
            method="GET",
            url=f'{self.config["DEST_URL"]}{remote_path}',
            headers=headers,
            request_timeout=request_timeout,
            streaming_callback=write_callback,
        )

        try:
            await self.http_client.fetch(request)
        finally:
            chunks.put_nowait(None)

        await writer_task

    @connection_semaphore
    async def rmfile(self, path: str, request_timeout: int = 600) -> None:
        LOG.info('RMFILE %s', path)
        fullpath = Path(self.config["DEST_BASE_PATH"]) / path.lstrip('/')
        self.rc._get_token()
        token = _decode_if_necessary(self.rc.access_token)
        headers = {
            'Authorization': f'bearer {token}',
        }
        req = HTTPRequest(
            method='DELETE',
            url=f'{self.config["DEST_URL"]}{fullpath}',
            headers=headers,
            request_timeout=request_timeout,
        )
        await self.http_client.fetch(req)

    @connection_semaphore
    async def rmtree(self, path: Path, request_timeout: int = 600) -> None:
        LOG.info('RMTREE %s', path)
        ret = await self.get_children(str(path.parent))
        if path.name not in ret:
            LOG.info("does not exist")
        elif ret[path.name]['type'] == DirObject.File:
            await self.rmfile(str(path))
        else:
            children = await self.get_children(str(path))
            async with asyncio.TaskGroup() as tg:
                for child in children.values():
                    if child['type'] == DirObject.File:
                        tg.create_task(_as_task(self.rmfile(str(path / child['name']), request_timeout)))
                    else:
                        tg.create_task(_as_task(self.rmtree(path / child['name'], request_timeout)))
            await self.rmfile(str(path))

    @connection_semaphore
    async def mkdir(self, path: str, request_timeout: int = 60) -> None:
        LOG.info('MKDIR %s', path)
        fullpath = Path(self.config["DEST_BASE_PATH"]) / path.lstrip('/')
        self.rc._get_token()
        token = _decode_if_necessary(self.rc.access_token)
        headers = {
            'Authorization': f'bearer {token}',
        }
        req = HTTPRequest(
            method='MKCOL',
            url=f'{self.config["DEST_URL"]}{fullpath}',
            headers=headers,
            request_timeout=request_timeout,
        )
        await self.http_client.fetch(req)

    # @connection_semaphore
    # async def put_file(self, path: str, request_timeout: int = 1200) -> None:
    #     """
    #     Uploads file to a tmp name first, checks the checksum, then
    #     moves it to the final location.
    #     """
    #     logging.info('PUT %s', path)
    #     fullpath = Path(self.config["DEST_BASE_PATH"]) / path.lstrip('/')
    #     uploadpath = fullpath.with_name('_upload_' + fullpath.name)
    #     self.rc._get_token()
    #     token = _decode_if_necessary(self.rc.access_token)
    #     filesize = Path(path).stat(follow_symlinks=True).st_size
    #     headers = {
    #         'Authorization': f'bearer {token}',
    #         'Content-Length': str(filesize),
    #         'Want-Digest': 'SHA-512',
    #         'Expect': '100-continue',
    #     }

    #     with open(path, 'rb') as f:
    #         def seek(offset: int, _origin: int) -> int:
    #             try:
    #                 f.seek(offset)
    #                 return pycurl.SEEKFUNC_OK
    #             except Exception:
    #                 return pycurl.SEEKFUNC_FAIL

    #         def cb(c: pycurl.Curl) -> None:
    #             setup_curl = bind_setup_curl(self.config)
    #             setup_curl(c)
    #             if filesize >= 2000000000:
    #                 # c.unsetopt(pycurl.INFILESIZE)
    #                 c.setopt(pycurl.INFILESIZE_LARGE, filesize)
    #             else:
    #                 c.setopt(pycurl.INFILESIZE, filesize)
    #             c.setopt(pycurl.READDATA, f)
    #             c.setopt(pycurl.SEEKFUNCTION, seek)

    #         req = HTTPRequest(
    #             method='PUT',
    #             url=f'{self.config["DEST_URL"]}{uploadpath}',
    #             headers=headers,
    #             request_timeout=request_timeout,
    #             prepare_curl_callback=cb,
    #         )
    #         ret = await self.http_client.fetch(req)

    #     checksum = ret.headers.get('Digest', None)
    #     expected_checksum = sha512sum(Path(path))
    #     if checksum:
    #         # we got a checksum back, so compare that directly
    #         checksum = convert_checksum_from_dcache(checksum)
    #     else:
    #         # read back file, and run checksum manually
    #         logging.info("PUT %s - no checksum in headers, so get manually", path)
    #         hasher = hashlib.sha512()
    #         req = HTTPRequest(
    #             method='GET',
    #             url=f'{self.config["DEST_URL"]}{uploadpath}',
    #             headers=headers,
    #             request_timeout=request_timeout,
    #             streaming_callback=hasher.update,
    #         )
    #         await self.http_client.fetch(req)
    #         checksum = hasher.hexdigest()

    #     if expected_checksum == checksum:
    #         logging.info("PUT %s complete - checksum successful!", path)
    #     else:
    #         logging.error('PUT %s - bad checksum. expected %s, but received %s', path, expected_checksum, checksum)
    #         raise RuntimeError('bad checksum!')

    #     self.rc._get_token()
    #     token = _decode_if_necessary(self.rc.access_token)
    #     headers = {
    #         'Authorization': f'bearer {token}',
    #         'Destination': str(fullpath),
    #     }
    #     req = HTTPRequest(
    #         method='MOVE',
    #         url=f'{self.config["DEST_URL"]}{uploadpath}',
    #         headers=headers,
    #         request_timeout=request_timeout,
    #         prepare_curl_callback=bind_setup_curl(self.config),
    #     )
    #     await self.http_client.fetch(req)
    @connection_semaphore
    async def put_file(self, path: str, request_timeout: int = 1200) -> None:
        """
        Upload a file to a temporary name, verify its checksum, and then
        move it to its final location.
        """
        LOG.info("PUT %s", path)

        # figure out source and destination filenames
        source_path = Path(path)
        destination_path = (
            Path(self.config["DEST_BASE_PATH"]) / source_path.as_posix().lstrip("/")
        )
        upload_path = destination_path.with_name(
            f"_upload_{destination_path.name}"
        )

        # obtain the token we need for auth
        self.rc._get_token()
        token = _decode_if_necessary(self.rc.access_token)

        # determine what our upload request looks like
        file_size = await asyncio.to_thread(_get_file_size, path)
        upload_headers = {
            "Authorization": f"bearer {token}",
            "Content-Length": str(file_size),
            "Want-Digest": "SHA-512",
            "Expect": "100-continue",
        }

        # open the file so we can upload it
        file = await asyncio.to_thread(_open_binary_file, path)

        # have pycurl to upload the file
        try:
            # define a seek() function for pycurl to use
            def seek(offset: int, origin: int) -> int:
                try:
                    file.seek(offset, origin)
                except OSError:
                    return pycurl.SEEKFUNC_FAIL
                else:
                    return pycurl.SEEKFUNC_OK

            # pycurl will use prepare_upload to set itself up
            def prepare_upload(curl: pycurl.Curl) -> None:
                # have pycurl do the static setup stuff
                # the setup stuff we do regardless of the file size/type
                setup_curl = bind_setup_curl(self.config)
                setup_curl(curl)

                # have pycurl do the dynamic setup stuff
                # the setup stuff that depends on the file we're uploading
                if file_size >= NUM_BYTES_LARGE_FILE:
                    # NOTE: pycurl.INFILESIZE_LARGE and pycurl.INFILESIZE are exactly the same constant
                    # we're making a distinction without a difference only because pycurl does
                    curl.setopt(pycurl.INFILESIZE_LARGE, file_size)
                else:
                    # NOTE: pycurl.INFILESIZE_LARGE and pycurl.INFILESIZE are exactly the same constant
                    # we're making a distinction without a difference only because pycurl does
                    curl.setopt(pycurl.INFILESIZE, file_size)
                curl.setopt(pycurl.READDATA, file)
                curl.setopt(pycurl.SEEKFUNCTION, seek)

            # create and execute the PUT request
            request = HTTPRequest(
                method="PUT",
                url=f'{self.config["DEST_URL"]}{upload_path}',
                headers=upload_headers,
                request_timeout=request_timeout,
                prepare_curl_callback=prepare_upload,
            )
            response = await self.http_client.fetch(request)
        finally:
            # when we're doing uploading, close the file
            await asyncio.to_thread(file.close)

        # compute the local checksum for the file
        expected_checksum = await asyncio.to_thread(sha512sum, source_path)

        # ask for the checksum returned by the remote system
        checksum = response.headers.get("Digest")
        # if we got a checksum
        if checksum is not None:
            # decode the base64 checksum to hex digits
            checksum = convert_checksum_from_dcache(checksum)
        # whoops, the remote system didn't provide a checksum
        else:
            # Plan B: Read it back and checksum what remote provides
            LOG.info(
                "PUT %s - no checksum in headers, so get manually",
                path,
            )

            # initialize a hasher to compute the sha512 checksum
            hasher = hashlib.sha512()

            # create and execute the GET request to checksum the data
            checksum_headers = {
                "Authorization": f"bearer {token}",
            }
            request = HTTPRequest(
                method="GET",
                url=f'{self.config["DEST_URL"]}{upload_path}',
                headers=checksum_headers,
                request_timeout=request_timeout,
                streaming_callback=hasher.update,
            )
            await self.http_client.fetch(request)

            # ask the hasher what checksum it computed in the form of hex digits
            checksum = hasher.hexdigest()

        # if our local checksum DOES NOT match the remote checksum
        if expected_checksum != checksum:
            # tell the logs and raise an exception
            LOG.error(
                "PUT %s - bad checksum; expected %s, but received %s",
                path,
                expected_checksum,
                checksum,
            )
            raise UploadChecksumMismatchError(
                path,
                expected_checksum,
                checksum,
            )

        # otherwise, yay our checksum matched; successful upload
        LOG.info("PUT %s complete - checksum successful!", path)

        # get the token again (it may need a refresh after a long upload and download)
        self.rc._get_token()
        token = _decode_if_necessary(self.rc.access_token)

        # create and execute the MOVE request
        # this renames the file from _upload_$NAME to $NAME at remote
        move_headers = {
            "Authorization": f"bearer {token}",
            "Destination": str(destination_path),
        }
        request = HTTPRequest(
            method="MOVE",
            url=f'{self.config["DEST_URL"]}{upload_path}',
            headers=move_headers,
            request_timeout=request_timeout,
            prepare_curl_callback=bind_setup_curl(self.config),
        )
        await self.http_client.fetch(request)

    def get_local_children(self, path: Path) -> DataDict:
        children = {}
        for p in path.iterdir():
            if p.name.startswith("Run") and '_' in p.name:
                LOG.debug('skipping versioned run directory')
                continue
            data: DataDict = {
                'name': p.name,
                'type': DirObject.Directory if p.is_dir() else DirObject.File,
            }
            if data['type'] == DirObject.File:
                data['size'] = p.stat(follow_symlinks=True).st_size
            children[p.name] = data
        return children

    async def sync_dir(self, path: Path) -> None:
        LOG.info("SYNC %s", path)
        # check if dir exists
        ret = await self.get_children(str(path.parent))
        if path.name not in ret:
            await self.mkdir(str(path))
            children = {}
        else:
            children = await self.get_children(str(path))

        # check contents
        expected_children = self.get_local_children(path)
        LOG.debug('expected children: %s', expected_children)
        LOG.debug('actual children: %s', children)

        # delete prev failed uploads
        async with asyncio.TaskGroup() as tg:
            for name in sorted(children):
                if name.startswith('_upload_'):
                    tg.create_task(_as_task(self.rmfile(str(path / name))))

        # now upload as necessary
        async with asyncio.TaskGroup() as tg:
            for name in sorted(expected_children):
                if name in children:
                    # verify size at least
                    e = expected_children[name]
                    c = children[name]
                    if e['type'] != c['type']:
                        LOG.error('Bad type on %s', path / name)
                        await self.rmtree(Path(path / name))
                    elif e['type'] == DirObject.File and e.get('size', -1) == c.get('size', -1):
                        LOG.info('verified %s', path / name)
                        continue
                else:
                    LOG.info('missing from dest: %s', path / name)

                if expected_children[name]['type'] == DirObject.Directory:
                    tg.create_task(_as_task(self.sync_dir(path / name)))
                else:
                    tg.create_task(_as_task(self.put_file(str(path / name))))

    @connection_semaphore
    async def mkdir_p(self, path: str, request_timeout: int = 60) -> None:
        LOG.info('MKDIR -p %s', path)
        dest_base = Path(self.config["DEST_BASE_PATH"])
        #  fullpath = dest_base / path.lstrip('/')
        fullpath = Path(self.config["DEST_BASE_PATH"]) / path.lstrip('/')
        # Break into components
        parts = fullpath.parts
        # We assume DEST_BASE_PATH exists
        base_parts = dest_base.parts
        missing_parts: list[str] = []

        self.rc._get_token()
        token = _decode_if_necessary(self.rc.access_token)
        headers = {
            'Authorization': f'bearer {token}',
            'Depth': '0',
        }

        # Walk from full path up to base to find the first existing directory
        for i in range(len(parts), len(base_parts), -1):
            candidate = Path(*parts[:i])
            url = f'{self.config["DEST_URL"]}{candidate}'
            req = HTTPRequest(
                method='PROPFIND',
                url=url,
                headers=headers,
                request_timeout=request_timeout,
            )
            try:
                await self.http_client.fetch(req)
                # If PROPFIND succeeds, we found the highest existing parent
                break
            except HTTPError as e:
                if e.code in (404, 405):
                    # Does not exist, add to missing
                    missing_parts.insert(0, parts[i - 1])
                else:
                    raise MkdirPathDiscoveryError(candidate, e)
        else:
            # If we got here, none of the ancestors existed, which shouldn't happen
            raise MkdirBasePathDoesNotExistError(dest_base)

        # Build up the path incrementally
        current = Path(*parts[:i])
        for part in missing_parts:
            current = current / part
            url = f'{self.config["DEST_URL"]}{current}'
            req = HTTPRequest(
                method='MKCOL',
                url=url,
                headers={'Authorization': f'bearer {token}'},
                request_timeout=request_timeout,
            )
            try:
                await self.http_client.fetch(req)
                LOG.info('Created directory %s', current)
            except HTTPError as e:
                if e.code in (405, 409):
                    # Already exists or conflict—ignore
                    LOG.info('Directory %s already exists', current)
                    continue
                else:
                    raise MkdirDirectoryCreationError(current, e)

    # @connection_semaphore
    # async def put_file_src_dest(self, src_path: str, dest_path: str, request_timeout: int = 1200) -> None:
    #     """
    #     Uploads file to a tmp name first, checks the checksum, then
    #     moves it to the final location.
    #     """
    #     logging.info('PUT %s', dest_path)
    #     fullpath = Path(self.config["DEST_BASE_PATH"]) / dest_path.lstrip('/')
    #     uploadpath = fullpath.with_name('_upload_' + fullpath.name)
    #     self.rc._get_token()
    #     token = _decode_if_necessary(self.rc.access_token)
    #     filesize = Path(src_path).stat(follow_symlinks=True).st_size
    #     headers = {
    #         'Authorization': f'bearer {token}',
    #         'Content-Length': str(filesize),
    #         'Want-Digest': 'SHA-512',
    #         'Expect': '100-continue',
    #     }
    #     # give ourselves a minimum of 10 minutes per GB
    #     request_timeout = max(request_timeout, int(filesize / 10**9) * 600)

    #     with open(src_path, 'rb') as f:
    #         def seek(offset: int, _origin: int) -> int:
    #             try:
    #                 f.seek(offset)
    #                 return pycurl.SEEKFUNC_OK
    #             except Exception:
    #                 return pycurl.SEEKFUNC_FAIL

    #         def cb(c: pycurl.Curl) -> None:
    #             setup_curl = bind_setup_curl(self.config)
    #             setup_curl(c)
    #             if filesize >= 2000000000:
    #                 # c.unsetopt(pycurl.INFILESIZE)
    #                 c.setopt(pycurl.INFILESIZE_LARGE, filesize)
    #             else:
    #                 c.setopt(pycurl.INFILESIZE, filesize)
    #             c.setopt(pycurl.READDATA, f)
    #             c.setopt(pycurl.SEEKFUNCTION, seek)

    #         upload_url = f'{self.config["DEST_URL"]}{uploadpath}'
    #         LOG.info(f"PUT {upload_url} (timeout={request_timeout})")
    #         req = HTTPRequest(
    #             method='PUT',
    #             url=upload_url,
    #             headers=headers,
    #             request_timeout=request_timeout,
    #             prepare_curl_callback=cb,
    #         )
    #         ret = await self.http_client.fetch(req)

    #     checksum = ret.headers.get('Digest', None)
    #     expected_checksum = sha512sum(Path(src_path))
    #     if checksum:
    #         # we got a checksum back, so compare that directly
    #         checksum = convert_checksum_from_dcache(checksum)
    #     else:
    #         # read back file, and run checksum manually
    #         logging.info("PUT %s - no checksum in headers, so get manually", dest_path)
    #         hasher = hashlib.sha512()
    #         req = HTTPRequest(
    #             method='GET',
    #             url=f'{self.config["DEST_URL"]}{uploadpath}',
    #             headers=headers,
    #             request_timeout=request_timeout,
    #             streaming_callback=hasher.update,
    #         )
    #         await self.http_client.fetch(req)
    #         checksum = hasher.hexdigest()

    #     if expected_checksum == checksum:
    #         logging.info("PUT %s complete - checksum successful!", dest_path)
    #     else:
    #         logging.error('PUT %s - bad checksum. expected %s, but received %s', dest_path, expected_checksum, checksum)
    #         raise RuntimeError('bad checksum!')

    #     self.rc._get_token()
    #     token = _decode_if_necessary(self.rc.access_token)
    #     headers = {
    #         'Authorization': f'bearer {token}',
    #         'Destination': str(fullpath),
    #     }
    #     req = HTTPRequest(
    #         method='MOVE',
    #         url=f'{self.config["DEST_URL"]}{uploadpath}',
    #         headers=headers,
    #         request_timeout=request_timeout,
    #         prepare_curl_callback=bind_setup_curl(self.config),
    #     )
    #     await self.http_client.fetch(req)
    @connection_semaphore
    async def put_file_src_dest(
        self,
        src_path: str,
        dest_path: str,
        request_timeout: int = 1200,
    ) -> None:
        """
        Upload a file to a temporary name, verify its checksum, and move it
        to its final location.
        """
        LOG.info("PUT %s", dest_path)

        source = anyio.Path(src_path)
        fullpath = anyio.Path(self.config["DEST_BASE_PATH"]) / dest_path.lstrip("/")
        uploadpath = fullpath.with_name(f"_upload_{fullpath.name}")

        source_stat = await source.stat(follow_symlinks=True)
        filesize = source_stat.st_size

        self.rc._get_token()
        token = _decode_if_necessary(self.rc.access_token)

        headers = {
            "Authorization": f"bearer {token}",
            "Content-Length": str(filesize),
            "Want-Digest": "SHA-512",
        }

        # Give ourselves a minimum of ten minutes per GB.
        request_timeout = max(
            request_timeout,
            int(filesize / 10**9) * 600,
        )

        upload_url = f'{self.config["DEST_URL"]}{uploadpath}'
        LOG.info("PUT %s (timeout=%d)", upload_url, request_timeout)

        upload_request = HTTPRequest(
            method="PUT",
            url=upload_url,
            headers=headers,
            request_timeout=request_timeout,
            body_producer=make_file_body_producer(source),
            expect_100_continue=True,
        )

        # body_producer is supported by SimpleAsyncHTTPClient, not curl_httpclient.
        upload_client = SimpleAsyncHTTPClient(
            force_instance=True,
        )

        try:
            response = await upload_client.fetch(upload_request)
        finally:
            upload_client.close()

        expected_checksum = await sha512sum_async(source)

        checksum = response.headers.get("Digest")
        if checksum is not None:
            checksum = convert_checksum_from_dcache(checksum)
        else:
            LOG.info(
                "PUT %s - no checksum in headers, so get manually",
                dest_path,
            )

            hasher = hashlib.sha512()
            checksum_request = HTTPRequest(
                method="GET",
                url=f'{self.config["DEST_URL"]}{uploadpath}',
                headers={
                    "Authorization": f"bearer {token}",
                },
                request_timeout=request_timeout,
                streaming_callback=hasher.update,
            )

            await self.http_client.fetch(checksum_request)
            checksum = hasher.hexdigest()

        if expected_checksum != checksum:
            LOG.error(
                "PUT %s - bad checksum. expected %s, but received %s",
                dest_path,
                expected_checksum,
                checksum,
            )
            raise UploadChecksumMismatchError(
                dest_path,
                expected_checksum,
                checksum,
            )

        LOG.info(
            "PUT %s complete - checksum successful!",
            dest_path,
        )

        self.rc._get_token()
        token = _decode_if_necessary(self.rc.access_token)

        move_request = HTTPRequest(
            method="MOVE",
            url=f'{self.config["DEST_URL"]}{uploadpath}',
            headers={
                "Authorization": f"bearer {token}",
                "Destination": str(fullpath),
            },
            request_timeout=request_timeout,
            prepare_curl_callback=bind_setup_curl(self.config),
        )

        await self.http_client.fetch(move_request)

    @connection_semaphore
    async def put_path(self, src_path: str, dest_path: str, request_timeout: int = 1200) -> None:
        """
        Ensures that the parent directory exists, then uploads the
        file to the final location.
        """
        dest_dir = str(Path(dest_path).parent)
        LOG.info(f"Ensuring {dest_dir} exists at destination")
        await self.mkdir_p(dest_dir, request_timeout)
        LOG.info(f"Uploading {src_path} -> {dest_path}")
        await self.put_file_src_dest(src_path, dest_path, request_timeout)
