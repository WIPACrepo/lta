# sync.py
"""Transfer implementation using WebDAV to copy files to DESY."""

import asyncio
import base64
from enum import Enum
from functools import wraps
import hashlib
import logging
from pathlib import Path
from typing import Any, Awaitable, Callable, cast, Concatenate, Coroutine, Optional, ParamSpec, TypeVar, Union
import xml.etree.ElementTree as ET
from xml.etree.ElementTree import Element

import pycurl
from rest_tools.client import ClientCredentialsAuth
from tornado.httpclient import AsyncHTTPClient, HTTPError, HTTPRequest

LOG = logging.getLogger(__name__)

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


class DirObject(Enum):
    Directory = 1
    File = 2


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
    if checksum.startswith('sha-512='):
        checksum = checksum[8:]
    return base64.b64decode(checksum).hex()


def _decode_if_necessary(value: Optional[Union[str, bytes]]) -> Optional[str]:
    if value is None:
        return None
    if isinstance(value, str):
        return value
    if isinstance(value, bytes):
        return value.decode("utf-8")
    raise TypeError(f"Expected str or bytes or None, got {type(value).__name__}")


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
        # self._semaphore = asyncio.Semaphore(int(config["MAX_PARALLEL"]))
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
        logging.debug(content)
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
                    if len(props) > 5:
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

    @connection_semaphore
    async def get_file(self, path: str, timeout: int = 1200) -> None:
        fullpath = Path(self.config["DEST_BASE_PATH"]) / path.lstrip('/')
        self.rc._get_token()
        token = _decode_if_necessary(self.rc.access_token)
        headers = {
            'Authorization': f'bearer {token}',
        }
        with open(path, 'wb') as f:
            def write_callback(data: bytes) -> None:
                f.write(data)

            req = HTTPRequest(
                method='GET',
                url=f'{self.config["DEST_URL"]}{fullpath}',
                headers=headers,
                request_timeout=timeout,
                streaming_callback=write_callback,
            )
            await self.http_client.fetch(req)

    @connection_semaphore
    async def rmfile(self, path: str, timeout: int = 600) -> None:
        logging.info('RMFILE %s', path)
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
            request_timeout=timeout,
        )
        await self.http_client.fetch(req)

    @connection_semaphore
    async def rmtree(self, path: Path, timeout: int = 600) -> None:
        logging.info('RMTREE %s', path)
        ret = await self.get_children(str(path.parent))
        if path.name not in ret:
            logging.info("does not exist")
        elif ret[path.name]['type'] == DirObject.File:
            await self.rmfile(str(path))
        else:
            children = await self.get_children(str(path))
            async with asyncio.TaskGroup() as tg:
                for child in children.values():
                    if child['type'] == DirObject.File:
                        tg.create_task(_as_task(self.rmfile(str(path / child['name']), timeout)))
                    else:
                        tg.create_task(_as_task(self.rmtree(path / child['name'], timeout)))
            await self.rmfile(str(path))

    @connection_semaphore
    async def mkdir(self, path: str, timeout: int = 60) -> None:
        logging.info('MKDIR %s', path)
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
            request_timeout=timeout,
        )
        await self.http_client.fetch(req)

    @connection_semaphore
    async def put_file(self, path: str, timeout: int = 1200) -> None:
        """
        Uploads file to a tmp name first, checks the checksum, then
        moves it to the final location.
        """
        logging.info('PUT %s', path)
        fullpath = Path(self.config["DEST_BASE_PATH"]) / path.lstrip('/')
        uploadpath = fullpath.with_name('_upload_' + fullpath.name)
        self.rc._get_token()
        token = _decode_if_necessary(self.rc.access_token)
        filesize = Path(path).stat(follow_symlinks=True).st_size
        headers = {
            'Authorization': f'bearer {token}',
            'Content-Length': str(filesize),
            'Want-Digest': 'SHA-512',
            'Expect': '100-continue',
        }

        with open(path, 'rb') as f:
            def seek(offset: int, _origin: int) -> int:
                try:
                    f.seek(offset)
                    return pycurl.SEEKFUNC_OK
                except Exception:
                    return pycurl.SEEKFUNC_FAIL

            def cb(c: pycurl.Curl) -> None:
                setup_curl = bind_setup_curl(self.config)
                setup_curl(c)
                if filesize >= 2000000000:
                    # c.unsetopt(pycurl.INFILESIZE)
                    c.setopt(pycurl.INFILESIZE_LARGE, filesize)
                else:
                    c.setopt(pycurl.INFILESIZE, filesize)
                c.setopt(pycurl.READDATA, f)
                c.setopt(pycurl.SEEKFUNCTION, seek)

            req = HTTPRequest(
                method='PUT',
                url=f'{self.config["DEST_URL"]}{uploadpath}',
                headers=headers,
                request_timeout=timeout,
                prepare_curl_callback=cb,
            )
            ret = await self.http_client.fetch(req)

        checksum = ret.headers.get('Digest', None)
        expected_checksum = sha512sum(Path(path))
        if checksum:
            # we got a checksum back, so compare that directly
            checksum = convert_checksum_from_dcache(checksum)
        else:
            # read back file, and run checksum manually
            logging.info("PUT %s - no checksum in headers, so get manually", path)
            hasher = hashlib.sha512()
            req = HTTPRequest(
                method='GET',
                url=f'{self.config["DEST_URL"]}{uploadpath}',
                headers=headers,
                request_timeout=timeout,
                streaming_callback=hasher.update,
            )
            await self.http_client.fetch(req)
            checksum = hasher.hexdigest()

        if expected_checksum == checksum:
            logging.info("PUT %s complete - checksum successful!", path)
        else:
            logging.error('PUT %s - bad checksum. expected %s, but received %s', path, expected_checksum, checksum)
            raise RuntimeError('bad checksum!')

        self.rc._get_token()
        token = _decode_if_necessary(self.rc.access_token)
        headers = {
            'Authorization': f'bearer {token}',
            'Destination': str(fullpath),
        }
        req = HTTPRequest(
            method='MOVE',
            url=f'{self.config["DEST_URL"]}{uploadpath}',
            headers=headers,
            request_timeout=timeout,
            prepare_curl_callback=bind_setup_curl(self.config),
        )
        await self.http_client.fetch(req)

    def get_local_children(self, path: Path) -> DataDict:
        children = {}
        for p in path.iterdir():
            if p.name.startswith("Run") and '_' in p.name:
                logging.debug('skipping versioned run directory')
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
        logging.info("SYNC %s", path)
        # check if dir exists
        ret = await self.get_children(str(path.parent))
        if path.name not in ret:
            await self.mkdir(str(path))
            children = {}
        else:
            children = await self.get_children(str(path))

        # check contents
        expected_children = self.get_local_children(path)
        logging.debug('expected children: %s', expected_children)
        logging.debug('actual children: %s', children)

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
                        logging.error('Bad type on %s', path / name)
                        await self.rmtree(Path(path / name))
                    elif e['type'] == DirObject.File and e.get('size', -1) == c.get('size', -1):
                        logging.info('verified %s', path / name)
                        continue
                else:
                    logging.info('missing from dest: %s', path / name)

                if expected_children[name]['type'] == DirObject.Directory:
                    tg.create_task(_as_task(self.sync_dir(path / name)))
                else:
                    tg.create_task(_as_task(self.put_file(str(path / name))))

    @connection_semaphore
    async def mkdir_p(self, path: str, timeout: int = 60) -> None:
        logging.info('MKDIR -p %s', path)
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
                request_timeout=timeout,
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
                    raise Exception(f'Unexpected error checking {candidate}: {e}')
        else:
            # If we got here, none of the ancestors existed, which shouldn't happen
            raise Exception(f'Base path {dest_base} does not exist on remote.')

        # Build up the path incrementally
        current = Path(*parts[:i])
        for part in missing_parts:
            current = current / part
            url = f'{self.config["DEST_URL"]}{current}'
            req = HTTPRequest(
                method='MKCOL',
                url=url,
                headers={'Authorization': f'bearer {token}'},
                request_timeout=timeout,
            )
            try:
                await self.http_client.fetch(req)
                logging.info('Created directory %s', current)
            except HTTPError as e:
                if e.code in (405, 409):
                    # Already exists or conflict—ignore
                    logging.info('Directory %s already exists', current)
                    continue
                else:
                    raise Exception(f'Error creating directory {current}: {e}')

    @connection_semaphore
    async def put_file_src_dest(self, src_path: str, dest_path: str, timeout: int = 1200) -> None:
        """
        Uploads file to a tmp name first, checks the checksum, then
        moves it to the final location.
        """
        logging.info('PUT %s', dest_path)
        fullpath = Path(self.config["DEST_BASE_PATH"]) / dest_path.lstrip('/')
        uploadpath = fullpath.with_name('_upload_' + fullpath.name)
        self.rc._get_token()
        token = _decode_if_necessary(self.rc.access_token)
        filesize = Path(src_path).stat(follow_symlinks=True).st_size
        headers = {
            'Authorization': f'bearer {token}',
            'Content-Length': str(filesize),
            'Want-Digest': 'SHA-512',
            'Expect': '100-continue',
        }
        # give ourselves a minimum of 10 minutes per GB
        timeout = max(timeout, int(filesize / 10**9)*600)

        with open(src_path, 'rb') as f:
            def seek(offset: int, _origin: int) -> int:
                try:
                    f.seek(offset)
                    return pycurl.SEEKFUNC_OK
                except Exception:
                    return pycurl.SEEKFUNC_FAIL

            def cb(c: pycurl.Curl) -> None:
                setup_curl = bind_setup_curl(self.config)
                setup_curl(c)
                if filesize >= 2000000000:
                    # c.unsetopt(pycurl.INFILESIZE)
                    c.setopt(pycurl.INFILESIZE_LARGE, filesize)
                else:
                    c.setopt(pycurl.INFILESIZE, filesize)
                c.setopt(pycurl.READDATA, f)
                c.setopt(pycurl.SEEKFUNCTION, seek)

            upload_url = f'{self.config["DEST_URL"]}{uploadpath}'
            LOG.info(f"PUT {upload_url} (timeout={timeout})")
            req = HTTPRequest(
                method='PUT',
                url=upload_url,
                headers=headers,
                request_timeout=timeout,
                prepare_curl_callback=cb,
            )
            ret = await self.http_client.fetch(req)

        checksum = ret.headers.get('Digest', None)
        expected_checksum = sha512sum(Path(src_path))
        if checksum:
            # we got a checksum back, so compare that directly
            checksum = convert_checksum_from_dcache(checksum)
        else:
            # read back file, and run checksum manually
            logging.info("PUT %s - no checksum in headers, so get manually", dest_path)
            hasher = hashlib.sha512()
            req = HTTPRequest(
                method='GET',
                url=f'{self.config["DEST_URL"]}{uploadpath}',
                headers=headers,
                request_timeout=timeout,
                streaming_callback=hasher.update,
            )
            await self.http_client.fetch(req)
            checksum = hasher.hexdigest()

        if expected_checksum == checksum:
            logging.info("PUT %s complete - checksum successful!", dest_path)
        else:
            logging.error('PUT %s - bad checksum. expected %s, but received %s', dest_path, expected_checksum, checksum)
            raise RuntimeError('bad checksum!')

        self.rc._get_token()
        token = _decode_if_necessary(self.rc.access_token)
        headers = {
            'Authorization': f'bearer {token}',
            'Destination': str(fullpath),
        }
        req = HTTPRequest(
            method='MOVE',
            url=f'{self.config["DEST_URL"]}{uploadpath}',
            headers=headers,
            request_timeout=timeout,
            prepare_curl_callback=bind_setup_curl(self.config),
        )
        await self.http_client.fetch(req)

    @connection_semaphore
    async def put_path(self, src_path: str, dest_path: str, timeout: int = 1200) -> None:
        """
        Ensures that the parent directory exists, then uploads the
        file to the final location.
        """
        dest_dir = str(Path(dest_path).parent)
        LOG.info(f"Ensuring {dest_dir} exists at destination")
        await self.mkdir_p(dest_dir, timeout)
        LOG.info(f"Uploading {src_path} -> {dest_path}")
        await self.put_file_src_dest(src_path, dest_path, timeout)
