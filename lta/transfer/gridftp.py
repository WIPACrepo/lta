# gridftp.py
"""Module provides an interface to GridFTP command-line interface."""

from collections import namedtuple
from datetime import datetime
import hashlib
import logging
import os
import shutil
import subprocess
import tempfile
from typing import Any, List, Optional, Tuple, Union

File = namedtuple('File', ['directory', 'perms', 'subfiles', 'owner', 'group', 'size', 'date', 'name'])
logger = logging.getLogger('gridftp')

def _cmd(cmd: List[str], timeout: int = 1200) -> None:
    completed_process = subprocess.run(cmd, timeout=timeout, check=False, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    # if our command failed
    if completed_process.returncode != 0:
        logger.info(f"GridFTP._cmd Command failed: {completed_process.args}")
        logger.info(f"returncode: {completed_process.returncode}")
        logger.info(f"stdout: {str(completed_process.stdout)}")
        logger.info(f"stderr: {str(completed_process.stderr)}")

def _cmd_output(cmd: List[str], timeout: int = 1200) -> Tuple[int, str]:
    p = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    try:
        output = p.communicate(timeout=timeout)[0].decode('utf-8')
        return (p.returncode, output)
    except subprocess.TimeoutExpired:
        p.kill()
        raise Exception('Request timed out')

def cksm(filename: str, type: str, buffersize: int = 16384, file: bool = True) -> Any:
    """Return checksum of file using algorithm specified."""
    if type not in ('md5', 'sha1', 'sha256', 'sha512'):
        raise Exception('cannot get checksum for type %r', type)

    try:
        digest = getattr(hashlib, type)()
    except Exception:
        raise Exception('cannot get checksum for type %r', type)

    if file and os.path.exists(filename):
        # checksum file contents
        with open(filename, 'rb') as filed:
            buffer = filed.read(buffersize)
            while buffer:
                digest.update(buffer)
                buffer = filed.read(buffersize)
    else:
        # just checksum the contents of the first argument
        digest.update(filename)
    return digest.hexdigest()

def listify(lines: str, details: bool = False, dotfiles: bool = False) -> List[Union[File, str]]:
    """Turn ls output into a list of NamedTuples."""
    out: List[Union[File, str]] = []
    if details:
        months = {'jan': 1, 'feb': 2, 'mar': 3, 'apr': 4, 'may': 5, 'jun': 6,
                  'jul': 7, 'aug': 8, 'sep': 9, 'oct': 10, 'nov': 11, 'dec': 12}
        for x in lines.split('\n'):
            if not x.strip():
                continue
            pieces = x.split()
            name = pieces[-1]
            if name.startswith('.') and not dotfiles:
                continue
            d = x[0] == 'd'
            perms = pieces[0][1:]
            year = datetime.now().year
            month = months[pieces[5].lower()]
            day = int(pieces[6])
            if ':' in pieces[7]:
                hour, minute = pieces[7].split(':')
                dt = datetime(year, month, day, int(hour), int(minute))
            else:
                year = int(pieces[7])
                dt = datetime(year, month, day)
            out.append(File(d, perms, int(pieces[1]), pieces[2], pieces[3],
                            int(pieces[4]), dt, name))
    else:
        for x in lines.split('\n'):
            if not x.strip():
                continue
            f = x.split()[-1]
            if not f.startswith('.') or dotfiles:
                out.append(f)
    return out

class GridFTP(object):
    """
    GridFTP interface to command line client.

    Example:
        GridFTP.get('gsiftp://data.icecube.wisc.edu/file',
                    filename='/path/to/file')
    """

    _timeout = 1200  # 20 min default timeout

    @classmethod
    def supported_address(cls, address: str) -> bool:
        """Return False for address types that are not supported."""
        if '://' not in address:
            return False
        addr_type = address.split(':')[0]
        if addr_type not in ('gsiftp', 'ftp'):
            return False
        return True

    @classmethod
    def address_split(cls, address: str) -> Tuple[str, str]:
        """Split an address into server/path parts."""
        pieces = address.split('://', 1)
        if '/' in pieces[1]:
            pieces2 = pieces[1].split('/', 1)
            return (pieces[0]+'://'+pieces2[0], '/'+pieces2[1])
        else:
            return (address, '/')

    @classmethod
    def get(cls, address: str, filename: Optional[str] = None, request_timeout: Optional[int] = None) -> Optional[str]:
        """
        Do a GridFTP get request.

        Either data is returned directly or filename must be defined.

        Args:
            address (str): url to get from
            filename (str): filename to write data to
            request_timeout (float): timeout in secodns

        Returns:
            str: data, if filename is not defined

        Raises:
            Exception for failure
        """
        if not cls.supported_address(address):
            raise Exception('address type not supported for address %s' % str(address))

        tmpdir = None
        if filename is None:
            tmpdir = tempfile.mkdtemp(dir=os.getcwd())
            dest = 'file:'+os.path.join(tmpdir, 'get_tmp_file')
        else:
            dest = 'file:'+filename

        cmd = ['globus-url-copy', address, dest]

        if request_timeout is None:
            timeout = cls._timeout
        else:
            timeout = request_timeout

        try:
            _cmd(cmd, timeout=timeout)
            if filename is None:
                with open(dest[5:]) as f:
                    return f.read()
        finally:
            if tmpdir:
                shutil.rmtree(tmpdir, ignore_errors=True)
        return None

    @classmethod
    def put(cls, address: str, data: Optional[str] = None, filename: Optional[str] = None, request_timeout: Optional[int] = None) -> None:
        """
        Do a GridFTP put request.

        Either data or filename must be defined.

        Args:
            address (str): url to put to
            data (str): the data to put
            filename (str): filename for data to put
            request_timeout (float): timeout in seconds

        Raises:
            Exception for failure
        """
        if not cls.supported_address(address):
            raise Exception('address type not supported for address %s' % str(address))

        tmpdir = None
        if data is not None:
            tmpdir = tempfile.mkdtemp(dir=os.getcwd())
            src = 'file:'+os.path.join(tmpdir, 'put_tmp_file')
            with open(src[5:], 'w' if isinstance(data, str) else 'wb') as f:
                f.write(data)
        elif filename is not None:
            src = 'file:'+filename
        else:
            raise Exception('Neither data or filename is defined')

        cmd = ['globus-url-copy', '-cd', src, address]

        if request_timeout is None:
            timeout = cls._timeout
        else:
            timeout = request_timeout

        try:
            _cmd(cmd, timeout=timeout)
        finally:
            if tmpdir:
                shutil.rmtree(tmpdir, ignore_errors=True)

    @classmethod
    def list(cls, address: str, request_timeout: Optional[int] = None, details: bool = False, dotfiles: bool = False) -> List[Union[File, str]]:
        """
        Do a GridFTP list request.

        Args:
            address (str): url to list
            request_timeout (float): timeout in seconds
            details (bool): result is a list of NamedTuples
            dotfiles (bool): result includes '.', '..', and other '.' files

        Returns:
           list: a list of files

        Raises:
            Exception on error
        """
        if not cls.supported_address(address):
            raise Exception('address type not supported for address %s' % str(address))

        cmd = ['uberftp', '-retry', '5', '-ls', address]

        if request_timeout is None:
            timeout = cls._timeout
        else:
            timeout = request_timeout

        ret = _cmd_output(cmd, timeout=timeout)
        if ret[0]:
            raise Exception('Error getting listing')
        return listify(ret[1], details=details, dotfiles=dotfiles)

    @classmethod
    def mkdir(cls, address: str, request_timeout: Optional[int] = None, parents: bool = False) -> None:
        """
        Make a directory on the ftp server.

        Args:
            address (str): url to directory
            request_timeout (float): timeout in seconds
            parents (bool): make parent directories as needed

        Raises:
            Exception on error
        """
        if not cls.supported_address(address):
            raise Exception('address type not supported for address %s' % str(address))

        if parents:
            # recursively make directory
            try:
                cls.mkdir(os.path.basename(address),
                          request_timeout=request_timeout, parents=True)
            except Exception:
                pass

        cmd = ['uberftp', '-retry', '5', '-mkdir', address]

        if request_timeout is None:
            timeout = cls._timeout
        else:
            timeout = request_timeout

        _cmd(cmd, timeout=timeout)

    @classmethod
    def rmdir(cls, address: str, request_timeout: Optional[int] = None) -> None:
        """
        Remove a directory on the ftp server.

        This fails if the directory is not empty.  Use :py:func:`rmtree` for
        recursive removal.

        Args:
            address (str): url to directory
            request_timeout (float): timeout in seconds

        Raises:
            Exception on error
        """
        if not cls.supported_address(address):
            raise Exception('address type not supported for address %s' % str(address))

        cmd = ['uberftp', '-retry', '5', '-rmdir', address]

        if request_timeout is None:
            timeout = cls._timeout
        else:
            timeout = request_timeout

        ret = _cmd_output(cmd, timeout=timeout)
        if ret[0] and 'No match for' not in ret[1]:
            raise Exception('Error removing dir')

    @classmethod
    def delete(cls, address: str, request_timeout: Optional[int] = None) -> None:
        """
        Delete a file on the ftp server.

        Args:
            address (str): url to file
            request_timeout (float): timeout in seconds

        Raises:
            Exception on error
        """
        if not cls.supported_address(address):
            raise Exception('address type not supported for address %s' % str(address))

        cmd = ['uberftp', '-retry', '5', '-rm', address]

        if request_timeout is None:
            timeout = cls._timeout
        else:
            timeout = request_timeout

        ret = _cmd_output(cmd, timeout=timeout)
        if ret[0] and 'No match for' not in ret[1]:
            raise Exception('Error removing dir')

    @classmethod
    def rmtree(cls, address: str, request_timeout: Optional[int] = None) -> None:
        """
        Delete a file or directory on the ftp server.

        This is recursive, like `rm -rf`.

        Args:
            address (str): url to file or directory
            request_timeout (float): timeout in seconds

        Raises:
            Exception on error
        """
        if not cls.supported_address(address):
            raise Exception('address type not supported for address %s' % str(address))

        cmd = ['uberftp', '-retry', '5', '-rm', '-r', address]

        if request_timeout is None:
            timeout = cls._timeout
        else:
            timeout = request_timeout

        ret = _cmd_output(cmd, timeout=timeout)
        if ret[0] and 'No match for' not in ret[1]:
            raise Exception('Error removing dir')

    @classmethod
    def move(cls, src: str, dest: str, request_timeout: Optional[int] = None) -> None:
        """
        Move a file on the ftp server.

        Args:
            src (str): url to source file
            dest (str): url to destination file
            request_timeout (float): timeout in seconds

        Raises:
            Exception on error
        """
        if not cls.supported_address(src):
            raise Exception('address type not supported for src %s' % str(src))
        if not cls.supported_address(dest):
            raise Exception('address type not supported for dest %s' % str(dest))

        cmd = ['uberftp', '-retry', '5', '-rename', src, cls.address_split(dest)[-1]]

        if request_timeout is None:
            timeout = cls._timeout
        else:
            timeout = request_timeout

        _cmd(cmd, timeout=timeout)

    @classmethod
    def exists(cls, address: str, request_timeout: Optional[int] = None) -> bool:
        """
        Check if a file exists on the ftp server.

        Args:
            address (str): url to file
            request_timeout (float): timeout in seconds

        Returns:
           bool: True, if the file exists on the ftp server, otherwise False

        Raises:
            Exception on error
        """
        if not cls.supported_address(address):
            raise Exception('address type not supported for address %s' % str(address))

        cmd = ['uberftp', '-retry', '5', '-size', address]

        if request_timeout is None:
            timeout = cls._timeout
        else:
            timeout = request_timeout

        ret = _cmd_output(cmd, timeout=timeout)
        return (not ret[0])

    @classmethod
    def chmod(cls, address: str, mode: str, request_timeout: Optional[int] = None) -> None:
        """
        Chmod a file on the ftp server.

        Args:
            address (str): url to file
            mode (str): mode of file
            request_timeout (float): timeout in seconds

        Raises:
            Exception on error
        """
        if not cls.supported_address(address):
            raise Exception('address type not supported for address %s' % str(address))

        cmd = ['uberftp', '-retry', '5', '-chmod', mode, address]

        if request_timeout is None:
            timeout = cls._timeout
        else:
            timeout = request_timeout

        _cmd(cmd, timeout=timeout)

    @classmethod
    def size(cls, address: str, request_timeout: Optional[int] = None) -> int:
        """
        Get the size of a file on the ftp server.

        Args:
            address (str): url to file
            request_timeout (float): timeout in seconds

        Returns:
            int: size of file in bytes

        Raises:
            Exception on error
        """
        if not cls.supported_address(address):
            raise Exception('address type not supported for address %s' % str(address))

        cmd = ['uberftp', '-retry', '5', '-size', address]

        if request_timeout is None:
            timeout = cls._timeout
        else:
            timeout = request_timeout

        ret = _cmd_output(cmd, timeout=timeout)
        if ret[0]:
            raise Exception('failed to get size')
        return int(ret[1])

    @classmethod
    def _chksum(cls, type: str, address: str, request_timeout: Optional[int] = None) -> Any:
        """Chksum is faked by redownloading the file and checksumming that."""
        if not cls.supported_address(address):
            raise Exception('address type not supported for address %s' % str(address))
        if type.endswith('sum'):
            type = type[:-3]

        tmpdir = tempfile.mkdtemp(dir=os.getcwd())
        dest = 'file:'+os.path.join(tmpdir, 'dest')

        cmd = ['globus-url-copy', address, dest]

        if request_timeout is None:
            timeout = cls._timeout
        else:
            timeout = request_timeout

        try:
            _cmd(cmd, timeout=timeout)
            if not os.path.exists(dest[5:]):
                raise Exception('failed to redownload')
            return cksm(dest[5:], type)
        finally:
            shutil.rmtree(tmpdir, ignore_errors=True)

    @classmethod
    def md5sum(cls, address: str, request_timeout: Optional[int] = None) -> Any:
        """
        Get the md5sum of a file on an ftp server.

        Args:
            address (str): url to file
            request_timeout (float): timeout in seconds

        Returns:
            str: the md5sum

        Raises:
            Exception on error
        """
        return cls._chksum('md5sum', address, request_timeout=request_timeout)

    @classmethod
    def sha1sum(cls, address: str, request_timeout: Optional[int] = None) -> Any:
        """
        Get the sha1sum of a file on an ftp server.

        Args:
            address (str): url to file
            request_timeout (float): timeout in seconds

        Returns:
            str: the sha1sum

        Raises:
            Exception on error
        """
        return cls._chksum('sha1sum', address, request_timeout=request_timeout)

    @classmethod
    def sha256sum(cls, address: str, request_timeout: Optional[int] = None) -> Any:
        """
        Get the sha256sum of a file on an ftp server.

        Args:
            address (str): url to file
            request_timeout (float): timeout in seconds

        Returns:
            str: the sha256sum

        Raises:
            Exception on error
        """
        return cls._chksum('sha256sum', address, request_timeout=request_timeout)

    @classmethod
    def sha512sum(cls, address: str, request_timeout: Optional[int] = None) -> Any:
        """
        Get the sha512sum of a file on an ftp server.

        Args:
            address (str): url to file
            request_timeout (float): timeout in seconds

        Returns:
            str: the sha512sum

        Raises:
            Exception on error
        """
        return cls._chksum('sha512sum', address, request_timeout=request_timeout)
