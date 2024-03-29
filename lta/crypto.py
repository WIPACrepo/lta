# crypto.py
"""Module that provides cryptographic support services."""

import hashlib
from typing import Dict
import zlib


# Adapted from sha512sum below; smh .tobytes()
def adler32sum(filename: str) -> str:
    """Compute the adler32 checksum of the data in the specified file."""
    value = 1
    b = bytearray(128 * 1024)
    mv = memoryview(b)
    with open(filename, 'rb', buffering=0) as f:
        # Known issue with MyPy: https://github.com/python/typeshed/issues/2166
        for n in iter(lambda: f.readinto(mv), 0):
            value = zlib.adler32(mv[:n].tobytes(), value)
    return ("%08X" % (value & 0xffffffff)).lower()


# Adapted from: https://stackoverflow.com/a/44873382
def sha512sum(filename: str) -> str:
    """Compute the SHA512 hash of the data in the specified file."""
    h = hashlib.sha512()
    b = bytearray(128 * 1024)
    mv = memoryview(b)
    with open(filename, 'rb', buffering=0) as f:
        # Known issue with MyPy: https://github.com/python/typeshed/issues/2166
        for n in iter(lambda: f.readinto(mv), 0):
            h.update(mv[:n])
    return h.hexdigest()


# A combination of the adler32sum and sha512sum above; so we read the data once
def lta_checksums(filename: str) -> Dict[str, str]:
    """Compute the adler32 and SHA512 hash of the data in the specified file."""
    value = 1
    h = hashlib.sha512()
    b = bytearray(128 * 1024)
    mv = memoryview(b)
    with open(filename, 'rb', buffering=0) as f:
        # Known issue with MyPy: https://github.com/python/typeshed/issues/2166
        for n in iter(lambda: f.readinto(mv), 0):
            value = zlib.adler32(mv[:n].tobytes(), value)
            h.update(mv[:n])
    return {
        "adler32": ("%08X" % (value & 0xffffffff)).lower(),
        "sha512": h.hexdigest(),
    }
