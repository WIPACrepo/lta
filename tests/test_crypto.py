# test_crypto.py
"""Unit tests for lta/crypto.py."""

import os
from tempfile import NamedTemporaryFile

from pytest_mock import MockerFixture

from lta.crypto import adler32sum, sha512sum, lta_checksums


def test_adler32sum_tempfile(mocker: MockerFixture) -> None:
    """Test that adler32sum hashes a temporary file correctly."""
    with NamedTemporaryFile(mode="wb", delete=False) as temp:
        temp.write(bytearray("The quick brown fox jumps over the lazy dog\n", "utf8"))
        temp.close()
    hashsum = adler32sum(temp.name)
    assert hashsum == "6bc00fe4"
    os.remove(temp.name)


def test_sha512sum_tempfile(mocker: MockerFixture) -> None:
    """Test that sha512sum hashes a temporary file correctly."""
    with NamedTemporaryFile(mode="wb", delete=False) as temp:
        temp.write(bytearray("The quick brown fox jumps over the lazy dog\n", "utf8"))
        temp.close()
    hashsum = sha512sum(temp.name)
    assert hashsum == "a12ac6bdd854ac30c5cc5b576e1ee2c060c0d8c2bec8797423d7119aa2b962f7f30ce2e39879cbff0109c8f0a3fd9389a369daae45df7d7b286d7d98272dc5b1"
    os.remove(temp.name)


def test_lta_checksums_tempfile(mocker: MockerFixture) -> None:
    """Test that lta_checksums hashes a temporary file correctly."""
    with NamedTemporaryFile(mode="wb", delete=False) as temp:
        temp.write(bytearray("The quick brown fox jumps over the lazy dog\n", "utf8"))
        temp.close()
    hashsum = lta_checksums(temp.name)
    assert hashsum["adler32"] == "6bc00fe4"
    assert hashsum["sha512"] == "a12ac6bdd854ac30c5cc5b576e1ee2c060c0d8c2bec8797423d7119aa2b962f7f30ce2e39879cbff0109c8f0a3fd9389a369daae45df7d7b286d7d98272dc5b1"
    os.remove(temp.name)
