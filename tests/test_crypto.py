# test_crypto.py
"""Unit tests for lta/crypto.py."""

import os
from tempfile import NamedTemporaryFile

from lta.crypto import sha512sum


def test_sha512sum_tempfile(mocker):
    """Test that sha512sum hashes a temporary file correctly."""
    with NamedTemporaryFile(mode="wb", delete=False) as temp:
        temp.write(bytearray("The quick brown fox jumps over the lazy dog\n", "utf8"))
        temp.close()
    hashsum = sha512sum(temp.name)
    assert hashsum == "a12ac6bdd854ac30c5cc5b576e1ee2c060c0d8c2bec8797423d7119aa2b962f7f30ce2e39879cbff0109c8f0a3fd9389a369daae45df7d7b286d7d98272dc5b1"
    os.remove(temp.name)
