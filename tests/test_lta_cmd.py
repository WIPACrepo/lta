# test_lta_cmd.py
"""Unit tests for lta/lta_cmd.py."""

import pytest

from lta.lta_cmd import normalize_path


def test_normalize_path() -> None:
    """Test that normalize_path will normalize paths properly."""
    assert normalize_path("/data/exp/IceCube/2018/unbiased/PFRaw/1109") == "/data/exp/IceCube/2018/unbiased/PFRaw/1109"
    assert normalize_path("/data/exp/IceCube/2018/unbiased/PFRaw/1109/") == "/data/exp/IceCube/2018/unbiased/PFRaw/1109"
    assert normalize_path("/data/exp/IceCube/2018/unbiased/PFRaw/1109/.") == "/data/exp/IceCube/2018/unbiased/PFRaw/1109"


def test_PATH_PREFIX_ALLOW_LIST() -> None:
    """Test that normalize_path will enforce PATH_PREFIX_ALLOW_LIST."""
    with pytest.raises(ValueError):
        normalize_path("/mnt/lfs7/exp/IceCube/2018/unbiased/PFRaw/1109")
