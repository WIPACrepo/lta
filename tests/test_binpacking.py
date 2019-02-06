# test_binpacking.py
"""Test the binpacking package from PyPI."""

import binpacking  # type: ignore
from math import floor
import pytest  # type: ignore
from random import random
from typing import List, Tuple
from uuid import uuid1


@pytest.fixture
def size_list() -> List[int]:
    """Return a list of files sizes that approximate a PFFilt directory."""
    # add typical PFFilt file sizes
    NUM_LARGE = 900
    BASE_SIZE = 101_000_000
    RAND_SIZE = 4_000_000
    file_size_list = []
    for i in range(1, NUM_LARGE):
        file_size_list.append(floor(BASE_SIZE + (RAND_SIZE * random())))
    # add a couple small last-in-the-run files
    NUM_SMALL = 3
    SMALL_BASE_SIZE = 10_000_000
    SMALL_RAND_SIZE = 5_000_000
    for i in range(1, NUM_SMALL):
        file_size_list.append(floor(SMALL_BASE_SIZE + (SMALL_RAND_SIZE * random())))
    # return the list to the caller
    return file_size_list


@pytest.fixture
def tuple_list(size_list) -> List[Tuple[str, int]]:
    """Return a list of file tuples that approximate a PFFilt directory."""
    file_tuple_list = []
    for size in size_list:
        file_tuple_list.append((uuid1().hex, size))
    return file_tuple_list


def test_packing(size_list) -> None:
    """Check to see how many 1 GB bundles we should build."""
    BUNDLE_SIZE = 1_000_000_000
    bins = binpacking.to_constant_volume(size_list, BUNDLE_SIZE)
    assert len(bins) == 100


def test_tuple_packing(tuple_list) -> None:
    """Check to see how many 1 GB bundles we should build."""
    BUNDLE_SIZE = 1_000_000_000
    bins = binpacking.to_constant_volume(tuple_list, BUNDLE_SIZE, weight_pos=1)
    assert len(bins) == 100
