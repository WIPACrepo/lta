# test_joiner.py
"""Unit tests for lta/joiner.py."""

# fmt:off

from lta.joiner import join_smart, join_smart_url


def test_join_smart_empty() -> None:
    """Test join_smart functionality."""
    assert join_smart([]) == "."


def test_join_smart_many_relative() -> None:
    """Test join_smart functionality."""
    assert join_smart(["data", "exp", "IceCube"]) == "data/exp/IceCube"


def test_join_smart_many_absolute() -> None:
    """Test join_smart functionality."""
    assert join_smart(["/data", "/exp", "/IceCube"]) == "/data/exp/IceCube"


def test_join_smart_many_absolute_trailing_slashes() -> None:
    """Test join_smart functionality."""
    assert join_smart(["/data/", "/exp/", "/IceCube/"]) == "/data/exp/IceCube/"


def test_join_smart_url() -> None:
    """Test join_smart functionality."""
    CORRECT = "gsiftp://gridftp.zeuthen.desy.de:2811/pnfs/ifh.de/acs/icecube/archive/mnt/lfss/jade-lta/bundler_out/fdd3c3865d1011eb97bb6224ddddaab7.zip"
    assert join_smart_url(["gsiftp://gridftp.zeuthen.desy.de:2811/pnfs/ifh.de/acs/icecube/archive/",
                           "/mnt/lfss/jade-lta/bundler_out/fdd3c3865d1011eb97bb6224ddddaab7.zip"]) == CORRECT


def test_join_smart_url_with_path_and_basename() -> None:
    """Test join_smart functionality."""
    CORRECT = "gsiftp://gridftp.zeuthen.desy.de:2811/pnfs/ifh.de/acs/icecube/archive/data/exp/IceCube/2015/filtered/level2/0320/fdd3c3865d1011eb97bb6224ddddaab7.zip"
    assert join_smart_url(["gsiftp://gridftp.zeuthen.desy.de:2811/pnfs/ifh.de/acs/icecube/archive/",
                           "/data/exp/IceCube/2015/filtered/level2/0320",
                           "fdd3c3865d1011eb97bb6224ddddaab7.zip"]) == CORRECT


def test_join_smart_desy_bundle_path() -> None:
    """Test join_smart functionality."""
    CORRECT = "/pnfs/ifh.de/acs/icecube/archive/data/exp/IceCube/2015/filtered/level2/0320/604b6c80659c11eb8ad66224ddddaab7.zip"
    assert join_smart(["/pnfs/ifh.de/acs/icecube/archive",
                       "/data/exp/IceCube/2015/filtered/level2/0320",
                       "604b6c80659c11eb8ad66224ddddaab7.zip"]) == CORRECT
    assert join_smart(["/pnfs/ifh.de/acs/icecube/archive/",
                       "/data/exp/IceCube/2015/filtered/level2/0320",
                       "604b6c80659c11eb8ad66224ddddaab7.zip"]) == CORRECT
    assert join_smart(["/pnfs/ifh.de/acs/icecube/archive",
                       "/data/exp/IceCube/2015/filtered/level2/0320/",
                       "604b6c80659c11eb8ad66224ddddaab7.zip"]) == CORRECT
    assert join_smart(["/pnfs/ifh.de/acs/icecube/archive/",
                       "/data/exp/IceCube/2015/filtered/level2/0320/",
                       "604b6c80659c11eb8ad66224ddddaab7.zip"]) == CORRECT
    assert join_smart(["/pnfs/ifh.de/acs/icecube/archive",
                       "/data/exp/IceCube/2015/filtered/level2/0320",
                       "/604b6c80659c11eb8ad66224ddddaab7.zip"]) == CORRECT
