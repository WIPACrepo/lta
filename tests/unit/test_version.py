# test_version.py
"""Unit tests for lta/version.py."""

# fmt:off

from lta.version import __version__, __version_tuple__, version, version_tuple


def test_always_succeed() -> None:
    """Canary test to verify test framework is operating properly."""
    assert True


def test_version() -> None:
    """Test that version is actually defined."""
    assert not (version is None)
    assert isinstance(version, str)
    assert __version__ == version


def test_version_tuple() -> None:
    """Test that version_tuple is actually defined."""
    assert not (version_tuple is None)
    assert isinstance(version_tuple, tuple)
    assert __version_tuple__ == version_tuple
