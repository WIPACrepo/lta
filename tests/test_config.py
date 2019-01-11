# test_config.py
"""Unit tests for lta/config.py."""

from lta.config import from_environment
import pytest


def test_from_environment_none():
    """Fail with a TypeError if we don't ask for any environment variables."""
    with pytest.raises(TypeError):
        from_environment(None)


def test_from_environment_scalar():
    """Fail with a TypeError if we don't ask for any environment variables."""
    with pytest.raises(TypeError):
        from_environment(50)


def test_from_environment_scalar_list():
    """Fail with a TypeError if we don't ask for any environment variables."""
    with pytest.raises(TypeError):
        from_environment([10, 20, 30, 40, 50])


def test_from_environment_missing(monkeypatch):
    """Fail with an OSError if we ask for an environment variable that does not exist."""
    with pytest.raises(OSError):
        monkeypatch.delenv("PAN_GALACTIC_GARGLE_BLASTER", raising=False)
        from_environment("PAN_GALACTIC_GARGLE_BLASTER")


def test_from_environment_missing_list(monkeypatch):
    """Fail with an OSError if we ask for an environment variable that does not exist on a list that we provide."""
    with pytest.raises(OSError):
        monkeypatch.delenv("PAN_GALACTIC_GARGLE_BLASTER", raising=False)
        from_environment(["PAN_GALACTIC_GARGLE_BLASTER"])


def test_from_environment_empty():
    """Return an empty dictionary if we ask for no environment variables."""
    obj = from_environment([])
    assert len(obj.keys()) == 0


def test_from_environment_key(monkeypatch):
    """Return a dictionary with a single environment variable."""
    monkeypatch.setenv("LANGUAGE", "ja_JP")
    obj = from_environment("LANGUAGE")
    assert len(obj.keys()) == 1
    assert obj["LANGUAGE"] == "ja_JP"


def test_from_environment_list(monkeypatch):
    """Return a dictionary with a list of environment variables."""
    monkeypatch.setenv("HOME", "/home/tux")
    monkeypatch.setenv("LANGUAGE", "ja_JP")
    obj = from_environment(["HOME", "LANGUAGE"])
    assert len(obj.keys()) == 2
    assert obj["HOME"] == "/home/tux"
    assert obj["LANGUAGE"] == "ja_JP"


def test_from_environment_dict(monkeypatch):
    """Return a dictionary where we override one default but leave the other."""
    EXPECTED_CONFIG = {
        'HOME': '/home/tux',
        'LANGUAGE': 'en_US'
    }
    monkeypatch.delenv("HOME", raising=False)
    monkeypatch.setenv("LANGUAGE", "ja_JP")
    obj = from_environment(EXPECTED_CONFIG)
    assert len(obj.keys()) == 2
    assert obj["HOME"] == "/home/tux"
    assert obj["LANGUAGE"] == "ja_JP"


def test_from_environment_dict_required(monkeypatch):
    """Raise an error where we require the environment to provide a value."""
    with pytest.raises(OSError):
        EXPECTED_CONFIG = {
            'HOME': None,
            'LANGUAGE': 'en_US'
        }
        monkeypatch.delenv("HOME", raising=False)
        monkeypatch.setenv("LANGUAGE", "ja_JP")
        from_environment(EXPECTED_CONFIG)
