# test_square.py

from lta.config import from_environment
import pytest


@pytest.mark.xfail(raises=TypeError, strict=True)
def test_from_environment_none():
    """
    Fail with a TypeError if we don't ask for any environment variables.
    """
    from_environment(None)


@pytest.mark.xfail(raises=TypeError, strict=True)
def test_from_environment_scalar():
    """
    Fail with a TypeError if we don't ask for any environment variables.
    """
    from_environment(50)


@pytest.mark.xfail(raises=TypeError, strict=True)
def test_from_environment_scalar_list():
    """
    Fail with a TypeError if we don't ask for any environment variables.
    """
    from_environment([10, 20, 30, 40, 50])


@pytest.mark.xfail(raises=OSError, strict=True)
def test_from_environment_missing(monkeypatch):
    """
    Fail with an OSError if we ask for an environment variable that does not
    exist.
    """
    monkeypatch.delenv("PAN_GALACTIC_GARGLE_BLASTER", raising=False)
    from_environment("PAN_GALACTIC_GARGLE_BLASTER")


@pytest.mark.xfail(raises=OSError, strict=True)
def test_from_environment_missing_list(monkeypatch):
    """
    Fail with an OSError if we ask for an environment variable that does not
    exist on a list that we provide.
    """
    monkeypatch.delenv("PAN_GALACTIC_GARGLE_BLASTER", raising=False)
    from_environment(["PAN_GALACTIC_GARGLE_BLASTER"])


def test_from_environment_empty():
    """
    Return an empty dictionary if we ask for no environment variables.
    """
    obj = from_environment([])
    assert len(obj.keys()) == 0


def test_from_environment_key(monkeypatch):
    """
    Return a dictionary with a single environment variable.
    """
    monkeypatch.setenv("LANGUAGE", "ja_JP")
    obj = from_environment("LANGUAGE")
    assert len(obj.keys()) == 1
    assert obj["LANGUAGE"] == "ja_JP"


def test_from_environment_list(monkeypatch):
    """
    Return a dictionary with a list of environment variables.
    """
    monkeypatch.setenv("HOME", "/home/tux")
    monkeypatch.setenv("LANGUAGE", "ja_JP")
    obj = from_environment(["HOME", "LANGUAGE"])
    assert len(obj.keys()) == 2
    assert obj["HOME"] == "/home/tux"
    assert obj["LANGUAGE"] == "ja_JP"
