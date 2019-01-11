# test_square.py
"""Module to provide a simple example of tests."""

import pytest
from lta.square import square


@pytest.mark.parameterize("x, expected", [
    pytest.param(-3, 9),
    pytest.param(-2, 4),
    pytest.param(-1, 1),
    pytest.param(0, 0),
    pytest.param(1, 1),
    pytest.param(2, 4),
    pytest.param(3, 9)
])
def test_square(x, expected) -> None:
    """Ensure that square() returns the proper square of provided numbers."""
    assert square(x) == expected
