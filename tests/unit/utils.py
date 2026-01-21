"""Module to provide testing utility functions, objects, etc."""

# fmt:off

from typing import Any


class ObjectLiteral:
    """
    ObjectLiteral transforms named arguments into object attributes.

    This is useful for creating object literals to be used as return
    values from mocked API calls.

    Source: https://stackoverflow.com/a/3335732
    """

    def __init__(self, **kwds: Any) -> None:
        """Add attributes to ourself with the provided named arguments."""
        self.__dict__.update(kwds)
