# test_util.py
"""Module to provide testing utility functions, objects, etc."""

from unittest.mock import MagicMock


class AsyncMock(MagicMock):
    """
    AsyncMock is the async version of a MagicMock.

    We use this class in place of MagicMock when we want to mock
    asynchronous callables.

    Source: https://stackoverflow.com/a/32498408
    """

    async def __call__(self, *args, **kwargs):
        """Allow MagicMock to work its magic too."""
        return super(AsyncMock, self).__call__(*args, **kwargs)


class ObjectLiteral:
    """
    ObjectLiteral transforms named arguments into object attributes.

    This is useful for creating object literals to be used as return
    values from mocked API calls.

    Source: https://stackoverflow.com/a/3335732
    """

    def __init__(self, **kwds):
        """Add attributes to ourself with the provided named arguments."""
        self.__dict__.update(kwds)
