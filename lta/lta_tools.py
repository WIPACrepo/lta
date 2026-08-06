# lta_tools.py
"""Module with some tools for components of the Long Term Archive."""

import os
from typing import cast


class InvalidEnvironmentKeysError(TypeError):
    """Raised when environment configuration keys are not provided as a dictionary."""

    def __init__(self) -> None:
        """Initialize the exception."""
        super().__init__("keys: expected dict[str, str | None]")


class MissingEnvironmentVariableError(KeyError):
    """Raised when a required environment variable has no value."""

    def __init__(self, key: str) -> None:
        """Initialize the exception."""
        self.key = key
        super().__init__(f"Missing environment variable: {key!r}")


def from_environment(keys: dict[str, str | None]) -> dict[str, str]:
    """Obtain configuration values from the OS environment."""
    # make sure we were given a dictionary of keys and defaults to work from
    if not isinstance(keys, dict):
        raise InvalidEnvironmentKeysError
    # copy the configuration and defaults to an object we can modify
    config = keys.copy()
    # for each key that we'd like to pull from the environment (or default)
    for key in config:
        # if the key was provided in the environment, use that value
        if key in os.environ:
            config[key] = os.environ[key]
        # if we still don't have an explicit value, raise an error
        if config[key] is None:
            raise MissingEnvironmentVariableError(key)
    # return the environment-populated configuration dictionary to the caller
    return cast(dict[str, str], config)
