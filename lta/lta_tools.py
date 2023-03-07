# lta_tools.py
"""Module with some tools for components of the Long Term Archive."""

import os
from typing import cast, Dict, Optional


def from_environment(keys: Dict[str, Optional[str]]) -> Dict[str, str]:
    """Obtain configuration values from the OS environment."""
    # make sure we were given a dictionary of keys and defaults to work from
    if not isinstance(keys, dict):
        raise TypeError("keys: Expected Dict[str, Optional[str]]")
    # copy the configuration and defaults to an object we can modify
    config = keys.copy()
    # for each key that we'd like to pull from the environment (or default)
    for key in config:
        # if the key was provided in the environment, use that value
        if key in os.environ:
            config[key] = os.environ[key]
        # if we still don't have an explicit value, raise an error
        if config[key] is None:
            raise OSError(f"Missing environment variable '{key}'")
    # return the environment-populated configuration dictionary to the caller
    return cast(Dict[str, str], config)
