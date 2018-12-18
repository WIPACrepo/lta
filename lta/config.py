# config.py

import os


def from_environment(keys):
    if isinstance(keys, str):
        keys = [keys]
    if not isinstance(keys, list):
        raise TypeError(f"keys: Expected list of strings")
    config = {}
    for key in keys:
        if key not in os.environ:
            raise OSError(f"Missing environment variable '{key}'")
        config[key] = os.environ[key]
    return config
