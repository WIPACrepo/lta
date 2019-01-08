# config.py

import os

NA = (0,) # use a tuple so 'is' operator works correctly

def from_environment(keys):
    if isinstance(keys, str):
        keys = {keys: NA}
    elif isinstance(keys, list):
        keys = dict.fromkeys(keys, NA)
    elif not isinstance(keys, dict):
        raise TypeError(f"keys: Expected string, list or dict")
    config = keys.copy()
    for key in config:
        if key in os.environ:
            config[key] = os.environ[key]
        elif config[key] is NA:
            raise OSError(f"Missing environment variable '{key}'")
    return config
