# config.py

from typing import Dict
import os
from typing import Sequence


def from_environment(keys: Sequence[str]) -> Dict[str, str]:
    if isinstance(keys, str):
        keys = [keys]
    if not isinstance(keys, list):
        raise TypeError("keys: Expected list of strings")
    config = {}
    for key in keys:
        if key not in os.environ:
            raise OSError(f"Missing environment variable '{key}'")
        config[key] = os.environ[key]
    return config
