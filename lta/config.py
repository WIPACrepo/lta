# config.py

from typing import Dict
from typing import Optional
import os
from typing import Sequence
from typing import Union

OptionalDict = Dict[str, Optional[str]]
KeySpec = Union[str, Sequence[str], OptionalDict]


def from_environment(keys: KeySpec) -> OptionalDict:
    if isinstance(keys, str):
        keys = {keys: None}
    elif isinstance(keys, list):
        keys = dict.fromkeys(keys, None)
    elif not isinstance(keys, dict):
        raise TypeError(f"keys: Expected string, list or dict")
    config = keys.copy()
    for key in config:
        if key in os.environ:
            config[key] = os.environ[key]
        elif config[key] is None:
            raise OSError(f"Missing environment variable '{key}'")
    return config
