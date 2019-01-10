# config.py

from typing import Dict
import os
from typing import Sequence
from typing import Tuple
from typing import Union

# we use a Tuple as a pseudo-None, so instead of Optional[str]...
NotQuiteOptional = Union[str, Tuple[int]]
NotQuiteOptionalDict = Dict[str, NotQuiteOptional]
# all the different ways we can request environment variable substitution
KeySpec = Union[str, Sequence[str], NotQuiteOptionalDict]


def from_environment(keys: KeySpec) -> NotQuiteOptionalDict:
    NA = (0,)  # use a tuple so 'is' operator works correctly
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
