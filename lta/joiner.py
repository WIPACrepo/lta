# joiner.py
"""Module to implement the DesyVerifier component of the Long Term Archive."""

import os
from typing import List

def join_smart(items: List[str]) -> str:
    """Join paths together the way Node.js does it."""
    if not items:
        return "."
    abnormal_path = os.path.sep.join(items)
    normal_path = os.path.normpath(abnormal_path)
    if items[-1].endswith(os.path.sep):
        normal_path += os.path.sep
    return normal_path

def join_smart_url(items: List[str]) -> str:
    """Join URL items together."""
    if not items:
        return ""
    base = items[0]
    if base.endswith(os.path.sep):
        base = base[:-1]
    items = items[1:]
    items_str = join_smart(items)
    if items_str.startswith(os.path.sep):
        items_str = items_str[1:]
    return "/".join([base, items_str])
