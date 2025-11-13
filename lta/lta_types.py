# lta_types.py
"""Central catalog of Python types for LTA entities."""

# fmt:off

from typing import Any, Dict, List

BundleType = Dict[str, Any]
BundleList = List[BundleType]
TransferRequestType = Dict[str, Any]