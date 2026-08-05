# lta_types.py
"""Central catalog of Python types for LTA entities."""

# fmt:off

from typing import Any

BundleType = dict[str, Any]
BundleList = list[BundleType]
TransferRequestType = dict[str, Any]
