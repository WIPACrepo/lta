# lta_types.py
"""Central catalog of Python types for LTA entities."""

from typing import Any, Dict, List, Tuple, Union

CatalogFileType = Dict[str, Any]
DestType = Tuple[str, str]
DestList = List[DestType]
FileType = Dict[str, Union[str, Dict[Any, Any]]]
FileList = List[FileType]
TransferRequestType = Dict[str, Any]
