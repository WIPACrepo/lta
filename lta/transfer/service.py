# service.py
"""Module providing a TransferService base class."""

import copy
import importlib
from typing import Any, Dict, Union

from ..lta_types import BundleType

TransferReference = str
TransferServiceConfig = Dict[str, Any]
TransferSpec = BundleType
TransferStatus = Dict[str, Union[bool, str]]

class TransferService:
    """TransferService is an abstract base class that specifies an interface to transfer services."""

    def __init__(self, config: TransferServiceConfig):
        """Initialize a TransferService object."""
        self.config = copy.deepcopy(config)

    async def cancel(self, ref: TransferReference) -> TransferStatus:
        """Ask the TransferService to cancel a transfer."""
        raise NotImplementedError("TransferService.cancel() is abstract and must be implemented in a subclass")

    async def start(self, spec: TransferSpec) -> TransferReference:
        """Ask the TransferService to start the specified transfer."""
        raise NotImplementedError("TransferService.start() is abstract and must be implemented in a subclass")

    async def status(self, ref: TransferReference) -> TransferStatus:
        """Query the TransferService about the status of a transfer."""
        raise NotImplementedError("TransferService.status() is abstract and must be implemented in a subclass")


def instantiate(config: TransferServiceConfig) -> TransferService:
    """Instantiate a transfer service object according to the provided configuration."""
    split_name = config["name"].split(".")
    module_name = ".".join(split_name[:-1])
    module = importlib.import_module(module_name)
    class_name = split_name[-1]
    class_obj = getattr(module, class_name)
    instance = class_obj(config)  # type: TransferService
    return instance
