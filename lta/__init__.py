"""The Long Term Archive and tools."""

# exports
from . import globus_replicator, transfer

__all__ = [
    "globus_replicator",
    "transfer",
]

# NOTE: `__version__` is not defined because this package is built using 'setuptools-scm' --
#   use `importlib.metadata.version(...)` if you need to access version info at runtime.
