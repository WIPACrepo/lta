# copy.py
"""Module providing a CopyTransferService class."""

from datetime import datetime
import os
from subprocess import Popen
from typing import Any, Dict

from .service import TransferReference, TransferService, TransferServiceConfig
from .service import TransferSpec, TransferStatus

def now() -> str:
    """Return string timestamp for current time, to the second."""
    return datetime.utcnow().isoformat(timespec='seconds')

class CopyTransferService(TransferService):
    """CopyTransferService simulates a transfer service with a local file copy."""

    def __init__(self, config: TransferServiceConfig):
        """Initialize a CopyTransferService object."""
        super(CopyTransferService, self).__init__(config)
        self.source_base = self.config.get("source_base", "")
        self.dest_base = self.config.get("dest_base", "")
        self.status_table: Dict[TransferReference, Dict[str, Any]] = {}

    async def cancel(self, ref: TransferReference) -> TransferStatus:
        """Ask the CopyTransferService to cancel the copy."""
        # TODO: Write some better code than returning a *shrug*
        return {
            "ref": ref,
            "create_timestamp": now(),
            "completed": False,
            "status": "UNKNOWN",
        }

    async def start(self, spec: TransferSpec) -> TransferReference:
        """Ask the CopyTransferService to start a local file copy."""
        source_path = os.path.join(self.source_base, spec["path"])
        dest_path = os.path.join(self.dest_base, spec["path"])
        popen = Popen(["/bin/cp", source_path, dest_path])
        ref = str(popen.pid)
        self.status_table[ref] = {
            "ref": ref,
            "create_timestamp": now(),
            "completed": False,
            "status": "PROCESSING",
            "_popen": popen,
        }
        return ref

    async def status(self, ref: TransferReference) -> TransferStatus:
        """Query the TransferService about the status of a prior transfer."""
        # if we've never heard of the transfer
        if ref not in self.status_table:
            return {
                "ref": ref,
                "create_timestamp": now(),
                "completed": False,
                "status": "UNKNOWN",
            }
        # otherwise, update the status table
        status = self.status_table[ref]
        popen = status["_popen"]
        if popen.poll():
            status["_returncode"] = popen.returncode
            status["completed"] = True
            if popen.returncode == 0:
                status["status"] = "COMPLETED"
            else:
                status["status"] = "ERROR"
        # return a copy of the status object
        return status.copy()
