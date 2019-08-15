# rucio.py
"""Module providing tools for interacting with Rucio."""

import asyncio
from datetime import datetime
import os
from typing import Any, Dict, List, Optional, Union
from urllib.parse import urljoin

from requests import Response
from rest_tools.client.session import AsyncSession  # type: ignore
from rest_tools.client.json_util import json_decode  # type: ignore

from .service import TransferReference
from .service import TransferService
from .service import TransferServiceConfig
from .service import TransferSpec
from .service import TransferStatus

RucioResponse = Optional[Union[Dict[str, Any], List[Dict[str, Any]]]]

DEFAULT_SITES = {
    "desy": "dataset-desy",
    "nersc": "dataset-nersc",
    "wipac": "dataset-wipac",
}

def now() -> str:
    """Return string timestamp for current time, to the second."""
    return datetime.utcnow().isoformat(timespec='seconds')


class RucioClient:
    """RucioClient helps to interact with Rucio via its RESTful interface."""

    def __init__(self,
                 rucio_url: str,
                 retries: int = 0):
        """Initialize a RucioClient object."""
        self.session = AsyncSession(retries)
        self.session.headers = {
            "Accept": "application/json, application/x-json-stream",
            "Content-Type": "application/json",
        }
        self.token: Optional[str] = None
        self.url = rucio_url

    async def auth(self,
                   account: str,
                   username: str,
                   password: str,
                   app_id: str = "") -> None:
        """Authenticate with Rucio and acquire a token."""
        headers = {
            # https://rucio.readthedocs.io/en/latest/restapi/authentication.html#get--auth-userpass
            "X-Rucio-Account": account,  # Account identifier as a string.
            "X-Rucio-Username": username,  # – Username as a string.
            "X-Rucio-Password": password,  # SHA1 hash of the password as a string.
            "X-Rucio-AppID": app_id,  # Application identifier as a string.
            # because the data of the response comes back in the 'X-Rucio-Auth-Token' header
            "Accept": None,
        }
        req_url = urljoin(self.url, "/auth/userpass")
        r = await asyncio.wrap_future(self.session.get(req_url, headers=headers))
        r.raise_for_status()
        self.token = r.headers["X-Rucio-Auth-Token"]

    def close(self) -> None:
        """Close the AsyncSession."""
        self.session.close()

    async def delete(self,
                     route: str,
                     json: Dict[str, Any],
                     headers: Dict[str, str] = {}) -> RucioResponse:
        """Execute a DELETE verb on the provided route."""
        req_headers = self._apply_auth(headers)
        req_url = urljoin(self.url, route)
        r = await asyncio.wrap_future(self.session.delete(req_url, json=json, headers=req_headers))
        r.raise_for_status()
        return self._decode(r)

    async def get(self,
                  route: str,
                  headers: Dict[str, str] = {}) -> RucioResponse:
        """Execute a GET verb on the provided route."""
        req_headers = self._apply_auth(headers)
        req_url = urljoin(self.url, route)
        r = await asyncio.wrap_future(self.session.get(req_url, headers=req_headers))
        r.raise_for_status()
        return self._decode(r)

    async def post(self,
                   route: str,
                   json: Dict[str, Any],
                   headers: Dict[str, str] = {}) -> RucioResponse:
        """Execute a POST verb on the provided route."""
        req_headers = self._apply_auth(headers)
        req_url = urljoin(self.url, route)
        r = await asyncio.wrap_future(self.session.post(req_url, json=json, headers=req_headers))
        r.raise_for_status()
        return self._decode(r)

    def _apply_auth(self, headers: Dict[str, str]) -> Dict[str, str]:
        """Add authentication header if an authentication token is available."""
        if not self.token:
            return headers
        auth_headers = headers.copy()
        auth_headers.update({
            "X-Rucio-Auth-Token": self.token
        })
        return auth_headers

    def _decode(self, r: Response) -> RucioResponse:
        """Parse JSON or NDJSON responses into language constructs."""
        if not r:
            return None
        # print(f"DEBUG[RucioClient._decode]: r.text = '{r.text}'")
        # if the response is NDJSON
        if r.headers["Content-Type"] == "application/x-json-stream":
            texts = r.text.split("\n")
            results = []
            for text in texts:
                result = self._safe_json_decode(text)
                if result:
                    results.append(result)
            return results
        # otherwise, this is just a plain old JSON answer (we hope)
        return self._safe_json_decode(r.text)

    def _safe_json_decode(self, text: str) -> Optional[Dict[str, Any]]:
        """Attempt to decode the provided text as a JSON object."""
        try:
            obj = json_decode(text)
            if isinstance(obj, dict):
                return obj
            return None
        except Exception:
            return None


class RucioTransferService(TransferService):
    """RucioTransferService uses Rucio to transfer a file from site to site."""

    def __init__(self, config: TransferServiceConfig):
        """Initialize a RucioTransferService object."""
        super(RucioTransferService, self).__init__(config)
        self.account = self.config.get("account", "root")
        self.password = self.config.get("password", "hunter2")  # http://bash.org/?244321
        self.pfn = self.config.get("pfn", "gsiftp://gridftp.icecube.wisc.edu:2811/")
        self.rest_url = self.config.get("rest_url", "http://rucio.icecube.wisc.edu:30475/")
        self.rse = self.config.get("rse", "LTA")
        self.scope = self.config.get("scope", "lta")
        self.sites = self.config.get("sites", DEFAULT_SITES)
        self.username = self.config.get("username", "icecube")

    async def start(self, spec: TransferSpec) -> TransferReference:
        """Ask the RucioTransferService to start a file transfer."""
        # ensure that we can connect to and authenticate with Rucio
        rc = await self._get_valid_rucio_client()
        # 2.1. register the bundle as a replica within rucio
        #     rucio upload --rse $RSE --scope SCOPE --register-after-upload --pfn PFN --name NAME /PATH/TO/BUNDLE
        file_did = await self._register_bundle_as_replica(rc, spec)
        # 2.2 add the BUNDLE_DID from 2.1 to the replica
        #     rucio attach DEST_CONTAINER_DID BUNDLE_DID
        dataset_did = await self._attach_replica_to_dataset(rc, spec)
        # return a transfer reference to identify this transfer
        return f"{dataset_did}|{file_did}"

    async def status(self, ref: TransferReference) -> TransferStatus:
        """Query the RucioTransferService about the status of a prior transfer."""
        # note: transfer references are of the form
        # f"{dataset_did}|{file_did}"
        # where `dataset_did` refers to the destination dataset
        # where `file_did` refers to the bundle file
        # TODO: Implement querying Rucio as to the status of the transfer
        return {
            "ref": ref,
            "create_timestamp": now(),
            "completed": False,
            "status": "UNKNOWN",
        }

    async def _attach_replica_to_dataset(self, rc: RucioClient, spec: TransferSpec) -> str:
        """Attach the Bundle replica to the site specific Dataset within Rucio."""
        # attach the FILE DID to the DATASET DID within Rucio
        scope = self.scope
        dest = spec['dest']
        dataset_name = self.sites[dest]  # type: str
        bundle_path = spec['bundle_path']
        name = os.path.basename(bundle_path)
        did_dict = {
            "dids": [
                {
                    "scope": scope,
                    "name": name,
                },
            ],
        }
        attach_url = f"/dids/{scope}/{dataset_name}/dids"
        r = await rc.post(attach_url, did_dict)
        if r:
            raise Exception(f"POST {attach_url} returned something; expected None")
        # check the DATASET DID to verify the replica as attached
        r = await rc.get(attach_url)
        if r is None:
            raise Exception(f"{attach_url} returned None; expected a list")
        if not isinstance(r, list):
            raise Exception(f"{attach_url} returned a dictionary; expected a list")
        found_replica = False
        for replica in r:
            if replica["name"] == name:
                found_replica = True
                break
        if not found_replica:
            raise Exception(f"{attach_url} replica name not found; expected name == '{name}' in the list")
        # return the dataset_did of the destination site
        return dataset_name

    async def _get_valid_rucio_client(self) -> RucioClient:
        """Ensure that we can connect to and authenticate with Rucio."""
        # create the RucioClient object and authenticate with Rucio
        rc = RucioClient(self.rest_url)
        await rc.auth(self.account, self.username, self.password)
        # check to see that our account authenticated properly
        r = await rc.get("/accounts/whoami")
        if r is None:
            raise Exception(f"/accounts/whoami returned None; expected dictionary")
        if isinstance(r, list):
            raise Exception(f"/accounts/whoami returned a list; expected dictionary")
        if r["status"] != "ACTIVE":
            raise Exception(f"/accounts/whoami status == '{r['status']}'; expected 'ACTIVE'")
        if r["account"] != self.account:
            raise Exception(f"/accounts/whoami account == '{r['account']}'; expected '{self.account}'")
        # check to see that our expected RSE is present
        found_rse = False
        r = await rc.get("/rses/")
        if r is None:
            raise Exception(f"/rses/ returned None; expected a list")
        if not isinstance(r, list):
            raise Exception(f"/rses/ returned a dictionary; expected a list")
        for rse in r:
            if rse["rse"] == self.rse:
                found_rse = True
                break
        if not found_rse:
            raise Exception(f"/rses/ expected RSE '{self.rse}' not found")
        # check to see that our expected datasets are present
        datasets_found: List[str] = []
        dids_scope = f"/dids/{self.scope}/"
        r = await rc.get(dids_scope)
        if r is None:
            raise Exception(f"{dids_scope} returned None; expected a list")
        if not isinstance(r, list):
            raise Exception(f"{dids_scope} returned a dictionary; expected a list")
        for did in r:
            if did["scope"] == self.scope:
                if did["type"] == "DATASET":
                    datasets_found.append(did["name"])
        for dataset in self.sites.values():
            if not (dataset in datasets_found):
                raise Exception(f"expected Rucio to contain DATASET '{dataset}'; not found")
        # return the RucioClient to the caller, ready to use
        return rc

    async def _register_bundle_as_replica(self, rc: RucioClient, spec: TransferSpec) -> str:
        """Register the provided Bundle as a replica within Rucio."""
        bundle_path = spec['bundle_path']
        name = os.path.basename(bundle_path)
        pfn = os.path.join(self.pfn, name)
        files = [
            {
                "scope": self.scope,
                "name": name,
                "bytes": spec["size"],
                "adler32": spec["checksum"]["adler32"],
                "pfn": pfn,
                "meta": {},
            },
        ]
        replicas_dict = {
            "rse": self.rse,
            "files": files,
            "ignore_availability": True,
        }
        r = await rc.post(f"/replicas/", replicas_dict)
        if r:
            raise Exception(f"POST /replicas/ returned something; expected None")
        # Query Rucio to verify that the replica has been created
        replica_url = f"/replicas/{self.scope}/{name}"
        r = await rc.get(replica_url)
        if r is None:
            raise Exception(f"{replica_url} returned None; expected a list")
        if not isinstance(r, list):
            raise Exception(f"{replica_url} returned a dictionary; expected a list")
        if len(r) != 1:
            raise Exception(f"{replica_url} returned a list of length {len(r)}; expected length 1")
        for replica in r:
            if not (replica["name"] == name):
                raise Exception(f"{replica_url} name == '{replica['name']}'; expected '{name}'")
        # return the file_did of the replica we created
        return f"{self.scope}:{name}"
