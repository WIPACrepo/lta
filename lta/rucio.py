# rucio.py
"""Module providing tools for interacting with Rucio."""

import asyncio
from typing import Any, Dict, List, Optional, Union
from urllib.parse import urljoin

from requests import Response
from rest_tools.client.session import AsyncSession  # type: ignore
from rest_tools.client.json_util import json_decode  # type: ignore

RucioResponse = Optional[Union[Dict[str, Any], List[Dict[str, Any]]]]

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
        }
        req_url = urljoin(self.url, "/auth/userpass")
        r = await asyncio.wrap_future(self.session.get(req_url, headers=headers))
        r.raise_for_status()
        self.token = r.headers["X-Rucio-Auth-Token"]

    def close(self) -> None:
        """Close the AsyncSession."""
        self.session.close()

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
