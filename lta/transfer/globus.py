# globus.py
"""Tools to help manage Globus proxies."""

# fmt:off

import subprocess
from typing import cast
import logging
import os
import time
from typing import Any, Dict, Optional
from urllib.parse import urlparse

import globus_sdk

from ..lta_tools import from_environment
from wipac_dev_tools import from_environment

EMPTY_STRING_SENTINEL_VALUE = "517c094b-739a-4a01-9d61-8d29eee99fda"

PROXY_CONFIG: dict[str, str | None] = {
    "GLOBUS_PROXY_DURATION": "72",
    "GLOBUS_PROXY_PASSPHRASE": EMPTY_STRING_SENTINEL_VALUE,
    "GLOBUS_PROXY_VOMS_ROLE": EMPTY_STRING_SENTINEL_VALUE,
    "GLOBUS_PROXY_VOMS_VO": EMPTY_STRING_SENTINEL_VALUE,
    "GLOBUS_PROXY_OUTPUT": EMPTY_STRING_SENTINEL_VALUE,
}

logger = logging.getLogger('globus')


class SiteGlobusProxy(object):
    """
    Manage site-wide globus proxy.

    :param duration: proxy duration (optional, default 72 hours)
    """

    def __init__(self, duration: Optional[int] = None):
        """Create a SiteGlobusProxy object."""
        # load what we can from the environment
        self.cfg = from_environment(PROXY_CONFIG)
        # remove anything optional that wasn't specified
        cfg_keys = list(self.cfg.keys())
        for key in cfg_keys:
            if self.cfg[key] == EMPTY_STRING_SENTINEL_VALUE:
                del self.cfg[key]
        # ensure duration is converted to an integer value
        if "GLOBUS_PROXY_DURATION" in self.cfg:
            self.cfg["GLOBUS_PROXY_DURATION"] = int(self.cfg["GLOBUS_PROXY_DURATION"])
        # ensure we have at least an empty string for passphrase
        if "GLOBUS_PROXY_PASSPHRASE" not in self.cfg:
            self.cfg["GLOBUS_PROXY_PASSPHRASE"] = ""
        # override the duration if specified during construction
        if duration:
            self.cfg['GLOBUS_PROXY_DURATION'] = duration

    def set_duration(self, d: str) -> None:
        """Set the duration."""
        self.cfg['GLOBUS_PROXY_DURATION'] = d

    def set_passphrase(self, p: str) -> None:
        """Set the passphrase."""
        self.cfg['GLOBUS_PROXY_PASSPHRASE'] = p

    def set_voms_role(self, r: str) -> None:
        """Set the voms role."""
        self.cfg['GLOBUS_PROXY_VOMS_ROLE'] = r

    def set_voms_vo(self, vo: str) -> None:
        """Set the voms VO."""
        self.cfg['GLOBUS_PROXY_VOMS_VO'] = vo

    def update_proxy(self) -> None:
        """Update the proxy."""
        logger.info('duration: %r', self.cfg['GLOBUS_PROXY_DURATION'])
        if subprocess.call(['grid-proxy-info', '-e', '-valid', f'{self.cfg["GLOBUS_PROXY_DURATION"]}:0'],
                           stdout=subprocess.DEVNULL,
                           stderr=subprocess.DEVNULL):
            # proxy needs updating
            if 'GLOBUS_PROXY_VOMS_VO' in self.cfg and self.cfg['GLOBUS_PROXY_VOMS_VO']:
                cmd = ['voms-proxy-init']
                if 'GLOBUS_PROXY_VOMS_ROLE' in self.cfg and self.cfg['GLOBUS_PROXY_VOMS_ROLE']:
                    vo = self.cfg['GLOBUS_PROXY_VOMS_VO']
                    role = self.cfg['GLOBUS_PROXY_VOMS_ROLE']
                    cmd.extend(['-voms', '{0}:/{0}/Role={1}'.format(vo, role)])
                else:
                    cmd.extend(['-voms', cast(str, self.cfg['GLOBUS_PROXY_VOMS_VO'])])
            else:
                cmd = ['grid-proxy-init']
            cmd.extend(['-debug', '-pwstdin', '-valid', f'{int(self.cfg["GLOBUS_PROXY_DURATION"])+1}:0'])
            if 'GLOBUS_PROXY_OUTPUT' in self.cfg and self.cfg['GLOBUS_PROXY_OUTPUT']:
                cmd.extend(['-out', cast(str, self.cfg['GLOBUS_PROXY_OUTPUT'])])
            inputbytes = (cast(str, self.cfg['GLOBUS_PROXY_PASSPHRASE']) + '\n').encode('utf-8')
            p = subprocess.run(cmd, input=inputbytes, capture_output=True, timeout=60, check=False)
            logger.info('proxy cmd: %r', p.args)
            logger.info('stdout: %s', p.stdout)
            logger.info('stderr: %s', p.stderr)
            if 'GLOBUS_PROXY_VOMS_VO' in self.cfg and self.cfg['GLOBUS_PROXY_VOMS_VO']:
                for line in p.stdout.decode('utf-8').split('\n'):
                    if line.startswith('Creating proxy') and line.endswith('Done'):
                        break  # this is a good proxy
                else:
                    raise Exception('voms-proxy-init failed')
            elif p.returncode > 0:
                raise Exception('grid-proxy-init failed')

    def get_proxy(self) -> Any:
        """Get the proxy location."""
        if 'GLOBUS_PROXY_OUTPUT' in self.cfg and self.cfg['GLOBUS_PROXY_OUTPUT']:
            return self.cfg['GLOBUS_PROXY_OUTPUT']
        FNULL = open(os.devnull, 'w')
        return subprocess.check_output(['grid-proxy-info', '-path'],
                                       stderr=FNULL).decode('utf-8').strip()


# fmt: on

GLOBUS_TRANSFER_CONFIG: dict[str, str | None] = {
    # Auth / client credentials
    "GLOBUS_CLIENT_ID": None,
    "GLOBUS_CLIENT_SECRET": None,
    # Source collection (where the bundles live)
    "GLOBUS_SOURCE_COLLECTION_ID": None,
    # Optional: default destination collection if not encoded in the URL
    "GLOBUS_DEST_COLLECTION_ID": None,
    # Optional: override scope / poll interval
    "GLOBUS_TRANSFER_SCOPE": "urn:globus:auth:scope:transfer.api.globus.org:all",
    "GLOBUS_POLL_INTERVAL_SECONDS": "10",
}


class GlobusTransfer:
    """Helper class to submit and wait for single-file Globus transfers."""

    def __init__(self, config: dict[str, str]) -> None:
        """Create a GlobusTransfer helper."""
        # Config must be fully expanded and contain all required fields.
        self._cfg: dict[str, str] = config
        self._transfer_client: globus_sdk.TransferClient = self._mk_client()

    def _mk_client(self) -> globus_sdk.TransferClient:
        """Create and return an authenticated TransferClient."""
        missing = [
            key
            for key in (
                "GLOBUS_CLIENT_ID",
                "GLOBUS_CLIENT_SECRET",
                "GLOBUS_SOURCE_COLLECTION_ID",
                "GLOBUS_TRANSFER_SCOPE",
            )
            if key not in self._cfg or not self._cfg[key]
        ]
        if missing:
            raise RuntimeError(f"Missing required Globus config: {', '.join(missing)}")

        client = globus_sdk.ConfidentialAppAuthClient(
            self._cfg["GLOBUS_CLIENT_ID"],
            self._cfg["GLOBUS_CLIENT_SECRET"],
        )

        token_response = client.oauth2_client_credentials_tokens(
            requested_scopes=self._cfg["GLOBUS_TRANSFER_SCOPE"]
        )
        rs = token_response.by_resource_server["transfer.api.globus.org"]
        access_token = rs["access_token"]

        authorizer = globus_sdk.AccessTokenAuthorizer(access_token)
        tc = globus_sdk.TransferClient(authorizer=authorizer)

        logger.info(
            "Initialized Globus TransferClient with source collection %s",
            self._cfg["GLOBUS_SOURCE_COLLECTION_ID"],
        )
        return tc

    def _parse_dest_url(self, dest_url: str) -> tuple[str, str]:
        """
        Parse dest_url into (dest_collection_id, dest_path).

        Supported forms:
          globus://<COLLECTION>/abs/path/file
          /abs/path/file   (requires GLOBUS_DEST_COLLECTION_ID)
        """
        parsed = urlparse(dest_url)

        if parsed.scheme == "globus":
            dest_collection_id = parsed.netloc
            dest_path = parsed.path
        else:
            if "GLOBUS_DEST_COLLECTION_ID" not in self._cfg:
                raise RuntimeError(
                    "Destination collection id missing: "
                    "Set GLOBUS_DEST_COLLECTION_ID or use globus://COLLECTION/path"
                )
            dest_collection_id = self._cfg["GLOBUS_DEST_COLLECTION_ID"]
            dest_path = dest_url

        if not dest_path.startswith("/"):
            dest_path = "/" + dest_path

        return dest_collection_id, dest_path

    def transfer_file(
        self,
        *,
        source_path: str,
        dest_url: str,
        request_timeout: int,
    ) -> str:
        """
        Transfer a single file via Globus Transfer and block until completion.

        :param source_path: Absolute path on the source collection.
        :param dest_url: globus://COLLECTION/path OR /path (with env id).
        :returns: Globus task_id for the submitted transfer.
        """
        if not os.path.isabs(source_path):
            raise ValueError(f"source_path must be absolute: {source_path}")

        dest_collection_id, dest_path = self._parse_dest_url(dest_url)
        src_collection_id = self._cfg["GLOBUS_SOURCE_COLLECTION_ID"]

        label = f"LTA bundle transfer: {os.path.basename(source_path)}"

        tdata = globus_sdk.TransferData(
            self._transfer_client,
            src_collection_id,
            dest_collection_id,
            label=label,
            sync_level="checksum",
        )
        tdata.add_item(source_path, dest_path)

        logger.info(
            "Submitting Globus transfer: src_collection=%s src_path=%s "
            "dest_collection=%s dest_path=%s",
            src_collection_id,
            source_path,
            dest_collection_id,
            dest_path,
        )

        submit_result: dict[str, Any] = self._transfer_client.submit_transfer(tdata)
        task_id = submit_result["task_id"]

        logger.info("Globus transfer submitted, task_id=%s", task_id)

        poll_interval = float(self._cfg["GLOBUS_POLL_INTERVAL_SECONDS"])
        deadline = time.monotonic() + request_timeout

        while True:
            task = self._transfer_client.get_task(task_id)
            status = task["status"]

            if status == "SUCCEEDED":
                logger.info("Globus transfer %s succeeded", task_id)
                return task_id

            if status == "FAILED" or status == "INACTIVE":
                logger.error(
                    "Globus transfer %s failed: status=%s task=%r",
                    task_id,
                    status,
                    task,
                )
                raise RuntimeError(
                    f"Globus transfer {task_id} failed with status {status}"
                )

            if time.monotonic() >= deadline:
                logger.error(
                    "Globus transfer %s timed out after %s seconds (last status=%s)",
                    task_id,
                    request_timeout,
                    status,
                )
                try:
                    self._transfer_client.cancel_task(task_id)
                except Exception:
                    logger.exception("Failed to cancel Globus task %s", task_id)
                raise TimeoutError(
                    f"Globus transfer {task_id} did not complete in {request_timeout}s"
                )

            time.sleep(poll_interval)
