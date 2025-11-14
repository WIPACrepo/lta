# globus.py
"""Tools to help manage Globus proxies."""

import asyncio
import itertools
import subprocess
import uuid
from typing import cast
import logging
import os
from typing import Any, Optional
from urllib.parse import urlparse
import dataclasses

import globus_sdk
from wipac_dev_tools import from_environment, from_environment_as_dataclass
from wipac_dev_tools.timing_tools import IntervalTimer

# fmt:off


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

# ---------------------------
# Environment Config Dataclass
# ---------------------------


@dataclasses.dataclass(frozen=True)
class GlobusTransferEnv:
    """Typed environment configuration for Globus transfers."""

    # Required
    GLOBUS_CLIENT_ID: str
    GLOBUS_CLIENT_SECRET: str
    GLOBUS_SOURCE_COLLECTION_ID: str
    GLOBUS_DEST_COLLECTION_ID: str

    # Defaults
    GLOBUS_TRANSFER_SCOPE: str = "urn:globus:auth:scope:transfer.api.globus.org:all"
    GLOBUS_POLL_INTERVAL_SECONDS: float = 10.0


# ---------------------------
# Globus Transfer Helper
# ---------------------------


class GlobusTransferFailedException(Exception):
    """Raised when globus transfer failed."""


class GlobusTransfer:
    """Submit and wait for single-file Globus transfers."""

    def __init__(self) -> None:
        """Load env config and initialize a TransferClient."""
        self._env = from_environment_as_dataclass(GlobusTransferEnv)
        self._transfer_client = self._create_client()

    # ---------------------------
    # Internal Helpers
    # ---------------------------

    def _create_client(self) -> globus_sdk.TransferClient:
        """Create an authenticated TransferClient."""

        # request token
        auth = globus_sdk.ConfidentialAppAuthClient(
            self._env.GLOBUS_CLIENT_ID,
            self._env.GLOBUS_CLIENT_SECRET,
        )
        token_resp = auth.oauth2_client_credentials_tokens(
            requested_scopes=self._env.GLOBUS_TRANSFER_SCOPE,
        )

        # assemble transfer client
        tc = globus_sdk.TransferClient(
            authorizer=globus_sdk.AccessTokenAuthorizer(
                token_resp.by_resource_server["transfer.api.globus.org"]["access_token"]
            )
        )
        logger.info(
            f"Initialized Globus TransferClient with source collection "
            f"{self._env.GLOBUS_SOURCE_COLLECTION_ID}",
        )
        return tc

    def _parse_dest_url(self, dest_url: str) -> tuple[str, str]:
        """
        Parse destination URL into (collection_id, absolute_path).

        Supports:
          globus://COLLECTION/abs/path/file
          /abs/path/file     → uses GLOBUS_DEST_COLLECTION_ID
        """
        parsed = urlparse(dest_url)

        # Explicit collection in URL
        if parsed.scheme == "globus":
            dest_collection_id = parsed.netloc
            dest_path = parsed.path
        else:
            dest_collection_id = self._env.GLOBUS_DEST_COLLECTION_ID
            dest_path = dest_url

        if not dest_path.startswith("/"):
            dest_path = "/" + dest_path

        return dest_collection_id, dest_path

    def _cancel_task(self, task_id: uuid.UUID | str, error_msg: str) -> None:
        # cancel task
        logger.error(error_msg)
        try:
            self._transfer_client.cancel_task(task_id)
        except Exception:
            logger.exception(f"Could not cancel Globus {task_id=}")

    # ---------------------------
    # Public API
    # ---------------------------

    async def transfer_file(
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
        :param request_timeout: Request timeout in seconds.

        :returns: Globus task_id for the submitted transfer.
        """
        if not os.path.isabs(source_path):
            raise ValueError(f"source_path must be absolute: {source_path}")

        # set up transfer document
        dest_collection_id, dest_path = self._parse_dest_url(dest_url)
        tdata = globus_sdk.TransferData(
            source_endpoint=self._env.GLOBUS_SOURCE_COLLECTION_ID,
            destination_endpoint=dest_collection_id,
            label=f"LTA bundle transfer: {os.path.basename(source_path)}",
            sync_level="checksum",
        )
        tdata.add_item(source_path, dest_path)

        # submit transfer
        logger.info(
            f"Submitting transfer: src_collection={self._env.GLOBUS_SOURCE_COLLECTION_ID} "
            f"{source_path=} {dest_collection_id=} {dest_path=}",
        )
        task_id = self._transfer_client.submit_transfer(tdata)["task_id"]
        logger.info(f"Globus transfer submitted: {task_id=}")
        await asyncio.sleep(0)  # since request is not async, handover to pending tasks

        # wait for transfer result
        # -- NOTE: 'globus_sdk.TransferClient.task_wait()' is *NOT* async, so diy
        deadline = IntervalTimer(request_timeout, logger=None)
        for i in itertools.count():

            # looping condition(s)
            # -- note: check if interval elapsed *before* sleeping to not waste time
            if deadline.has_interval_elapsed():
                self._cancel_task(
                    task_id,
                    f"Globus transfer {task_id=} timed out after {request_timeout=} seconds",
                )
                raise TimeoutError(f"Globus transfer {task_id} timed out")
            elif i > 0:
                await asyncio.sleep(self._env.GLOBUS_POLL_INTERVAL_SECONDS)

            # look at status
            logger.debug("checking transfer status...")
            task = self._transfer_client.get_task(task_id)
            status = task["status"]
            logger.debug(f"{status=}")
            match status:
                case "SUCCEEDED":
                    logger.info(f"Globus transfer succeeded: {task_id=} {task=}")
                    return task_id
                case "FAILED" | "INACTIVE":
                    msg = f"Globus transfer failed ({status=}): {task_id=} {task=}"
                    logger.error(msg)
                    raise GlobusTransferFailedException(msg)
                case "ACTIVE":
                    continue
                case _:
                    logger.warning(
                        f"received unknown {status=}: {task_id=} {task=} — continuing..."
                    )
                    continue
