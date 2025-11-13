# globus.py
"""Tools to help manage Globus proxies."""

# fmt:off

import os
import subprocess
import logging
from typing import Any, cast, Dict, Optional

from wipac_dev_tools import from_environment

EMPTY_STRING_SENTINEL_VALUE = "517c094b-739a-4a01-9d61-8d29eee99fda"

PROXY_CONFIG: Dict[str, Optional[str]] = {
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