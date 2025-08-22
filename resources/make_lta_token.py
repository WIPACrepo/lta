#!/usr/bin/env python3
# make_lta_token.py
# Ask keycloak for an LTA token

import logging
import sys
from typing import Optional, Union

from lta.lta_tools import from_environment
from rest_tools.client import ClientCredentialsAuth


EXPECTED_CONFIG = {
    "AUTH_OPENID_URL": "https://keycloak.icecube.wisc.edu/auth/realms/IceCube",
    "CLIENT_ID": "long-term-archive",
    "CLIENT_SECRET": None,  # ${CLIENT_SECRET:="$(<keycloak-client-secret)"}
    "LOG_LEVEL": "DEBUG",
}

LOG = logging.getLogger(__name__)


def _decode_if_necessary(value: Optional[Union[str, bytes]]) -> Optional[str]:
    if value is None:
        return None
    if isinstance(value, str):
        return value
    if isinstance(value, bytes):
        return value.decode("utf-8")
    raise TypeError(f"Expected str or bytes or None, got {type(value).__name__}")


def run():
    # obtain our configuration from the environment
    config = from_environment(EXPECTED_CONFIG)
    log_level = getattr(logging, config["LOG_LEVEL"].upper())
    logging.basicConfig(
        format="{asctime} [{threadName}] {levelname:5} ({filename}:{lineno}) - {message}",
        level=log_level,
        stream=sys.stdout,
        style="{",
    )

    # show me the money!
    rc = ClientCredentialsAuth(
        address="http://localhost:8080/",
        token_url=config["AUTH_OPENID_URL"],
        client_id=config["CLIENT_ID"],
        client_secret=config["CLIENT_SECRET"],
        timeout=60,
        retries=3,
    )
    rc._get_token()
    token = _decode_if_necessary(rc.access_token)
    print(f"\n\nAuthorization: Bearer {token}\n\n")


if __name__ == "__main__":
    run()
