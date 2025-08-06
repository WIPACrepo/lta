#!/usr/bin/env python3
# webdav_proxy.py
# Proxy a WebDAV service to add Authorization header

from http.server import BaseHTTPRequestHandler, HTTPServer
import logging
import sys
from typing import Optional, Union
from urllib.parse import urljoin

import requests

from lta.lta_tools import from_environment
from rest_tools.client import ClientCredentialsAuth


EXPECTED_CONFIG = {
    "AUTH_OPENID_URL": "https://keycloak.icecube.wisc.edu/auth/realms/IceCube",
    "CLIENT_ID": "long-term-archive",
    "CLIENT_SECRET": None,  # ${CLIENT_SECRET:="$(<keycloak-client-secret)"}
    "DEPTH": "1",
    "LOG_LEVEL": "DEBUG",
    "PROXY_HOST": "localhost",
    "PROXY_PORT": "8080",
    "RETRIES": "3",
    "TIMEOUT_SECONDS": "60",
    "UPSTREAM_URL": "https://globe-door.ifh.de:2880",
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


def make_webdav_proxy_class(config):
    class CustomHandler(WebDAVProxy):
        pass

    CustomHandler.config = config

    host = config["PROXY_HOST"]
    port = config["PROXY_PORT"]
    CustomHandler.rc = ClientCredentialsAuth(
        address=f"http://{host}:{port}/",
        token_url=config["AUTH_OPENID_URL"],
        client_id=config["CLIENT_ID"],
        client_secret=config["CLIENT_SECRET"],
        timeout=int(config["TIMEOUT_SECONDS"]),
        retries=int(config["RETRIES"]),
    )

    return CustomHandler


class WebDAVProxy(BaseHTTPRequestHandler):
    config = None
    rc = None

    def do_common(self):
        config = self.config
        method = self.command
        upstream_url = urljoin(config["UPSTREAM_URL"], self.path.lstrip("/"))

        # Prepare headers
        self.rc._get_token()
        token = _decode_if_necessary(self.rc.access_token)
        LOG.debug(f"Using token: {token}")
        headers = {k: v for k, v in self.headers.items() if k.lower() != "host"}
        headers["Authorization"] = f"Bearer {token}"
        headers["Depth"] = config["DEPTH"]

        # Get body if present
        content_length = int(self.headers.get("Content-Length", 0))
        body = self.rfile.read(content_length) if content_length > 0 else None

        # Forward the request
        try:
            LOG.info(f"{method} {upstream_url}")
            response = requests.request(
                method=method,
                url=upstream_url,
                headers=headers,
                data=body,
                allow_redirects=False,
                stream=True,
                verify=False,  # --insecure ; doesn't check the SSL cert!
            )
            self.send_response(response.status_code)
            for key, value in response.headers.items():
                if key.lower() != "transfer-encoding":
                    self.send_header(key, value)
            self.end_headers()
            for chunk in response.iter_content(chunk_size=4096):
                self.wfile.write(chunk)
        except Exception as e:
            self.send_error(502, f"Bad gateway: {e}")

    def do_GET(self): self.do_common()
    def do_PUT(self): self.do_common()
    def do_PROPFIND(self): self.do_common()
    def do_MKCOL(self): self.do_common()
    def do_DELETE(self): self.do_common()
    def do_COPY(self): self.do_common()
    def do_MOVE(self): self.do_common()
    def do_OPTIONS(self): self.do_common()
    def do_HEAD(self): self.do_common()
    def do_POST(self): self.do_common()

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
    # start the WebDAV proxy
    LISTEN_ADDR = (config["PROXY_HOST"], int(config["PROXY_PORT"]))
    print(f"Starting WebDAV proxy at http://{LISTEN_ADDR[0]}:{LISTEN_ADDR[1]}")
    webdav_proxy_class = make_webdav_proxy_class(config)
    server = HTTPServer(LISTEN_ADDR, webdav_proxy_class)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("Shutting down.")
        server.server_close()

if __name__ == "__main__":
    run()
