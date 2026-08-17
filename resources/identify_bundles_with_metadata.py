#!/usr/bin/env python3
# identify_bundles_with_metadata.py

"""
Generate a JSON file containing a list of distinct Bundle UUIDs
that have Metadata records in the Long Term Archive database.
"""

import asyncio
import json
import logging
from typing import cast

from rest_tools.client import ClientCredentialsAuth
from wipac_dev_tools import from_environment


EXPECTED_CONFIG = {
    "CLIENT_ID": None,
    "CLIENT_SECRET": None,
    "FILE_CATALOG_CLIENT_ID": None,
    "FILE_CATALOG_CLIENT_SECRET": None,
    "FILE_CATALOG_REST_URL": None,
    "LTA_AUTH_OPENID_URL": None,
    "LTA_REST_URL": None,
}


async def main() -> None:
    config = from_environment(EXPECTED_CONFIG)
    lta_rc = ClientCredentialsAuth(address=cast(str, config["LTA_REST_URL"]),
                                   token_url=cast(str, config["LTA_AUTH_OPENID_URL"]),
                                   client_id=cast(str, config["CLIENT_ID"]),
                                   client_secret=cast(str, config["CLIENT_SECRET"]))

    ret = await lta_rc.request("GET", "/Metadata/actions/distinct_bundles?status=finished")
    print(json.dumps(ret, indent=4, sort_keys=True))


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    asyncio.run(main())
