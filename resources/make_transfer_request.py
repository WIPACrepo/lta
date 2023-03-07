"""
Small utility to POST /TransferRequest

Run with `python -m lta.make_transfer_request WIPAC NERSC /data/exp/IceCube/blah`.
"""

import asyncio
from rest_tools.client import RestClient
from rest_tools.server import from_environment  # type: ignore
import sys

EXPECTED_CONFIG = {
    'LTA_REST_URL': None
}


async def main() -> None:
    # make sure we were given source and destination
    if len(sys.argv) < 3:
        print("Usage: make_transfer_request.py <source_site> <dest_site> <path>")
        return
    # construct the TransferRequest body
    request_body = {
        "source": sys.argv[1],
        "dest": sys.argv[2],
        "path": sys.argv[3],
    }
    # configure a RestClient from the environment
    config = from_environment(EXPECTED_CONFIG)
    rc = RestClient(config["LTA_REST_URL"])
    # attempt to post the TransferRequest to the LTA DB
    try:
        response = await rc.request("POST", "/TransferRequests", request_body)
        print(response)
    except Exception as e:
        print(e)


if __name__ == '__main__':
    asyncio.run(main())
