"""
Small utility to POST /TransferRequest

Run with `python -m lta.make_transfer_request WIPAC:/data/exp/blah DESY:/data/exp/blah NERSC:/data/exp/blah`.
"""

import asyncio
from rest_tools.client import RestClient  # type: ignore
import sys

from lta.config import from_environment

EXPECTED_CONFIG = {
    'LTA_REST_TOKEN': None,
    'LTA_REST_URL': None
}


async def main():
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
    rc = RestClient(config["LTA_REST_URL"], token=config["LTA_REST_TOKEN"])
    # attempt to post the TransferRequest to the LTA DB
    try:
        response = await rc.request("POST", "/TransferRequests", request_body)
        print(response)
    except Exception as e:
        print(e)


if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
