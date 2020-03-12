"""
Ask the (testing) token service for a token.

Run with `python -m lta.solicit_token`.
"""

import asyncio

from rest_tools.client import RestClient  # type: ignore
from rest_tools.server import from_environment

EXPECTED_CONFIG = {
    'LTA_AUTH_ROLE': None,
    'TOKEN_SERVICE_URL': None,
}

async def solicit_token(url, scope):
    """Obtain a service token from the token service."""
    rc = RestClient(url, "")
    result = await rc.request("GET", f"/token?scope={scope}")
    print(result["access"])

if __name__ == '__main__':
    config = from_environment(EXPECTED_CONFIG)
    asyncio.run(solicit_token(config["TOKEN_SERVICE_URL"], config["LTA_AUTH_ROLE"]))
