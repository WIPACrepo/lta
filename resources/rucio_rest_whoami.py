"""Small workbench to poke at Rucio via the REST interface."""
import asyncio
from uuid import uuid4

from lta.config import from_environment
from lta.rucio import RucioClient, RucioResponse

EXPECTED_CONFIG = {
    "RUCIO_ACCOUNT": None,
    "RUCIO_APP_ID": None,
    "RUCIO_PASSWORD": None,
    "RUCIO_REST_URL": None,
    "RUCIO_USERNAME": None,
}


def print_response(response: RucioResponse) -> None:
    """Print some interesting things from the Response object."""
    # print(response.apparent_encoding)
    # print(response.headers)
    # print(response.ok)
    # print(response.reason)
    # print(response.status_code)
    # print(response.text)
    print(response)
    print("")


async def main():
    """Execute some Rucio commands."""
    config = from_environment(EXPECTED_CONFIG)
    rc = RucioClient(config["RUCIO_REST_URL"])
    await rc.auth(config["RUCIO_ACCOUNT"], config["RUCIO_USERNAME"], config["RUCIO_PASSWORD"])

    r = await rc.get("/accounts/whoami")
    print_response(r)

    r = await rc.get("/rses/")
    print_response(r)

    r = await rc.get("/scopes/")
    print_response(r)

    r = await rc.get("/dids/new")
    print_response(r)

    r = await rc.get("/dids/user.root/")
    print_response(r)

    # try to create a Container DID within Rucio
    scope = "user.root"
    name = str(uuid4())
    did_dict = {
        "type": "CONTAINER",
        "lifetime": 300,
    }
    r = await rc.post(f"/dids/{scope}/{name}", did_dict)
    print_response(r)

if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
