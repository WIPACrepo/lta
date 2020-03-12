"""Small workbench to poke at Rucio via the REST interface."""
import asyncio
import json
from uuid import uuid4

from rest_tools.server import from_environment  # type: ignore

from lta.transfer.rucio import RucioClient, RucioResponse

EXPECTED_CONFIG = {
    "RUCIO_ACCOUNT": None,
    "RUCIO_APP_ID": None,
    "RUCIO_PASSWORD": None,
    "RUCIO_REST_URL": None,
    "RUCIO_USERNAME": None,
}


def print_response(title: str, response: RucioResponse) -> None:
    """Print some interesting things from the Response object."""
    # print(response.apparent_encoding)
    # print(response.headers)
    # print(response.ok)
    # print(response.reason)
    # print(response.status_code)
    # print(response.text)
    print(title)
    print(json.dumps(response, indent=4, sort_keys=True))
    print("")


async def rucio_get(rc: RucioClient, route: str) -> None:
    """Query Rucio and print the response."""
    r = await rc.get(route)
    print_response(route, r)


async def main():
    """Execute some Rucio commands."""
    config = from_environment(EXPECTED_CONFIG)
    rc = RucioClient(config["RUCIO_REST_URL"])
    await rc.auth(config["RUCIO_ACCOUNT"], config["RUCIO_USERNAME"], config["RUCIO_PASSWORD"])

    await rucio_get(rc, "/accounts/whoami")

    await rucio_get(rc, "/rses/")

    await rucio_get(rc, "/scopes/")

    # await rucio_get(rc, "/dids/new")

    await rucio_get(rc, "/dids/lta/")

    # try to create a File within Rucio
    # ignore_availability = True
    # rse = "LTA-ND-A"
    # files = [
    #     {
    #         "scope": "user.root",
    #         "name": "0c977538-a491-4eb4-94d4-b0fc5b0f0a85.zip",
    #         "bytes": 1048900,
    #         "adler32": "89d5efeb",
    #         "pfn": "gsiftp://gridftp.icecube.wisc.edu:2811/mnt/lfss/rucio-test/LTA-ND-A/0c977538-a491-4eb4-94d4-b0fc5b0f0a85.zip",
    #         "md5": "778b0c448c3d750f189f543da9caac83",
    #         "meta": {},
    #     },
    # ]
    # replicas_dict = {
    #     "rse": rse,
    #     "files": files,
    #     "ignore_availability": ignore_availability,
    # }
    # r = await rc.post(f"/replicas/", replicas_dict)
    # print_response(r)

    # await rucio_get(rc, "/replicas/user.root/0c977538-a491-4eb4-94d4-b0fc5b0f0a85.zip")

    # try to create a Container DID within Rucio
    # scope = "user.root"
    # name = str(uuid4())
    # did_dict = {
    #     "type": "CONTAINER",
    #     "lifetime": 300,
    # }
    # r = await rc.post(f"/dids/{scope}/{name}", did_dict)
    # print_response(r)

    # # try to create a Dataset DID within Rucio
    # scope = "lta"
    # name = "dataset-test-1"
    # did_dict = {
    #     "type": "DATASET",
    #     # "lifetime": 300,
    # }
    # r = await rc.post(f"/dids/{scope}/{name}", did_dict)
    # print_response(f"POST /dids/{scope}/{name}", r)

    # # show the information about the Dataset DID that we created
    # scope = "lta"
    # name = "dataset-test-1"
    # r = await rucio_get(f"/dids/{scope}/{name}")

    # # attach the FILE DID to the DATASET DID within Rucio
    # scope = "user.root"
    # name = "dataset-nersc"
    # # rse = "LTA-ND-A"
    # did_dict = {
    #     "dids": [
    #         {
    #             "scope": scope,
    #             "name": "date2",
    #         },
    #     ],
    #     # "rse": rse,
    # }
    # r = await rc.post(f"/dids/{scope}/{name}/dids", did_dict)
    # print_response(r)

    # check the DATASET DID to see what is attached
    scope = "lta"
    name = "dataset-test-1"
    await rucio_get(rc, f"/dids/{scope}/{name}/dids")

    # List all replicas for data identifiers
    scope = "user.root"
    list_dict = {
        "dids": [
            {
                "scope": scope,
                "name": "0c977538-a491-4eb4-94d4-b0fc5b0f0a85.zip",
            },
        ],
        "all_states": True,
    }
    r = await rc.post(f"/replicas/list", list_dict)
    print_response("POST /replicas/list", r)


if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
