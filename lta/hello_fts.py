# hello_fts.py
"""This module is a proof of concept for contacting FTS."""

import asyncio
import json
from pathlib import Path
from typing import Any, Dict

from rest_tools.client import RestClient  # type: ignore
from rest_tools.server import from_environment  # type: ignore

from .fts_client import Delegator, FTSClient


EXPECTED_CONFIG = {
    "FTS_ENDPOINT_URL": None,
    # "FTS_X509_JSON": None,  # used by the FTS library for authentication
}


# async def _replicate_bundle_to_destination_site(self, lta_rc: RestClient, bundle: BundleType) -> None:
#     """Replicate the supplied bundle using the FTS transfer service."""
#     bundle_id = bundle["uuid"]
#     # establish a Context for communicating with FTS3
#     context = fts3.Context(self.endpoint, verify=True)
#     self.logger.info(f"FTS3 Endpoint Info: {context.get_endpoint_info()}")
#     self.logger.info(f"FTS3 whoami: {fts3.whoami(context)}")
#     # log some stuff about the FTS instance
#     jobs_json = fts3.list_jobs(context)
#     jobs = json.load(jobs_json)
#     self.logger.debug(f"There are {len(jobs)} at FTS3 {self.endpoint}")
#     self.logger.debug(f"{jobs_json}")
#     # construct the transfer object
#     source = ""
#     destination = ""
#     checksum = f"sha512:{bundle['checksum']['sha512']}"
#     filesize = bundle['bundle_size']
#     metadata = f"Bundle {bundle_id}"
#     transfer = fts3.new_transfer(
#         source, destination, checksum=checksum,
#         filesize=filesize, metadata=metadata)
#     # construct the job object
#     transfers = [transfer]
#     job = fts3.new_job(
#         transfers, verify_checksum=True,
#         reuse=True, overwrite=True, metadata=metadata)
#     # submit the job to FTS
#     xfer_ref = fts3.submit(context, job)
#     # update the Bundle in the LTA DB
#     patch_body = {
#         "status": "transferring",
#         "reason": "",
#         "update_timestamp": now(),
#         "claimed": False,
#         "transfer_reference": xfer_ref,
#     }
#     self.logger.info(f"PATCH /Bundles/{bundle_id} - '{patch_body}'")
#     await lta_rc.request('PATCH', f'/Bundles/{bundle_id}', patch_body)


async def auth_with_fts(config: Dict[str, Any]):
    """Attempt to call /whoami on the FTS endpoint."""
    # print(f"config['FTS_X509']: {config['FTS_X509']}")
    # print(f"config['FTS_ENDPOINT_URL']: {config['FTS_ENDPOINT_URL']}")

    fts_url = config["FTS_ENDPOINT_URL"]
    sslcert = "cert.pem"
    sslkey = "key.pem"
    cacert = "chain.pem"

    # lta_rc = RestClient(fts_url, timeout=30, retries=3, sslcert=sslcert, sslkey=sslkey, cacert=cacert)
    # response = await lta_rc.request('GET', '/whoami')
    # print(response)

    fc = FTSClient(fts_url, timeout=30, retries=3, sslcert=sslcert, sslkey=sslkey, cacert=cacert)
    endpoint_info = await fc.get_endpoint_info()
    print(f"\n{endpoint_info}")

    whoami = await fc.whoami()
    print(f"\n{whoami}")

    my_jobs = await fc.list_jobs()
    print(f"\n{my_jobs}")

    delegate_info = await fc.get_delegation_info()
    print(f"\n{delegate_info}")

    delegator = Delegator(fc)
    result = await delegator.delegate()
    print(f"\n{result}")

    asyncio.get_event_loop().stop()

def runner() -> None:
    """Configure a FTS3Replicator component from the environment and set it running."""
    # obtain our configuration from the environment
    config = from_environment(EXPECTED_CONFIG)
    # config["FTS_X509"] = json.loads(Path(config["FTS_X509_JSON"]).read_text())
    # with open("cert.pem.base64", "w", encoding="utf-8") as outfile:
    #     outfile.write(config["FTS_X509"]["data"]["cert.pem"])
    # with open("key.pem.base64", "w", encoding="utf-8") as outfile:
    #     outfile.write(config["FTS_X509"]["data"]["key.pem"])
    # with open("chain.pem.base64", "w", encoding="utf-8") as outfile:
    #     outfile.write(config["FTS_X509"]["data"]["chain.pem"])
    # let's get to work
    loop = asyncio.get_event_loop()
    loop.create_task(auth_with_fts(config))

def main() -> None:
    runner()
    asyncio.get_event_loop().run_forever()

if __name__ == "__main__":
    main()
