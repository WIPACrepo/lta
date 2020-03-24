"""
Small utility to assist with test data setup.

Subcommands:
    add-catalog <site> <path>
        Add all of the files in <path> to the File Catalog at site <site>
    clear-catalog
        Remove all files from the File Catalog
    clear-lta-bundles
        Remove all Bundles from the LTA DB
    clear-lta-transfer-requests
        Remove all Transfer Requests from the LTA DB
"""

import asyncio
import os
from secrets import token_hex
import sys
from uuid import uuid4

from rest_tools.client import RestClient  # type: ignore
from rest_tools.server import from_environment  # type: ignore

from lta.crypto import sha512sum

EXPECTED_CONFIG = {
    'FAKE_CHECKSUM': "False",
    'FILE_CATALOG_REST_TOKEN': None,
    'FILE_CATALOG_REST_URL': None,
    'LTA_REST_TOKEN': None,
    'LTA_REST_URL': None,
}


async def add_catalog(site, path):
    # configure a RestClient from the environment
    config = from_environment(EXPECTED_CONFIG)
    rc = RestClient(config["FILE_CATALOG_REST_URL"], token=config["FILE_CATALOG_REST_TOKEN"])
    # for each (dirpath, dirnames, filenames) tuple in the walk
    for root, dirs, files in os.walk(path):
        # don't recurse into deeper subdirectories
        if root != path:
            continue
        # for each file in our directory
        for data_file in files:
            # determine the logical name of the file
            logical_name = os.path.join(root, data_file)
            # create a catalog record for it
            file_record = {
                "uuid": str(uuid4()),
                "logical_name": logical_name,
                "checksum": {
                    "sha512": token_hex(64),
                },
                "locations": [
                    {
                        "site": f"{site}",
                        "path": logical_name,
                    }
                ],
                "file_size": os.path.getsize(logical_name),
            }
            # if we're being pedantic about real checksums in test data
            if config["FAKE_CHECKSUM"] != "True":
                file_record["checksum"]["sha512"] = sha512sum(logical_name)
            # add the file to the File Catalog
            try:
                print(f"POST /api/files - {logical_name}")
                response = await rc.request("POST", "/api/files", file_record)
            except Exception as e:
                # whoopsy daisy...
                print(e)


async def clear_catalog():
    # configure a RestClient from the environment
    config = from_environment(EXPECTED_CONFIG)
    rc = RestClient(config["FILE_CATALOG_REST_URL"], token=config["FILE_CATALOG_REST_TOKEN"])
    # while there are still files
    clearing = True
    while clearing:
        try:
            # get a list of up to 50 files
            response = await rc.request("GET", "/api/files?start=0&limit=50")
            files = response["files"]
            # for each file that we found
            for x in files:
                # remove it from the file catalog
                uuid = x["uuid"]
                logical_name = x["logical_name"]
                print(f"DELETE /api/files/{uuid} - {logical_name}")
                response2 = await rc.request("DELETE", f"/api/files/{uuid}")
            # if we didn't get any files back, we're done
            if len(files) < 1:
                clearing = False
        except Exception as e:
            # whoopsy daisy...
            clearing = False
            print(e)


async def clear_lta_bundles():
    # configure a RestClient from the environment
    config = from_environment(EXPECTED_CONFIG)
    rc = RestClient(config["LTA_REST_URL"], token=config["LTA_REST_TOKEN"])
    # while there are still bundles
    clearing = True
    while clearing:
        try:
            # get a list of all the bundles in the LTA DB
            response = await rc.request("GET", "/Bundles")
            results = response["results"]
            # for each bundle that we found
            for uuid in results:
                # remove it from the LTA DB
                print(f"DELETE /Bundles/{uuid}")
                response2 = await rc.request("DELETE", f"/Bundles/{uuid}")
            # if we didn't get any files back, we're done
            if len(results) < 1:
                clearing = False
        except Exception as e:
            # whoopsy daisy...
            clearing = False
            print(e)


async def clear_lta_transfer_requests():
    # configure a RestClient from the environment
    config = from_environment(EXPECTED_CONFIG)
    rc = RestClient(config["LTA_REST_URL"], token=config["LTA_REST_TOKEN"])
    # while there are still transfer requests
    clearing = True
    while clearing:
        try:
            # get a list of up to 50 transfer requests
            # technically a lie; the LTA DB honors neither start nor limit
            response = await rc.request("GET", "/TransferRequests?start=0&limit=50")
            results = response["results"]
            # for each file that we found
            for x in results:
                # remove it from the file catalog
                uuid = x["uuid"]
                print(f"DELETE /TransferRequests/{uuid}")
                response2 = await rc.request("DELETE", f"/TransferRequests/{uuid}")
            # if we didn't get any files back, we're done
            if len(results) < 1:
                clearing = False
        except Exception as e:
            # whoopsy daisy...
            clearing = False
            print(e)


async def main():
    # make sure we were given a subcommand
    if len(sys.argv) < 2:
        print("Usage: test_data_helper.py [add-catalog <site> <path> | clear-catalog | clear-lta-transfer-requests]")
        return
    # obtain the subcommand
    subcommand = sys.argv[1]
    # if we're adding files to the catalog
    if subcommand == "add-catalog":
        if len(sys.argv) >= 4:
            await add_catalog(sys.argv[2], sys.argv[3])
        else:
            print(f"test_data_helper.py: Subcommand '{subcommand}' missing <site> and <path> arguments")
            return
    # if we're clearing files from the catalog
    elif subcommand == "clear-catalog":
        await clear_catalog()
    # if we're clearing bundles from the LTA DB
    elif subcommand == "clear-lta-bundles":
        await clear_lta_bundles()
    # if we're clearing transfer requests from the LTA DB
    elif subcommand == "clear-lta-transfer-requests":
        await clear_lta_transfer_requests()
    # otherwise, what the heck is the user trying to do?
    else:
        print(f"test_data_helper.py: Unknown subcommand '{subcommand}'")
        return


if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
