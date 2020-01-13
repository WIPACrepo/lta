"""
Command line utility for Long Term Archive.

Run with `python -m lta.lta_cmd $@`.
"""

import argparse
from argparse import Namespace
import asyncio
from datetime import datetime, timedelta
import json
import logging
import os
from typing import Any, Dict, List, Optional

import hurry.filesize  # type: ignore
from rest_tools.client import RestClient  # type: ignore

from lta.config import from_environment
from lta.crypto import sha512sum


COMPONENT_NAMES = [
    "bundler",
    "picker",
    "replicator",
]

EXPECTED_CONFIG = {
    'FILE_CATALOG_REST_TOKEN': None,
    'FILE_CATALOG_REST_URL': None,
    'LTA_REST_TOKEN': None,
    'LTA_REST_URL': None,
}


def print_dict_as_pretty_json(d: Dict[str, Any]) -> None:
    """Print the provided Dict as pretty-print JSON."""
    print(json.dumps(d, indent=4, sort_keys=True))

# -----------------------------------------------------------------------------

async def _catalog_get(rc: RestClient, path: str) -> Optional[Any]:
    """Get the File Catalog record (if any) of the provided path."""
    query_dict = {
        "locations.site": {
            "$eq": "WIPAC"
        },
        "locations.path": {
            "$eq": path
        }
    }
    query_json = json.dumps(query_dict)
    fc_response = await rc.request('GET', f'/api/files?query={query_json}')
    num_files = len(fc_response["files"])
    if num_files < 1:
        return None
    catalog_file = fc_response["files"][0]
    return await rc.request('GET', f'/api/files/{catalog_file["uuid"]}')

def _enumerate_path(path: str) -> List[str]:
    """Recursively walk the file system to enumerate files at provided path."""
    # enumerate all of the files on disk to be checked
    disk_files = []
    for root, dirs, files in os.walk(path):
        disk_files.extend([os.path.join(root, file) for file in files])
    return disk_files

async def _get_bundles_status(rc: RestClient, bundle_uuids: List[str]) -> List[Dict[str, Any]]:
    bundles = []
    for uuid in bundle_uuids:
        response = await rc.request('GET', f"/Bundles/{uuid}")
        KEYS = ['checksum', 'claimant', 'claimed', 'path', 'request', 'size', 'status', 'type', 'update_timestamp', 'uuid']
        bundle = {k: response[k] for k in KEYS}
        bundle["file_count"] = len(response["files"])
        bundles.append(bundle)
    return bundles

# -----------------------------------------------------------------------------

async def catalog_check(args: Namespace) -> None:
    """Check the files on disk vs. the file catalog and vice versa."""
    # something to hold our results
    catalog_missing = []
    disk_missing = []
    mismatch = []
    # enumerate all of the files on disk to be checked
    disk_files = _enumerate_path(args.path)
    # for all of the files we want to check
    for disk_file in disk_files:
        # determine the size of the file
        size = os.path.getsize(disk_file)
        # if we were told to compute the checksum
        checksum = None
        if args.checksums:
            checksum = sha512sum(disk_file)
        # ask the file catalog to retrieve the record of the file
        catalog_record = await _catalog_get(args.fc_rc, disk_file)
        if not catalog_record:
            if not args.json:
                print(f"Missing from the File Catalog: {disk_file}")
            catalog_missing.append(disk_file)
            continue
        # check the record for discrepancies
        if catalog_record["file_size"] != size:
            if not args.json:
                print(f"Mismatch between Catalog and Disk: {disk_file}")
            mismatch.append((disk_file, catalog_record, size, checksum))
            continue
        if args.checksums:
            if catalog_record["checksum"]["sha512"] != checksum:
                if not args.json:
                    print(f"Mismatch between Catalog and Disk: {disk_file}")
                mismatch.append((disk_file, catalog_record, size, checksum))
                continue

    # enumerate all of the catalog files to be checked
    query_dict = {
        "locations.site": {
            "$eq": "WIPAC"
        },
        "locations.path": {
            "$regex": f"^{args.path}"
        }
    }
    query_json = json.dumps(query_dict)
    fc_response = await args.fc_rc.request('GET', f'/api/files?query={query_json}')
    for catalog_file in fc_response["files"]:
        if catalog_file["logical_name"] not in disk_files:
            if not args.json:
                print(f"Missing from the Disk: {catalog_file['logical_name']}")
            disk_missing.append(catalog_file["logical_name"])

    # display the results to the caller
    if args.json:
        results_dict = {
            "catalog_missing": catalog_missing,
            "disk_missing": disk_missing,
            "mismatch": mismatch,
        }
        print_dict_as_pretty_json(results_dict)


async def catalog_display(args: Namespace) -> None:
    """Display a record from the File Catalog."""
    # if the user specified a path
    if args.path:
        # ask the file catalog to retrieve the record of the file
        catalog_record = await _catalog_get(args.fc_rc, args.path)

    # if the user specified a uuid
    if args.uuid:
        try:
            catalog_record = await args.fc_rc.request("GET", f"/api/files/{args.uuid}")
        except Exception:
            catalog_record = None

    # display the record to the caller
    if catalog_record:
        print_dict_as_pretty_json(catalog_record)
    else:
        print_dict_as_pretty_json({})


async def catalog_load(args: Namespace) -> None:
    """Load the files on disk into the file catalog."""
    # something to hold our results
    checked = []
    created = []
    updated = []
    # enumerate all of the files on disk to be checked
    disk_files = _enumerate_path(args.path)
    # for all of the files we want to check
    for disk_file in disk_files:
        # determine the size of the file
        size = os.path.getsize(disk_file)
        # ask the file catalog to retrieve the record of the file
        catalog_record = await _catalog_get(args.fc_rc, disk_file)
        # if there is a record
        disk_checksum = None
        if catalog_record:
            # validate some basic facts about the record
            check = True
            check &= (catalog_record["logical_name"] == disk_file)
            check &= (catalog_record["file_size"] == size)
            if args.checksums:
                disk_checksum = sha512sum(disk_file)
                check &= (catalog_record["checksum"]["sha512"] == disk_checksum)
            # if we passed the gauntlet, then move on the to next file
            if check:
                if not args.json:
                    print(f"Verified record for {disk_file}")
                checked.append(disk_file)
                continue
        # ut oh, you got here because you've got no record or a bad record
        if not disk_checksum:
            disk_checksum = sha512sum(disk_file)
        # if we've got a bad record, then we need to clean it up
        if catalog_record:
            patch_dict = {
                "logical_name": disk_file,
                "file_size": size,
                "checksum": catalog_record["checksum"]
            }
            patch_dict["checksum"]["sha512"] = disk_checksum
            await args.fc_rc.request("PATCH", f'/api/files/{catalog_record["uuid"]}', patch_dict)
            if not args.json:
                print(f"Updated record for {disk_file}")
            updated.append(disk_file)
        # otherwise, if we've got no record, then we need to make one
        else:
            post_dict = {
                "logical_name": disk_file,
                "checksum": {"sha512": disk_checksum},
                "file_size": size,
                "locations": [{"site": "WIPAC", "path": disk_file}],
            }
            await args.fc_rc.request("POST", "/api/files", post_dict)
            if not args.json:
                print(f"Created record for {disk_file}")
            created.append(disk_file)

    # display the results to the caller
    if args.json:
        results_dict = {
            "checked": checked,
            "created": created,
            "updated": updated,
        }
        print_dict_as_pretty_json(results_dict)


async def display_config(args: Namespace) -> None:
    """Display the configuration provided to the application."""
    if args.json:
        print_dict_as_pretty_json(args.config)
    else:
        for key in args.config:
            print(f"{key}:\t\t{args.config[key]}")


async def request_estimate(args: Namespace) -> None:
    """Estimate the count and size of a new TransferRequest."""
    # enumerate all of the files on disk to be checked
    disk_files = _enumerate_path(args.path)
    # for all of the files we want to check
    size = 0
    for disk_file in disk_files:
        # determine the size of the file
        size += os.path.getsize(disk_file)
    # build the result dictionary
    result = {
        "path": args.path,
        "count": len(disk_files),
        "size": size,
    }
    # for all of the files we want to check
    if args.json:
        print_dict_as_pretty_json(result)
    else:
        print(f"TransferReqeust for {args.path}")
        print(f"{size:,} bytes ({hurry.filesize.size(size)}) in {len(disk_files):,} files.")


async def request_ls(args: Namespace) -> None:
    """List all of the TransferRequest objects in the LTA DB."""
    response = await args.lta_rc.request("GET", "/TransferRequests")
    if args.json:
        print_dict_as_pretty_json(response)
    else:
        results = response["results"]
        for x in results:
            display_id = x["uuid"]
            if not args.long:
                display_id = display_id[:8]
            create_time = x["create_timestamp"].replace("T", " ")
            path = x["path"]
            source = x["source"]
            dest = x["dest"]
            status = x["status"]
            print(f"{display_id} {status}  {create_time} {source} -> {dest} {path}")


async def request_new(args: Namespace) -> None:
    """Create a new TransferRequest and add it to the LTA DB."""
    # get some stuff
    source = args.source
    dest = args.dest
    path = args.path
    # construct the TransferRequest body
    request_body = {
        "source": source,
        "dest": dest,
        "path": path,
    }
    response = await args.lta_rc.request("POST", "/TransferRequests", request_body)
    uuid = response["TransferRequest"]
    tr = await args.lta_rc.request("GET", f"/TransferRequests/{uuid}")
    if args.json:
        print_dict_as_pretty_json(tr)
    else:
        display_id = tr["uuid"]
        create_time = tr["create_timestamp"].replace("T", " ")
        print(f"{display_id}  {create_time} {path} {source} -> {dest}")


async def request_status(args: Namespace) -> None:
    """Query the status of a TransferRequest in the LTA DB."""
    response = await args.lta_rc.request("GET", "/TransferRequests")
    results = response["results"]
    for x in results:
        if x["uuid"].startswith(args.uuid):
            res2 = await args.lta_rc.request("GET", f"/Bundles?request={x['uuid']}")
            x["bundles"] = await _get_bundles_status(args.lta_rc, res2["results"])
            if args.json:
                print_dict_as_pretty_json(x)
            else:
                display_id = x["uuid"]
                status = x["status"]
                status_time = x["create_timestamp"].replace("T", " ")
                if status == "unclaimed":
                    status_desc = "Waiting to be claimed"
                elif status == "processing":
                    status_desc = "Request is processing"
                elif status == "completed":
                    status_desc = "Request is complete"
                else:
                    status_desc = "Ut oh; Unknown status type"
                print(f"{display_id} {status}  {status_time} - {status_desc}")
                for b in x["bundles"]:
                    if b['claimed']:
                        print(f"    {b['uuid']} [{b['status']}] claimant:{b['claimant']}")
                    else:
                        print(f"    {b['uuid']} [{b['status']}] claimed:{b['claimed']}")


async def status(args: Namespace) -> None:
    """Query the status of the LTA DB or a component of LTA."""
    old_data = (datetime.utcnow() - timedelta(seconds=60*5)).isoformat()

    def date_ok(d: str) -> bool:
        return d > old_data

    # if we want the status of a particular component type
    if args.component:
        response = await args.lta_rc.request("GET", f"/status/{args.component}")
        if args.json:
            print_dict_as_pretty_json(response)
        else:
            for key in response:
                timestamp = response[key]['timestamp']
                status = "WARN"
                if date_ok(timestamp):
                    status = "OK"
                print(f"{(key+':'):<25}[{status:<4}] {timestamp.replace('T', ' ')}")
    # otherwise we want the status of the whole system
    else:
        response = await args.lta_rc.request("GET", "/status")
        if args.json:
            print_dict_as_pretty_json(response)
        else:
            print(f"LTA:          {response['health']}")
            for key in response:
                if key != "health":
                    print(f"{(key+':'):<14}{response[key]}")

# -----------------------------------------------------------------------------

async def main() -> None:
    """Process a request from the Command Line."""
    # configure the application from the environment
    config = from_environment(EXPECTED_CONFIG)
    fc_rc = RestClient(config["FILE_CATALOG_REST_URL"], token=config["FILE_CATALOG_REST_TOKEN"])
    lta_rc = RestClient(config["LTA_REST_URL"], token=config["LTA_REST_TOKEN"])

    # define our top-level argument parsing
    parser = argparse.ArgumentParser(prog="ltacmd")
    parser.set_defaults(config=config, fc_rc=fc_rc, lta_rc=lta_rc)
    subparser = parser.add_subparsers(help='command help')

    # define a subparser for the 'catalog' subcommand
    parser_catalog = subparser.add_parser('catalog', help='interact with the file catalog')
    catalog_subparser = parser_catalog.add_subparsers(help='catalog command help')

    # define a subparser for the 'catalog check' subcommand
    parser_catalog_check = catalog_subparser.add_parser('check', help='compare catalog and disk entries')
    parser_catalog_check.add_argument("--checksums",
                                      help="check using sha512sum checksums",
                                      action="store_true")
    parser_catalog_check.add_argument("--json",
                                      help="display output in JSON",
                                      action="store_true")
    parser_catalog_check.add_argument("--path",
                                      help="Data Warehouse path to be checked",
                                      required=True)
    parser_catalog_check.set_defaults(func=catalog_check)

    # define a subparser for the 'catalog check' subcommand
    parser_catalog_display = catalog_subparser.add_parser('display', help='display a file catalog record')
    parser_catalog_display.add_argument("--path",
                                        help="Data Warehouse path to be displayed")
    parser_catalog_display.add_argument("--uuid",
                                        help="Catalog UUID to be displayed")
    parser_catalog_display.set_defaults(func=catalog_display)

    # define a subparser for the 'catalog load' subcommand
    parser_catalog_load = catalog_subparser.add_parser('load', help='load disk entries into the catalog')
    parser_catalog_load.add_argument("--checksums",
                                     help="verify checksums of existing files",
                                     action="store_true")
    parser_catalog_load.add_argument("--json",
                                     help="display output in JSON",
                                     action="store_true")
    parser_catalog_load.add_argument("--path",
                                     help="Data Warehouse path to be loaded",
                                     required=True)
    parser_catalog_load.set_defaults(func=catalog_load)

    # define a subparser for the 'display-config' subcommand
    parser_display_config = subparser.add_parser('display-config', help='display environment configuration')
    parser_display_config.add_argument("--json",
                                       help="display output in JSON",
                                       action="store_true")
    parser_display_config.set_defaults(func=display_config)

    # define a subparser for the 'request' subcommand
    parser_request = subparser.add_parser('request', help='interact with transfer requests')
    request_subparser = parser_request.add_subparsers(help='request command help')

    # define a subparser for the 'request estimate' subcommand
    parser_request_estimate = request_subparser.add_parser('estimate', help='estimate new transfer request')
    # parser_request_estimate.add_argument("--source",
    #                                      help="site as source of files",
    #                                      required=True)
    # parser_request_estimate.add_argument("--dest",
    #                                      help="site as destination of bundles",
    #                                      required=True)
    parser_request_estimate.add_argument("--path",
                                         help="Data Warehouse path to be transferred",
                                         required=True)
    parser_request_estimate.add_argument("--json",
                                         help="display output in JSON",
                                         action="store_true")
    parser_request_estimate.set_defaults(func=request_estimate)

    # define a subparser for the 'request ls' subcommand
    parser_request_ls = request_subparser.add_parser('ls', help='list transfer requests')
    parser_request_ls.add_argument("--json",
                                   help="display output in JSON",
                                   action="store_true")
    parser_request_ls.add_argument("--long",
                                   help="display long format UUIDs",
                                   action="store_true")
    parser_request_ls.set_defaults(func=request_ls)

    # define a subparser for the 'request new' subcommand
    parser_request_new = request_subparser.add_parser('new', help='create new transfer request')
    parser_request_new.add_argument("--source",
                                    help="site as source of files",
                                    required=True)
    parser_request_new.add_argument("--dest",
                                    help="site as destination of bundles",
                                    required=True)
    parser_request_new.add_argument("--path",
                                    help="Data Warehouse path to be transferred",
                                    required=True)
    parser_request_new.add_argument("--json",
                                    help="display output in JSON",
                                    action="store_true")
    parser_request_new.set_defaults(func=request_new)

    # define a subparser for the 'request status' subcommand
    parser_request_status = request_subparser.add_parser('status', help='query transfer request status')
    parser_request_status.add_argument("--uuid",
                                       help="identity of transfer request",
                                       required=True)
    parser_request_status.add_argument("--json",
                                       help="display output in JSON",
                                       action="store_true")
    parser_request_status.set_defaults(func=request_status)

    # define a subparser for the 'status' subcommand
    parser_status = subparser.add_parser('status', help='perform a status query')
    parser_status.add_argument("component",
                               choices=COMPONENT_NAMES,
                               help="optional LTA component",
                               nargs='?')
    parser_status.add_argument("--json",
                               help="display output in JSON",
                               action="store_true")
    parser_status.set_defaults(func=status)

    # parse the provided command line arguments and call the function
    args = parser.parse_args()
    if hasattr(args, "func"):
        try:
            await args.func(args)
        except Exception as e:
            print(e)
    else:
        parser.print_usage()


if __name__ == '__main__':
    logging.basicConfig(level=logging.CRITICAL)
    asyncio.get_event_loop().run_until_complete(main())
