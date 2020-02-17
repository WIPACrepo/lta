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
from time import mktime, strptime
from typing import Any, Dict, List, Optional, Tuple

import hurry.filesize  # type: ignore
from rest_tools.client import RestClient  # type: ignore

from lta.component import now
from lta.config import from_environment
from lta.crypto import sha512sum


COMPONENT_NAMES = [
    "bundler",
    "deleter",
    "nersc_mover",
    "nersc_verifier",
    "picker",
    "replicator",
    "site_move_verifier",
]

EXPECTED_CONFIG = {
    'FILE_CATALOG_REST_TOKEN': None,
    'FILE_CATALOG_REST_URL': None,
    'LTA_REST_TOKEN': None,
    'LTA_REST_URL': None,
}

KILOBYTE = 1024
MEGABYTE = KILOBYTE * KILOBYTE
GIGABYTE = MEGABYTE * KILOBYTE
MINIMUM_REQUEST_SIZE = 100 * GIGABYTE

def as_datetime(s: str) -> datetime:
    """Convert a timestamp string into a datetime object."""
    # if Python 3.7+
    # return datetime.fromisoformat(s)

    # before Python 3.7
    st = strptime(s, "%Y-%m-%dT%H:%M:%S")
    return datetime.fromtimestamp(mktime(st))

def display_time(s: str) -> str:
    """Make a timestamp string look nice."""
    return s.replace("T", " ")

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
        },
        "logical_name": {
            "$eq": path
        },
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
        KEYS = ['claimant', 'claimed', 'path', 'request', 'status', 'type', 'update_timestamp', 'uuid']
        bundle = {k: response[k] for k in KEYS}
        bundle["file_count"] = len(response["files"])
        bundles.append(bundle)
    return bundles

def _get_files_and_size(path: str) -> Tuple[List[str], int]:
    """Recursively walk and add the files of files in the file system."""
    # enumerate all of the files on disk to be checked
    disk_files = _enumerate_path(path)
    # for all of the files we want to check
    size = 0
    for disk_file in disk_files:
        # determine the size of the file
        size += os.path.getsize(disk_file)
    return (disk_files, size)

# -----------------------------------------------------------------------------

async def bundle_ls(args: Namespace) -> None:
    """List all of the Bundle objects in the LTA DB."""
    response = await args.lta_rc.request("GET", "/Bundles")
    if args.json:
        print_dict_as_pretty_json(response)
    else:
        results = response["results"]
        print(f"total {len(results)}")
        for uuid in results:
            print(f"Bundle {uuid}")


async def bundle_overdue(args: Namespace) -> None:
    """List of the problematic Bundle objects in the LTA DB."""
    # calculate our cutoff time for bundles not making progress
    cutoff_time = datetime.utcnow() - timedelta(days=args.days)
    # query the LTA DB to get a list of bundles to check
    response = await args.lta_rc.request("GET", "/Bundles")
    results = response["results"]
    # for each bundle, query the LTA DB and check it
    problem_bundles = []
    for uuid in results:
        bundle = await args.lta_rc.request("GET", f"/Bundles/{uuid}")
        del bundle["files"]
        if bundle["status"] == "quarantined":
            problem_bundles.append(bundle)
        elif as_datetime(bundle["update_timestamp"]) < cutoff_time:
            problem_bundles.append(bundle)
    # report the list of miscreants to the user
    if args.json:
        print_dict_as_pretty_json({"bundles": problem_bundles})
    else:
        for bundle in problem_bundles:
            print(f"Bundle {bundle['uuid']}")
            print(f"    Status: {bundle['status']} ({display_time(bundle['update_timestamp'])})")
            print(f"    Claimed: {bundle['claimed']}")
            if bundle['claimed']:
                print(f"        Claimant: {bundle['claimant']} ({display_time(bundle['claim_timestamp'])})")


async def bundle_status(args: Namespace) -> None:
    """Query the status of a Bundle in the LTA DB."""
    response = await args.lta_rc.request("GET", f"/Bundles/{args.uuid}")
    if args.json:
        print_dict_as_pretty_json(response)
    else:
        # display information about the core fields
        print(f"Bundle {args.uuid}")
        print(f"    Status: {response['status']} ({display_time(response['update_timestamp'])})")
        print(f"    Claimed: {response['claimed']}")
        if response['claimed']:
            print(f"        Claimant: {response['claimant']} ({display_time(response['claim_timestamp'])})")
        print(f"    TransferRequest: {response['request']}")
        print(f"    Source: {response['source']} -> Dest: {response['dest']}")
        print(f"    Path: {response['path']}")
        print(f"    Files: {len(response['files'])}")
        # display additional information if available
        if 'bundle_path' in response:
            print(f"    Bundle File: {response['bundle_path']}")
        if 'size' in response:
            print(f"    Size: {response['size']}")
        if 'checksum' in response:
            print(f"    Checksum")
            print(f"        adler32: {response['checksum']['adler32']}")
            print(f"        sha512:  {response['checksum']['sha512']}")
        # display the contents of the bundle, if requested
        if args.contents:
            print(f"    Contents:")
            for file in response["files"]:
                print(f"        {file['logical_name']} {file['file_size']}")


async def bundle_update_status(args: Namespace) -> None:
    """Update the status of a Bundle in the LTA DB."""
    patch_body = {}
    patch_body["status"] = args.new_status
    patch_body["reason"] = ""
    patch_body["update_timestamp"] = now()
    if not args.keep_claim:
        patch_body["claimed"] = False
    await args.lta_rc.request("PATCH", f"/Bundles/{args.uuid}", patch_body)


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
        },
        "logical_name": {
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


async def display_config(args: Namespace) -> None:
    """Display the configuration provided to the application."""
    if args.json:
        print_dict_as_pretty_json(args.config)
    else:
        for key in args.config:
            print(f"{key}:\t\t{args.config[key]}")


async def request_estimate(args: Namespace) -> None:
    """Estimate the count and size of a new TransferRequest."""
    files_and_size = _get_files_and_size(args.path)
    disk_files = files_and_size[0]
    size = files_and_size[1]
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
        print(f"TransferRequest for {args.path}")
        print(f"{size:,} bytes ({hurry.filesize.size(size)}) in {len(disk_files):,} files.")


async def request_ls(args: Namespace) -> None:
    """List all of the TransferRequest objects in the LTA DB."""
    response = await args.lta_rc.request("GET", "/TransferRequests")
    if args.json:
        print_dict_as_pretty_json(response)
    else:
        results = response["results"]
        print(f"total {len(results)}")
        for request in results:
            print(f"{display_time(request['create_timestamp'])} TransferRequest {request['uuid']} {request['source']} -> {request['dest']} {request['path']}")


async def request_new(args: Namespace) -> None:
    """Create a new TransferRequest and add it to the LTA DB."""
    # determine how big the transfer request is going to be
    files_and_size = _get_files_and_size(args.path)
    disk_files = files_and_size[0]
    size = files_and_size[1]
    # if it doesn't meet our minimize size requirement
    if size < MINIMUM_REQUEST_SIZE:
        # and the operator has not forced the issue
        if not args.force:
            # raise an Exception to prevent the command from creating a too small request
            raise Exception(f"TransferRequest for {args.path}\n{size:,} bytes ({hurry.filesize.size(size)}) in {len(disk_files):,} files.\nMinimum required size: {MINIMUM_REQUEST_SIZE:,} bytes.")
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


async def request_rm(args: Namespace) -> None:
    """Remove a TransferRequest from the LTA DB."""
    response = await args.lta_rc.request("GET", f"/TransferRequests/{args.uuid}")
    path = response["path"]
    if args.confirm != path:
        print(f"request rm: cannot remove TransferRequest {args.uuid}: path is not --confirm {args.confirm}")
        return
    await args.lta_rc.request("DELETE", f"/TransferRequests/{args.uuid}")
    if args.verbose:
        print(f"removed TransferRequest {args.uuid}")
    res3 = await args.lta_rc.request("GET", f"/Bundles?request={args.uuid}")
    bundles = await _get_bundles_status(args.lta_rc, res3["results"])
    for bundle in bundles:
        await args.lta_rc.request("DELETE", f"/Bundles/{bundle['uuid']}")
        if args.verbose:
            print(f"removed Bundle {bundle['uuid']}")


async def request_status(args: Namespace) -> None:
    """Query the status of a TransferRequest in the LTA DB."""
    response = await args.lta_rc.request("GET", f"/TransferRequests/{args.uuid}")
    res2 = await args.lta_rc.request("GET", f"/Bundles?request={args.uuid}")
    response["bundles"] = await _get_bundles_status(args.lta_rc, res2["results"])
    if args.json:
        print_dict_as_pretty_json(response)
    else:
        # display information about the core fields
        print(f"TransferRequest {args.uuid}")
        print(f"    Status: {response['status']} ({display_time(response['update_timestamp'])})")
        print(f"    Claimed: {response['claimed']}")
        if response['claimed']:
            print(f"        Claimant: {response['claimant']} ({display_time(response['claim_timestamp'])})")
        print(f"    Source: {response['source']} -> Dest: {response['dest']}")
        print(f"    Path: {response['path']}")
        print(f"    Bundles: {len(response['bundles'])}")
        # display the contents of the transfer request, if requested
        if args.contents:
            print(f"    Contents:")
            for bundle in response["bundles"]:
                print(f"        Bundle {bundle['uuid']}")
                print(f"            Status: {bundle['status']} ({display_time(bundle['update_timestamp'])})")
                print(f"            Claimed: {bundle['claimed']}")
                if bundle['claimed']:
                    print(f"                Claimant: {response['claimant']} ({display_time(response['claim_timestamp'])})")
                print(f"            Files: {bundle['file_count']}")


async def request_update_status(args: Namespace) -> None:
    """Update the status of a TransferRequest in the LTA DB."""
    patch_body = {}
    patch_body["status"] = args.new_status
    patch_body["update_timestamp"] = now()
    if not args.keep_claim:
        patch_body["claimed"] = False
    await args.lta_rc.request("PATCH", f"/TransferRequests/{args.uuid}", patch_body)


async def status(args: Namespace) -> None:
    """Query the status of the LTA DB or a component of LTA."""
    old_data = (datetime.utcnow() - timedelta(days=args.days)).isoformat()

    def date_ok(d: str) -> bool:
        return d > old_data

    # if we want the status of a particular component type
    if args.component:
        response = await args.lta_rc.request("GET", f"/status/{args.component}")
        if args.json:
            r = response.copy()
            for key in response:
                timestamp = response[key]['timestamp']
                if not date_ok(timestamp):
                    del r[key]
            print_dict_as_pretty_json(r)
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

    # define a subparser for the 'bundle' subcommand
    parser_bundle = subparser.add_parser('bundle', help='interact with bundles')
    bundle_subparser = parser_bundle.add_subparsers(help='bundle command help')

    # define a subparser for the 'bundle ls' subcommand
    parser_bundle_ls = bundle_subparser.add_parser('ls', help='list bundles')
    parser_bundle_ls.add_argument("--json",
                                  help="display output in JSON",
                                  action="store_true")
    parser_bundle_ls.set_defaults(func=bundle_ls)

    # define a subparser for the 'bundle overdue' subcommand
    parser_bundle_overdue = bundle_subparser.add_parser('overdue', help='list problematic bundles')
    parser_bundle_overdue.add_argument("--days",
                                       help="upper limit of days without progress",
                                       type=int,
                                       default=3)
    parser_bundle_overdue.add_argument("--json",
                                       help="display output in JSON",
                                       action="store_true")
    parser_bundle_overdue.set_defaults(func=bundle_overdue)

    # define a subparser for the 'bundle status' subcommand
    parser_bundle_status = bundle_subparser.add_parser('status', help='query bundle status')
    parser_bundle_status.add_argument("--uuid",
                                      help="identity of bundle",
                                      required=True)
    parser_bundle_status.add_argument("--contents",
                                      help="list the contents of the bundle",
                                      action="store_true")
    parser_bundle_status.add_argument("--json",
                                      help="display output in JSON",
                                      action="store_true")
    parser_bundle_status.set_defaults(func=bundle_status)

    # define a subparser for the 'bundle update-status' subcommand
    parser_bundle_update_status = bundle_subparser.add_parser('update-status', help='update bundle status')
    parser_bundle_update_status.add_argument("--uuid",
                                             help="identity of bundle",
                                             required=True)
    parser_bundle_update_status.add_argument("--new-status",
                                             dest="new_status",
                                             help="new status of the bundle",
                                             required=True)
    parser_bundle_update_status.add_argument("--keep-claim",
                                             dest="keep_claim",
                                             help="don't unclaim the bundle",
                                             action="store_true")
    parser_bundle_update_status.set_defaults(func=bundle_update_status)

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

    # define a subparser for the 'catalog display' subcommand
    parser_catalog_display = catalog_subparser.add_parser('display', help='display a file catalog record')
    parser_catalog_display.add_argument("--path",
                                        help="Data Warehouse path to be displayed")
    parser_catalog_display.add_argument("--uuid",
                                        help="Catalog UUID to be displayed")
    parser_catalog_display.set_defaults(func=catalog_display)

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
    parser_request_new.add_argument("--force",
                                    help="force small size transfer request",
                                    action="store_true")
    parser_request_new.set_defaults(func=request_new)

    # define a subparser for the 'request rm' subcommand
    parser_request_rm = request_subparser.add_parser('rm', help='delete a transfer request')
    parser_request_rm.add_argument("--uuid",
                                   help="identity of transfer request",
                                   required=True)
    parser_request_rm.add_argument("--confirm",
                                   help="Data Warehouse path of the request",
                                   required=True)
    parser_request_rm.add_argument("--verbose",
                                   help="display an output line on success",
                                   action="store_true")
    parser_request_rm.set_defaults(func=request_rm)

    # define a subparser for the 'request status' subcommand
    parser_request_status = request_subparser.add_parser('status', help='query transfer request status')
    parser_request_status.add_argument("--uuid",
                                       help="identity of transfer request",
                                       required=True)
    parser_request_status.add_argument("--contents",
                                       help="list the contents of the transfer request",
                                       action="store_true")
    parser_request_status.add_argument("--json",
                                       help="display output in JSON",
                                       action="store_true")
    parser_request_status.set_defaults(func=request_status)

    # define a subparser for the 'request update-status' subcommand
    parser_request_update_status = request_subparser.add_parser('update-status', help='update transfer request status')
    parser_request_update_status.add_argument("--uuid",
                                              help="identity of transfer request",
                                              required=True)
    parser_request_update_status.add_argument("--new-status",
                                              dest="new_status",
                                              help="new status of the transfer request",
                                              required=True)
    parser_request_update_status.add_argument("--keep-claim",
                                              dest="keep_claim",
                                              help="don't unclaim the transfer request",
                                              action="store_true")
    parser_request_update_status.set_defaults(func=request_update_status)

    # define a subparser for the 'status' subcommand
    parser_status = subparser.add_parser('status', help='perform a status query')
    parser_status.add_argument("component",
                               choices=COMPONENT_NAMES,
                               help="optional LTA component",
                               nargs='?')
    parser_status.add_argument("--days",
                               help="ignore status reports older than",
                               type=int,
                               default=2)
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
