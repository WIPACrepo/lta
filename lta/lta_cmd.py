"""
Command line utility for Long Term Archive.

Run with `python -m lta.lta_cmd $@`.
"""

import argparse
import asyncio
from datetime import datetime, timedelta
import json
import logging
from operator import itemgetter
import os
import sys
from time import mktime, strptime
from typing import Any, cast, Dict, List, Optional, Tuple

import colorama  # type: ignore
import hurry.filesize  # type: ignore
from rest_tools.client import RestClient
from rest_tools.server import from_environment
import urllib

from lta.component import now
from lta.crypto import sha512sum

Namespace = argparse.Namespace

Fore = colorama.Fore
Style = colorama.Style

ExitCode = int
EXIT_OK = 0
EXIT_ERROR = 1

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

PATH_PREFIX_ALLOW_LIST = [
    "/data/ana",
    "/data/exp",
    "/data/sim",
]

# -----------------------------------------------------------------------------

def as_datetime(s: str) -> datetime:
    """Convert a timestamp string into a datetime object."""
    # if Python 3.7+
    # return datetime.fromisoformat(s)

    # before Python 3.7
    st = strptime(s, "%Y-%m-%dT%H:%M:%S")
    return datetime.fromtimestamp(mktime(st))

def display_time(s: Optional[str]) -> str:
    """Make a timestamp string look nice."""
    if s:
        return s.replace("T", " ")
    return "Unknown"

def normalize_path(path: str) -> str:
    """Validate and normalize the provided request path."""
    path = os.path.normpath(path)
    for prefix in PATH_PREFIX_ALLOW_LIST:
        if path.startswith(prefix):
            return path
    raise ValueError(f"{path} does not begin with a prefix on the allow-list prefix")

def print_catalog_record_as_line(d: Dict[str, Any]) -> None:
    """Print the provided File Catalog record as a stat line."""
    date = d["meta_modify_date"][:19]
    if "create_date" in d:
        date = d["create_date"].replace("T", " ")
    size = d["file_size"]
    uuid = d["uuid"]
    logical_name = d["logical_name"]
    print(f"{date} | {size} | {uuid} | {logical_name}")

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
        # this is probably OK; finding a single file catalog record by complete path
        "logical_name": {
            "$eq": path
        },
    }
    query_json = json.dumps(query_dict)
    fc_response = await rc.request('GET', f'/api/files?query={query_json}&limit=1')
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
        KEYS = ['claim_timestamp', 'claimant', 'claimed', 'create_timestamp', 'path', 'request', 'status', 'type', 'update_timestamp', 'uuid']
        bundle = {}
        for k in KEYS:
            if k in response:
                bundle[k] = response[k]
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

def _get_status_bar(status_list: List[str],
                    status: str,
                    module_map: Optional[Dict[str, str]] = None,
                    claimant: Optional[str] = None) -> str:
    """Create a colorful status bar."""
    # this is our status bar string
    sb = ""
    # flag: is this a regular or error state?
    error_state = False
    # how long is the list of statuses?
    list_len = len(status_list)
    # what is the widest status we'll see?
    status_width = len(status)
    for listed in status_list:
        status_width = max(status_width, len(listed))
    # where on the status list are we?
    status_index = -1
    try:
        status_index = status_list.index(status)
    except ValueError:
        # okay, where on the status list should we be?
        error_state = True
        if module_map and claimant:
            for key in module_map.keys():
                if key in claimant:
                    status_index = status_list.index(module_map[key])
    # start building the status bar
    sb += Style.NORMAL
    sb += Fore.WHITE
    sb += "["
    sb += Style.BRIGHT
    # if we've got no idea where we go on the status list
    if status_index < 0:
        sb += Fore.RED
        for i in range(0, list_len):
            sb += "?"
    # or if we've reached the final status
    elif (list_len-status_index) == 1:
        sb += Fore.GREEN
        for i in range(0, list_len):
            sb += "#"
    # otherwise we need to render something in progress
    else:
        sb += Fore.GREEN
        for i in range(0, status_index):
            sb += "#"
        if error_state:
            sb += Fore.RED
            sb += "X"
        else:
            sb += Fore.YELLOW
            sb += ">"
        sb += Fore.BLUE
        for i in range(1, list_len-status_index):
            sb += "_"
    # finish building the status bar
    sb += Style.NORMAL
    sb += Fore.WHITE
    sb += "]:"
    sb += Style.BRIGHT
    if error_state:
        sb += Fore.RED
    else:
        sb += Fore.CYAN
    sb += f"{status:<{status_width}} "
    # return the status bar to the caller
    return sb


def _is_nersc_bundle_record(d: Dict[str, Any]) -> bool:
    """Determine if the provided catalog record is a bundle at NERSC."""
    # if we didn't get a record, this is not a bundle at NERSC
    if not d:
        return False
    # if the record doesn't contain locations, this is not a bundle at NERSC
    if "locations" not in d:
        return False
    # for each location
    all_good = False
    for location in d["locations"]:
        # if the location record doesn't have the appropriate keys, skip it
        if "site" not in location:
            continue
        if "hpss" not in location:
            continue
        if "online" not in location:
            continue
        # if this isn't a NERSC location, skip it
        if location["site"] != "NERSC":
            continue
        # if this isn't on hpss, skip it
        if not location["hpss"]:
            continue
        # if this record is online, skip it
        if location["online"]:
            continue
        # winner, winner, chicken dinner!
        all_good = True
    # tell the caller what we think about the record
    return all_good

# -----------------------------------------------------------------------------

async def bundle_ls(args: Namespace) -> ExitCode:
    """List all of the Bundle objects in the LTA DB."""
    response = await args.di["lta_rc"].request("GET", "/Bundles")
    if args.json:
        print_dict_as_pretty_json(response)
    else:
        results = response["results"]
        print(f"total {len(results)}")
        for uuid in results:
            if args.show_status:
                bundle = await args.di["lta_rc"].request("GET", f"/Bundles/{uuid}?contents=0")
                print(f"Bundle {uuid} {bundle['status']}")
            else:
                print(f"Bundle {uuid}")
    return EXIT_OK


async def bundle_overdue(args: Namespace) -> ExitCode:
    """List of the problematic Bundle objects in the LTA DB."""
    # calculate our cutoff time for bundles not making progress
    cutoff_time = datetime.utcnow() - timedelta(days=args.days)
    # query the LTA DB to get a list of bundles to check
    response = await args.di["lta_rc"].request("GET", "/Bundles")
    results = response["results"]
    # for each bundle, query the LTA DB and check it
    problem_bundles = []
    for uuid in results:
        bundle = await args.di["lta_rc"].request("GET", f"/Bundles/{uuid}?contents=0")
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
    return EXIT_OK


async def bundle_priority_reset(args: Namespace) -> ExitCode:
    """List all of the Bundle objects in the LTA DB."""
    response = await args.di["lta_rc"].request("GET", "/Bundles")
    results = response["results"]
    for uuid in results:
        response2 = await args.di["lta_rc"].request("GET", f"/Bundles/{uuid}?contents=0")
        patch_body = {
            "update_timestamp": now(),
            "work_priority_timestamp": response2["create_timestamp"],
        }
        await args.di["lta_rc"].request("PATCH", f"/Bundles/{uuid}", patch_body)
    return EXIT_OK


async def bundle_status(args: Namespace) -> ExitCode:
    """Query the status of a Bundle in the LTA DB."""
    response = await args.di["lta_rc"].request("GET", f"/Bundles/{args.uuid}")
    if args.json:
        print_dict_as_pretty_json(response)
    else:
        # display information about the core fields
        print(f"Bundle {args.uuid}")
        print(f"    Priority: {display_time(response['work_priority_timestamp'])}")
        print(f"    Status: {response['status']} ({display_time(response['update_timestamp'])})")
        if response['status'] == "quarantined":
            print(f"        Reason: {response['reason']}")
        print(f"    Claimed: {response['claimed']}")
        if response['claimed']:
            print(f"        Claimant: {response['claimant']} ({display_time(response['claim_timestamp'])})")
        print(f"    TransferRequest: {response['request']}")
        print(f"    Source: {response['source']} -> Dest: {response['dest']}")
        print(f"    Path: {response['path']}")
        if 'files' in response:
            print(f"    Files: {len(response['files'])}")
        else:
            print("    Files: Not Listed")
        # display additional information if available
        if 'bundle_path' in response:
            print(f"    Bundle File: {response['bundle_path']}")
        if 'size' in response:
            print(f"    Size: {response['size']}")
        if 'checksum' in response:
            print("    Checksum")
            print(f"        adler32: {response['checksum']['adler32']}")
            print(f"        sha512:  {response['checksum']['sha512']}")
        # display the contents of the bundle, if requested
        if args.contents:
            print("    Contents: Not Listed")
    return EXIT_OK


async def bundle_update_status(args: Namespace) -> ExitCode:
    """Update the status of a Bundle in the LTA DB."""
    right_now = now()
    patch_body = {}
    patch_body["status"] = args.new_status
    patch_body["reason"] = ""
    patch_body["update_timestamp"] = right_now
    if not args.keep_claim:
        patch_body["claimed"] = False
    if not args.keep_priority:
        patch_body["work_priority_timestamp"] = right_now
    await args.di["lta_rc"].request("PATCH", f"/Bundles/{args.uuid}", patch_body)
    return EXIT_OK


async def catalog_check(args: Namespace) -> ExitCode:
    """Check the files on disk vs. the file catalog and vice versa."""
    exit_code = EXIT_OK
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
        catalog_record = await _catalog_get(args.di["fc_rc"], disk_file)
        if not catalog_record:
            exit_code = EXIT_ERROR
            if not args.json:
                print(f"Missing from the File Catalog: {disk_file}")
            catalog_missing.append(disk_file)
            continue
        # check the record for discrepancies
        if catalog_record["file_size"] != size:
            exit_code = EXIT_ERROR
            if not args.json:
                print(f"Mismatch between Catalog and Disk: {disk_file}")
            mismatch.append((disk_file, catalog_record, size, checksum))
            continue
        if args.checksums:
            if catalog_record["checksum"]["sha512"] != checksum:
                exit_code = EXIT_ERROR
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
        # this isn't going to work; searching 'logical_name' by regular expression
        # "logical_name": {
        #     "$regex": f"^{args.path}"
        # }
    }
    query_json = json.dumps(query_dict)
    fc_response = await args.di["fc_rc"].request('GET', f'/api/files?query={query_json}')
    for catalog_file in fc_response["files"]:
        if catalog_file["logical_name"] not in disk_files:
            exit_code = EXIT_ERROR
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

    # return the appropriate exit code based on what we found
    return exit_code


async def catalog_query(args: Namespace) -> ExitCode:
    """Run a freeform query against the File Catalog."""
    # URL encode the provided query string if requested
    query = args.query
    if args.url_encode:
        query = urllib.parse.quote_plus(query)

    # print out the query string if we're debugging
    if args.debug:
        print(f"Using query string: {query}\n\nURL: /api/files?query={query}")

    # run the query
    fc_response = await args.di["fc_rc"].request('GET', f'/api/files?query={query}')

    # display the results to the caller
    if args.json:
        print_dict_as_pretty_json(fc_response)
    else:
        print(fc_response)

    # return the appropriate exit code based on what we found
    return EXIT_OK


async def catalog_display(args: Namespace) -> ExitCode:
    """Display a record from the File Catalog."""
    # if the user specified a path
    if args.path:
        # ask the file catalog to retrieve the record of the file
        catalog_record = await _catalog_get(args.di["fc_rc"], args.path)

    # if the user specified a uuid
    if args.uuid:
        try:
            catalog_record = await args.di["fc_rc"].request("GET", f"/api/files/{args.uuid}")
        except Exception:
            catalog_record = None

    # display the record to the caller
    if catalog_record:
        print_dict_as_pretty_json(catalog_record)
    else:
        print_dict_as_pretty_json({})

    return EXIT_OK


async def catalog_stats(args: Namespace) -> ExitCode:
    """Query for the bundles archived at NERSC."""
    exit_code = EXIT_OK

    # we want files at NERSC that are not contained within archives
    query_dict = {
        # this isn't going to work; searching 'logical_name' by regular expression
        # "logical_name": {
        #     "$regex": "^/home/projects/icecube"
        # },
        "locations.site": {
            "$eq": "NERSC"
        },
        "locations.path": {
            "$regex": "^/home/projects/icecube"
        },
    }
    query_json = json.dumps(query_dict)
    keys = "create_date|file_size|locations|logical_name|meta_modify_date|uuid"
    start = 0
    limit = 500
    finished = False

    # until we're done querying the File Catalog
    while not finished:
        # ask it for another {limit} file records to check
        fc_response = await args.di["fc_rc"].request('GET', f'/api/files?query={query_json}&keys={keys}&start={start}&limit={limit}')
        # for each record we got back
        for catalog_file in fc_response["files"]:
            # if it's an LTA bundle at NERSC
            if _is_nersc_bundle_record(catalog_file):
                # display the record
                print_catalog_record_as_line(catalog_file)

        # if we got {limit} file records to check
        if len(fc_response["files"]) == limit:
            # then update our indexes to check the next bunch
            start = start + limit
        else:
            # otherwise, this was the last bunch, we're done
            finished = True

    # return the appropriate exit code based on what we found
    return exit_code


async def dashboard(args: Namespace) -> ExitCode:
    """Display a Dashboard of on-going transfers."""
    # define the list of TransferRequest statuses
    REQUEST_STATUS = [
        "unclaimed",
        "processing",
        "completed",
    ]
    # define the list of Bundle statuses
    BUNDLE_STATUS = [
        "specified",
        "created",
        "staged",
        "transferring",
        "taping",
        "verifying",
        "completed",
        "source-deleted",
        "deleted",
        "finished",
    ]
    # define a mapping between LTA module and Bundle status
    MODULE_MAP = {
        "bundler": "specified",
        "rate-limiter": "created",
        "replicator": "staged",
        "site-move-verifier": "transferring",
        "nersc-mover": "taping",
        "nersc-verifier": "verifying",
        "deleter": "completed",
        "transfer-request-finisher": "deleted",
    }
    # get a list of all requests in the system
    response = await args.di["lta_rc"].request("GET", "/TransferRequests")
    results = response["results"]
    requests = []
    for result in results:
        if args.uuid:
            if result['uuid'] == args.uuid:
                requests.append(result)
        elif not args.active_only:
            requests.append(result)
        elif result["status"] != "finished":
            requests.append(result)
    # sort the list by create time
    requests = sorted(requests, key=itemgetter('create_timestamp'))
    # limit the size of the list if necessary
    requests = requests[:args.limit]
    num_requests = len(requests)
    req_width = len(f"{num_requests}")
    # for each request
    request_count = 1
    for request in requests:
        print(f"{request_count:>{req_width}}/{num_requests:>{req_width}}", end="\r")
        # obtain the bundles associated with the request
        res2 = await args.di["lta_rc"].request("GET", f"/Bundles?request={request['uuid']}")
        # print(f"res2: {res2}")
        request["bundles"] = await _get_bundles_status(args.di["lta_rc"], res2["results"])
        # print(f"request['bundles']: {request['bundles']}")
        # sort the bundles by create time
        request["bundles"] = sorted(request["bundles"], key=itemgetter('create_timestamp'))
        # print(f"request['bundles']: {request['bundles']}")
        request_count += 1
    # now let's make a colorful dashboard display
    try:
        # Fore: BLACK, RED, GREEN, YELLOW, BLUE, MAGENTA, CYAN, WHITE, RESET.
        # Back: BLACK, RED, GREEN, YELLOW, BLUE, MAGENTA, CYAN, WHITE, RESET.
        # Style: DIM, NORMAL, BRIGHT, RESET_ALL.
        colorama.init(autoreset=True)
        # for each transfer request
        for request in requests:
            sb = _get_status_bar(REQUEST_STATUS, request["status"])
            print(Style.BRIGHT + Fore.CYAN + "Request " + Fore.YELLOW + f"{request['uuid']} " + sb + Fore.YELLOW + f"{request['path']}")
            # for each bundle in the request
            for bundle in request["bundles"]:
                sb = _get_status_bar(BUNDLE_STATUS, bundle["status"], MODULE_MAP, bundle["claimant"])
                print(Style.BRIGHT + Fore.CYAN + "      Bundle " + Fore.YELLOW + f"{bundle['uuid']} " + sb)
            # blank line between requests
            print("")
    except Exception as e:
        print(f"Error while rendering dashboard: {e}")
        colorama.deinit()
        return EXIT_ERROR
    # tell the caller we rendered the dashboard successfully
    colorama.deinit()
    return EXIT_OK


async def display_config(args: Namespace) -> ExitCode:
    """Display the configuration provided to the application."""
    if args.json:
        print_dict_as_pretty_json(args.di["config"])
    else:
        for key in args.di["config"]:
            print(f"{key}:\t\t{args.di['config'][key]}")
    return EXIT_OK


async def metadata_ls(args: Namespace) -> ExitCode:
    """List Metadata records in the LTA DB."""
    if not args.uuid and not args.bundle:
        print("metadata ls: must supply --uuid UUID or --bundle UUID to identify records to list")
        return EXIT_ERROR
    if args.bundle:
        obj: Dict[str, List[Any]] = {"metadata": []}
        done = False
        skip = 0
        while not done:
            result = await args.di["lta_rc"].request("GET", f"/Metadata?bundle_uuid={args.bundle}&skip={skip}")
            num_results = len(result["results"])
            skip = skip + num_results
            done = (num_results == 0)
            if args.json:
                obj["metadata"].extend(result["results"])
            else:
                for record in result["results"]:
                    print(f"uuid:{record['uuid']} bundle:{record['bundle_uuid']} fc:{record['file_catalog_uuid']}")
        if args.json:
            print_dict_as_pretty_json(obj)
        return EXIT_OK
    if args.uuid:
        response = await args.di["lta_rc"].request("GET", f"/Metadata/{args.uuid}")
        if args.json:
            print_dict_as_pretty_json(response)
        else:
            print(f"uuid:              {response['uuid']}")
            print(f"bundle_uuid:       {response['bundle_uuid']}")
            print(f"file_catalog_uuid: {response['file_catalog_uuid']}")
    return EXIT_OK


async def metadata_rm(args: Namespace) -> ExitCode:
    """Remove Metadata records from the LTA DB."""
    if not args.uuid and not args.bundle:
        print("metadata rm: must supply --uuid UUID or --bundle UUID to identify records to remove")
        return EXIT_ERROR
    if args.bundle:
        await args.di["lta_rc"].request("DELETE", f"/Metadata?bundle_uuid={args.bundle}")
        if args.verbose:
            print(f"removed Metadata records for Bundle {args.bundle}")
        return EXIT_OK
    if args.uuid:
        await args.di["lta_rc"].request("DELETE", f"/Metadata/{args.uuid}")
        if args.verbose:
            print(f"removed Metadata record {args.uuid}")
    return EXIT_OK


async def request_estimate(args: Namespace) -> ExitCode:
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
    return EXIT_OK


async def request_ls(args: Namespace) -> ExitCode:
    """List all of the TransferRequest objects in the LTA DB."""
    response = await args.di["lta_rc"].request("GET", "/TransferRequests")
    if args.json:
        print_dict_as_pretty_json(response)
    else:
        results = response["results"]
        print(f"total {len(results)}")
        for request in results:
            print(f"{display_time(request['create_timestamp'])} TransferRequest {request['uuid']} {request['source']} -> {request['dest']} {request['path']}")
    return EXIT_OK


async def request_new(args: Namespace) -> ExitCode:
    """Create a new TransferRequest and add it to the LTA DB."""
    # determine how big the transfer request is going to be
    files_and_size = _get_files_and_size(args.path)
    disk_files = files_and_size[0]
    size = files_and_size[1]
    # get some stuff
    source = args.source
    dest = args.dest
    path = normalize_path(args.path)
    # if it doesn't meet our minimize size requirement
    if size < MINIMUM_REQUEST_SIZE:
        # and the operator has not forced the issue
        if not args.force:
            # raise an Exception to prevent the command from creating a too small request
            raise Exception(f"TransferRequest for {path}\n{size:,} bytes ({hurry.filesize.size(size)}) in {len(disk_files):,} files.\nMinimum required size: {MINIMUM_REQUEST_SIZE:,} bytes.")
    # check to see if we've already got an open TransferRequest on that path
    response = await args.di["lta_rc"].request("GET", "/TransferRequests")
    results = response["results"]
    for request in results:
        old_path = os.path.normpath(request['path'])
        # if a non-complete request matches the path
        if (old_path == path) and (request['status'] != "completed"):
            # and the operator has not forced the issue
            if not args.force:
                # raise an Exception to prevent the command from creating a duplicate request
                raise Exception(f"TransferRequest for {path}\nDuplicates TransferRequest {request['uuid']}\n    Status: {request['status']}\n    Path: {request['path']}")
    # construct the TransferRequest body
    request_body = {
        "source": source,
        "dest": dest,
        "path": path,
    }
    response = await args.di["lta_rc"].request("POST", "/TransferRequests", request_body)
    uuid = response["TransferRequest"]
    tr = await args.di["lta_rc"].request("GET", f"/TransferRequests/{uuid}")
    if args.json:
        print_dict_as_pretty_json(tr)
    else:
        display_id = tr["uuid"]
        create_time = tr["create_timestamp"].replace("T", " ")
        print(f"{display_id}  {create_time} {path} {source} -> {dest}")
    return EXIT_OK


async def request_priority_reset(args: Namespace) -> ExitCode:
    """Reset the work priority timestamp for every TransferRequest."""
    # find every transfer request and set work_priority_timestamp to create_timestamp
    response = await args.di["lta_rc"].request("GET", "/TransferRequests")
    results = response["results"]
    for request in results:
        uuid = request["uuid"]
        patch_body = {
            "update_timestamp": now(),
            "work_priority_timestamp": request["create_timestamp"],
        }
        await args.di["lta_rc"].request("PATCH", f"/TransferRequests/{uuid}", patch_body)
    return EXIT_OK


async def request_rm(args: Namespace) -> ExitCode:
    """Remove a TransferRequest from the LTA DB."""
    response = await args.di["lta_rc"].request("GET", f"/TransferRequests/{args.uuid}")
    path = response["path"]
    if args.confirm != path:
        print(f"request rm: cannot remove TransferRequest {args.uuid}: path is not --confirm {args.confirm}")
        return EXIT_ERROR
    await args.di["lta_rc"].request("DELETE", f"/TransferRequests/{args.uuid}")
    if args.verbose:
        print(f"removed TransferRequest {args.uuid}")
    res3 = await args.di["lta_rc"].request("GET", f"/Bundles?request={args.uuid}")
    bundles = await _get_bundles_status(args.di["lta_rc"], res3["results"])
    for bundle in bundles:
        await args.di["lta_rc"].request("DELETE", f"/Bundles/{bundle['uuid']}")
        if args.verbose:
            print(f"removed Bundle {bundle['uuid']}")
        await args.di["lta_rc"].request("DELETE", f"/Metadata?bundle_uuid={bundle['uuid']}")
        if args.verbose:
            print(f"removed Metadata records for Bundle {bundle['uuid']}")
    return EXIT_OK


async def request_status(args: Namespace) -> ExitCode:
    """Query the status of a TransferRequest in the LTA DB."""
    response = await args.di["lta_rc"].request("GET", f"/TransferRequests/{args.uuid}")
    res2 = await args.di["lta_rc"].request("GET", f"/Bundles?request={args.uuid}")
    response["bundles"] = await _get_bundles_status(args.di["lta_rc"], res2["results"])
    if args.json:
        print_dict_as_pretty_json(response)
    else:
        # display information about the core fields
        print(f"TransferRequest {args.uuid}")
        print(f"    Priority: {display_time(response['work_priority_timestamp'])}")
        print(f"    Status: {response['status']} ({display_time(response['update_timestamp'])})")
        if response['status'] == "quarantined":
            print(f"        Reason: {response['reason']}")
        print(f"    Claimed: {response['claimed']}")
        if response['claimed']:
            print(f"        Claimant: {response['claimant']} ({display_time(response['claim_timestamp'])})")
        print(f"    Source: {response['source']} -> Dest: {response['dest']}")
        print(f"    Path: {response['path']}")
        print(f"    Bundles: {len(response['bundles'])}")
        # display the contents of the transfer request, if requested
        if args.contents:
            print("    Contents:")
            for bundle in response["bundles"]:
                print(f"        Bundle {bundle['uuid']}")
                print(f"            Status: {bundle['status']} ({display_time(bundle['update_timestamp'])})")
                print(f"            Claimed: {bundle['claimed']}")
                if bundle['claimed']:
                    print(f"                Claimant: {bundle['claimant']} ({display_time(bundle['claim_timestamp'])})")
                print(f"            Files: {bundle['file_count']}")
    return EXIT_OK


async def request_update_status(args: Namespace) -> ExitCode:
    """Update the status of a TransferRequest in the LTA DB."""
    right_now = now()
    patch_body = {}
    patch_body["status"] = args.new_status
    patch_body["update_timestamp"] = right_now
    if not args.keep_claim:
        patch_body["claimed"] = False
    if not args.keep_priority:
        patch_body["work_priority_timestamp"] = right_now
    await args.di["lta_rc"].request("PATCH", f"/TransferRequests/{args.uuid}", patch_body)
    return EXIT_OK


async def status(args: Namespace) -> ExitCode:
    """Query the status of the LTA DB or a component of LTA."""
    old_data = (datetime.utcnow() - timedelta(days=args.days)).isoformat()

    def date_ok(d: str) -> bool:
        return d > old_data

    # if we want the status of a particular component type
    if args.component:
        response = await args.di["lta_rc"].request("GET", f"/status/{args.component}")
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
        response = await args.di["lta_rc"].request("GET", "/status")
        if args.json:
            print_dict_as_pretty_json(response)
        else:
            print(f"LTA:          {response['health']}")
            for key in response:
                if key != "health":
                    print(f"{(key+':'):<14}{response[key]}")

    return EXIT_OK


async def status_nersc(args: Namespace) -> ExitCode:
    """Query the status of the quota at NERSC."""
    response = await args.di["lta_rc"].request("GET", "/status/nersc")
    print_dict_as_pretty_json(response)
    return EXIT_OK

# -----------------------------------------------------------------------------

async def main() -> None:
    """Process a request from the Command Line."""
    # create a dictionary that we can inject dependencies into later if necessary
    di: Dict[str, Any] = {}

    # define our top-level argument parsing
    parser = argparse.ArgumentParser(prog="ltacmd")
    parser.set_defaults(di=di)
    subparser = parser.add_subparsers(help='command help')

    # define a subparser for the 'bundle' subcommand
    parser_bundle = subparser.add_parser('bundle', help='interact with bundles')
    bundle_subparser = parser_bundle.add_subparsers(help='bundle command help')

    # define a subparser for the 'bundle ls' subcommand
    parser_bundle_ls = bundle_subparser.add_parser('ls', help='list bundles')
    parser_bundle_ls.add_argument("--json",
                                  help="display output in JSON",
                                  action="store_true")
    parser_bundle_ls.add_argument("--status",
                                  dest="show_status",
                                  help="display the status of the bundle",
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

    # define a subparser for the 'bundle priority' subcommand
    parser_bundle_priority = bundle_subparser.add_parser('priority', help='modify bundle priority dates')
    bundle_priority_subparser = parser_bundle_priority.add_subparsers(help='priority command help')

    # define a subparser for the 'bundle priority reset' subcommand
    parser_bundle_priority_reset = bundle_priority_subparser.add_parser('reset', help='reset all priority dates')
    parser_bundle_priority_reset.set_defaults(func=bundle_priority_reset)

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
    parser_bundle_update_status.add_argument("--keep-priority",
                                             dest="keep_priority",
                                             help="don't change the priority date",
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

    # define a subparser for the 'catalog query' subcommand
    parser_catalog_query = catalog_subparser.add_parser('query', help='run a query on the catalog')
    parser_catalog_query.add_argument("--debug",
                                      help="display debugging output",
                                      action="store_true")
    parser_catalog_query.add_argument("--json",
                                      help="display output in JSON",
                                      action="store_true")
    parser_catalog_query.add_argument("--query",
                                      help="query string to provide to the catalog",
                                      required=True)
    parser_catalog_query.add_argument("--url-encode",
                                      dest="url_encode",
                                      help="URL encode the query string before use",
                                      action="store_true")
    parser_catalog_query.set_defaults(func=catalog_query)

    # define a subparser for the 'catalog stats' subcommand
    parser_catalog_stats = catalog_subparser.add_parser('stats', help='display the bundles archived to NERSC')
    parser_catalog_stats.set_defaults(func=catalog_stats)

    # define a subparser for the 'dashboard' subcommand
    parser_dashboard_config = subparser.add_parser('dashboard', help='dashboard system dashboard')
    parser_dashboard_config.add_argument("--active-only",
                                         dest="active_only",
                                         help="hide finished items",
                                         action="store_true")
    parser_dashboard_config.add_argument("--limit",
                                         help="limit the number dashboarded",
                                         type=int,
                                         default=10000)
    parser_dashboard_config.add_argument("--uuid",
                                         help="display request uuid")
    parser_dashboard_config.set_defaults(func=dashboard)

    # define a subparser for the 'display-config' subcommand
    parser_display_config = subparser.add_parser('display-config', help='display environment configuration')
    parser_display_config.add_argument("--json",
                                       help="display output in JSON",
                                       action="store_true")
    parser_display_config.set_defaults(func=display_config)

    # define a subparser for the 'metadata' subcommand
    parser_metadata = subparser.add_parser('metadata', help='interact with metadata')
    metadata_subparser = parser_metadata.add_subparsers(help='metadata command help')

    # define a subparser for the 'metadata ls' subcommand
    parser_metadata_ls = metadata_subparser.add_parser('ls', help='list metadata records')
    parser_metadata_ls.add_argument("--bundle",
                                    help="UUID of a bundle")
    parser_metadata_ls.add_argument("--json",
                                    help="display output in JSON",
                                    action="store_true")
    parser_metadata_ls.add_argument("--uuid",
                                    help="UUID of a metadata record")
    parser_metadata_ls.set_defaults(func=metadata_ls)

    # define a subparser for the 'metadata rm' subcommand
    parser_metadata_rm = metadata_subparser.add_parser('rm', help='delete a metadata record')
    parser_metadata_rm.add_argument("--bundle",
                                    help="UUID of a bundle")
    parser_metadata_rm.add_argument("--uuid",
                                    help="UUID of a metadata record")
    parser_metadata_rm.add_argument("--verbose",
                                    help="display an output line on success",
                                    action="store_true")
    parser_metadata_rm.set_defaults(func=metadata_rm)

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

    # define a subparser for the 'request priority' subcommand
    parser_request_priority = request_subparser.add_parser('priority', help='modify transfer request priority dates')
    request_priority_subparser = parser_request_priority.add_subparsers(help='priority command help')

    # define a subparser for the 'request priority reset' subcommand
    parser_request_priority_reset = request_priority_subparser.add_parser('reset', help='reset all priority dates')
    parser_request_priority_reset.set_defaults(func=request_priority_reset)

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
    parser_request_update_status.add_argument("--keep-priority",
                                              dest="keep_priority",
                                              help="don't change the priority date",
                                              action="store_true")
    parser_request_update_status.set_defaults(func=request_update_status)

    # define a subparser for the 'status' subcommand
    parser_status = subparser.add_parser('status', help='perform a status query')
    status_subparser = parser_status.add_subparsers(help='status command help')
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

    # define a subparser for the 'request update-status' subcommand
    parser_status_nersc = status_subparser.add_parser('nersc', help='get latest quota at NERSC')
    parser_status_nersc.set_defaults(func=status_nersc)

    # parse the provided command line arguments and call the function
    args = parser.parse_args()
    if hasattr(args, "func"):
        try:
            # load and inject the dependencies needed by the command
            config = from_environment(EXPECTED_CONFIG)
            di["config"] = config
            di["fc_rc"] = RestClient(cast(str, config["FILE_CATALOG_REST_URL"]), token=cast(str, config["FILE_CATALOG_REST_TOKEN"]))
            di["lta_rc"] = RestClient(cast(str, config["LTA_REST_URL"]), token=cast(str, config["LTA_REST_TOKEN"]))
            # execute the command indicated by the user
            exit_code = await args.func(args)
            sys.exit(exit_code)
        except Exception as e:
            print(e)
            sys.exit(EXIT_ERROR)
    else:
        parser.print_usage()
        sys.exit(EXIT_OK)


if __name__ == '__main__':
    logging.basicConfig(level=logging.CRITICAL)
    asyncio.get_event_loop().run_until_complete(main())
