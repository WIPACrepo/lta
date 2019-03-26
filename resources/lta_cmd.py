"""
Command line utility for Long Term Archive.

Run with `python -m resources.lta_cmd $@`.
"""

import argparse
import asyncio
from datetime import datetime, timedelta
import json
import logging
from typing import Dict

from rest_tools.client import RestClient  # type: ignore

from lta.config import from_environment

EXPECTED_CONFIG = {
    'LTA_REST_TOKEN': None,
    'LTA_REST_URL': None
}


async def display_config(args) -> None:
    """Display the configuration provided to the application."""
    if args.json:
        print_dict_as_pretty_json(args.config)
    else:
        for key in args.config:
            print(f"{key}:\t\t{args.config[key]}")


def print_dict_as_pretty_json(d: Dict) -> None:
    """Print the provided Dict as pretty-print JSON."""
    print(json.dumps(d, indent=4, sort_keys=True))


async def request_ls(args) -> None:
    """List all of the TransferRequest objects in the LTA DB."""
    response = await args.rc.request("GET", "/TransferRequests")
    if args.json:
        print_dict_as_pretty_json(response)
    else:
        results = response["results"]
        for x in results:
            display_id = x["uuid"]
            if not args.long:
                display_id = display_id[:8]
            create_time = x["create_timestamp"].replace("T", " ")
            source_cook = x["source"].split(":")
            path = source_cook[1]
            source = source_cook[0]
            dests = [y.split(":")[0] for y in x["dest"]]
            print(f"{display_id}  {create_time} {path} {source} -> {dests}")


async def request_new(args) -> None:
    """Create a new TransferRequest and add it to the LTA DB."""
    # get some stuff
    source = args.source
    dest = args.dest
    path = args.path
    # construct the TransferRequest body
    request_body = {
        "source": f"{source}:{path}",
        "dest": [f"{x}:{path}" for x in dest],
    }
    response = await args.rc.request("POST", "/TransferRequests", request_body)
    uuid = response["TransferRequest"]
    tr = await args.rc.request("GET", f"/TransferRequests/{uuid}")
    if args.json:
        print_dict_as_pretty_json(tr)
    else:
        display_id = tr["uuid"]
        create_time = tr["create_timestamp"].replace("T", " ")
        source_cook = tr["source"].split(":")
        path = source_cook[1]
        source = source_cook[0]
        dests = [y.split(":")[0] for y in tr["dest"]]
        print(f"{display_id}  {create_time} {path} {source} -> {dests}")


async def request_status(args) -> None:
    """Query the status of a TransferRequest in the LTA DB."""
    response = await args.rc.request("GET", "/TransferRequests")
    results = response["results"]
    for x in results:
        if x["uuid"].startswith(args.uuid):
            if args.json:
                print_dict_as_pretty_json(x)
            else:
                display_id = x["uuid"]
                status_name = "Submitted - Waiting to be claimed"
                status_time = x["create_timestamp"].replace("T", " ")
                if x["claimed"]:
                    status_name = "Claimed - Currently processing"
                    status_time = x["claim_time"].replace("T", " ")
                    if "complete" in x:
                        status_name = "Completed - Files submitted to LTA DB"
                        status_time = x["complete"]["timestamp"].replace("T", " ")
                print(f"{display_id}  {status_time} {status_name}")


async def status(args) -> None:
    """Query the status of the LTA DB or a component of LTA."""
    old_data = (datetime.utcnow() - timedelta(seconds=60*5)).isoformat()

    def date_ok(d: str) -> bool:
        return d > old_data

    # if we want the status of a particular component type
    if args.component:
        response = await args.rc.request("GET", f"/status/{args.component}")
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
        response = await args.rc.request("GET", "/status")
        if args.json:
            print_dict_as_pretty_json(response)
        else:
            print(f"LTA:          {response['health']}")
            for key in response:
                if key != "health":
                    print(f"{(key+':'):<14}{response[key]}")


async def main():
    """Process a request from the Command Line."""
    # configure the application from the environment
    config = from_environment(EXPECTED_CONFIG)
    rc = RestClient(config["LTA_REST_URL"], token=config["LTA_REST_TOKEN"])

    # define our top-level argument parsing
    parser = argparse.ArgumentParser(prog="ltacmd")
    parser.set_defaults(config=config, rc=rc)
    subparser = parser.add_subparsers(help='command help')

    # define a subparser for the 'display-config' subcommand
    parser_display_config = subparser.add_parser('display-config', help='display environment configuration')
    parser_display_config.add_argument("--json",
                                       help="display output in JSON",
                                       action="store_true")
    parser_display_config.set_defaults(func=display_config)

    # define a subparser for the 'request' subcommand
    parser_request = subparser.add_parser('request', help='interact with transfer requests')
    request_subparser = parser_request.add_subparsers(help='request command help')

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
                                    help="source of files",
                                    required=True)
    parser_request_new.add_argument("--dest",
                                    action="append",
                                    help="destination of bundles",
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
                               choices=["picker", "bundler"],
                               help="optional LTA component",
                               nargs='?')
    parser_status.add_argument("--json",
                               help="display output in JSON",
                               action="store_true")
    parser_status.set_defaults(func=status)

    # parse the provided command line arguments and call the function
    args = parser.parse_args()
    if hasattr(args, "func"):
        await args.func(args)
    else:
        parser.print_usage()


if __name__ == '__main__':
    logging.basicConfig(level=logging.CRITICAL)
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
