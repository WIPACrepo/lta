"""
Command line utility for Long Term Archive.

Run with `python -m resources.lta_cmd $@`.
"""

import argparse
import asyncio
import json
import sys
from typing import Dict

from rest_tools.client import RestClient  # type: ignore

from lta.config import from_environment

EXPECTED_CONFIG = {
    'LTA_REST_TOKEN': None,
    'LTA_REST_URL': None
}


async def display_config(args) -> int:
    if args.json:
        print_dict_as_pretty_json(args.config)
    else:
        for key in args.config:
            print(f"{key}:\t\t{args.config[key]}")
    return 0


def print_dict_as_pretty_json(d: Dict) -> None:
    print(json.dumps(d, indent=4, sort_keys=True))


async def request_ls(args) -> int:
    rc = RestClient(args.config["LTA_REST_URL"], token=args.config["LTA_REST_TOKEN"])
    try:
        response = await rc.request("GET", "/TransferRequests")
        if args.json:
            print_dict_as_pretty_json(response)
        else:
            results = response["results"]
            for x in results:
                short_id = x["uuid"][:7]
                create_time = x["create_timestamp"]
                source_cook = x["source"].split(":")
                path = source_cook[1]
                source = source_cook[0]
                dests = [y.split(":")[0] for y in x["dest"]]
                print(f"{short_id}\t{create_time}\t{path}\t{source} -> {dests}")
    except Exception as e:
        print(e)
        return -1
    return 0


async def request_new(args) -> int:
    # configure a RestClient from the environment
    rc = RestClient(args.config["LTA_REST_URL"], token=args.config["LTA_REST_TOKEN"])
    # get some stuff
    source = args.source
    dest = args.dest
    path = args.path
    # construct the TransferRequest body
    request_body = {
        "source": f"{source}:{path}",
        "dest": [f"{x}:{path}" for x in dest],
    }
    try:
        response = await rc.request("POST", "/TransferRequests", request_body)
        uuid = response["TransferRequest"]
        tr = await rc.request("GET", f"/TransferRequests/{uuid}")
        if args.json:
            print_dict_as_pretty_json(tr)
        else:
            print(f"{uuid} - {tr['create_timestamp']}\n{source} -> {[x for x in args.dest]}\n{path}")
    except Exception as e:
        print(e)
        return -1
    return 0


async def request_status(args) -> int:
    rc = RestClient(args.config["LTA_REST_URL"], token=args.config["LTA_REST_TOKEN"])
    try:
        response = await rc.request("GET", "/TransferRequests")
        results = response["results"]
        for x in results:
            if x["uuid"].startswith(args.uuid):
                if args.json:
                    print_dict_as_pretty_json(x)
                else:
                    print(f"Claimed: {x['claimed']}")
    except Exception as e:
        print(e)
        return -1
    return 0


async def status(args) -> int:
    rc = RestClient(args.config["LTA_REST_URL"], token=args.config["LTA_REST_TOKEN"])
    try:
        # if we want the status of a particular component type
        if args.component:
            response = await rc.request("GET", f"/status/{args.component}")
            if args.json:
                print_dict_as_pretty_json(response)
            else:
                for key in response:
                    print(f"{(key+':'):<25}{response[key]['timestamp']}")
        # otherwise we want the status of the whole system
        else:
            response = await rc.request("GET", "/status")
            if args.json:
                print_dict_as_pretty_json(response)
            else:
                print(f"LTA:          {response['health']}")
                for key in response:
                    if key != "health":
                        print(f"{(key+':'):<14}{response[key]}")
    except Exception as e:
        print(e)
        return -1
    return 0


async def main():
    # configure the application from the environment
    config = from_environment(EXPECTED_CONFIG)

    # define our top-level argument parsing
    parser = argparse.ArgumentParser(prog="ltacmd")
    parser.add_argument("-v",
                        "--verbose",
                        help="display verbose output",
                        action="store_true")
    parser.set_defaults(config=config)
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
    exit_code = await args.func(args)
    sys.exit(exit_code)


if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
