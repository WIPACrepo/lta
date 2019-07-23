"""
Command line utility for Long Term Archive.

Run with `python -m resources.lta_cmd $@`.
"""

import argparse
from argparse import Namespace
import asyncio
from datetime import datetime, timedelta
import functools
from importlib import import_module
import json
import logging
import os
from pathlib import Path
from typing import Any, Callable, Dict

from rest_tools.client import RestClient  # type: ignore

from lta.config import from_environment
from lta.daemon import Daemon
from lta.lta_const import drain_semaphore_filename, pid_filename


COMPONENT_NAMES = [
    "bundler",
    "picker",
    "replicator",
]

EXPECTED_CONFIG = {
    'LTA_REST_TOKEN': None,
    'LTA_REST_URL': None
}


def deamon_for(component: str) -> Daemon:
    """Create a Daemon object for the specified component name."""
    pidfile = pid_filename(component)
    # Known issue with MyPy: https://github.com/python/mypy/issues/5059
    runner = import_module(f".{component}", "lta").runner  # type: ignore
    chdir = os.getcwd()
    stdout = os.path.join(chdir, f"{component}.log")
    return Daemon(pidfile, runner, stdout=stdout, chdir=chdir)


def print_dict_as_pretty_json(d: Dict[str, Any]) -> None:
    """Print the provided Dict as pretty-print JSON."""
    print(json.dumps(d, indent=4, sort_keys=True))


def stop_event_loop() -> Callable[[Any], Any]:
    """Wrap the decorated coroutine to stop the asyncio event loop."""
    def wrapper(func: Callable[[Any], Any]) -> Callable[[Any], Any]:
        @functools.wraps(func)
        async def wrapped(*args: Any) -> None:
            await func(*args)
            asyncio.get_event_loop().stop()
        return wrapped
    return wrapper

# -----------------------------------------------------------------------------

@stop_event_loop()
async def display_config(args: Namespace) -> None:
    """Display the configuration provided to the application."""
    if args.json:
        print_dict_as_pretty_json(args.config)
    else:
        for key in args.config:
            print(f"{key}:\t\t{args.config[key]}")


@stop_event_loop()
async def drain(args: Namespace) -> None:
    """Create a semaphore file to signal the component to drain and shut down."""
    cwd = os.getcwd()
    semaphore_name = drain_semaphore_filename(args.component)
    semaphore_path = os.path.join(cwd, semaphore_name)
    Path(semaphore_path).touch()


@stop_event_loop()
async def kill(args: Namespace) -> None:
    """Kill the running component."""
    deamon_for(args.component).kill()


@stop_event_loop()
async def request_ls(args: Namespace) -> None:
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
            path = x["path"]
            source = x["source"]
            dest = x["dest"]
            status = x["status"]
            print(f"{display_id} {status}  {create_time} {source} -> {dest} {path}")


@stop_event_loop()
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


@stop_event_loop()
async def request_status(args: Namespace) -> None:
    """Query the status of a TransferRequest in the LTA DB."""
    response = await args.rc.request("GET", "/TransferRequests")
    results = response["results"]
    for x in results:
        if x["uuid"].startswith(args.uuid):
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


async def restart(args: Namespace) -> None:
    """Restart the running component."""
    deamon_for(args.component).restart()


async def start(args: Namespace) -> None:
    """Start the component running."""
    deamon_for(args.component).start()


@stop_event_loop()
async def status(args: Namespace) -> None:
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


@stop_event_loop()
async def stop(args: Namespace) -> None:
    """Stop the component running."""
    deamon_for(args.component).stop()

# -----------------------------------------------------------------------------

async def main() -> None:
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

    # define a subparser for the 'drain' subcommand
    parser_drain = subparser.add_parser('drain', help='finish existing work and shut down')
    parser_drain.add_argument("component",
                              choices=COMPONENT_NAMES,
                              help="LTA component")
    parser_drain.set_defaults(func=drain)

    # define a subparser for the 'kill' subcommand
    parser_kill = subparser.add_parser('kill', help='immediately kill component process')
    parser_kill.add_argument("component",
                             choices=COMPONENT_NAMES,
                             help="LTA component")
    parser_kill.set_defaults(func=kill)

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

    # define a subparser for the 'restart' subcommand
    parser_restart = subparser.add_parser('restart', help='restart the component')
    parser_restart.add_argument("component",
                                choices=COMPONENT_NAMES,
                                help="LTA component")
    parser_restart.set_defaults(func=restart)

    # define a subparser for the 'start' subcommand
    parser_start = subparser.add_parser('start', help='start the component')
    parser_start.add_argument("component",
                              choices=COMPONENT_NAMES,
                              help="LTA component")
    parser_start.set_defaults(func=start)

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

    # define a subparser for the 'stop' subcommand
    parser_stop = subparser.add_parser('stop', help='stop the component')
    parser_stop.add_argument("component",
                             choices=COMPONENT_NAMES,
                             help="LTA component")
    parser_stop.set_defaults(func=stop)

    # parse the provided command line arguments and call the function
    args = parser.parse_args()
    if hasattr(args, "func"):
        try:
            await args.func(args)
        except Exception as e:
            print(e)
            asyncio.get_event_loop().stop()
    else:
        parser.print_usage()
        asyncio.get_event_loop().stop()


if __name__ == '__main__':
    logging.basicConfig(level=logging.CRITICAL)
    loop = asyncio.get_event_loop()
    loop.create_task(main())
    loop.run_forever()
