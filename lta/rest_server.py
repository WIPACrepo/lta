"""
Long Term Archive REST API server.

Run with `python -m lta.rest_server`.
"""

import asyncio
from datetime import datetime, timedelta
from functools import wraps
import json
import logging
from math import floor
from random import random
from typing import Any, Callable
from uuid import uuid1

import binpacking  # type: ignore
from rest_tools.client import json_decode  # type: ignore
from rest_tools.server import authenticated, catch_error, RestHandler, RestHandlerSetup, RestServer  # type: ignore
import tornado.web  # type: ignore

from .config import from_environment


EXPECTED_CONFIG = {
    'LTA_AUTH_ALGORITHM': 'RS256',
    'LTA_AUTH_ISSUER': 'lta',
    'LTA_AUTH_SECRET': 'secret',
    'LTA_MAX_CLAIM_AGE_HOURS': '12',
    'LTA_REST_HOST': 'localhost',
    'LTA_REST_PORT': '8080',
    'LTA_SITE_CONFIG': 'etc/site.json',
}

# -----------------------------------------------------------------------------

def now() -> str:
    """Return string timestamp for current time, to the second."""
    return datetime.utcnow().isoformat(timespec='seconds')

def site(sitepath: str) -> str:
    """Return SITE from SITE:PATH."""
    return sitepath.split(':', 1)[0]

def lta_auth(**_auth: Any) -> Callable[..., Any]:
    """
    Handle RBAC authorization for LTA.

    Like :py:func:`authenticated`, this requires the Authorization header
    to be filled with a valid token.  Note that calling both decorators
    is not necessary, as this decorator will perform authentication
    checking as well.

    Args:
        roles (list): The roles to match

    Raises:
        :py:class:`tornado.web.HTTPError`

    """
    def make_wrapper(method: Callable[..., Any]) -> Any:
        @authenticated  # type: ignore
        @catch_error  # type: ignore
        @wraps(method)
        async def wrapper(self: Any, *args: Any, **kwargs: Any) -> Any:
            roles = _auth.get('roles', [])

            authorized = False

            auth_role = self.auth_data.get('long-term-archive', {}).get('role', None)
            if roles and auth_role in roles:
                authorized = True
            else:
                logging.info('roles: %r', roles)
                logging.info('token_role: %r', auth_role)
                logging.info('role mismatch')

            if not authorized:
                raise tornado.web.HTTPError(403, reason="authorization failed")

            return await method(self, *args, **kwargs)
        return wrapper
    return make_wrapper

# -----------------------------------------------------------------------------

class CheckClaims:
    """CheckClaims determines if claims are old/expired."""

    def __init__(self, claim_age: int = 12):
        """Intialize a CheckClaims object."""
        self.claim_age = claim_age

    def old_claim(self, stamp: str) -> bool:
        """Determine if a claim is old/expired."""
        cutoff_time = datetime.utcnow() - timedelta(hours=self.claim_age)
        stamp_time = datetime.strptime(stamp, '%Y-%m-%dT%H:%M:%S')
        return bool(cutoff_time > stamp_time)

# -----------------------------------------------------------------------------

class BaseLTAHandler(RestHandler):
    """BaseLTAHandler is a RestHandler for all LTA routes."""

    def initialize(self, check_claims: CheckClaims, db: Any, sites: Any, *args: Any, **kwargs: Any) -> None:
        """Initialize a BaseLTAHandler object."""
        super(BaseLTAHandler, self).initialize(*args, **kwargs)
        self.check_claims = check_claims
        self.db = db
        self.sites = sites["sites"]

# -----------------------------------------------------------------------------

class FilesActionsBulkCreateHandler(BaseLTAHandler):
    """Handler for /Files/actions/bulk_create."""

    @lta_auth(roles=['system'])
    async def post(self) -> None:
        """Handle POST /Files/actions/bulk_create."""
        req = json_decode(self.request.body)
        if 'files' not in req:
            raise tornado.web.HTTPError(400, reason="missing files field")
        if not isinstance(req['files'], list):
            raise tornado.web.HTTPError(400, reason="files field is not a list")
        if not req['files']:
            raise tornado.web.HTTPError(400, reason="files field is empty")

        create_count = 0
        for xfer_file in req["files"]:
            xfer_file["uuid"] = uuid1().hex
            xfer_file["create_timestamp"] = now()
            xfer_file["status"] = "waiting"
            self.db['Files'][xfer_file['uuid']] = xfer_file
            create_count = create_count + 1

        uuids = [x["uuid"] for x in req["files"]]
        self.set_status(201)
        self.write({'files': uuids, 'count': create_count})

class FilesActionsBulkDeleteHandler(BaseLTAHandler):
    """Handler for /Files/actions/bulk_delete."""

    @lta_auth(roles=['system'])
    async def post(self) -> None:
        """Handle POST /Files/actions/bulk_delete."""
        req = json_decode(self.request.body)
        if 'files' not in req:
            raise tornado.web.HTTPError(400, reason="missing files field")
        if not isinstance(req['files'], list):
            raise tornado.web.HTTPError(400, reason="files field is not a list")
        if not req['files']:
            raise tornado.web.HTTPError(400, reason="files field is empty")

        results = []
        for uuid in req["files"]:
            if uuid in self.db['Files']:
                del self.db['Files'][uuid]
                results.append(uuid)

        self.write({'files': results, 'count': len(results)})

class FilesActionsBulkUpdateHandler(BaseLTAHandler):
    """Handler for /Files/actions/bulk_update."""

    @lta_auth(roles=['system'])
    async def post(self) -> None:
        """Handle POST /Files/actions/bulk_update."""
        req = json_decode(self.request.body)
        if 'update' not in req:
            raise tornado.web.HTTPError(400, reason="missing update field")
        if not isinstance(req['update'], dict):
            raise tornado.web.HTTPError(400, reason="update field is not an object")
        if 'files' not in req:
            raise tornado.web.HTTPError(400, reason="missing files field")
        if not isinstance(req['files'], list):
            raise tornado.web.HTTPError(400, reason="files field is not a list")
        if not req['files']:
            raise tornado.web.HTTPError(400, reason="files field is empty")

        results = []
        for uuid in req["files"]:
            if uuid in self.db['Files']:
                self.db['Files'][uuid].update(req["update"])
                results.append(uuid)

        self.write({'files': results, 'count': len(results)})

class FilesHandler(BaseLTAHandler):
    """FilesHandler handles collection level routes for Files."""

    @lta_auth(roles=['admin', 'user', 'system'])
    async def get(self) -> None:
        """Handle GET /Files."""
        location = self.get_query_argument("location", default=None)
        transfer_request_uuid = self.get_query_argument("transfer_request_uuid", default=None)
        bundle_uuid = self.get_query_argument("bundle_uuid", default=None)
        status = self.get_query_argument("status", default=None)

        results = []
        for file_uuid in self.db['Files']:
            lta_file = self.db['Files'][file_uuid]
            if location:
                if not ("source" in lta_file):
                    continue
                if not (lta_file["source"].startswith(location)):
                    continue
            if transfer_request_uuid:
                if not ("request" in lta_file):
                    continue
                if not (lta_file["request"] == transfer_request_uuid):
                    continue
            if bundle_uuid:
                if not ("bundle" in lta_file):
                    continue
                if not (lta_file["bundle"] == bundle_uuid):
                    continue
            if status:
                if not ("status" in lta_file):
                    continue
                if not (lta_file["status"] == status):
                    continue
            results.append(file_uuid)

        ret = {
            'results': results,
        }
        self.write(ret)

class FilesActionsPopHandler(BaseLTAHandler):
    """
    FilesActionsPopHandler handles /Files/actions/pop.

    Picker provides:
        file_obj = {
            "source": "SITE:PATH",
            "dest": "SITE:PATH",
            "request": "UUID",
            "catalog": file_catalog_record
        }

    Catalog Schema:
        catalog_file = {
            "uuid": uuid,
            "logical_name": "/data/exp/IceCube...",
            "locations": [
                {"site": "WIPAC", "path": "/data/exp/IceCube..."},
                {"site": "NERSC", "path": "/data/archive/ad43d3...23.zip", archive: True},
            ]
            "file_size": 123456789,
            "data_type": "real",
        }

    LTA DB annotates:
        xfer_file["uuid"] = uuid1().hex
        xfer_file["create_timestamp"] = now()
        xfer_file["status"] = "waiting"
    """

    @lta_auth(roles=['system'])
    async def post(self) -> None:
        """Handle POST /Files/actions/pop."""
        # if there aren't enough files to fill the bundle, should we force it?
        force = self.get_argument('force', False)
        # figure out which site is bundling files
        src = self.get_argument('source')
        if src not in self.sites:
            raise tornado.web.HTTPError(400, reason="invalid source site")
        # figure out where the bundle is headed
        dest = self.get_argument('dest')
        if dest not in self.sites:
            raise tornado.web.HTTPError(400, reason="invalid dest site")
        # figure out how big the bundle should be
        bundle_size = self.sites[dest]["bundle_size"]
        # get the self-identification of the claiming bundler
        pop_body = json_decode(self.request.body)
        # let's build up a list of things to bundle
        src_tuples = []
        for uuid in self.db["Files"]:
            db_file = self.db["Files"][uuid]
            if not (site(db_file["source"]) == src):
                continue
            if not (site(db_file["dest"]) == dest):
                continue
            if not (db_file["status"] == "waiting"):
                continue
            src_tuples.append((uuid, db_file["catalog"]["file_size"]))
        # we can't bundle nothing
        if len(src_tuples) < 1:
            self.write({'results': []})
            return
        # pack those files into bundle sized bins
        bins = binpacking.to_constant_volume(src_tuples, bundle_size, weight_pos=1)
        bin_count = len(bins)-1
        # if we got exactly one bin
        if bin_count == 0:
            # bail, if we're not forcing it
            if not force:
                self.write({'results': []})
                return
            # otherwise, pikachu, I choose you
            bin_choice = 0
        # otherwise, we have multiple bins to choose from
        else:
            # randomly select one of the available bins
            bin_choice = floor(bin_count * random())
        # get a list of the files in the bin
        pop_bin = bins[bin_choice]
        bin_files = [self.db["Files"][x[0]] for x in pop_bin]
        # update the status of the files we're handing out
        claim_time = now()
        for bin_file in bin_files:
            bin_file["claimant"] = pop_body
            bin_file["claimed"] = True
            bin_file["claim_time"] = claim_time
            bin_file["status"] = "processing"
        # hand the files out to the caller
        self.write({'results': bin_files})

class FilesSingleHandler(BaseLTAHandler):
    """FilesSingleHandler handles object level routes for Files."""

    @lta_auth(roles=['admin', 'user', 'system'])
    async def get(self, file_id: str) -> None:
        """Handle GET /Files/{uuid}."""
        if file_id not in self.db['Files']:
            raise tornado.web.HTTPError(404, reason="not found")
        self.write(self.db['Files'][file_id])

    @lta_auth(roles=['admin', 'user', 'system'])
    async def patch(self, file_id: str) -> None:
        """Handle PATCH /Files/{uuid}."""
        if file_id not in self.db['Files']:
            raise tornado.web.HTTPError(404, reason="not found")
        req = json_decode(self.request.body)
        if 'uuid' in req and req['uuid'] != file_id:
            raise tornado.web.HTTPError(400, reason="bad request")
        self.db['Files'][file_id].update(req)
        self.write(self.db['Files'][file_id])

    @lta_auth(roles=['admin', 'user', 'system'])
    async def delete(self, file_id: str) -> None:
        """Handle DELETE /Files/{uuid}."""
        if file_id in self.db['Files']:
            del self.db['Files'][file_id]
        self.set_status(204)

# -----------------------------------------------------------------------------

class MainHandler(BaseLTAHandler):
    """MainHandler is a BaseLTAHandler that handles the root route."""

    def get(self) -> None:
        """Handle GET /."""
        self.write({})

# -----------------------------------------------------------------------------

class TransferRequestsHandler(BaseLTAHandler):
    """TransferRequestsHandler is a BaseLTAHandler that handles TransferRequests routes."""

    @lta_auth(roles=['admin', 'user', 'system'])
    async def get(self) -> None:
        """Handle GET /TransferRequests."""
        ret = {
            'results': list(self.db['TransferRequests'].values()),
        }
        self.write(ret)

    @lta_auth(roles=['admin', 'user', 'system'])
    async def post(self) -> None:
        """Handle POST /TransferRequests."""
        req = json_decode(self.request.body)
        if 'source' not in req:
            raise tornado.web.HTTPError(400, reason="missing source field")
        if 'dest' not in req:
            raise tornado.web.HTTPError(400, reason="missing dest field")
        if not isinstance(req['dest'], list):
            raise tornado.web.HTTPError(400, reason="dest field is not a list")
        if not req['dest']:
            raise tornado.web.HTTPError(400, reason="dest field is empty")

        req['uuid'] = uuid1().hex
        req['claimed'] = False
        req['claim_time'] = ''
        self.db['TransferRequests'][req['uuid']] = req
        self.set_status(201)
        self.write({'TransferRequest': req['uuid']})

class TransferRequestSingleHandler(BaseLTAHandler):
    """TransferRequestSingleHandler is a BaseLTAHandler that handles routes related to single TransferRequest objects."""

    @lta_auth(roles=['admin', 'user', 'system'])
    async def get(self, request_id: str) -> None:
        """Handle GET /TransferRequests/{uuid}."""
        if request_id not in self.db['TransferRequests']:
            raise tornado.web.HTTPError(404, reason="not found")
        self.write(self.db['TransferRequests'][request_id])

    @lta_auth(roles=['admin', 'user', 'system'])
    async def patch(self, request_id: str) -> None:
        """Handle PATCH /TransferRequests/{uuid}."""
        if request_id not in self.db['TransferRequests']:
            raise tornado.web.HTTPError(404, reason="not found")
        req = json_decode(self.request.body)
        if 'uuid' in req and req['uuid'] != request_id:
            raise tornado.web.HTTPError(400, reason="bad request")
        self.db['TransferRequests'][request_id].update(req)
        self.write({})

    @lta_auth(roles=['admin', 'user', 'system'])
    async def delete(self, request_id: str) -> None:
        """Handle DELETE /TransferRequests/{uuid}."""
        if request_id in self.db['TransferRequests']:
            del self.db['TransferRequests'][request_id]
        self.set_status(204)

class TransferRequestActionsPopHandler(BaseLTAHandler):
    """TransferRequestActionsPopHandler handles /TransferRequests/actions/pop."""

    @lta_auth(roles=['system'])
    async def post(self) -> None:
        """Handle POST /TransferRequests/actions/pop."""
        src = self.get_argument('source')
        limit = self.get_argument('limit', 10)
        try:
            limit = int(limit)
        except Exception:
            raise tornado.web.HTTPError(400, reason="limit is not an int")
        pop_body = json_decode(self.request.body)
        ret = []
        for req in self.db['TransferRequests'].values():
            if (req['source'].split(':', 1)[0] == src and
                    (req['claimed'] is False or
                     self.check_claims.old_claim(req['claim_time']))):
                ret.append(req)
                req['claimant'] = pop_body
                req['claimed'] = True
                req['claim_time'] = now()
                limit -= 1
                if limit <= 0:
                    break
        self.write({'results': ret})

# -----------------------------------------------------------------------------

class StatusHandler(BaseLTAHandler):
    """StatusHandler is a BaseLTAHandler that handles system status routes."""

    @lta_auth(roles=['admin', 'user', 'system'])
    async def get(self) -> None:
        """Get the overall status of the system."""
        ret = {}
        health = 'OK'
        old_data = (datetime.utcnow() - timedelta(seconds=60*5)).isoformat()

        def date_ok(d: str) -> bool:
            return d > old_data

        for name in self.db['status']:
            component_health = 'OK'
            if not any(date_ok(c['t']) for c in self.db['status'][name].values()):
                component_health = 'WARN'
                health = 'WARN'
            ret[name] = component_health
        ret['health'] = health
        self.write(ret)

class StatusComponentHandler(BaseLTAHandler):
    """StatusComponentHandler is a BaseLTAHandler that handles component status routes."""

    @lta_auth(roles=['admin', 'user', 'system'])
    async def get(self, component: str) -> None:
        """Get the detailed status of a component."""
        if component not in self.db['status']:
            raise tornado.web.HTTPError(404, reason="not found")
        self.write(self.db['status'][component])

    @lta_auth(roles=['system'])
    async def patch(self, component: str) -> None:
        """Update the detailed status of a component."""
        req = json_decode(self.request.body)
        if component in self.db['status']:
            self.db['status'][component].update(req)
        else:
            self.db['status'][component] = req
        self.write({})

# -----------------------------------------------------------------------------

def start(debug: bool = False) -> RestServer:
    """Start a LTA REST DB service."""
    config = from_environment(EXPECTED_CONFIG)
    # logger = logging.getLogger('lta.rest')

    args = RestHandlerSetup({
        'auth': {
            'secret': config['LTA_AUTH_SECRET'],
            'issuer': config['LTA_AUTH_ISSUER'],
            'algorithm': config['LTA_AUTH_ALGORITHM'],
        },
        'debug': debug
    })
    args['check_claims'] = CheckClaims(int(config['LTA_MAX_CLAIM_AGE_HOURS']))
    # this could be a DB, but a dict works for now
    args['db'] = {
        'Files': {},
        'status': {},
        'TransferRequests': {}
    }
    # site configuration
    with open(config["LTA_SITE_CONFIG"]) as site_data:
        args['sites'] = json.load(site_data)

    server = RestServer(debug=debug)
    server.add_route(r'/', MainHandler, args)
    server.add_route(r'/Files', FilesHandler, args)
    server.add_route(r'/Files/actions/bulk_create', FilesActionsBulkCreateHandler, args)
    server.add_route(r'/Files/actions/bulk_delete', FilesActionsBulkDeleteHandler, args)
    server.add_route(r'/Files/actions/bulk_update', FilesActionsBulkUpdateHandler, args)
    server.add_route(r'/Files/actions/pop', FilesActionsPopHandler, args)
    server.add_route(r'/Files/(?P<file_id>\w+)', FilesSingleHandler, args)
    server.add_route(r'/TransferRequests', TransferRequestsHandler, args)
    server.add_route(r'/TransferRequests/(?P<request_id>\w+)', TransferRequestSingleHandler, args)
    server.add_route(r'/TransferRequests/actions/pop', TransferRequestActionsPopHandler, args)
    server.add_route(r'/status', StatusHandler, args)
    server.add_route(r'/status/(?P<component>\w+)', StatusComponentHandler, args)

    server.startup(address=config['LTA_REST_HOST'],
                   port=int(config['LTA_REST_PORT']))
    return server

def main() -> None:
    """Configure logging and start a LTA REST DB service."""
    logging.basicConfig(level=logging.DEBUG)
    start(debug=True)
    loop = asyncio.get_event_loop()
    loop.run_forever()

if __name__ == '__main__':
    main()
