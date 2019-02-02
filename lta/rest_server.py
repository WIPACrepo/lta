"""
Long Term Archive REST API server.

Run with `python -m lta.rest_server`.
"""

import asyncio
from datetime import datetime, timedelta
import logging
from functools import wraps
from uuid import uuid1
from typing import Any, Callable

import tornado.web  # type: ignore
from rest_tools.client import json_decode  # type: ignore
from rest_tools.server import RestServer, RestHandler, RestHandlerSetup, authenticated, catch_error  # type: ignore

from .config import from_environment

EXPECTED_CONFIG = {
    'LTA_REST_HOST': 'localhost',
    'LTA_REST_PORT': '8080',
    'LTA_AUTH_SECRET': 'secret',
    'LTA_AUTH_ISSUER': 'lta',
    'LTA_AUTH_ALGORITHM': 'RS256',
    'LTA_MAX_CLAIM_AGE_HOURS': '12',
}

# -----------------------------------------------------------------------------

def now() -> str:
    """Return string timestamp for current time, to the second."""
    return datetime.utcnow().isoformat(timespec='seconds')

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

    def initialize(self, db: Any, check_claims: CheckClaims, *args: Any, **kwargs: Any) -> None:
        """Initialize a BaseLTAHandler object."""
        super(BaseLTAHandler, self).initialize(*args, **kwargs)
        self.db = db
        self.check_claims = check_claims

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

        self.set_status(200)
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

        print(req["update"])
        print(req["files"])

        results = []
        for uuid in req["files"]:
            if uuid in self.db['Files']:
                self.db['Files'][uuid].update(req["update"])
                results.append(uuid)

        self.set_status(200)
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
                if lta_file["source"].startswith(location):
                    results.append(file_uuid)
                    continue
            if transfer_request_uuid:
                if lta_file["request"] is transfer_request_uuid:
                    results.append(file_uuid)
                    continue
            if bundle_uuid:
                if lta_file["bundle"] is bundle_uuid:
                    results.append(file_uuid)
                    continue
            if status:
                if lta_file["status"] is status:
                    results.append(file_uuid)
                    continue
            if (location is None) and (transfer_request_uuid is None) and (bundle_uuid is None) and (status is None):
                results.append(file_uuid)

        ret = {
            'results': results,
        }
        self.set_status(200)
        self.write(ret)

class FilesSingleHandler(BaseLTAHandler):
    """FilesSingleHandler handles object level routes for Files."""

    @lta_auth(roles=['admin', 'user', 'system'])
    async def get(self, file_id: str) -> None:
        """Handle GET /Files/{uuid}."""
        if file_id not in self.db['Files']:
            raise tornado.web.HTTPError(404, reason="not found")
        self.set_status(200)
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
        self.set_status(200)
        self.write(self.db['Files'][file_id])

    @lta_auth(roles=['admin', 'user', 'system'])
    async def delete(self, file_id: str) -> None:
        """Handle DELETE /Files/{uuid}."""
        if file_id in self.db['Files']:
            del self.db['Files'][file_id]
            self.set_status(204)
            return
        self.set_status(404)

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
            return
        self.set_status(404)

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
    # this could be a DB, but a dict works for now
    args['db'] = {
        'Files': {},
        'status': {},
        'TransferRequests': {}
    }
    args['check_claims'] = CheckClaims(int(config['LTA_MAX_CLAIM_AGE_HOURS']))

    server = RestServer(debug=debug)
    server.add_route(r'/', MainHandler, args)
    server.add_route(r'/Files', FilesHandler, args)
    server.add_route(r'/Files/actions/bulk_create', FilesActionsBulkCreateHandler, args)
    server.add_route(r'/Files/actions/bulk_delete', FilesActionsBulkDeleteHandler, args)
    server.add_route(r'/Files/actions/bulk_update', FilesActionsBulkUpdateHandler, args)
    # server.add_route(r'/Files/actions/pop', FilesActionsPopHandler, args)
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
