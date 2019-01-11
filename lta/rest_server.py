"""
Long Term Archive REST API server.

Run with `python -m lta.rest_server`.
"""

import asyncio
import logging
from functools import wraps
from uuid import uuid1
from typing import Any, Callable

import tornado.web  # type: ignore
from rest_tools.client import json_decode  # type: ignore
from rest_tools.server import RestServer, RestHandler, RestHandlerSetup, authenticated, catch_error  # type: ignore

from .config import from_environment


EXPECTED_CONFIG = {
    'LTA_REST_URL': 'localhost',
    'LTA_REST_PORT': '8080',
    'LTA_AUTH_SECRET': 'secret',
    'LTA_AUTH_ISSUER': 'lta',
    'LTA_AUTH_ALGORITHM': 'RS256',
}

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

class BaseLTAHandler(RestHandler):
    def initialize(self, db: Any, *args: Any, **kwargs: Any) -> None:
        super(BaseLTAHandler, self).initialize(*args, **kwargs)
        self.db = db

class MainHandler(BaseLTAHandler):
    def get(self) -> None:
        self.write({})

class TransferRequestsHandler(BaseLTAHandler):
    @lta_auth(roles=['admin', 'user', 'system'])
    async def get(self) -> None:
        ret = {
            'results': list(self.db['TransferRequests'].values()),
        }
        self.write(ret)

    @lta_auth(roles=['admin', 'user', 'system'])
    async def post(self) -> None:
        req = json_decode(self.request.body)
        req['uuid'] = uuid1().hex
        self.db['TransferRequests'][req['uuid']] = req
        self.set_status(201)
        self.write({'TransferRequest': req['uuid']})

class TransferRequestSingleHandler(BaseLTAHandler):
    @lta_auth(roles=['admin', 'user', 'system'])
    async def get(self, request_id: str) -> None:
        if request_id not in self.db['TransferRequests']:
            raise tornado.web.HTTPError(404, reason="not found")
        self.write(self.db['TransferRequests'][request_id])

    @lta_auth(roles=['admin', 'user', 'system'])
    async def patch(self, request_id: str) -> None:
        if request_id not in self.db['TransferRequests']:
            raise tornado.web.HTTPError(404, reason="not found")
        req = json_decode(self.request.body)
        if 'uuid' in req and req['uuid'] != request_id:
            raise tornado.web.HTTPError(400, reason="bad request")
        self.db['TransferRequests'][request_id].update(req)
        self.write({})

    @lta_auth(roles=['admin', 'user', 'system'])
    async def delete(self, request_id: str) -> None:
        if request_id in self.db['TransferRequests']:
            del self.db['TransferRequests'][request_id]
            self.set_status(204)

class TransferRequestActionsPopHandler(BaseLTAHandler):
    @lta_auth(roles=['system'])
    async def post(self) -> None:
        raise tornado.web.HTTPError(500, reason="not implemented")
        self.write({})

def start(debug: bool = False) -> RestServer:
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
    args['db'] = {'TransferRequests': {}}

    server = RestServer(debug=debug)
    server.add_route(r'/', MainHandler, args)
    server.add_route(r'/TransferRequests', TransferRequestsHandler, args)
    server.add_route(r'/TransferRequests/(?P<request_id>\w+)', TransferRequestSingleHandler, args)
    server.add_route(r'/TransferRequests/actions/pop', TransferRequestActionsPopHandler, args)

    server.startup(address=config['LTA_REST_URL'],
                   port=int(config['LTA_REST_PORT']))
    return server

def main() -> None:
    logging.basicConfig(level=logging.DEBUG)
    start(debug=True)
    loop = asyncio.get_event_loop()
    loop.run_forever()

if __name__ == '__main__':
    main()
