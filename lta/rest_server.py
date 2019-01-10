"""
Long Term Archive REST API server.

Run with `python -m lta.rest_server`.
"""

import logging
from functools import wraps

import tornado.web
from tornado.ioloop import IOLoop

from rest_tools.client import json_decode
from rest_tools.server import RestServer, RestHandler, RestHandlerSetup, authenticated, catch_error

from .config import from_environment


EXPECTED_CONFIG = {
    'LTA_REST_URL': 'localhost',
    'LTA_REST_PORT': 8080,
}

def lta_auth(**_auth):
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
    def make_wrapper(method):
        @authenticated
        @catch_error
        @wraps(method)
        async def wrapper(self, *args, **kwargs):
            roles = _auth.get('roles', [])

            authorized = False

            auth_role = self.auth_data.get('long-term-archive',{}).get('role',None)
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
    def initialize(self, db, *args, **kwargs):
        super(BaseLTAHandler, self).initialize(*args, **kwargs)
        self.db = db

class MainHandler(BaseLTAHandler):
    def get(self):
        self.write({})

def start(debug=False):
    config = from_environment(EXPECTED_CONFIG)
    logger = logging.getLogger('lta.rest')
    
    args = RestHandlerSetup({
#        'auth':{
#            'secret':'secret',
#        },
        'debug': debug
    })
    args['db'] = {} # this could be a DB, but a dict works for now

    server = RestServer(debug=debug)
    server.add_route('/', MainHandler, args)
    server.startup(address=config['LTA_REST_URL'],
                   port=config['LTA_REST_PORT'])
    return server

if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG)
    start(debug=True)
    IOLoop.current().start()
