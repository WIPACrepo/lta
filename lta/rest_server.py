"""
Long Term Archive REST API server.

Run with `python -m lta.rest_server`.
"""

import asyncio
from datetime import datetime, timedelta
from functools import wraps
import logging
from typing import Any, Callable, Dict
from urllib.parse import quote_plus
from uuid import uuid1

from motor.motor_tornado import MotorClient, MotorDatabase  # type: ignore
import pymongo  # type: ignore
from pymongo import MongoClient
from rest_tools.client import json_decode  # type: ignore
from rest_tools.server import authenticated, catch_error, RestHandler, RestHandlerSetup, RestServer  # type: ignore
import tornado.web

from .config import from_environment


EXPECTED_CONFIG = {
    'LTA_AUTH_ALGORITHM': 'RS256',
    'LTA_AUTH_ISSUER': 'lta',
    'LTA_AUTH_SECRET': 'secret',
    'LTA_MAX_CLAIM_AGE_HOURS': '12',
    'LTA_MONGODB_AUTH_USER': '',  # None means required to specify
    'LTA_MONGODB_AUTH_PASS': '',  # empty means no authentication required
    'LTA_MONGODB_DATABASE_NAME': 'lta',
    'LTA_MONGODB_HOST': 'localhost',
    'LTA_MONGODB_PORT': '27017',
    'LTA_REST_HOST': 'localhost',
    'LTA_REST_PORT': '8080',
}

# -----------------------------------------------------------------------------

AFTER = pymongo.ReturnDocument.AFTER
ALL_DOCUMENTS: Dict[str, str] = {}
FIRST_IN_FIRST_OUT = [("create_timestamp", pymongo.ASCENDING)]
REMOVE_ID = {"_id": False}
TRUE_SET = {'1', 't', 'true', 'y', 'yes'}

def boolify(value: str) -> bool:
    """Convert a string into a True or False value."""
    return isinstance(value, str) and value.lower() in TRUE_SET

def now() -> str:
    """Return string timestamp for current time, to the second."""
    return datetime.utcnow().isoformat(timespec='seconds')

def unique_id() -> str:
    """Return a unique ID for an LTA database entity."""
    return uuid1().hex

# -----------------------------------------------------------------------------

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

            auth_role = None
            for scope in self.auth_data.get('scope', '').split():
                if scope.startswith('lta:'):
                    auth_role = scope.split(':', 1)[-1]
                    break
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

    def old_age(self) -> str:
        """Determine the current event horizon for claims."""
        cutoff_time = datetime.utcnow() - timedelta(hours=self.claim_age)
        return cutoff_time.isoformat()

# -----------------------------------------------------------------------------

class BaseLTAHandler(RestHandler):
    """BaseLTAHandler is a RestHandler for all LTA routes."""

    def initialize(self, check_claims: CheckClaims, db: MotorDatabase, *args: Any, **kwargs: Any) -> None:
        """Initialize a BaseLTAHandler object."""
        super(BaseLTAHandler, self).initialize(*args, **kwargs)
        self.check_claims = check_claims
        self.db = db

# -----------------------------------------------------------------------------

class BundlesActionsBulkCreateHandler(BaseLTAHandler):
    """Handler for /Bundles/actions/bulk_create."""

    @lta_auth(roles=['system'])
    async def post(self) -> None:
        """Handle POST /Bundles/actions/bulk_create."""
        req = json_decode(self.request.body)
        if 'bundles' not in req:
            raise tornado.web.HTTPError(400, reason="missing bundles field")
        if not isinstance(req['bundles'], list):
            raise tornado.web.HTTPError(400, reason="bundles field is not a list")
        if not req['bundles']:
            raise tornado.web.HTTPError(400, reason="bundles field is empty")

        for xfer_bundle in req["bundles"]:
            right_now = now()  # https://www.youtube.com/watch?v=BQkFEG_iZUA
            xfer_bundle["uuid"] = unique_id()
            xfer_bundle["create_timestamp"] = right_now
            xfer_bundle["update_timestamp"] = right_now
            xfer_bundle["claimed"] = False

        ret = await self.db.Bundles.insert_many(documents=req["bundles"])
        create_count = len(ret.inserted_ids)

        uuids = []
        for x in req["bundles"]:
            uuid = x["uuid"]
            uuids.append(uuid)
            logging.info(f"created Bundle {uuid}")

        self.set_status(201)
        self.write({'bundles': uuids, 'count': create_count})

class BundlesActionsBulkDeleteHandler(BaseLTAHandler):
    """Handler for /Bundles/actions/bulk_delete."""

    @lta_auth(roles=['system'])
    async def post(self) -> None:
        """Handle POST /Bundles/actions/bulk_delete."""
        req = json_decode(self.request.body)
        if 'bundles' not in req:
            raise tornado.web.HTTPError(400, reason="missing bundles field")
        if not isinstance(req['bundles'], list):
            raise tornado.web.HTTPError(400, reason="bundles field is not a list")
        if not req['bundles']:
            raise tornado.web.HTTPError(400, reason="bundles field is empty")

        results = []
        for uuid in req["bundles"]:
            query = {"uuid": uuid}
            ret = await self.db.Bundles.delete_one(filter=query)
            if ret.deleted_count > 0:
                logging.info(f"deleted Bundle {uuid}")
                results.append(uuid)

        self.write({'bundles': results, 'count': len(results)})

class BundlesActionsBulkUpdateHandler(BaseLTAHandler):
    """Handler for /Bundles/actions/bulk_update."""

    @lta_auth(roles=['system'])
    async def post(self) -> None:
        """Handle POST /Bundles/actions/bulk_update."""
        req = json_decode(self.request.body)
        if 'update' not in req:
            raise tornado.web.HTTPError(400, reason="missing update field")
        if not isinstance(req['update'], dict):
            raise tornado.web.HTTPError(400, reason="update field is not an object")
        if 'bundles' not in req:
            raise tornado.web.HTTPError(400, reason="missing bundles field")
        if not isinstance(req['bundles'], list):
            raise tornado.web.HTTPError(400, reason="bundles field is not a list")
        if not req['bundles']:
            raise tornado.web.HTTPError(400, reason="bundles field is empty")

        results = []
        for uuid in req["bundles"]:
            query = {"uuid": uuid}
            update_doc = {"$set": req["update"]}
            ret = await self.db.Bundles.update_one(filter=query, update=update_doc)
            if ret.modified_count > 0:
                logging.info(f"updated Bundle {uuid}")
                results.append(uuid)

        self.write({'bundles': results, 'count': len(results)})

class BundlesHandler(BaseLTAHandler):
    """BundlesHandler handles collection level routes for Bundles."""

    @lta_auth(roles=['admin', 'user', 'system'])
    async def get(self) -> None:
        """Handle GET /Bundles."""
        location = self.get_query_argument("location", default=None)
        status = self.get_query_argument("status", default=None)
        verified = self.get_query_argument("verified", default=None)

        query: Dict[str, Any] = {}
        if location:
            query["source"] = {"$regex": f"^{location}"}
        if status:
            query["status"] = status
        if verified:
            query["verified"] = boolify(verified)

        results = []
        async for row in self.db.Bundles.find(filter=query,
                                              projection=REMOVE_ID):
            results.append(row["uuid"])

        ret = {
            'results': results,
        }
        self.write(ret)

class BundlesActionsPopHandler(BaseLTAHandler):
    """BundlesActionsPopHandler handles /Bundles/actions/pop."""

    @lta_auth(roles=['system'])
    async def post(self) -> None:
        """Handle POST /Bundles/actions/pop."""
        source = self.get_argument('source')
        status = self.get_argument('status')
        pop_body = json_decode(self.request.body)
        if 'claimant' not in pop_body:
            raise tornado.web.HTTPError(400, reason="missing claimant field")
        claimant = pop_body["claimant"]
        # find and claim a bundle for the specified source
        sdb = self.db.Bundles
        find_query = {
            "source": source,
            "status": status,
            "claimed": False,
        }
        right_now = now()  # https://www.youtube.com/watch?v=WaSy8yy-mr8
        update_doc = {
            "$set": {
                "update_timestamp": right_now,
                "claimed": True,
                "claimant": claimant,
                "claim_timestamp": right_now,
            }
        }
        bundle = await sdb.find_one_and_update(filter=find_query,
                                               update=update_doc,
                                               projection=REMOVE_ID,
                                               sort=FIRST_IN_FIRST_OUT,
                                               return_document=AFTER)
        # return what we found to the caller
        if not bundle:
            logging.info(f"Unclaimed Bundle with source {source} and status {status} does not exist.")
        else:
            logging.info(f"Bundle {bundle['uuid']} claimed by {claimant}")
        self.write({'bundle': bundle})

class BundlesSingleHandler(BaseLTAHandler):
    """BundlesSingleHandler handles object level routes for Bundles."""

    @lta_auth(roles=['admin', 'user', 'system'])
    async def get(self, bundle_id: str) -> None:
        """Handle GET /Bundles/{uuid}."""
        query = {"uuid": bundle_id}
        ret = await self.db.Bundles.find_one(filter=query, projection=REMOVE_ID)
        if not ret:
            raise tornado.web.HTTPError(404, reason="not found")
        self.write(ret)

    @lta_auth(roles=['admin', 'user', 'system'])
    async def patch(self, bundle_id: str) -> None:
        """Handle PATCH /Bundles/{uuid}."""
        req = json_decode(self.request.body)
        if 'uuid' in req and req['uuid'] != bundle_id:
            raise tornado.web.HTTPError(400, reason="bad request")
        query = {"uuid": bundle_id}
        update_doc = {"$set": req}
        ret = await self.db.Bundles.find_one_and_update(filter=query,
                                                        update=update_doc,
                                                        projection=REMOVE_ID,
                                                        return_document=AFTER)
        if not ret:
            raise tornado.web.HTTPError(404, reason="not found")
        logging.info(f"patched Bundle {bundle_id} with {req}")
        self.write(ret)

    @lta_auth(roles=['admin', 'user', 'system'])
    async def delete(self, bundle_id: str) -> None:
        """Handle DELETE /Bundles/{uuid}."""
        query = {"uuid": bundle_id}
        await self.db.Bundles.delete_one(filter=query)
        logging.info(f"deleted Bundle {bundle_id}")
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
        ret = []
        async for row in self.db.TransferRequests.find(filter=ALL_DOCUMENTS,
                                                       projection=REMOVE_ID):
            ret.append(row)
        self.write({'results': ret})

    @lta_auth(roles=['admin', 'user', 'system'])
    async def post(self) -> None:
        """Handle POST /TransferRequests."""
        req = json_decode(self.request.body)
        if 'source' not in req:
            raise tornado.web.HTTPError(400, reason="missing source field")
        if 'dest' not in req:
            raise tornado.web.HTTPError(400, reason="missing dest field")
        if 'path' not in req:
            raise tornado.web.HTTPError(400, reason="missing path field")
        if not isinstance(req['source'], str):
            raise tornado.web.HTTPError(400, reason="source field is not a string")
        if not isinstance(req['dest'], str):
            raise tornado.web.HTTPError(400, reason="dest field is not a string")
        if not isinstance(req['path'], str):
            raise tornado.web.HTTPError(400, reason="path field is not a string")
        if not req['source']:
            raise tornado.web.HTTPError(400, reason="source field is empty")
        if not req['dest']:
            raise tornado.web.HTTPError(400, reason="dest field is empty")
        if not req['path']:
            raise tornado.web.HTTPError(400, reason="path field is empty")

        right_now = now()  # https://www.youtube.com/watch?v=He0p5I0b8j8

        req['type'] = "TransferRequest"
        req['uuid'] = unique_id()
        req['status'] = "unclaimed"
        req['create_timestamp'] = right_now
        req['update_timestamp'] = right_now
        req['claimed'] = False
        await self.db.TransferRequests.insert_one(document=req)
        logging.info(f"created TransferRequest {req['uuid']}")
        self.set_status(201)
        self.write({'TransferRequest': req['uuid']})

class TransferRequestSingleHandler(BaseLTAHandler):
    """TransferRequestSingleHandler is a BaseLTAHandler that handles routes related to single TransferRequest objects."""

    @lta_auth(roles=['admin', 'user', 'system'])
    async def get(self, request_id: str) -> None:
        """Handle GET /TransferRequests/{uuid}."""
        query = {'uuid': request_id}
        ret = await self.db.TransferRequests.find_one(filter=query, projection=REMOVE_ID)
        if not ret:
            raise tornado.web.HTTPError(404, reason="not found")
        self.write(ret)

    @lta_auth(roles=['admin', 'user', 'system'])
    async def patch(self, request_id: str) -> None:
        """Handle PATCH /TransferRequests/{uuid}."""
        req = json_decode(self.request.body)
        if 'uuid' in req and req['uuid'] != request_id:
            raise tornado.web.HTTPError(400, reason="bad request")
        sbtr = self.db.TransferRequests
        query = {"uuid": request_id}
        update = {"$set": req}
        ret = await sbtr.find_one_and_update(filter=query,
                                             update=update,
                                             projection=REMOVE_ID,
                                             return_document=AFTER)
        if not ret:
            raise tornado.web.HTTPError(404, reason="not found")
        logging.info(f"patched TransferRequest {request_id} with {req}")
        self.write({})

    @lta_auth(roles=['admin', 'user', 'system'])
    async def delete(self, request_id: str) -> None:
        """Handle DELETE /TransferRequests/{uuid}."""
        query = {"uuid": request_id}
        await self.db.TransferRequests.delete_one(filter=query)
        logging.info(f"deleted TransferRequest {request_id}")
        self.set_status(204)


class TransferRequestActionsPopHandler(BaseLTAHandler):
    """TransferRequestActionsPopHandler handles /TransferRequests/actions/pop."""

    @lta_auth(roles=['system'])
    async def post(self) -> None:
        """Handle POST /TransferRequests/actions/pop."""
        source = self.get_argument('source')
        pop_body = json_decode(self.request.body)
        if 'claimant' not in pop_body:
            raise tornado.web.HTTPError(400, reason="missing claimant field")
        claimant = pop_body["claimant"]
        # find and claim a transfer request for the specified source
        sdtr = self.db.TransferRequests
        find_query = {
            "source": source,
            "status": "unclaimed",
        }
        right_now = now()  # https://www.youtube.com/watch?v=nRGCZh5A8T4
        update_doc = {
            "$set": {
                "status": "processing",
                "update_timestamp": right_now,
                "claimed": True,
                "claimant": claimant,
                "claim_timestamp": right_now,
            }
        }
        tr = await sdtr.find_one_and_update(filter=find_query,
                                            update=update_doc,
                                            projection=REMOVE_ID,
                                            sort=FIRST_IN_FIRST_OUT,
                                            return_document=AFTER)
        # return what we found to the caller
        if not tr:
            logging.info(f"Unclaimed TransferRequest with source {source} does not exist.")
        else:
            logging.info(f"TransferRequest {tr['uuid']} claimed by {claimant}")
        self.write({'transfer_request': tr})

# -----------------------------------------------------------------------------

class StatusHandler(BaseLTAHandler):
    """StatusHandler is a BaseLTAHandler that handles system status routes."""

    @lta_auth(roles=['admin', 'user', 'system'])
    async def get(self) -> None:
        """Get the overall status of the system."""
        ret: Dict[str, str] = {}
        health = 'OK'
        old_data = (datetime.utcnow() - timedelta(seconds=60*5)).isoformat()

        def date_ok(d: str) -> bool:
            return d > old_data

        sds = self.db.Status
        async for row in sds.find(filter=ALL_DOCUMENTS,
                                  projection=REMOVE_ID):
            # each component defaults to OK
            component = row["component"]
            if component not in ret:
                ret[component] = 'OK'
            # if any of that component type have an old heartbeat
            if not date_ok(row["timestamp"]):
                ret[component] = 'WARN'
                health = 'WARN'
        ret["health"] = health
        self.write(ret)


class StatusComponentHandler(BaseLTAHandler):
    """StatusComponentHandler is a BaseLTAHandler that handles component status routes."""

    @lta_auth(roles=['admin', 'user', 'system'])
    async def get(self, component: str) -> None:
        """
        Get the detailed status of components of a given type.

        This handles the route: GET /status/{component-type}

        In MongoDB, we store the status records like this:
            {
                "component": "picker"
                "name": "picker-node001"
                keys: values
            }

        But the response to GET /status/picker should be like this:
            {
                "picker-node001": {
                    keys: values
                },
                "picker-node002": {
                    keys: values
                }
            }

        So this route takes a lot of Mongo records and folds them into
        the proper response structure.
        """
        # forge, in secret, a master record, to control all others
        ret = {}
        # obtain all the records of the specified component type
        sds = self.db.Status
        query = {"component": component}
        async for row in sds.find(filter=query,
                                  projection=REMOVE_ID):
            # get the proper name of the component
            name = row["name"]
            # remove the old component type and name values from the record
            del row["component"]
            del row["name"]
            # pour into the master record, our cruelty, malice, and will to dominate all life
            update_dict = {name: row}
            ret.update(update_dict)
        # if there was no cruelty or malice, return a not found error
        if len(list(ret.keys())) < 1:
            raise tornado.web.HTTPError(404, reason="not found")
        self.write(ret)

    @lta_auth(roles=['system'])
    async def patch(self, component: str) -> None:
        """Update the detailed status of a component."""
        req = json_decode(self.request.body)
        sds = self.db.Status
        name = list(req.keys())[0]
        query = {"component": component, "name": name}
        status_doc = req[name]
        status_doc["name"] = name
        status_doc["component"] = component
        update_doc = {"$set": status_doc}
        ret = await sds.update_one(filter=query,
                                   update=update_doc,
                                   upsert=True)
        if (ret.modified_count) or (ret.upserted_id):
            logging.info(f"PATCH /status/{component} with {req}")
        else:
            logging.error(f"Unable to PATCH /status/{component} with {req}")
        self.write({})

# -----------------------------------------------------------------------------

def ensure_mongo_indexes(mongo_url: str, mongo_port: int, mongo_db: str) -> None:
    """Ensure that necessary indexes exist in MongoDB."""
    logging.info(f"Configuring MongoDB client at: {mongo_url}")
    client = MongoClient(mongo_url, port=mongo_port)
    db = client[mongo_db]
    logging.info(f"Creating indexes in MongoDB database: {mongo_db}")
    # Bundle.uuid
    if 'bundles_create_timestamp_index' not in db.Bundles.index_information():
        logging.info(f"Creating index for {mongo_db}.Bundles.create_timestamp")
        db.Bundles.create_index('create_timestamp', name='bundles_create_timestamp_index', unique=False)
    if 'bundles_uuid_index' not in db.Bundles.index_information():
        logging.info(f"Creating index for {mongo_db}.Bundles.uuid")
        db.Bundles.create_index('uuid', name='bundles_uuid_index', unique=True)
    # Status.{component, name}
    if 'status_component_index' not in db.Status.index_information():
        logging.info(f"Creating index for {mongo_db}.Status.component")
        db.Status.create_index('component', name='status_component_index', unique=False)
    if 'status_name_index' not in db.Status.index_information():
        logging.info(f"Creating index for {mongo_db}.Status.name")
        db.Status.create_index('name', name='status_name_index', unique=False)
    # TransferRequests.uuid
    if 'transfer_requests_create_timestamp_index' not in db.Bundles.index_information():
        logging.info(f"Creating index for {mongo_db}.TransferRequests.create_timestamp")
        db.TransferRequests.create_index('create_timestamp', name='transfer_requests_create_timestamp_index', unique=False)
    if 'transfer_requests_uuid_index' not in db.TransferRequests.index_information():
        logging.info(f"Creating index for {mongo_db}.TransferRequests.uuid")
        db.TransferRequests.create_index('uuid', name='transfer_requests_uuid_index', unique=True)
    logging.info("Done creating indexes in MongoDB.")


def start(debug: bool = False) -> RestServer:
    """Start a LTA DB service."""
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
    # configure access to MongoDB as a backing store
    mongo_user = quote_plus(config["LTA_MONGODB_AUTH_USER"])
    mongo_pass = quote_plus(config["LTA_MONGODB_AUTH_PASS"])
    mongo_host = config["LTA_MONGODB_HOST"]
    mongo_port = int(config["LTA_MONGODB_PORT"])
    lta_mongodb_url = f"mongodb://{mongo_host}"
    if mongo_user and mongo_pass:
        lta_mongodb_url = f"mongodb://{mongo_user}:{mongo_pass}@{mongo_host}"
    ensure_mongo_indexes(lta_mongodb_url, mongo_port, config["LTA_MONGODB_DATABASE_NAME"])
    motor_client = MotorClient(lta_mongodb_url, port=mongo_port)
    args['db'] = motor_client[config["LTA_MONGODB_DATABASE_NAME"]]

    server = RestServer(debug=debug)
    server.add_route(r'/', MainHandler, args)
    server.add_route(r'/Bundles', BundlesHandler, args)
    server.add_route(r'/Bundles/actions/bulk_create', BundlesActionsBulkCreateHandler, args)
    server.add_route(r'/Bundles/actions/bulk_delete', BundlesActionsBulkDeleteHandler, args)
    server.add_route(r'/Bundles/actions/bulk_update', BundlesActionsBulkUpdateHandler, args)
    server.add_route(r'/Bundles/actions/pop', BundlesActionsPopHandler, args)
    server.add_route(r'/Bundles/(?P<bundle_id>\w+)', BundlesSingleHandler, args)
    server.add_route(r'/TransferRequests', TransferRequestsHandler, args)
    server.add_route(r'/TransferRequests/(?P<request_id>\w+)', TransferRequestSingleHandler, args)
    server.add_route(r'/TransferRequests/actions/pop', TransferRequestActionsPopHandler, args)
    server.add_route(r'/status', StatusHandler, args)
    server.add_route(r'/status/(?P<component>\w+)', StatusComponentHandler, args)

    server.startup(address=config['LTA_REST_HOST'],
                   port=int(config['LTA_REST_PORT']))
    return server

def main() -> None:
    """Configure logging and start a LTA DB service."""
    logging.basicConfig(level=logging.DEBUG)
    start(debug=True)
    loop = asyncio.get_event_loop()
    loop.run_forever()

if __name__ == '__main__':
    main()
