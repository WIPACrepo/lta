"""
Long Term Archive REST API server.

Run with `python -m lta.rest_server`.
"""

import asyncio
from datetime import datetime, timedelta
from functools import wraps
import json
import logging
from typing import Any, Callable, Dict
from uuid import uuid1

from motor.motor_tornado import MotorClient, MotorDatabase  # type: ignore
import pymongo  # type: ignore
from pymongo import MongoClient
from rest_tools.client import json_decode  # type: ignore
from rest_tools.server import authenticated, catch_error, RestHandler, RestHandlerSetup, RestServer  # type: ignore
import tornado.web  # type: ignore

from .config import from_environment


EXPECTED_CONFIG = {
    'LTA_AUTH_ALGORITHM': 'RS256',
    'LTA_AUTH_ISSUER': 'lta',
    'LTA_AUTH_SECRET': 'secret',
    'LTA_MAX_CLAIM_AGE_HOURS': '12',
    'LTA_MONGODB_URL': 'mongodb://localhost:27017/',
    'LTA_REST_HOST': 'localhost',
    'LTA_REST_PORT': '8080',
    'LTA_SITE_CONFIG': 'etc/site.json',
}

# -----------------------------------------------------------------------------

AFTER = pymongo.ReturnDocument.AFTER
ALL_DOCUMENTS: Dict[str, str] = {}
ASCENDING = pymongo.ASCENDING
REMOVE_ID = {"_id": False}

def now() -> str:
    """Return string timestamp for current time, to the second."""
    return datetime.utcnow().isoformat(timespec='seconds')

def site(sitepath: str) -> str:
    """Return SITE from SITE:PATH."""
    return sitepath.split(':', 1)[0]

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

    def old_age(self) -> str:
        """Determine the current event horizon for claims."""
        cutoff_time = datetime.utcnow() - timedelta(hours=self.claim_age)
        return cutoff_time.isoformat()

    def old_claim(self, stamp: str) -> bool:
        """Determine if a claim is old/expired."""
        cutoff_time = datetime.utcnow() - timedelta(hours=self.claim_age)
        stamp_time = datetime.strptime(stamp, '%Y-%m-%dT%H:%M:%S')
        return bool(cutoff_time > stamp_time)

# -----------------------------------------------------------------------------

class BaseLTAHandler(RestHandler):
    """BaseLTAHandler is a RestHandler for all LTA routes."""

    def initialize(self, check_claims: CheckClaims, db: MotorDatabase, sites: Any, *args: Any, **kwargs: Any) -> None:
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

        for xfer_file in req["files"]:
            xfer_file["uuid"] = unique_id()
            xfer_file["create_timestamp"] = now()
            xfer_file["status"] = "waiting"

        ret = await self.db.Files.insert_many(req["files"])
        create_count = len(ret.inserted_ids)

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
            query = {"uuid": uuid}
            ret = await self.db.Files.delete_one(query)
            if ret:
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
            query = {"uuid": uuid}
            update_doc = {"$set": req["update"]}
            ret = await self.db.Files.update_one(query, update_doc)
            if ret:
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

        query = {}
        if location:
            query["source"] = {"$regex": f"^{location}"}
        if transfer_request_uuid:
            query["request"] = transfer_request_uuid
        if bundle_uuid:
            query["bundle"] = bundle_uuid
        if status:
            query["status"] = status

        results = []
        async for row in self.db.Files.find(query, REMOVE_ID):
            results.append(row["uuid"])

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
        xfer_file["uuid"] = unique_id()
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
        # change the limit on the number of files that can be bundled
        limit = self.get_argument('limit', 20000)
        try:
            limit = int(limit)
        except Exception:
            raise tornado.web.HTTPError(400, reason="limit is not an int")
        # skip a number of files in the sorted results
        skip = self.get_argument('skip', 0)
        try:
            skip = int(skip)
        except Exception:
            raise tornado.web.HTTPError(400, reason="skip is not an int")
        # figure out how big the bundle should be
        bundle_size = self.sites[dest]["bundle_size"]
        # get the self-identification of the claiming bundler
        pop_body = json_decode(self.request.body)
        # let's find some files we can bundle
        sdf = self.db.Files
        query = {
            "source": {"$regex": f"^{src}"},
            "dest": {"$regex": f"^{dest}"},
            "status": "waiting"
        }
        sort = [('catalog.logical_name', ASCENDING)]
        db_files = []
        async for row in sdf.find(filter=query,
                                  projection=REMOVE_ID,
                                  skip=skip,
                                  limit=limit,
                                  sort=sort):
            db_files.append(row)
        # do a sanity check; individual files aren't too big
        for db_file in db_files:
            db_file_size = db_file["catalog"]["file_size"]
            db_file_uuid = db_file["uuid"]
            if db_file_size > bundle_size:
                # TODO: Move the monster file to quarantine?
                raise tornado.web.HTTPError(400, reason=f"cannot bundle file {db_file_uuid} (size: {db_file_size}); bundle size is {bundle_size}")
        # do a sanity check; enough files to make a full bundle
        full_house = (len(db_files) >= limit)
        full_size = 0
        for db_file in db_files:
            full_size = full_size + db_file["catalog"]["file_size"]
        if (full_house) and (full_size < bundle_size):
            raise tornado.web.HTTPError(400, reason="limit must be raised to reach full bundle size")
        if (not force) and (full_size < bundle_size):
            self.write({'results': []})
            return
        # use The Price is Right rules to pick files for bundling
        build_files = []
        build_size = 0
        for db_file in db_files:
            db_file_size = db_file["catalog"]["file_size"]
            if (build_size + db_file_size) > bundle_size:
                break
            build_files.append(db_file)
            build_size = build_size + db_file_size
        # update the status of the files we're handing out
        claim_time = now()
        for build_file in build_files:
            build_file["claimant"] = pop_body
            build_file["claimed"] = True
            build_file["claim_time"] = claim_time
            build_file["status"] = "processing"
            update_query = {"uuid": build_file["uuid"]}
            update_doc = {"$set": build_file}
            ret = await sdf.find_one_and_update(update_query, update_doc)
            if not ret:
                raise tornado.web.HTTPError(500, reason=f"unable to claim file for bundling")
        # hand the files out to the caller
        self.write({'results': build_files})

class FilesSingleHandler(BaseLTAHandler):
    """FilesSingleHandler handles object level routes for Files."""

    @lta_auth(roles=['admin', 'user', 'system'])
    async def get(self, file_id: str) -> None:
        """Handle GET /Files/{uuid}."""
        query = {"uuid": file_id}
        ret = await self.db.Files.find_one(query, REMOVE_ID)
        if not ret:
            raise tornado.web.HTTPError(404, reason="not found")
        self.write(ret)

    @lta_auth(roles=['admin', 'user', 'system'])
    async def patch(self, file_id: str) -> None:
        """Handle PATCH /Files/{uuid}."""
        req = json_decode(self.request.body)
        if 'uuid' in req and req['uuid'] != file_id:
            raise tornado.web.HTTPError(400, reason="bad request")
        query = {"uuid": file_id}
        update_doc = {"$set": req}
        ret = await self.db.Files.find_one_and_update(query, update_doc, REMOVE_ID, return_document=AFTER)
        if not ret:
            raise tornado.web.HTTPError(404, reason="not found")
        self.write(ret)

    @lta_auth(roles=['admin', 'user', 'system'])
    async def delete(self, file_id: str) -> None:
        """Handle DELETE /Files/{uuid}."""
        query = {"uuid": file_id}
        await self.db.Files.delete_one(query)
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
        # ret = {
        #     'results': list(self.db['TransferRequests'].values()),
        # }
        # self.write(ret)
        ret = []
        async for row in self.db.TransferRequests.find(ALL_DOCUMENTS, REMOVE_ID):
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
        if not isinstance(req['dest'], list):
            raise tornado.web.HTTPError(400, reason="dest field is not a list")
        if not req['dest']:
            raise tornado.web.HTTPError(400, reason="dest field is empty")

        req['uuid'] = unique_id()
        req['claimed'] = False
        req['claim_time'] = ''
        req['create_timestamp'] = now()
        # self.db['TransferRequests'][req['uuid']] = req
        logging.info(f"Creating TransferRequest {req}")
        await self.db.TransferRequests.insert_one(req)
        self.set_status(201)
        self.write({'TransferRequest': req['uuid']})

class TransferRequestSingleHandler(BaseLTAHandler):
    """TransferRequestSingleHandler is a BaseLTAHandler that handles routes related to single TransferRequest objects."""

    @lta_auth(roles=['admin', 'user', 'system'])
    async def get(self, request_id: str) -> None:
        """Handle GET /TransferRequests/{uuid}."""
        # if request_id not in self.db['TransferRequests']:
        #     raise tornado.web.HTTPError(404, reason="not found")
        # self.write(self.db['TransferRequests'][request_id])
        ret = await self.db.TransferRequests.find_one({'uuid': request_id}, REMOVE_ID)
        if not ret:
            raise tornado.web.HTTPError(404, reason="not found")
        else:
            self.write(ret)

    @lta_auth(roles=['admin', 'user', 'system'])
    async def patch(self, request_id: str) -> None:
        """Handle PATCH /TransferRequests/{uuid}."""
        req = json_decode(self.request.body)
        if 'uuid' in req and req['uuid'] != request_id:
            raise tornado.web.HTTPError(400, reason="bad request")
        # self.db['TransferRequests'][request_id].update(req)
        # self.write({})
        sbtr = self.db.TransferRequests
        query = {"uuid": request_id}
        update = {"$set": req}
        ret = await sbtr.find_one_and_update(query,
                                             update,
                                             projection=REMOVE_ID,
                                             return_document=AFTER)
        if not ret:
            raise tornado.web.HTTPError(404, reason="not found")
        else:
            self.write({})

    @lta_auth(roles=['admin', 'user', 'system'])
    async def delete(self, request_id: str) -> None:
        """Handle DELETE /TransferRequests/{uuid}."""
        # if request_id in self.db['TransferRequests']:
        #     del self.db['TransferRequests'][request_id]
        # self.set_status(204)
        query = {"uuid": request_id}
        await self.db.TransferRequests.delete_one(query)
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
        # find unclaimed or old transfer requests for the specified source
        sdtr = self.db.TransferRequests
        old_age = self.check_claims.old_age()
        query = {
            "$and": [
                {"source": {"$regex": f"^{src}"}},
                {"$or": [
                    {"claimed": False},
                    {"claim_time": {"$lt": f"{old_age}"}}
                ]}
            ]
        }
        ret = []
        async for row in sdtr.find(query, REMOVE_ID, limit=limit):
            row["claimant"] = pop_body
            row["claimed"] = True
            row["claim_time"] = now()
            update_query = {"uuid": row["uuid"]}
            update_doc = {"$set": row}
            ret2 = await sdtr.find_one_and_update(update_query,
                                                  update_doc,
                                                  REMOVE_ID,
                                                  return_document=AFTER)
            if not ret2:
                raise tornado.web.HTTPError(500, reason="unable to update transfer request")
            ret.append(row)
        self.write({'results': ret})


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
        async for row in sds.find(ALL_DOCUMENTS, REMOVE_ID):
            # each component defaults to OK
            component = row["component"]
            if component not in ret:
                ret[component] = 'OK'
            # if any of that component type have an old heartbeat
            if not date_ok(row["t"]):
                ret[component] = 'WARN'
                health = 'WARN'
        ret["health"] = health
        self.write(ret)


class StatusComponentHandler(BaseLTAHandler):
    """StatusComponentHandler is a BaseLTAHandler that handles component status routes."""

    @lta_auth(roles=['admin', 'user', 'system'])
    async def get(self, component: str) -> None:
        """Get the detailed status of a component."""
        # if component not in self.db['status']:
        #     raise tornado.web.HTTPError(404, reason="not found")
        # self.write(self.db['status'][component])
        ret = {}
        sds = self.db.Status
        query = {"component": component}
        async for row in sds.find(query, REMOVE_ID):
            name = row["name"]
            del row["component"]
            del row["name"]
            update_dict = {name: row}
            ret.update(update_dict)
        if len(list(ret.keys())) < 1:
            raise tornado.web.HTTPError(404, reason="not found")
        self.write(ret)

    @lta_auth(roles=['system'])
    async def patch(self, component: str) -> None:
        """Update the detailed status of a component."""
        req = json_decode(self.request.body)
        # if component in self.db['status']:
        #     self.db['status'][component].update(req)
        # else:
        #     self.db['status'][component] = req
        sds = self.db.Status
        name = list(req.keys())[0]
        query = {"component": component, "name": name}
        ret = await sds.find_one(query, REMOVE_ID)
        status_doc = req[name]
        status_doc["name"] = name
        status_doc["component"] = component
        if not ret:
            ret2 = await sds.insert_one(status_doc)
        else:
            update_doc = {"$set": status_doc}
            ret2 = await sds.find_one_and_update(query, update_doc, REMOVE_ID, return_document=AFTER)
        if not ret2:
            raise tornado.web.HTTPError(500, reason="unable to insert/update Status")
        self.write({})


# -----------------------------------------------------------------------------

def ensure_mongo_indexes(mongo_url: str, mongo_db: str) -> None:
    """Ensure that necessary indexes exist in MongoDB."""
    logging.info(f"Configuring MongoDB client at: {mongo_url}")
    client = MongoClient(mongo_url)
    db = client[mongo_db]
    logging.info(f"Creating indexes in MongoDB database: {mongo_db}")
    # TransferRequests.uuid
    if 'transfer_requests_uuid_index' not in db.TransferRequests.index_information():
        logging.info(f"Creating index for {mongo_db}.TransferRequests.uuid")
        db.TransferRequests.create_index('uuid', name='transfer_requests_uuid_index', unique=True)
    # Status.{component, name}
    if 'status_component_index' not in db.Status.index_information():
        logging.info(f"Creating index for {mongo_db}.Status.component")
        db.Status.create_index('component', name='status_component_index', unique=False)
    if 'status_name_index' not in db.Status.index_information():
        logging.info(f"Creating index for {mongo_db}.Status.name")
        db.Status.create_index('name', name='status_name_index', unique=False)
    # Files.uuid
    if 'files_uuid_index' not in db.Files.index_information():
        logging.info(f"Creating index for {mongo_db}.Files.uuid")
        db.Files.create_index('uuid', name='files_uuid_index', unique=True)
    logging.info("Done creating indexes in MongoDB.")


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
    # configure access to MongoDB as a backing store
    ensure_mongo_indexes(config["LTA_MONGODB_URL"], 'lta')
    motor_client = MotorClient(config["LTA_MONGODB_URL"])
    args['db'] = motor_client['lta']
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
