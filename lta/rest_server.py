"""
Long Term Archive REST API server.

Run with `python -m lta.rest_server`.
"""

import asyncio
import functools
import re
from datetime import datetime
import logging
import os
import sys
from typing import Any, cast, List, Optional, Tuple, Union
from urllib.parse import quote_plus
from uuid import uuid1

from pymongo import AsyncMongoClient
from pymongo.asynchronous.database import AsyncDatabase
from prometheus_client import Counter, start_http_server
import pymongo
from pymongo import MongoClient
from rest_tools.utils.json_util import json_decode
from rest_tools.server import (
    RestHandler,
    RestHandlerSetup,
    RestServer,
    ArgumentHandler,
    ArgumentSource,
)
from rest_tools.server.decorators import keycloak_role_auth
import tornado.web
from wipac_dev_tools import from_environment, strtobool

# fmt:off

# maximum number of Metadata UUIDs to supply to MongoDB.deleteMany() during bulk_delete
DELETE_CHUNK_SIZE = 1000

EXPECTED_CONFIG = {
    'LOG_LEVEL': 'DEBUG',
    'CI_TEST': 'FALSE',
    'LTA_AUTH_AUDIENCE': 'long-term-archive',
    'LTA_AUTH_OPENID_URL': '',
    'LTA_MAX_BODY_SIZE': '16777216',  # 16 MB is the limit of MongoDB documents
    'LTA_MONGODB_AUTH_USER': '',  # None means required to specify
    'LTA_MONGODB_AUTH_PASS': '',  # empty means no authentication required
    'LTA_MONGODB_DATABASE_NAME': 'lta',
    'LTA_MONGODB_HOST': 'localhost',
    'LTA_MONGODB_PORT': '27017',
    'LTA_REST_HOST': 'localhost',
    'LTA_REST_PORT': '8080',
    'PROMETHEUS_METRICS_PORT': '8090',
}

LOG = logging.getLogger(__name__)

# -----------------------------------------------------------------------------

AFTER = pymongo.ReturnDocument.AFTER
ALL_DOCUMENTS: dict[str, Any] = {"uuid": {"$exists": True}}
FIRST_IN_FIRST_OUT = [("work_priority_timestamp", pymongo.ASCENDING)]
LOGGING_DENY_LIST = ["LTA_MONGODB_AUTH_PASS"]
LTA_AUTH_PREFIX = "resource_access.long-term-archive.roles"
LTA_AUTH_ROLES = ["system"]
REMOVE_ID = {"_id": False}

# -----------------------------------------------------------------------------

# these are the indexes we expect in our backing MongoDB
MONGO_INDEXES: List[Tuple[str, str, str, Optional[bool]]] = [
    # (collection,       field,                     index_name,                                        unique)
    ("Bundles",          "create_timestamp",        "bundles_create_timestamp_index",                  False),  # noqa: E241
    ("Bundles",          "request",                 "bundles_request_index",                           None),   # noqa: E241
    ("Bundles",          "source",                  "bundles_source_index",                            None),   # noqa: E241
    ("Bundles",          "status",                  "bundles_status_index",                            None),   # noqa: E241
    ("Bundles",          "uuid",                    "bundles_uuid_index",                              True),   # noqa: E241
    ("Bundles",          "verified",                "bundles_verified_index",                          None),   # noqa: E241
    ("Bundles",          "work_priority_timestamp", "bundles_work_priority_timestamp_index",           False),  # noqa: E241

    ("Metadata",         "bundle_uuid",             "metadata_bundle_uuid_index",                      None),   # noqa: E241
    ("Metadata",         "uuid",                    "metadata_uuid_index",                             True),   # noqa: E241

    ("TransferRequests", "create_timestamp",        "transfer_requests_create_timestamp_index",        False),  # noqa: E241
    ("TransferRequests", "uuid",                    "transfer_requests_uuid_index",                    True),   # noqa: E241
    ("TransferRequests", "work_priority_timestamp", "transfer_requests_work_priority_timestamp_index", False),  # noqa: E241
]

# -----------------------------------------------------------------------------

lta_auth = keycloak_role_auth

# -----------------------------------------------------------------------------


def now() -> str:
    """Return string timestamp for current time, to the second."""
    return datetime.utcnow().isoformat(timespec='seconds')


def unique_id() -> str:
    """Return a unique ID for an LTA database entity."""
    return uuid1().hex

# -----------------------------------------------------------------------------


DatabaseType = dict[str, Any]


@functools.lru_cache()
def path_regex_to_human(rstring: str) -> str:
    """Transform a regex-based path string to a human-friendly path.

    Ex: r"/TransferRequests/(?P<request_id>...+)" -> "/TransferRequests/{request_id}"
    """
    out = re.sub(
        # match named capture groups like "...(?P<request_id>\w+)..."
        r"\(\?P<([A-Za-z_][A-Za-z0-9_]*)>(?:\\.|[^\\)])+\)",
        # and replace with "...{request_id}..."
        r"{\1}",
        rstring
    )
    out.rstrip("$")
    return out


class BaseLTAHandler(RestHandler):
    """BaseLTAHandler is a RestHandler for all LTA routes."""

    def initialize(  # type: ignore[override]
            self,
            db: AsyncDatabase[DatabaseType],
            request_counter: Counter,
            response_counter: Counter,
            *args: Any,
            **kwargs: Any) -> None:
        """Initialize a BaseLTAHandler object."""
        super(BaseLTAHandler, self).initialize(*args, **kwargs)
        self.db = db
        self.request_counter = request_counter
        self.response_counter = response_counter

    def prepare(self):
        """Prepare before http-method request handlers."""
        route = path_regex_to_human(getattr(self, "ROUTE"))
        self.request_counter.labels(
            method=self.request.method,
            route=route
        ).inc()
        if os.getenv("CI"):
            logging.info(f"Prometheus metric incremented for '{route}'")
            logging.info(f"Prometheus labels: {self.request_counter.labels=}")

        super().prepare()

    def on_finish(self):
        """Cleanup after http-method request handlers."""
        route = path_regex_to_human(getattr(self, "ROUTE"))
        self.response_counter.labels(
            method=self.request.method,
            response=str(self.get_status()),
            route=route,
        ).inc()
        if os.getenv("CI"):
            logging.info(f"Prometheus metric incremented for '{route}'")
            logging.info(f"Prometheus labels: {self.response_counter.labels=}")

        super().on_finish()


# -----------------------------------------------------------------------------


class BundlesActionsBulkCreateHandler(BaseLTAHandler):
    """Handler for /Bundles/actions/bulk_create."""

    ROUTE = r"/Bundles/actions/bulk_create"

    @lta_auth(prefix=LTA_AUTH_PREFIX, roles=LTA_AUTH_ROLES)  # type: ignore
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
            xfer_bundle["work_priority_timestamp"] = right_now
            xfer_bundle["claimed"] = False

        logging.debug(f"MONGO-START: db.Bundles.insert_many(documents={req['bundles']})")
        ret = await self.db.Bundles.insert_many(documents=req["bundles"])
        logging.debug("MONGO-END:   db.Bundles.insert_many(documents)")
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

    ROUTE = r"/Bundles/actions/bulk_delete"

    @lta_auth(prefix=LTA_AUTH_PREFIX, roles=LTA_AUTH_ROLES)  # type: ignore
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
            logging.debug(f"MONGO-START: db.Bundles.delete_one(filter={query})")
            ret = await self.db.Bundles.delete_one(filter=query)
            logging.debug("MONGO-END:   db.Bundles.delete_one(filter)")
            if ret.deleted_count > 0:
                logging.info(f"deleted Bundle {uuid}")
                results.append(uuid)

        self.write({'bundles': results, 'count': len(results)})


class BundlesActionsBulkUpdateHandler(BaseLTAHandler):
    """Handler for /Bundles/actions/bulk_update."""

    ROUTE = r"/Bundles/actions/bulk_update"

    @lta_auth(prefix=LTA_AUTH_PREFIX, roles=LTA_AUTH_ROLES)  # type: ignore
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
            logging.debug(f"MONGO-START: db.Bundles.update_one(filter={query}, update={update_doc})")
            ret = await self.db.Bundles.update_one(filter=query, update=update_doc)
            logging.debug("MONGO-END:   db.Bundles.update_one(filter, update)")
            if ret.modified_count > 0:
                logging.info(f"updated Bundle {uuid}")
                results.append(uuid)

        self.write({'bundles': results, 'count': len(results)})


class BundlesHandler(BaseLTAHandler):
    """BundlesHandler handles collection level routes for Bundles."""

    ROUTE = r"/Bundles"

    @lta_auth(prefix=LTA_AUTH_PREFIX, roles=LTA_AUTH_ROLES)  # type: ignore
    async def get(self) -> None:
        """Handle GET /Bundles."""
        location = self.get_query_argument("location", default=None)
        request = self.get_query_argument("request", default=None)
        status = self.get_query_argument("status", default=None)
        verified = self.get_query_argument("verified", default=None)

        query: dict[str, Any] = {
            "uuid": {"$exists": True},
        }
        if location:
            query["source"] = {"$regex": f"^{location}"}
        if request:
            query["request"] = request
        if status:
            query["status"] = status
        if verified:
            query["verified"] = strtobool(verified)

        projection: dict[str, bool] = {
            "_id": False,
            "uuid": True,
        }

        results = []
        logging.debug(f"MONGO-START: db.Bundles.find(filter={query}, projection={projection})")
        async for row in self.db.Bundles.find(filter=query,
                                              projection=projection):
            results.append(row["uuid"])
        logging.debug("MONGO-END*:   db.Bundles.find(filter, projection)")

        ret = {
            'results': results,
        }
        self.write(ret)


class BundlesActionsPopHandler(BaseLTAHandler):
    """BundlesActionsPopHandler handles /Bundles/actions/pop."""

    ROUTE = r"/Bundles/actions/pop"

    @lta_auth(prefix=LTA_AUTH_PREFIX, roles=LTA_AUTH_ROLES)  # type: ignore
    async def post(self) -> None:
        """Handle POST /Bundles/actions/pop."""
        dest: Optional[str] = self.get_argument('dest', default=None)
        source: Optional[str] = self.get_argument('source', default=None)
        status: str = self.get_argument('status')
        if (not dest) and (not source):
            raise tornado.web.HTTPError(400, reason="missing source and dest fields")
        pop_body = json_decode(self.request.body)
        if 'claimant' not in pop_body:
            raise tornado.web.HTTPError(400, reason="missing claimant field")
        claimant = pop_body["claimant"]
        # find and claim a bundle for the specified source
        sdb = self.db.Bundles
        find_query = {
            "status": status,
            "claimed": False,
        }
        if dest:
            find_query["dest"] = dest
        if source:
            find_query["source"] = source
        right_now = now()  # https://www.youtube.com/watch?v=WaSy8yy-mr8
        update_doc = {
            "$set": {
                "update_timestamp": right_now,
                "claimed": True,
                "claimant": claimant,
                "claim_timestamp": right_now,
            }
        }
        logging.debug(f"MONGO-START: db.Bundles.find_one_and_update(filter={find_query}, update={update_doc}, projection={REMOVE_ID}, sort={FIRST_IN_FIRST_OUT}, return_document={AFTER})")
        bundle = await sdb.find_one_and_update(filter=find_query,
                                               update=update_doc,
                                               projection=REMOVE_ID,
                                               sort=FIRST_IN_FIRST_OUT,
                                               return_document=AFTER)
        logging.debug("MONGO-END:   db.Bundles.find_one_and_update(filter, update, projection, sort, return_document)")
        # return what we found to the caller
        if not bundle:
            logging.info(f"Unclaimed Bundle with source {source} and status {status} does not exist.")
        else:
            logging.info(f"Bundle {bundle['uuid']} claimed by {claimant}")
        self.write({'bundle': bundle})


class BundlesSingleHandler(BaseLTAHandler):
    """BundlesSingleHandler handles object level routes for Bundles."""

    ROUTE = r"/Bundles/(?P<bundle_id>\w+)"

    @lta_auth(prefix=LTA_AUTH_PREFIX, roles=LTA_AUTH_ROLES)  # type: ignore
    async def get(self, bundle_id: str) -> None:
        """Handle GET /Bundles/{uuid}."""
        query = {"uuid": bundle_id}
        projection = {
            "_id": False,
            "files": False,
        }
        logging.debug(f"MONGO-START: db.Bundles.find_one(filter={query}, projection={projection})")
        ret = await self.db.Bundles.find_one(filter=query, projection=projection)
        logging.debug("MONGO-END:   db.Bundles.find_one(filter, projection)")
        if not ret:
            raise tornado.web.HTTPError(404, reason="not found")
        self.write(ret)

    @lta_auth(prefix=LTA_AUTH_PREFIX, roles=LTA_AUTH_ROLES)  # type: ignore
    async def patch(self, bundle_id: str) -> None:
        """Handle PATCH /Bundles/{uuid}."""
        req = json_decode(self.request.body)
        if 'uuid' in req and req['uuid'] != bundle_id:
            raise tornado.web.HTTPError(400, reason="bad request")
        query = {"uuid": bundle_id}
        update_doc = {"$set": req}
        logging.debug(f"MONGO-START: db.Bundles.find_one_and_update(filter={query}, update={update_doc}, projection={REMOVE_ID}, return_document={AFTER})")
        ret = await self.db.Bundles.find_one_and_update(filter=query,
                                                        update=update_doc,
                                                        projection=REMOVE_ID,
                                                        return_document=AFTER)
        logging.debug("MONGO-END:   db.Bundles.find_one_and_update(filter, update, projection, return_document)")
        if not ret:
            raise tornado.web.HTTPError(404, reason="not found")
        logging.info(f"patched Bundle {bundle_id} with {req}")
        self.write(ret)

    @lta_auth(prefix=LTA_AUTH_PREFIX, roles=LTA_AUTH_ROLES)  # type: ignore
    async def delete(self, bundle_id: str) -> None:
        """Handle DELETE /Bundles/{uuid}."""
        query = {"uuid": bundle_id}
        logging.debug(f"MONGO-START: db.Bundles.delete_one(filter={query})")
        await self.db.Bundles.delete_one(filter=query)
        logging.debug("MONGO-END:   db.Bundles.delete_one(filter)")
        logging.info(f"deleted Bundle {bundle_id}")
        self.set_status(204)

# -----------------------------------------------------------------------------


class MainHandler(BaseLTAHandler):
    """MainHandler is a BaseLTAHandler that handles the root route."""

    ROUTE = r"/"

    def get(self) -> None:
        """Handle GET /."""
        self.write({})

# -----------------------------------------------------------------------------


class MetadataActionsBulkCreateHandler(BaseLTAHandler):
    """Handler for /Metadata/actions/bulk_create."""

    ROUTE = r"/Metadata/actions/bulk_create"

    @lta_auth(prefix=LTA_AUTH_PREFIX, roles=LTA_AUTH_ROLES)  # type: ignore
    async def post(self) -> None:
        """Handle POST /Metadata/actions/bulk_create."""
        argo = ArgumentHandler(ArgumentSource.JSON_BODY_ARGUMENTS, self)
        argo.add_argument('bundle_uuid', type=str)
        argo.add_argument("files", type=list)
        args = argo.parse_args()
        bundle_uuid = args.bundle_uuid
        files = args.files
        if not bundle_uuid:
            raise tornado.web.HTTPError(400, reason='bundle_uuid must not be empty')
        if not files:
            raise tornado.web.HTTPError(400, reason='files must not be empty')

        documents = []
        for file_catalog_uuid in files:
            documents.append({
                "uuid": unique_id(),
                "bundle_uuid": bundle_uuid,
                "file_catalog_uuid": file_catalog_uuid,
            })

        logging.debug(f"MONGO-START: db.Metadata.insert_many(documents=[{len(documents)} documents])")
        ret = await self.db.Metadata.insert_many(documents=documents)
        logging.debug("MONGO-END:   db.Metadata.insert_many(documents)")
        create_count = len(ret.inserted_ids)

        uuids = []
        for x in documents:
            uuid = x["uuid"]
            uuids.append(uuid)
            logging.info(f"created Metadata {uuid}")

        self.set_status(201)
        self.write({'metadata': uuids, 'count': create_count})


class MetadataActionsBulkDeleteHandler(BaseLTAHandler):
    """Handler for /Metadata/actions/bulk_delete."""

    ROUTE = r"/Metadata/actions/bulk_delete"

    @lta_auth(prefix=LTA_AUTH_PREFIX, roles=LTA_AUTH_ROLES)  # type: ignore
    async def post(self) -> None:
        """Handle POST /Metadata/actions/bulk_delete."""
        argo = ArgumentHandler(ArgumentSource.JSON_BODY_ARGUMENTS, self)
        argo.add_argument("metadata", type=list)
        args = argo.parse_args()
        metadata = args.metadata
        if not metadata:
            raise tornado.web.HTTPError(400, reason='metadata must not be empty')

        count = 0
        slice_index = 0
        NUM_UUIDS = len(metadata)
        for i in range(slice_index, NUM_UUIDS, DELETE_CHUNK_SIZE):
            slice_index = i
            delete_slice = metadata[slice_index:(slice_index + DELETE_CHUNK_SIZE)]
            query = {"uuid": {"$in": delete_slice}}
            logging.debug(f"MONGO-START: db.Metadata.delete_many(filter={len(delete_slice)} UUIDs)")
            ret = await self.db.Metadata.delete_many(filter=query)
            logging.debug("MONGO-END:   db.Metadata.delete_many(filter)")
            count = count + ret.deleted_count

        self.write({'metadata': metadata, 'count': count})


class MetadataHandler(BaseLTAHandler):
    """MetadataHandler handles collection level routes for Metadata."""

    ROUTE = r"/Metadata"

    @lta_auth(prefix=LTA_AUTH_PREFIX, roles=LTA_AUTH_ROLES)  # type: ignore
    async def get(self) -> None:
        """Handle GET /Metadata."""
        bundle_uuid = self.get_query_argument("bundle_uuid", default=None)
        limit = int(self.get_query_argument("limit", default="1000"))
        skip = int(self.get_query_argument("skip", default="0"))

        query: dict[str, Any] = {
            "bundle_uuid": bundle_uuid,
        }

        projection: dict[str, bool] = {"_id": False}

        results = []
        logging.debug(f"MONGO-START: db.Metadata.find(filter={query}, projection={projection}, limit={limit}, skip={skip})")
        async for row in self.db.Metadata.find(filter=query,
                                               projection=projection,
                                               skip=skip,
                                               limit=limit):
            results.append(row)
        logging.debug("MONGO-END*:   db.Metadata.find(filter, projection, limit, skip)")

        ret = {
            'results': results,
        }
        self.write(ret)

    @lta_auth(prefix=LTA_AUTH_PREFIX, roles=LTA_AUTH_ROLES)  # type: ignore
    async def delete(self) -> None:
        """Handle DELETE /Metadata?bundle_uuid={uuid}."""
        bundle_uuid = self.get_argument("bundle_uuid", None)
        if not bundle_uuid:
            raise tornado.web.HTTPError(400, reason='bundle_uuid must not be empty')
        query = {"bundle_uuid": bundle_uuid}
        logging.debug(f"MONGO-START: db.Metadata.delete_many(filter={query})")
        await self.db.Metadata.delete_many(filter=query)
        logging.debug("MONGO-END:   db.Metadata.delete_many(filter)")
        logging.info(f"deleted all Metadata records for Bundle {bundle_uuid}")
        self.set_status(204)


class MetadataSingleHandler(BaseLTAHandler):
    """MetadataSingleHandler handles object level routes for Metadata."""

    ROUTE = r"/Metadata/(?P<metadata_id>\w+)"

    @lta_auth(prefix=LTA_AUTH_PREFIX, roles=LTA_AUTH_ROLES)  # type: ignore
    async def get(self, metadata_id: str) -> None:
        """Handle GET /Metadata/{uuid}."""
        query = {"uuid": metadata_id}
        projection = {"_id": False}
        logging.debug(f"MONGO-START: db.Metadata.find_one(filter={query}, projection={projection})")
        ret = await self.db.Metadata.find_one(filter=query, projection=projection)
        logging.debug("MONGO-END:   db.Metadata.find_one(filter, projection)")
        if not ret:
            raise tornado.web.HTTPError(404, reason="not found")
        self.write(ret)

    @lta_auth(prefix=LTA_AUTH_PREFIX, roles=LTA_AUTH_ROLES)  # type: ignore
    async def delete(self, metadata_id: str) -> None:
        """Handle DELETE /Metadata/{uuid}."""
        query = {"uuid": metadata_id}
        logging.debug(f"MONGO-START: db.Metadata.delete_one(filter={query})")
        await self.db.Metadata.delete_one(filter=query)
        logging.debug("MONGO-END:   db.Metadata.delete_one(filter)")
        logging.info(f"deleted Bundle {metadata_id}")
        self.set_status(204)

# -----------------------------------------------------------------------------


class TransferRequestsHandler(BaseLTAHandler):
    """TransferRequestsHandler is a BaseLTAHandler that handles TransferRequests routes."""

    ROUTE = r"/TransferRequests"

    @lta_auth(prefix=LTA_AUTH_PREFIX, roles=LTA_AUTH_ROLES)  # type: ignore
    async def get(self) -> None:
        """Handle GET /TransferRequests."""
        ret = []
        logging.debug(f"MONGO-START: db.TransferRequests.find(filter={ALL_DOCUMENTS}, projection={REMOVE_ID})")
        async for row in self.db.TransferRequests.find(filter=ALL_DOCUMENTS,
                                                       projection=REMOVE_ID):
            ret.append(row)
        logging.debug("MONGO-END*:  db.TransferRequests.find(filter, projection)")
        self.write({'results': ret})

    @lta_auth(prefix=LTA_AUTH_PREFIX, roles=LTA_AUTH_ROLES)  # type: ignore
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
        req['work_priority_timestamp'] = right_now
        req['claimed'] = False
        logging.debug(f"MONGO-START: db.TransferRequests.insert_one(document={req}")
        await self.db.TransferRequests.insert_one(document=req)
        logging.debug("MONGO-END:   db.TransferRequests.insert_one(document)")
        logging.info(f"created TransferRequest {req['uuid']}")
        self.set_status(201)
        self.write({'TransferRequest': req['uuid']})


class TransferRequestSingleHandler(BaseLTAHandler):
    """TransferRequestSingleHandler is a BaseLTAHandler that handles routes related to single TransferRequest objects."""

    ROUTE = r"/TransferRequests/(?P<request_id>\w+)"

    @lta_auth(prefix=LTA_AUTH_PREFIX, roles=LTA_AUTH_ROLES)  # type: ignore
    async def get(self, request_id: str) -> None:
        """Handle GET /TransferRequests/{uuid}."""
        query = {'uuid': request_id}
        logging.debug(f"MONGO-START: db.TransferRequests.find_one(filter={query}, projection={REMOVE_ID}")
        ret = await self.db.TransferRequests.find_one(filter=query, projection=REMOVE_ID)
        logging.debug("MONGO-END:   db.TransferRequests.find_one(filter, projection)")
        if not ret:
            raise tornado.web.HTTPError(404, reason="not found")
        self.write(ret)

    @lta_auth(prefix=LTA_AUTH_PREFIX, roles=LTA_AUTH_ROLES)  # type: ignore
    async def patch(self, request_id: str) -> None:
        """Handle PATCH /TransferRequests/{uuid}."""
        req = json_decode(self.request.body)
        if 'uuid' in req and req['uuid'] != request_id:
            raise tornado.web.HTTPError(400, reason="bad request")
        sbtr = self.db.TransferRequests
        query = {"uuid": request_id}
        update = {"$set": req}
        logging.debug(f"MONGO-START: db.TransferRequests.find_one_and_update(filter={query}, update={update}, projection={REMOVE_ID}, return_document={AFTER}")
        ret = await sbtr.find_one_and_update(filter=query,
                                             update=update,
                                             projection=REMOVE_ID,
                                             return_document=AFTER)
        logging.debug("MONGO-END:   db.TransferRequests.find_one_and_update(filter, update, projection, return_document")
        if not ret:
            raise tornado.web.HTTPError(404, reason="not found")
        logging.info(f"patched TransferRequest {request_id} with {req}")
        self.write({})

    @lta_auth(prefix=LTA_AUTH_PREFIX, roles=LTA_AUTH_ROLES)  # type: ignore
    async def delete(self, request_id: str) -> None:
        """Handle DELETE /TransferRequests/{uuid}."""
        query = {"uuid": request_id}
        logging.debug(f"MONGO-START: db.TransferRequests.delete_one(filter={query})")
        await self.db.TransferRequests.delete_one(filter=query)
        logging.debug("MONGO-END:   db.TransferRequests.delete_one(filter)")
        logging.info(f"deleted TransferRequest {request_id}")
        self.set_status(204)


class TransferRequestActionsPopHandler(BaseLTAHandler):
    """TransferRequestActionsPopHandler handles /TransferRequests/actions/pop."""

    ROUTE = r"/TransferRequests/actions/pop"

    @lta_auth(prefix=LTA_AUTH_PREFIX, roles=LTA_AUTH_ROLES)  # type: ignore
    async def post(self) -> None:
        """Handle POST /TransferRequests/actions/pop."""
        source = self.get_argument("source")
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
        logging.debug(f"MONGO-START: db.TransferRequests.find_one_and_update(filter={find_query}, update={update_doc}, projection={REMOVE_ID}, sort={FIRST_IN_FIRST_OUT}, return_document={AFTER})")
        tr = await sdtr.find_one_and_update(filter=find_query,
                                            update=update_doc,
                                            projection=REMOVE_ID,
                                            sort=FIRST_IN_FIRST_OUT,
                                            return_document=AFTER)
        logging.debug("MONGO-END:   db.TransferRequests.find_one_and_update(filter, update, projection, sort, return_document)")
        # return what we found to the caller
        if not tr:
            logging.info(f"Unclaimed TransferRequest with source {source} does not exist.")
        else:
            logging.info(f"TransferRequest {tr['uuid']} claimed by {claimant}")
        self.write({'transfer_request': tr})

# -----------------------------------------------------------------------------


def ensure_mongo_indexes(mongo_url: str, mongo_db: str) -> None:
    """Ensure that necessary indexes exist in MongoDB."""
    logging.info(f"Configuring MongoDB client at: {mongo_url}")
    client: MongoClient[dict[str, Any]] = MongoClient(mongo_url)
    db = client[mongo_db]
    logging.info(f"Creating indexes in MongoDB database: {mongo_db}")

    for collection_name, field, index_name, unique in MONGO_INDEXES:
        collection = getattr(db, collection_name)
        existing_indexes = collection.index_information()
        if index_name not in existing_indexes:
            logging.info(f"Creating index for {mongo_db}.{collection_name}.{field}")
            kwargs: dict[str, Union[str, bool]] = {"name": index_name}
            if unique is not None:
                kwargs["unique"] = unique
            collection.create_index(field, **kwargs)

    logging.info("Done creating indexes in MongoDB.")


def start(debug: bool = False) -> RestServer:
    """Start a LTA DB service."""
    config = from_environment(EXPECTED_CONFIG)
    # logger = logging.getLogger('lta.rest')
    for name in config:
        if name not in LOGGING_DENY_LIST:
            logging.info(f"{name} = {config[name]}")
        else:
            logging.info(f"{name} = [秘密]")

    # configure auth
    auth: dict[str, Union[str, int, float, bool]] = {}
    if config["CI_TEST"] == "TRUE":
        auth = {
            "secret": "secret",
        }
    else:
        auth = {
            "audience": config["LTA_AUTH_AUDIENCE"],
            "openid_url": config["LTA_AUTH_OPENID_URL"]
        }

    # set up rest handler
    args = RestHandlerSetup({  # type: ignore
        "auth": auth,
        "debug": debug
    })

    # configure access to MongoDB as a backing store
    mongo_user = quote_plus(cast(str, config["LTA_MONGODB_AUTH_USER"]))
    mongo_pass = quote_plus(cast(str, config["LTA_MONGODB_AUTH_PASS"]))
    mongo_host = config["LTA_MONGODB_HOST"]
    mongo_port = int(config["LTA_MONGODB_PORT"])
    mongo_db = cast(str, config["LTA_MONGODB_DATABASE_NAME"])
    lta_mongodb_url = f"mongodb://{mongo_host}:{mongo_port}/{mongo_db}"
    if mongo_user and mongo_pass:
        lta_mongodb_url = f"mongodb://{mongo_user}:{mongo_pass}@{mongo_host}:{mongo_port}/{mongo_db}"
    ensure_mongo_indexes(lta_mongodb_url, mongo_db)
    mongo_client: AsyncMongoClient[DatabaseType] = AsyncMongoClient(lta_mongodb_url)
    args['db'] = mongo_client[mongo_db]

    # configure prometheus metrics
    args['request_counter'] = Counter('lta_requests', 'LTA DB requests', ['method', 'route'])
    args['response_counter'] = Counter('lta_responses', 'LTA DB responses', ['method', 'response', 'route'])

    # See: https://github.com/WIPACrepo/rest-tools/issues/2
    max_body_size = int(config["LTA_MAX_BODY_SIZE"])
    server = RestServer(debug=debug, max_body_size=max_body_size)  # type: ignore[no-untyped-call]
    for handler in [
        MainHandler,
        BundlesHandler,
        BundlesActionsBulkCreateHandler,
        BundlesActionsBulkDeleteHandler,
        BundlesActionsBulkUpdateHandler,
        BundlesActionsPopHandler,
        BundlesSingleHandler,
        MetadataHandler,
        MetadataActionsBulkCreateHandler,
        MetadataActionsBulkDeleteHandler,
        MetadataSingleHandler,
        TransferRequestsHandler,
        TransferRequestSingleHandler,
        TransferRequestActionsPopHandler,
    ]:
        server.add_route(getattr(handler, "ROUTE"), handler, args)  # type: ignore[no-untyped-call]

    server.startup(address=config['LTA_REST_HOST'],
                   port=int(config['LTA_REST_PORT']))  # type: ignore[no-untyped-call]
    return server


async def main() -> None:
    """Configure logging and start a LTA DB service."""
    # obtain our configuration from the environment
    config = from_environment(EXPECTED_CONFIG)
    # configure logging for the application
    log_level = getattr(logging, os.getenv("LOG_LEVEL", default="DEBUG"))
    logging.basicConfig(
        format="{asctime} [{threadName}] {levelname:5} ({filename}:{lineno}) - {message}",
        level=log_level,
        stream=sys.stdout,
        style="{",
    )
    start(debug=True)
    metrics_port = int(config["PROMETHEUS_METRICS_PORT"])
    start_http_server(metrics_port)
    await asyncio.Event().wait()


if __name__ == '__main__':
    asyncio.run(main())
