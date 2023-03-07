#!/usr/bin/env python3

import os
from typing import Any, cast, Dict

from pymongo import MongoClient  # type: ignore[import]
from pymongo.database import Database  # type: ignore[import]

# PyMongo profiling level constants from PyMongo 3 (removed in PyMongo 4)
# See: https://api.mongodb.com/python/3.0.3/api/pymongo/database.html#pymongo.ALL
# See: https://www.mongodb.com/docs/manual/reference/command/profile/#mongodb-dbcommand-dbcmd.profile
OFF = 0
SLOW_ONLY = 1
ALL = 2

FCDoc = Dict[str, Any]

env = {
    'LTA_MONGODB_HOST': 'localhost',
    'LTA_MONGODB_PORT': '27017',
}
for k in env:
    if k in os.environ:
        env[k] = os.environ[k]

test_database_host = str(env['LTA_MONGODB_HOST'])
test_database_port = int(str(env['LTA_MONGODB_PORT']))
db: Database[FCDoc] = cast(Database[FCDoc], MongoClient(host=test_database_host, port=test_database_port).lta)

# db.set_profiling_level(pymongo.OFF)
# See: https://pymongo.readthedocs.io/en/stable/migrate-to-pymongo4.html#database-set-profiling-level-is-removed
db.command('profile', ALL, filter={'op': 'query'})
print('MongoDB profiling enabled')
