#!/usr/bin/env python3

import os
from typing import Any, cast, Dict

from pymongo import MongoClient
from pymongo.database import Database

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

# level = db.profiling_level()
# See: https://pymongo.readthedocs.io/en/stable/migrate-to-pymongo4.html#database-profiling-level-is-removed
profile: Dict[str, Any] = db.command('profile', -1)
level = profile['was']
if level != ALL:
    raise Exception('profiling disabled')
# db.set_profiling_level(pymongo.OFF)
# See: https://pymongo.readthedocs.io/en/stable/migrate-to-pymongo4.html#database-set-profiling-level-is-removed
db.command('profile', ALL, filter={'op': 'query'})

unrealistic_queries = [
    # the query to get the queries that we're profiling doesn't count!
    {'op': {'$nin': ['command', 'insert']}},
]

bad_queries = []
ret = db.system.profile.find({'op': {'$nin': ['command', 'insert']}})
for query in ret:
    try:
        # exclude unrealistic test queries
        if 'filter' in query['command'] and query['command']['filter'] in unrealistic_queries:
            continue
        if 'planSummary' not in query:
            print(query)
            continue
        if 'IXSCAN' not in query['planSummary']:
            bad_queries.append((query['command'], query['planSummary']))
    except Exception:
        print(query)
        raise

if bad_queries:
    for q, p in bad_queries:
        print(q)
        print(p)
        print('---')
    raise Exception('Non-indexed queries')

print('MongoDB profiling OK')
