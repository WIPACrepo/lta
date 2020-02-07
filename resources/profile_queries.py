#!/usr/bin/env python
import os

import pymongo
from pymongo import MongoClient

env = {
    'LTA_MONGODB_URL': 'mongodb://localhost:27017',
    'LTA_MONGODB_NAME': 'lta',
}
for k in env:
    if k in os.environ:
        env[k] = os.environ[k]

client = MongoClient(env['LTA_MONGODB_URL'])
db = client[env['LTA_MONGODB_NAME']]
ret = db.profiling_level()
if ret != pymongo.ALL:
    raise Exception('profiling disabled')
db.set_profiling_level(pymongo.OFF)

bad_queries = []
ret = db.system.profile.find({ 'op': { '$nin' : ['command', 'insert'] } })
for query in ret:
    try:
        if 'find' in query['command'] and query['command']['find'] == 'collections':
            continue
        if 'planSummary' not in query:
            print(query)
            continue
        if 'IXSCAN' not in query['planSummary']:
            bad_queries.append((query['command'],query['planSummary']))
    except Exception:
        print(query)
        raise

if bad_queries:
    for q,p in bad_queries:
        print(q)
        print(p)
        print('---')
    raise Exception('Non-indexed queries')

print('MongoDB profiling OK')