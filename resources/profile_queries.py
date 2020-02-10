#!/usr/bin/env python
import os
from urllib.parse import quote_plus

import pymongo
from pymongo import MongoClient

CONFIG = {
    'LTA_MONGODB_AUTH_USER': '',
    'LTA_MONGODB_AUTH_PASS': '',
    'LTA_MONGODB_DATABASE_NAME': 'lta',
    'LTA_MONGODB_HOST': 'localhost',
    'LTA_MONGODB_PORT': '27017',
}
for k in CONFIG:
    if k in os.environ:
        CONFIG[k] = os.environ[k]

mongo_user = quote_plus(CONFIG["LTA_MONGODB_AUTH_USER"])
mongo_pass = quote_plus(CONFIG["LTA_MONGODB_AUTH_PASS"])
mongo_host = CONFIG["LTA_MONGODB_HOST"]
mongo_port = int(CONFIG["LTA_MONGODB_PORT"])
lta_mongodb_url = f"mongodb://{mongo_host}"
if mongo_user and mongo_pass:
    lta_mongodb_url = f"mongodb://{mongo_user}:{mongo_pass}@{mongo_host}"
client = MongoClient(lta_mongodb_url, port=mongo_port)
db = client[CONFIG['LTA_MONGODB_DATABASE_NAME']]

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
        if 'find' in query['command'] and query['command']['filter'] == {} and query['command']['projection'] in ({}, {'_id': False}):
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