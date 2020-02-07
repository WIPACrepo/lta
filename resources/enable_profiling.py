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
db.set_profiling_level(pymongo.ALL)
print('MongoDB profiling enabled')