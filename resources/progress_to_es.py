"""
Report progress of NERSC archving to ES.

If using --from-file, you want to make a DB dump first:

  mongoexport --db=file_catalog --collection=files -u file_catalog -p XXXX
    -q '{"locations.site": "NERSC", "locations.path": {"$regex": "^/home/projects/icecube/"}, "locations.archive": true}'
    -f 'create_date,meta_modify_date,file_size,locations,logical_name,uuid'
    | gzip > nersc.json.gz

Or for bundles only:

  mongoexport --db=file_catalog --collection=files -u file_catalog -p XXXX
    -q '{"locations.site": "NERSC", "locations.path": {"$regex": "^/home/projects/icecube/"}, "locations.hpss": true}'
    -f 'create_date,meta_modify_date,file_size,locations,logical_name,uuid,lta'
    | gzip > nersc-bundles.json.gz

"""

from argparse import ArgumentParser
import asyncio
import json
import gzip
import logging
from pprint import pprint

from elasticsearch import AsyncElasticsearch
from lta.lta_tools import from_environment
from rest_tools.client import ClientCredentialsAuth


def es_date_formatter(val):
    return val.replace(' ', 'T').split('.')[0]


class Collect:
    def __init__(self, bundle_mode=False, start_date=None, end_date=None, rest_client=None, index_name='long-term-archive', es_client=None, dryrun=False):
        self.bundle_mode = bundle_mode
        self.start_date = start_date
        self.end_date = end_date
        self.rest_client = rest_client
        self.index_name = index_name
        self.es = es_client
        self.dryrun = dryrun

    async def create_es_entry(self, catalog_file, **doc):
        name = catalog_file['logical_name']
        name_parts = name.lstrip('/').split('/')
        if name.startswith('/data/sim'):
            type = '/'.join(name_parts[4:6])
        elif name.startswith('/data/exp'):
            type = '/'.join(name_parts[4:6])
        else:
            type = 'unknown'
        create_date = catalog_file['meta_modify_date']
        if 'create_date' in catalog_file:
            create_date = catalog_file['create_date']
        elif name.startswith('/data'):
            try:
                create_date = f'{name_parts[3]}-01-01'
                if 'internal-system/pDAQ' in name:
                    create_date = f'{name_parts[3]}-{name_parts[6][0:2]}-{name_parts[6][2:4]}'
                    if name_parts[-1].startswith('ukey_'):
                        parts = name_parts[-1].split('_')
                        create_date = f'{parts[3][0:4]}-{parts[3][4:6]}-{parts[3][6:8]}T{parts[4][0:2]}:{parts[4][2:4]}:{parts[4][4:6]}'
            except Exception:
                pass
        elif self.bundle_mode and catalog_file.get('lta', {}).get('date_archived', None):
            create_date = catalog_file['lta']['date_archived']

        doc.update({
            '@timestamp': es_date_formatter(catalog_file['meta_modify_date']),
            '@date': es_date_formatter(create_date),
            'sim': name.startswith('/data/sim'),
            'exp': name.startswith('/data/exp'),
            'type': type,
            'season': name_parts[3] if len(name_parts) > 3 else 'unknown',
            'size': int(catalog_file['file_size']['$numberLong'] if isinstance(catalog_file['file_size'], dict) else catalog_file['file_size']),
        })
        if self.dryrun:
            pprint(doc)
        else:
            try:
                await self.es.index(index=self.index_name, id=catalog_file['uuid'], document=doc)
            except Exception:
                pprint(doc)
                pprint(catalog_file)
                raise

    def exclude_file(self, catalog_file):
        if self.bundle_mode:
            return not any(loc.get('hpss', False) and loc.get('site', None) == 'NERSC' for loc in catalog_file['locations'])
        else:
            return not any(loc.get('archive', False) and loc.get('site', None) == 'NERSC' for loc in catalog_file['locations'])

    async def run(self):
        # we want files at NERSC that are not contained within archives
        query_dict = {
            "locations.site": {
                "$eq": "NERSC"
            },
            "locations.path": {
                "$regex": "^/home/projects/icecube/"
            },
        }
        if self.bundle_mode:
            query_dict['locations.hpss'] = True
        else:
            query_dict['locations.archive'] = True
        if self.start_date and self.end_date:
            query_dict['meta_modify_date'] = {'$gte': self.start_date, '$lte': self.end_date}
        elif self.start_date:
            query_dict['meta_modify_date'] = {'$gte': self.start_date}
        elif self.end_date:
            query_dict['meta_modify_date'] = {'$lte': self.end_date}
        query_json = json.dumps(query_dict)
        keys = "create_date|meta_modify_date|file_size|locations|logical_name|uuid"
        start = 0
        limit = 500
        finished = False

        # until we're done querying the File Catalog
        while not finished:
            # ask it for another {limit} file records to check
            fc_response = await self.rest_client.request('GET', f'/api/files?query={query_json}&keys={keys}&start={start}&limit={limit}')
            # for each record we got back
            for catalog_file in fc_response["files"]:
                if self.exclude_file(catalog_file):
                    logging.debug('skipping file %s', catalog_file['logical_name'])
                    continue
                logging.info('indexing file %s', catalog_file['logical_name'])
                await self.create_es_entry(catalog_file)

            # if we got {limit} file records to check
            if len(fc_response["files"]) == limit:
                # then update our indexes to check the next bunch
                start = start + limit
            else:
                # otherwise, this was the last bunch, we're done
                finished = True

    async def from_file(self, filename):
        tasks = set()
        with gzip.open(filename, 'r') as f:
            for line in f:
                catalog_file = json.loads(line)

                if self.exclude_file(catalog_file):
                    logging.debug('skipping file: %s', catalog_file['logical_name'])
                    continue

                if self.start_date and catalog_file['meta_modify_date'] < self.start_date:
                    logging.debug('skipping file due to start date: %s', catalog_file['logical_name'])
                    continue

                if self.end_date and catalog_file['meta_modify_date'] > self.end_date:
                    logging.debug('skipping file due to end date: %s', catalog_file['logical_name'])
                    continue

                logging.info('indexing file %s', catalog_file['logical_name'])
                tasks.add(asyncio.create_task(self.create_es_entry(catalog_file)))

                while len(tasks) > 100:
                    done, tasks = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
                    for t in done:
                        await t

        while tasks:
            done, tasks = await asyncio.wait(tasks)
            for t in done:
                await t

def main():
    config = from_environment({
        'CLIENT_ID': 'long-term-archive',
        'CLIENT_SECRET': '',
        'OPENID_URL': 'https://keycloak.icecube.wisc.edu/auth/realms/IceCube',
        'FILE_CATALOG_URL': 'https://file-catalog.icecube.wisc.edu',
        'ES_ADDRESS': 'http://elk-1.icecube.wisc.edu:9200',
        'ES_INDEX': 'long-term-archive',
        'ES_TIMEOUT': 60.,
        'START_DATE': '',
        'END_DATE': '',
    })

    parser = ArgumentParser()
    parser.add_argument('-b', '--bundles', default=False, action='store_true',
                        help='lookup bundles instead of files')
    parser.add_argument('-s', '--start-date', default=config['START_DATE'],
                        help='filter to only entries after date')
    parser.add_argument('-e', '--end-date', default=config['END_DATE'],
                        help='filter to only entries before date')
    parser.add_argument('-f', '--from-file', help="don't query FC, but read from .json.gz")
    parser.add_argument('-n', '--dry-run', default=False, action='store_true',
                        help='do not ingest into ES, just print')
    parser.add_argument('--log-level', default='info', choices=['debug', 'info', 'warning', 'error'])
    args = parser.parse_args()
    if not args:
        parser.error('no condor history files or collectors')

    logging.basicConfig(level=getattr(logging, args.log_level.upper()), format='%(asctime)s %(levelname)s %(name)s : %(message)s')

    if args.from_file:
        rest_client = None
    else:
        if not config['CLIENT_SECRET']:
            raise ArgumentError('env variable CLIENT_SECRET is required')
        rest_client = ClientCredentialsAuth(
            address=config['FILE_CATALOG_URL'],
            token_url=config['OPENID_URL'],
            client_id=config['CLIENT_ID'],
            client_secret=config['CLIENT_SECRET'],
        )

    es_client = AsyncElasticsearch(
        config['ES_ADDRESS'],
        timeout=config['ES_TIMEOUT'],
        request_timeout=config['ES_TIMEOUT'],
        retry_on_timeout=True,
        max_retries=2,
    )

    c = Collect(
        bundle_mode=args.bundles,
        start_date=args.start_date,
        end_date=args.end_date,
        rest_client=rest_client,
        index_name=config['ES_INDEX'],
        es_client=es_client,
        dryrun=args.dry_run
    )
    if args.from_file:
        asyncio.run(c.from_file(args.from_file))
    else:
        asyncio.run(c.run())
    asyncio.run(es_client.close())


if __name__ == '__main__':
    main()
