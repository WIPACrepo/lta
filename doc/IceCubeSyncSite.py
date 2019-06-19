#! /bin/env python                                                                                                                                          
"""                                                                                                                                                         
Command line tool for registering an IceCube dataset into rucio                                                                                             
Based on Brian Bockelman's for CMS                                                                                                                           
"""

from __future__ import absolute_import, division, print_function

import json
import multiprocessing
import re
from argparse import ArgumentParser
from subprocess import Popen, PIPE

from gfal2 import Gfal2Context, GError
import rucio.rse.rsemanager as rsemgr
from rucio.client.didclient import DIDClient
from rucio.client.replicaclient import ReplicaClient
from rucio.common.exception import DataIdentifierAlreadyExists
from rucio.common.exception import RucioException
from rucio.common.exception import FileAlreadyExists
from rucio.common.exception import DuplicateRule
from rucio.common.exception import InvalidObject
from rucio.client import RuleClient

import gfal2

DEFAULT_SCOPE = 'exp'
DEBUG_FLAG = False
DEFAULT_LIMIT = 10
DEFAULT_ORIGIN_RSE = 'WIPAC-ORIGINAL2'

#Default dataset like IceCube/2016/filtered/level2pass2/0101/Run00127347

class RunSync(object):
    """
    Synchronize the replica of a given run at WIPAC-ORIG 
    the corresponding Rucio site.
    """

    def __init__(self, run, originrse=DEFAULT_ORIGIN_RSE, destrse=None, scope=DEFAULT_SCOPE,
                 check=True, lifetime=None, dry_run=False, container=None):
        """
           :param dataset: Name of the PhEDEx dataset to synchronize with Rucio.
           :param pnn: PhEDEx node name to filter on for replica information.
        """
        self.run = run
        self.originrse = originrse
        self.destrse = destrse
        self.scope = scope
        self.check = check
        self.lifetime = lifetime
        self.dry_run = dry_run
        self.container = container

        self.rucio_datasets = {}
        self.run_files = {}
        self.existent_replica_files = {}
        self.url = ''
        self.gfal = Gfal2Context()

        self.run_Number = None

        self.get_run_Number()
        self.files_storage = {}
        self.get_global_url()

        self.didc = DIDClient()
        self.repc = ReplicaClient()
        self.rulesClient = RuleClient()
        
        # Right now obtaining the Metadata from the storage at WIPAC                                                                                     
        # Hopefully in the future from JADE                                                                                                                      # TODO                                                                                                                                           
        self.get_run_Files()
        self.get_rucio_metadata()
        self.update_run_Files()
        self.get_files_metadata()
    

    def update_run_Files(self):
        """
        Updating the run files wiht only the files that have not been registered
        """
        for f in self.existent_replica_files:
            file_name = f.split('/')[-1:][0]
            if file_name in self.run_files:
                print("File: %s already registered. Skipping it" % file_name)
                self.run_files.pop(file_name)

    def get_files_metadata(self):
        for f in self.run_files:
             if self.run + '/' + f not in self.existent_replica_files:
                self.obtain_metadata(f)
        print("Metadat initialization done")

    def obtain_metadata(self, filename):
        """
        Get the size and checksum for every file in the run from the gftp server
        """
        url = self.get_file_url(filename)
        print("checking metadata for url %s" % url)
        try:
            size = self.gfal.stat(str(url)).st_size
            adler32 = self.gfal.checksum(str(url), 'adler32')
            print("got size and adler 32checksum of file: pfn=%s size=%s checksum=%s"% (url, size, adler32))
            self.run_files[filename] = {'size':size, 'adler32':adler32, 'name': self.run + '/' + filename}
        except GError:
            print("no file found at %s" % url)
            return False

    def get_file_url(self, filename):
        return self.url + '/' + self.run + '/' + filename

    def get_global_url(self):
        """
        Return the base path of the rucio url
        """
        print("Getting parameters for rse %s" % self.originrse)
        rse = rsemgr.get_rse_info(self.originrse)
        proto = rse['protocols'][0]

        schema = proto['scheme']
        prefix = proto['prefix'] + self.scope.replace('.', '/')
        if schema == 'srm':
            prefix = proto['extended_attributes']['web_service_path'] + prefix
        url = schema + '://' + proto['hostname']
        if proto['port'] != 0:
            url = url + ':' + str(proto['port'])
        self.url = url + prefix
        print("Determined base url %s" % self.url)

    def get_run_Number(self):
        """
        Obtain the run number out of whole run IceCube/2016/filtered/level2pass2/0101/Run00127347
        """
        print("Obtaining run number out of run(dataset): %s" % self.run)
        self.run_Number = self.run.split("/")[-1]
        print("Run number (dataset): %s" % self.run_Number)

    def get_run_Files(self):
        """
        Gets the list of files for a given run and their checksums from the storage
        """
        self.run_url = self.url + '/' + self.run 
        print("Listin files from url : %s" % self.run_url)
        run_files = []
        try:
            run_files = self.gfal.listdir(str(self.run_url))
        except GError:
            print("No files found at %s" % str(self.run_url))
        print("Files found in storage:")
        count = 0
        for f in run_files:
            if len(f) > 3:
                if count < 5000:
                    self.run_files[f] = {}
                    count = count + 1
                else:
                    break
        
    def get_rucio_metadata(self):
        """                                                                                                                                         
        Gets the list of datasets at the Rucio RSE, the files, and the metadata.                                                                           
        """
        print("Initializing Rucio... getting the list of blocks and files at %s"
              % self.originrse)
        registered_datasets = self.repc.list_datasets_per_rse(self.originrse)
        for dataset in registered_datasets:
            self.rucio_datasets[dataset] = {}
        
        replica_info = self.repc.list_replicas([{"scope": self.scope,
                                                 "name": '/'+self.run_Number}],
                                               rse_expression="rse=%s" % self.originrse)
        replica_files = set()
        for file_info in replica_info:
            name = file_info['name']
            if self.originrse in file_info['rses']:
                replica_files.add(name)
        
        self.existent_replica_files = replica_files
        print("Rucio initialization done.")

    def register(self):
        """
        Create the container, the datasets and attach them to the container.
        """
        print("Registering...")
        self.register_dataset(self.run_Number)
        self.register_replicas(self.run_files)
        self.register_container(self.container)
        self.attach_dataset_to_container(self.run_Number, self.container)
        self.add_replica_rule(dataset=self.run_Number, destRSE=self.destrse)

    def register_container(self, container):
        """
        Registering the container
        """
        print("Registering the container %s with scope: %s" % (container,self.scope))
        if container is None:
            print ('No container added, not registering any container')
            return
        if self.dry_run:
             print ('Dry run only, not registering the container')
             return
        try:
            self.didc.add_container(scope=self.scope, name=container, lifetime=self.lifetime)
        except DataIdentifierAlreadyExists:
            print("Container %s already exists" % container)
        except InvalidObject:
            print("Problem with container name: %s" % container)
    
    def attach_dataset_to_container(self, dataset, container):
        """
        Attaching the dataset to a container
        """
        print("Attaching dataset %s, to container: %s" % (dataset, container))
        if container is None:
            print ('No container added, not registering dataset in container')
            return
        if self.dry_run:
            print ('Dry run only, not attaching dataset container')
            return
        try:
            self.didc.attach_dids(scope=self.scope, name=container,
                                  dids=[{'scope': self.scope, 'name': '/'+dataset}])
        except RucioException:
                print("dataset already attached to container")    
        return
    
    def register_dataset(self, run):
        """
        Registering a dataset in the rucio database
        """
        print("registering dataset %s"% run)
        if self.dry_run:
            print(' Dry run only. Not creating dataset.')
            return
        try:
            self.didc.add_dataset(scope=self.scope, name=run, lifetime=self.lifetime)
        except DataIdentifierAlreadyExists:
            print(" Dataset %s already exists" % run)

        
    def register_replicas(self, replicas):
        """
        Register file replica.
        """
        if not replicas:
            return
        print("registering files in Rucio: %s" % ", ".join([replicas[filemd]['name'] for filemd in replicas]))
        if self.dry_run:
            print(' Dry run only. Not registering files.')
            return
        try:
            self.repc.add_replicas(rse=self.originrse, files=[{
                'scope': self.scope,
                'name': replicas[filemd]['name'],
                'adler32': replicas[filemd]['adler32'],
                'bytes': replicas[filemd]['size'],
               } for filemd in replicas])
            print("Adding files to dataset: %s" % self.run_Number)
        except InvalidObject:
            print("Problem with file name does not match pattern")
            
            
        for filemd in replicas:
            try:
                self.didc.attach_dids(scope=self.scope, name=self.run_Number, dids=[{         
                    'scope': self.scope, 
                    'name': replicas[filemd]['name']}])
            except FileAlreadyExists:
                print("File already attached")

    def add_replica_rule(self, destRSE, dataset):
        """
        Create a replication rule for one dataset "Run" at an RSE
        """
        print("Creating replica rule for dataset %s at rse: %s" % (dataset, destRSE))
        if self.dry_run:
            print(' Dry run only. Not creating rules')
            return
        if destRSE:
            try:
                self.rulesClient.add_replication_rule([{"scope":self.scope,"name":"/"+dataset}],copies=1, rse_expression=destRSE)
            except DuplicateRule:
                print('Rule already exists')
            
        

def sync_one_dataset(dataset, originrse, destrse, scope, check, dry_run, container):
    """                                                                                                                                                       
    Helper function for DatasetSync                                                                                                                           
    """
    instance = RunSync(
        run=dataset,
        originrse=originrse,
        destrse=destrse,
        scope=scope,
        check=check,
        dry_run=dry_run,
        container=container,
    )
    instance.register()


def getDatasetListFromFile(datasetFile):
    with open(datasetFile) as f:
        content = f.readlines()
    content = [x[9:].strip() for x in content] 
    return content

def main():
    """                                                                                                                                                       
    Main function.                                                                                                                                            
    """
    parser = ArgumentParser(description="Given a Dataset (Run), like 2016/filtered/level2pass2/0101/Run00127347 "
                            "register files in Rucio in case they are not there")
    parser.add_argument('--scope', dest='scope', help='scope of the dataset (default %s).'
                        % DEFAULT_SCOPE, default=DEFAULT_SCOPE)
    parser.add_argument('--originrse', dest='originrse', help='Origin RSE the default is WIPAC-ORIGINAL', default=DEFAULT_ORIGIN_RSE)
    parser.add_argument('--destrse', dest='destrse', help='Destination RSE where the files should be rpelicated')
    parser.add_argument('--nocheck', dest='check', action='store_false',
                        help='do not check size and checksum of files replicas on storage.')
    parser.add_argument('--limit', dest='limit', default=DEFAULT_LIMIT, type=int,
                        help="limit on the number of datasets to attempt sync. default %s. -1 for unlimited" % DEFAULT_LIMIT)
    parser.add_argument('--pool', dest='pool', default=5, type=int,
                        help="number of helper processes to use.")
    parser.add_argument('--dryrun', dest='dry_run', action='store_true',
                        help='do not change anything in rucio, checking only')
    parser.add_argument('--dataset', dest='dataset', action='append',
                        help='specific dataset or runs to sync')
    parser.add_argument('--container', dest='container', action='store', help='Container to attach the dataset to')
    parser.add_argument('--datasetFile', dest='datasetFile', action='store', help='File with the list of runs')
    
    

    options = parser.parse_args()
    
    pool = multiprocessing.Pool(options.pool)

    datasets = options.dataset
    datasetFile = options.datasetFile
    limit = options.limit
    count = 0
    futures = []

    if datasetFile:
        if datasets:
            datasets = datasets.extend(getDatasetListFromFile(datasetFile))
        else:
            datasets = getDatasetListFromFile(datasetFile)

    for dataset in datasets:
        count += 1
        if limit > 0 and count >= limit:
            break
        future = pool.apply_async(sync_one_dataset,(dataset, options.originrse, options.destrse,options.scope, options.check, options.dry_run, options.container))
        futures.append((dataset, future))

    pool.close()
     
    for dataset, future in futures:
        future.get()
        print("Finished processing dataset %s" % dataset)                               

if __name__ == '__main__':
    main()
