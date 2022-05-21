# AutoFiles.py
'''
        Fetch the directories handed off to LTA ("LTArequest") : FullDirectory
         Find the real directory name and return the list [ [idealName, realName, dirkey], ]
        Pull the whole list of transfer requests
        Loop over the directories handed off, and locate the transfer request(s)
         for each one.  If none, don't bother adding this one to the new list yet
        Loop over the new list of directory information 
          Loop over the list of transfer request UUIDs and fetch the bundle UUIDs for 
           them.  If all of them are done or otherwise properly accounted for, add the
           directory information to a third list
        Loop over the third list (real directory name, dirkey) and
           delete the files in that directory
           reset the FullDirectory row status (hence the dirkey)
'''
import os
import sys
import glob
import json
import socket
import requests
import Utils as U

JNB_DEBUG = False
JNB_DEBUG_REMOVE = False
#######################################################
#
class BearerAuth(requests.auth.AuthBase):
    ''' Utility class for using the token with requests package '''
    def __init__(self, token):
        self.token = token
    def __call__(self, r):
        r.headers["authorization"] = "Bearer " + self.token
        return r


########################################################
#
class AutoFiles():
    ''' Class to handle dumped file deletion '''
    def __init__(self, name='service-token', configname='Dump.json'):
        ''' __init__ for AutoFiles; load LTA token '''
        #+
        # Arguments:	optional name of service token file; default service-token
        #		optional name of configuration file; default Dump.json
        # Returns:	Nothing
        # Side Effects: Subroutine reads a file
        # Relies on:	getLTAToken
        #		BearerAuth
        #		Utils.GiveTarget
        #		ReadConfig
        #-
        token = self.getLTAToken(name)
        self.bearer = BearerAuth(token)
        self.config = self.ReadConfig(configname)
        if os.path.isfile('/bin/ls'):
            self.execls = '/bin/ls'
        else:
            self.execls = '/usr/bin/ls'
        self.apriori = ['PFRaw', 'PFDST', 'pDAQ-2ndBld']
        self.dumptargetdir = U.GiveTarget()
        self.dirsplit = '/exp/'
    #
    def getLTAToken(self, tokenfilename):
        ''' Read the LTA REST server token from file "tokenfilename"; set it for the class '''
        #+
        # Arguments:	token file name string
        # Returns:	token
        # Side Effects:	reads a file, if possible
        # Relies on:	file with token
        #-
        try:
            tf = open(tokenfilename, 'r')
            token = tf.readline()
            tf.close()
            return token
        except:
            print('getLTAToken failed to read from', tokenfilename)
            self.ReleaseToken()
            sys.exit(1)
    #
    def ReadConfig(self, configfilename):
        '''  Read the configuration -- which filesystems and what limits
            from the specified file '''
        #+
        # Arguments:	configuration file name
        # Returns:	json with configuration from file
        # Side Effects:	reads a file
        # Relies on:	file exists, has json configuration
        #-
        try:
            with open(configfilename) as f:
                data = json.load(f)
            return data
        except:
            print('ReadConfig failed to read', configfilename)
            return None
    #
    def GetToken(self):
        ''' Tell the REST server that we have begun, fail if another
            instance is still busy '''
        #+
        # Arguments:	None
        # Returns:	boolean for success/failure
        # Side Effects:	query REST server
        # Relies on:	Utils.mangle
        #		REST server is up
        #-
        answers = requests.post(U.targetgluedeleter + U.mangle(socket.gethostname().split('.')[0]))
        if answers.text != '0':
            return False
        return True
    #
    def ReleaseToken(self):
        ''' Tell the REST server that we are done, fail if it cannot release '''
        #+
        # Arguments:	None
        # Returns:	boolean for success/failure
        # Side Effects:	query REST server
        # Relies on:	REST server is up
        #-
        answers = requests.post(U.targetgluedeleter + 'RELEASE')
        if answers.text != '0':
            return False
        return True
    #
    def CompRight(self, string1, string2):
        ''' Compare 2 directory names.  Split on /exp/ and compare
            the right-hand halves '''
        #+
        # Arguments:           strings for directory names
        # Returns:             boolean
        # Side Effects:        None
        # Relies on:           Nothing
        #-
        words1 = string1.split('/exp/')
        words2 = string2.split('/exp/')
        if len(words1) != 2 or len(words2) != 2:
            return False
        return words1[1] == words2[1]
    #
    def VetNotOfficialTree(self, logicalDirectoryName):
        ''' Check that this directory is
             1) Really a directory
             2) Has a real path that is part of the dump target area,
               not the true data warehouse.  It should be linked to
               from the warehouse
            returns True if OK; False if something not OK '''
        #+
        # Arguments:	directory path name
        # NOT USED
        # Returns:	boolean
        # Side Effects:	os filesystem metadata retrieved
        # Relies on:	fileystem is up
        #-
        if not os.path.isdir(logicalDirectoryName):
            return False
        if self.dumptargetdir not in os.path.realpath(logicalDirectoryName):
            return False
        if '/ceph/' in os.path.realpath(logicalDirectoryName):
            return False
        return True
    #
    #
    def compareDirectoryToArchive(self, directory):
        ''' Does the listing of the directory include files not found
            in the locally (WIPAC) archived or remotely (NERSC) archived
            list?  If so, return a non-zero code
            If everything is in the archive lists, return 0 '''
        #+
        # Arguments:	directory name to investigate; ideal name
        #		NOTE:  must be linked to from the warehouse
        #		The real name is not what's wanted
        # Returns:	Integer code 0=OK, others represent different problems
        # Side Effects:	filesystem listing
        #		FileCatalog queries
        # Relies on:	filesystem up and directory accessible
        #		FileCatalog up and its REST server
        #		directory name is of the form /data/exp/ETC
        #-
        cmd = [self.execls, directory]
        answer, error, code = U.getoutputerrorsimplecommand(cmd, 2)
        if code != 0 or len(error) > 2:
            print('compareDirectoryToArchive failed to do a ls on', directory)
            return 1
        foundFiles = answer.splitlines()
        #
        # Now fetch info from the FileCatalog--the files need to match
        # If I'm missing files in the warehouse, that may not matter,
        # what's a problem is when there are files that aren't registered
        # here or at NERSC
        dwords = directory.split(self.dirsplit)
        if len(dwords) != 2:
            print('compareDirectoryToArchive could not parse the exp in the directory name', directory)
            return 2
        #
        directoryFrag = '^/data/exp/' + dwords[1]
        #
        #query_dictn = {"locations.archive": {"$eq": True,}, "locations.site": {"$eq": "NERSC"}, "logical_name": {"$regex": directoryFrag}}
        query_dictn = {"locations.site": {"$eq": "NERSC"}, "logical_name": {"$regex": directoryFrag}}
        query_jsonn = json.dumps(query_dictn)
        overalln = self.config['FILE_CATALOG_REST_URL'] + f'/api/files?query={query_jsonn}'
        rn = requests.get(overalln, auth=self.bearer)
        # Try to unpack the info.
        try:
            fileNERSC = rn.json()['files']
        except:
            print('compareDirectoryToArchive failed to unpack NERSC-based files')
            return 4
        if len(fileNERSC) <= 0:
            return 5
        #
        ner = []
        for z in fileNERSC:
            ner.append(z['logical_name'])
        # logical_name
        for ff in foundFiles:
            foundIt = False
            for arch in ner:
                if ff in arch:
                    foundIt = True
                    break
            if not foundIt:
                return 7
        return 0
    #
    #
    def GetFullDirsDone(self):
        ''' Get a list of sets of [idealdirectory, realdirectory, dirkey] names where the
            status is LTArequest:  handed off to LTA system '''
        #+
        # Arguments:	None
        # Returns:	List of [idealName, realName, dirkey] sets for LTArequest'ed
        # Side Effects:	Print if failure (keeps going w/o failed one)
        # Relies on:	MatchIdealToRealDir
        #		Utils.mangle
        #		Utils.UnpackDBReturnJson
        #		My REST server working (FullDirectory)
        #		Working with /data/exp dumps!
        #-
        # Request everything handed off, and parse it for the handed-off
        quer = {}
        quer['status'] = 'LTArequest'
        mangled = U.mangle(json.dumps(quer))
        bulkReceive = requests.get(U.curltargethost + 'directory/info/' + mangled)
        bulkList = U.UnpackDBReturnJson(bulkReceive.text)
        breturn = []
        if len(bulkList) <= 0:
            print('jnb bulklist=0')
            return breturn
        #
        for chunk in bulkList:
            dname = chunk['idealName']
            disposable = False
            for datatype in self.apriori:
                if datatype in dname:
                    disposable = True
                    break
            if not disposable:
                continue	# Only delete specified types of files!
            dirkey = chunk['dirkey']
            match = self.MatchIdealToRealDir(dname)
            if match == '':
                continue	# Don't monkey with the real /data/exp!!!
            breturn.append([dname, match, dirkey])
        return breturn
    #
    ####
    #
    def MatchIdealToRealDir(self, idealDir):
        ''' Find the matching realDir for the idealDir, or blank
            if this is in the real /data/exp and not a link to
            a safe dumping area '''
        #+
        # Arguments:	ideal directory name
        # Returns:		real directory name, if unique
        #			FAILED NONE if no match
        #			FAILED DUPLICATE if multiple matches
        # Side Effects:	Print if failure
        # Relies on:	My REST server working
        #-
        # Get a list of dump targets
        # Get the real path for the directory given
        # If one of the list of dump targets is in the real path
        #  we have a safe area for deletion, return the real path
        # If not, this isn't safe, print an error and return ''
        # Note that the /ceph/ directories, not being in the list of dump
        # targets, should not be deleted.
        dumpTargets = U.GiveTarget()
        try:
            realPath = os.path.realpath(idealDir)
        except:
            print('MatchIdealToRealDir failed to find a real path for', idealDir)
            return ''
        if '/ceph/' in realPath:
            return ''   # readonly
        for targ in dumpTargets:
            if targ in realPath:
                return realPath
        print('MatchIdealToRealDir WARNING:  ', idealDir, 'does not appear to be in a dump target area')
        return ''
    #
    #
    def GetAllTransfer(self, listOfDirectories):
        ''' Get the full list of transfers from LTA that match the given list of directories
            Comparison is by whatever is to the right of /exp/ '''
        #+
        # Arguments:	list of setss of [ideal,real,dirkey] directories to review
        # Returns:	array of [realDir, [array of transfer UUID], dirkey] sets
        #               There may be multiple transfer requests for a single directory
        #		Only when all are done or exported do we delete
        # Side Effects:	reads LTA server
        # Relies on:	CompRight
        #		LTA REST server working
        #-
        if len(listOfDirectories) <= 0:
            return ''
        #
        allTransferRequests = requests.get('https://lta.icecube.aq/TransferRequests', auth=self.bearer)
        returnList = []
        for direct in listOfDirectories:
            uuidset = []
            for entry in allTransferRequests.json()['results']:
                if self.CompRight(direct[0], entry['path']):
                    uuidset.append(entry['uuid'])
            if len(uuidset) <= 0:
                continue		# nothing matches, nothing to do
            returnList.append([direct[1], uuidset, direct[2]])
        return returnList
    #
    ####
    #
    def AreTransfersComplete(self, infoRow):
        ''' Go through the transfer requests and get the bundle info
            for each.  Find the bundle status for each.  If all are
            external or finished, return true; else false '''
        #+
        # Arguments:	array of [real-path, [array of uuid of transfer requests]]
        # Returns:	Boolean
        # Side Effects:	multiple accesses to LTA REST server
        #		Print if error
        # Relies on:	LTA REST server working
        #-
        #
        trUUID = infoRow[1]
        if len(trUUID) <= 0:
            print('AreTransfersComplete: SHOULD NOT HAPPEN')
            return False	# Something is wrong, don't break anything
        # Which bundle states are OK to allow deletion of raw files
        #acceptable = ['external', 'finished', 'deleted', 'source-deleted', 'detached', 'completed', 'deprecated']
        # Note:  deprecated is a status that must be assigned manually.
        # The obvious danger case of a pair of duplicate bundles in which 1 is done and 3 are deprecated 
        # is something the operator can simply avoid.
        acceptable = ['external', 'finished', 'deleted', 'deprecated']
        bundleuuid = []
        for tuuid in trUUID:
            try:
                transferRequestData = requests.get('https://lta.icecube.aq/TransferRequests/' + tuuid, auth=self.bearer)
                bundleRequest = requests.get('https://lta.icecube.aq/Bundles?request=' + tuuid, auth=self.bearer)
            except:
                print('AreTransfersComplete died when lta.icecube.aq failed to reply', tuuid, len(trUUID))
                self.ReleaseToken()
                sys.exit(1)
            trbundle = bundleRequest.json()['results']
            if len(trbundle) <= 0:
                print('AreTransfersComplete: TransferRequest has no bundles', transferRequestData.text)
                continue
            for uu in trbundle:
                bundleuuid.append(uu)
            #
        for uu in bundleuuid:
            bundleStatus = requests.get('https://lta.icecube.aq/Bundles/' + uu, auth=self.bearer)
            stat = bundleStatus.json()['status']
            if JNB_DEBUG:
                print('dEBUG AreTransfersComplete:', stat, bundleStatus.json()['uuid'], infoRow[0])
            if stat not in acceptable:
                return False
        return True
    #
    def DeleteDir(self, realDir):
        ''' Delete the contents of the specified directory '''
        #+
        # Arguments:	real directory with contents to be deleted
        #			assumed to have no subdirectories
        # Returns:		True if no problems, False if problems
        # Side Effects:	contents of the directory deleted
        #			Print if error
        # Relies on:	Nothing
        #-
        try:
            list_o_files = glob.glob(realDir + '/*')
        except:
            print('DeleteDir failed to glob', realDir)
            return False
        try:
            for donefile in list_o_files:
                if JNB_DEBUG_REMOVE:
                    print('dEBUG DeleteDir pretending to remove', donefile)
                else:
                    os.remove(donefile)
            return True
        except:
            print('DeleteDir: failed to delete some files from', realDir)
        return False
    #
    def FindAndDelete(self):
        ''' Main routine to drive the deletion of raw files '''
        #+
        # Arguments:	None
        # Returns:	Nothing
        # Side Effects:	Deletes files if appropriate
        #		Lots of LTA REST server accesses
        #		Access my REST server
        #		Print errors
        # Relies on:	GetToken
        #		ReleaseToken
        #		GetFullDirsDone
        #		GetAllTransfer
        #		AreTransfersComplete
        #		compareDirectoryToArchive
        #		DeleteDir
        #		ResetStatus
        #-
        if not self.GetToken():
            return
        #
        directoryTriples = self.GetFullDirsDone()
        print('jnb directoryTriples#=', len(directoryTriples))
        if len(directoryTriples) <= 0:
            self.ReleaseToken()
            return
        transferRows = self.GetAllTransfer(directoryTriples)
        print('jnb transferRows#=', len(transferRows))
        if len(transferRows) <= 0:
            self.ReleaseToken()
            return
        for transfer in transferRows:
            if self.AreTransfersComplete(transfer):
                answerkey = self.compareDirectoryToArchive(transfer[0])
                if answerkey != 0:
                    if JNB_DEBUG:
                        print('FindAndDelete: not in Archive', transfer[0], answerkey)
                    continue
                ok = self.DeleteDir(transfer[0])
                if not ok:
                    print('FindAndDelete failed in deleting', transfer[0])
                    self.ReleaseToken()
                    return	# Do not try to continue
                ok = self.ResetStatus(transfer[0], transfer[2])
                if not ok:
                    print('FindAndDelete failed to reset the FullDirectory entry status', transfer[0])
                    self.ReleaseToken()
                    return	# Do not try to continue
            else:
                print('FindAndDelete: not all the Transfers are complete in', transfer)
            if JNB_DEBUG:
                print('dEBUG FindAndDelete:  stop here for test A', transfer)
        self.ReleaseToken()

    ##
    def ResetStatus(self, idealDirectory, dirkey):
        ''' Set toLTA=3 in FullDirectories for directory idealDirectory '''
        #+
        # Arguments:	directory name (string)
        #		dirkey = integer DB row key
        # Returns:	boolean for success/failure
        # Side Effects:	updates my REST server
        # Relies on:	Utils.mangle
        #		my REST server working
        #-
        if JNB_DEBUG_REMOVE:
            print('ResetStatus', idealDirectory, dirkey)
            return True
        quer = str(dirkey) + ' filesdeleted'
        mangled = U.mangle(quer)
        rw = requests.post(U.curltargethost + 'directory/modify/' + mangled)
        if 'FAILURE' in rw.text:
            print('ResetStatus', idealDirectory, dirkey, rw.text)
            return False
        return True

if __name__ == '__main__':
    app = AutoFiles()
    app.FindAndDelete()
