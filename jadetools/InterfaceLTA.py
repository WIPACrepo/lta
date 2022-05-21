# InterfaceLTA.py
'''  Check whether the Dump has created full directories, and start
      a job to load the into first one the LTA database and Picker/Bundler
      them.  Dump works with Pole disks which have ideal names in the
      /data/exp tree.
FullDirectory instead of FullDirectories
New scripts to manage the FullDirectory updates
'''
import json
import subprocess
import socket
import os
import sys
from datetime import datetime
import requests
import Utils as U
import checkNERSCOnly as NQ

JNBDEBUG = True

class InterfaceLTA():
    ''' Find full directories signaled by the DumpControl.
        Run a job to process the first in the list '''
    #
    def __init__(self, config_name='/home/jadelta/Glue.json'):
        #+
        # Arguments:	optional name of config file
        # Returns:	Nothing
        # Side Effects:	None
        # Relies on:	Nothing
        #-
        # This just sets up defaults.  Reading the file is the first step
        #  in program execution
        self.config_file = config_name
        # set defaults
        self.config = {}
        self.config['YEAR'] = '2018'
        self.config['ROOT'] = '/mnt/lfs7/exp'
        self.config['PARTIAL'] = False
        self.config['SCAN_ONLY'] = False
        self.config['SUB_TREES'] = []
        self.config['FORCE'] = False
        self.config['FORCE_LIST'] = []
        self.config['FORBID'] = False
        self.config['FORBID_LIST'] = []
        self.config['CROOT'] = '/tmp'
        self.config['INITIAL_DIR'] = '/home/jadelta/dumpcontrol/DumpStream/jadetools'
    #
    def GetToken(self):
        ''' Get the token, if possible to let the Dump to LTA interface run : gets 0, 1, 2'''
        #+
        # Arguments:	None
        # Returns:	True if we got the Token OK
        #		False if we didn't
        # Side Effects:	Print error if there was a problem
        #		REST server status change if success
        # Relies on:	REST server working
        #		Utils.mangle
        #-
        hostname = socket.gethostname()
        if JNBDEBUG:
            print('InterfaceLTA::DEBUG: GetToken, PID=', str(os.getpid()), datetime.now().strftime("%d/%m/%Y %H:%M:%S"))
        answer = requests.post(U.targetgluetoken + U.mangle(hostname))
        gmycode = answer.text
        if gmycode == '1':
            return False
        if gmycode == '2':
            print('InterfaceLTA::GetToken failure code 2')
            return False
        return True
    #
    def ReleaseToken(self):
        ''' Release the token for running '''
        #+
        # Arguments:	None
        # Returns:	True if we released the Token OK
        #		False if we didn't
        # Side Effects:	Print error if there was a problem
        #		REST server status change if success
        # Relies on:	REST server working
        #-
        if JNBDEBUG:
            print('InterfaceLTA::ReleaseToken')
        answer = requests.post(U.targetgluetoken + U.mangle('RELEASE'))
        gmycode = answer.text
        if gmycode == '0':
            return True
        return False
    #
    def ReadyToRun(self):
        ''' Initial program configuration '''
        #+
        # Arguments:	None
        # Returns:	True if we can run
        #		False if we should not
        #		False if there's nothing to do
        # Side Effects:	Print error/warning if there are problems
        #		REST server status change
        # Relies on:	GetToken
        #		ParseParams
        #-
        # Parse parameters, if any
        #
        if not NQ.checkNERSCOnly():
            if JNBDEBUG:
                print('InterfaceLTA::ReadyToRun NERSC is near quota')
            return False
        # Test the utilities
        if not self.GetToken():
            if JNBDEBUG:
                print('InterfaceLTA::ReadyToRun busy, bailing')
            return False
        #
        return self.ParseParams()
    #
    def ParseParams(self):
        ''' Parse out what the parameters tell this job to do '''
        #+
        # Arguments:	None [REVISIT THIS.  Probably want to take arguments from command line!]
        # Returns:	boolean
        # Side Effects:	Resets GLOBAL parameters from the configuration file
        #                Prints message and exit-3 if the reading fails
        # Relies on:	Configuration file available and readable
        #-
        # These will be globals
        # If not set by the parameters, take these globals from
        #  the configuration file
        # Year = 
        # Config = location of config file
        # Subtrees	= array of subtree names
        # Force = "directory name"	(Do only this one)
        # Forbid = array of directory names (Don't do these from Subtrees)
        # Root = root for dumping files (currently /mnt/lfs7/exp)
        # Partial => write out which directories ARE NOT FULL YET
        # ScanOnly => only write out the directores that would be processed
        #
        try:
            with open(self.config_file, 'r') as json_file:
                data = json.load(json_file)
                self.config['YEAR'] = data['YEAR']
                self.config['ROOT'] = data['ROOT']
                self.config['PARTIAL'] = bool(data['PARTIAL'])
                self.config['SCAN_ONLY'] = bool(data['SCAN_ONLY'])
                self.config['SUB_TREES'] = []
                for tree in data['SUB_TREES']:
                    self.config['SUB_TREES'].append(tree['tree'])
                self.config['FORCE'] = bool(data['FORCE'])
                self.config['FORCE_LIST'] = []
                self.config['FORBID'] = bool(data['FORBID'])
                self.config['FORBID_LIST'] = []
                self.config['CROOT'] = data['CROOT']
        except:
            print('InterfaceLTA::ParseParams:  failed to read the config file', self.config_file)
            self.CloseDown()
            sys.exit(3)
        #
        # Parse arguments to override the config file values
        # But do this later.  I don't need this right now.
        return True
    #
    def GetNextUnclaimedDirectory(self):
        ''' Return the next unclaimed full directories TODO (returned) '''
        # Check FullDirectory for the next full directory to deal with
        #+
        # Arguments:	None
        # Returns:	list of arrays of live and ideal directories
        #		 for which the file count == expected
        # Side Effects:	Print errors if problem
        # Relies on:	Utils.UnpackDBReturnJson
        #		Utils.mangle
        #-
        TODO = []
        quer = {}
        quer['status'] = 'unclaimed'
        mangled = U.mangle(json.dumps(quer))
        answers = requests.get(U.curltargethost + '/directory/info/' + mangled)
        arrans = U.UnpackDBReturnJson(answers.text)
        if len(arrans) == 0:
            return TODO
        #
        # Can implement FORBID here, but I haven't been using it yet, so
        # never mind for now.
        # I could also put in some sanity check to make sure there's no
        # duplication, but I do that already in findfull.py, so why bother.
        try:
            TODO = [arrans[0]['idealName'], arrans[0]['dirkey']]
        except:
            print('InterfaceLTA::GetNextUnclaimedDirectory failed to retrieve information from', arrans)
            return []
        if JNBDEBUG:
            print("InterfaceLTA::DEBUG TODO", TODO)
        return TODO
    
    def SpawnScript(self, pair_directory):
        ''' Do the submission stuff here '''
        #+
        # Arguments:	list of arrays of ideal directory and DB dirkey to bundle
        # Returns:	boolean:  True if OK or nothing to do, False otherwise
        # Side Effects:	execute process_directory_v2.sh script
        # Relies on:	U.mangle
        #		REST server working
        #		process_directory_v2.sh script (which relies on LTA environment)
        #-
        # Anything to do?
        if len(pair_directory) <= 0:
            return True
        print('InterfaceLTA::SpawnScript About to try', pair_directory, flush=True)
        hostname = socket.gethostname().split('.')[0]
        processID = str(os.getpid())
        mangled = U.mangle(str(pair_directory[1]) + ' processing ' + hostname + ' ' + processID)
        answer = requests.post(U.curltargethost + '/directory/modify/' + mangled)
        if 'FAILURE' in answer.text:
            print('InterfaceLTA::SpawnScript: Failed to set new status for', pair_directory, answer.text)
            return False
        #
        try:
            command = [self.config['INITIAL_DIR'] + '/process_directory_v2.sh', pair_directory[0], str(pair_directory[1])]
            output, error, code = U.getoutputerrorsimplecommand(command, 172800) # 2 days
        except subprocess.TimeoutExpired:
            print('InterfaceLTA::SpawnScript: Timeout on process_directory_v2.sh on', pair_directory)
            return False
        except subprocess.CalledProcessError as e:
            print('InterfaceLTA::SpawnScript: Failure with process_directory_v2.sh on', pair_directory, 'with', e.stderr, e.output, 'and', output, error, code)
            return False
        except:
            print('InterfaceLTA::SpawnScript: Failed to execute process_directory.sh on', pair_directory, error, code)
            print('InterfaceLTA::SpawnScript:', command)
            return False
        if code != 0:
            print('InterfaceLTA::SpawnScript: Problem with process_directory_v2.sh', output, error, code)
        else:
            if 'Error:' in str(output) or 'Error:' in str(error):
                print('InterfaceLTA::SpawnScript:', pair_directory, output, error, code)
        #
        return True
    #
    def CloseDown(self):
        ''' Release the token, do anything else needed '''
        #+
        # Arguments:	None
        # Returns:	Nothing
        # Side Effects:	Print error/warning if there are problems
        # Relies on:	ReleaseToken
        #-
        if not self.ReleaseToken():
            print('InterfaceLTA::CloseDown failed to realease the token')
            sys.exit(1)
    #
    def RunOnce(self):
        ''' Runs the job '''
        #+
        # Arguments:	None
	# Returns:	Nothing
        # Side Effects:	checksums a directory's worth of files
        #		tells LTA about them
        # Relies on:	ReadyToRun
        #		GetNextUnclaimedDirectory
        #		SpawnScript
        #		CloseDown
        #-
        if not self.ReadyToRun():
            if not self.CloseDown():
                print('InterfaceLTA::RunOnce failed to end politely')
                sys.exit(1)
        my_todo = self.GetNextUnclaimedDirectory()
        # Anything we think reasonable goes here
        if not self.SpawnScript(my_todo):
            print('InterfaceLTA::RunOnce received a problem from SpawnScript')
        self.CloseDown()

if __name__ == '__main__':
    interf = InterfaceLTA()
    interf.RunOnce()
