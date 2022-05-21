'''
   coordinate2.py
   Second generation of coordinate.py
   To run in a cron on lta-vm-2 as jadelta.
   Communicates w/ cluster nodes
   Keeps the appropriate number of bundler and checksum jobs running. 
   Modified from the original to 
    1) keep track of different pipes
    2) keep config info in one place for easy adjustment
    3) distinguish between CPU/I/O-intensive work and not
NOTE:  debugmode just writes out what this _would_ do; it doesn't do it.
'''
import json
import Utils as U

class coordinate():
    '''  Ping cluster; query cluster; compare with the desired;
        execute on cluster (using sudo to jadelta) '''
    def __init__(self, configname='Interface.json'):
        self.config = self.ReadConfig(configname)
        self.cluster = self.config["available"]
        self.pipes = self.config["pipes"]
        # hotlimit = self.config["hotlimit"]
        # Initialize the module counts
        self.countModule = {}
        for x in self.pipes:
            prekey = x["pipe"] + '_'
            for y in x["types"]:
                key = prekey + y["name"]
                self.countModule[key] = 0
        #
        #self.workerscripts = '/home/jadelta/dumpcontrol/DumpStream/'
        self.workerscripts = '/home/jadelta/'
        self.cmdping = '/bin/ping'
        self.cmdssh = '/usr/bin/ssh'
        # Find out which systems are actually available
        self.candidatePool = {}
        self.GetCandidates()  # fills candidatePool
    #
    def ReadConfig(self, configfilename):
        '''  Read the configuration -- which filesystems and what limits
            from the specified file '''
        #+
        # Arguments:    configuration file name
        # Returns:      json with configuration from file
        # Side Effects: reads a file
        # Relies on:    file exists, has json configuration
        #-
        try:
            with open(configfilename) as f:
                data = json.load(f)
            return data
        except:
            print('ReadConfig failed to read', configfilename)
            return None
    #
    def GetCandidates(self):
        ''' Ping workerpool machines to figure out which are live 
             Fill the candidatePool with live ones '''
        #+
        # Arguments:    None
        # Returns:      Nothing
        # Side Effects: multiple pings
        # Relies on:    Utils.getoutputerrorsimplecommand
        #-
        for host in self.cluster:
            cmd = [self.cmdping, '-c1', '-w', '1', host]
            # timeout is 1 second
            answer, _, code = U.getoutputerrorsimplecommand(cmd, 1)
            if code != 0:
                continue
            if 'Unreachable' in answer:
                continue
            self.candidatePool[host] = 0   # # of cpu-heavy jobs running
    #
    def Launch(self):
        ''' Query each of the candidate hosts to see what jadelta
            jobs they are running.  Increment the counts of modules
            Note that this does not check modules running on other
            hosts.  I may change that in the future, since I don't
            want multiple instances of the deleter--though deleter
            is a fast module.
            Return Nothing '''
        #+
        # Arguments:    None
        # Returns:      Nothing
        # Side Effects: multiple calls of script in remote hosts
        # Relies on:    Utils.getoutputerrorsimplecommand
        #-
        for host in self.candidatePool:
            cmd = [self.cmdssh, 'jadelta@' + host, self.workerscripts + 'getmex']
            answer, _, code = U.getoutputerrorsimplecommand(cmd, 1)
            if code != 0:
                continue
            #
            count = 0
            replies = answer.splitlines()
            for line in replies:
                if self.config["debuglevel"] > 1:
                    print(host, line)
                for pipe in self.pipes:
                    prekey = pipe["pipe"] + '_'
                    for mtype in pipe["types"]:
                        if mtype["key"] in line:
                            self.countModule[prekey + mtype["name"]] += 1
                            if mtype["hot"]:
                                count = count + 1   # another CPU-heavy job running
            # At this point, for this host, we have the number of high-CPU/high-I/O jobs
            self.candidatePool[host] = count
        if int(self.config["debuglevel"]) > 0:
            print('coordinate2::Launch candidatePool', self.candidatePool)
            print('coordinate2::Launch candidatePool', self.countModule)
        #
        # Now do the scan by type, see what's missing, and then locate a host to run the job
        # Then run it.  This preferentially loads the hosts at the start of the list.
        for pipe in self.pipes:
            prekey = pipe["pipe"] + '_'
            for mtype in pipe["types"]:
                if not mtype["on"]:
                    continue   # nothing to do
                if int(mtype["count"]) <= int(self.countModule[prekey + mtype["name"]]):
                    if int(self.config["debuglevel"]) > 0:
                        print('coordinate2::Launch, alldone', prekey + mtype["name"], mtype["count"], self.countModule[prekey + mtype["name"]])
                    continue   # all set already
                for host in self.candidatePool:
                    # Don't launch hot jobs into busy nodes
                    if int(mtype["count"]) <= int(self.countModule[prekey + mtype["name"]]):
                        continue
                    if int(self.candidatePool[host]) >= (int(self.config["hotlimit"]) ) and mtype["hot"]:
                        continue
                    cmd = [self.cmdssh, 'jadelta@' + host, self.workerscripts + mtype["submitter"]]
                    if int(self.config["debuglevel"]) > 0:
                        print('coordinate2::Launch', cmd, '===', self.countModule[prekey + mtype["name"]], 'vs', mtype["count"])
                    if self.config["debugmode"]:
                        print('Ready to launch', cmd)
                        code = 0
                    else:
                        answer, error, code = U.getoutputerrorsimplecommand(cmd, 1)
                    if code != 0:
                        print('coordinate2::Launch command failure: ', cmd, answer, error, code)
                        continue   # may just be one machine, keep going
                    self.countModule[prekey + mtype["name"]] += 1
    #

if __name__ == '__main__':
    launch = coordinate()
    launch.Launch()
