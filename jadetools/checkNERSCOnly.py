''' Check the NERSC quota.  If we're running into trouble, checkQuotaNERSC returns false; if OK true
Pared down from version that checked local disk limits also '''
import os
import sys
import json
from datetime import datetime
from time import mktime, strptime
import requests

#####
#
class BearerAuth(requests.auth.AuthBase):
    ''' Translate the LTA REST server token into something useful.
        This relies on the "requests" package
        This initialzes with a string token, and on call returns
        a Bearer token for the requests call to use '''
    def __init__(self, stoken):
        self.token = stoken
    def __call__(self, r):
        r.headers["authorization"] = "Bearer " + self.token
        return r

#####
#
#####
#
def normalizeAnswer(quotaString):
    ''' Given a string with the info from NERSC quota reply,
        return the value in GiB, no matter the original units
        If there is a problem, the answer is 0 '''
    if len(quotaString) <= 0:
        return 0
    keychar = ['Ki', 'Mi', 'Gi', 'Ti', 'Pi']
    scale = 1/(1024*1024*1024)
    for keyv in keychar:
        scale = scale * 1024
        if keyv in quotaString:
            words = quotaString.split(keyv)
            try:
                iv = float(words[0])
                return scale*iv
            except:
                return 0   # Assume no problems
    return 0  # Assume no problems

#####
#
class checkNERSCOnly():
    ''' Encapsulate the NERSC quota check code '''
    def __init__(self, name='service-token', configname='Dump.json'):
        self.tokenfilename = name
        self.configfilename = configname
        self.getLTAToken(name)
        self.config = self.ReadConfig()
        if os.path.isfile('/bin/du'):
            self.execdu = '/bin/du'
        else:
            self.execdu = '/usr/bin/du'
        if os.path.isfile('/bin/df'):
            self.execdf = '/bin/df'
        else:
            self.execdf = '/usr/bin/df'
    #
    def __call__(self):
        return self.checkQuotaNERSC()
    #
    def getLTAToken(self, name):
        ''' Read the LTA REST server token from file "name"; return the same '''
        try:
            tf = open(name, 'r')
            self.token = tf.readline()
            tf.close()
        except:
            print('getLTAToken failed to read from', name)
            sys.exit(1)
    #
    def checkQuotaNERSC(self):
        ''' get the cscratch1 use from NERSC--are we too full? '''
        lasttime, used, avail = self.getQuotaNERSC()
        if used < 0 or avail < 0:
            return False
        if used > self.config['FRACTION_NERSC_QUOTA'] * avail:
            return False
        rightnow = datetime.utcnow()
        st = strptime(lasttime, "%Y-%m-%dT%H:%M:%S.%f")
        rightthen = datetime.fromtimestamp(mktime(st))
        diff = rightnow - rightthen
        seconds = diff.days*86400 + diff.seconds
        if seconds > 86400:
            return False
        #
        return True
    #
    def getQuotaNERSC(self):
        ''' Retrieve the NERSC cscratch1 space usage and quota and most recent time '''
        r3 = requests.get('https://lta.icecube.aq/status/site_move_verifier', auth=BearerAuth(self.token))
        if r3.status_code != 200:
            print('getQuotaNERSC: site_move_verifier check', r3.status_code)
            return '', -1, -1
        details = r3.json()
        latestTime = ''
        latestQuota = ''
        latestUsed = ''
        for blob in details:
            det = details[blob]
            quoti = det['quota']
            ts = det['timestamp']
            for htype in quoti:
                if htype['FILESYSTEM'] == 'cscratch1':
                    if ts > latestTime:
                        latestTime = ts
                        latestUsed = htype['SPACE_USED']
                        latestQuota = htype['SPACE_QUOTA']
        return latestTime, normalizeAnswer(latestUsed), normalizeAnswer(latestQuota)
    
    def ReadConfig(self):
        '''  Read the configuration -- which filesystems and what limits
            from the specified file '''
        try:
            with open(self.configfilename) as f:
                data = json.load(f)
            return data
        except:
            print('ReadConfig failed to read', self.configfilename)
            return None
