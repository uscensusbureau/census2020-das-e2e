"""notify.py: notify using SNS or some other approach"""

import sys
import os
import copy
from subprocess import run,Popen,PIPE

def notify(msg):
    if 'DAS_SNSTOPIC' in os.environ:
        old_env = copy.deepcopy(os.environ)
        if 'BCC_PROXY' in os.environ:
            os.environ['HTTP_PROXY'] = os.environ['HTTPS_PROXY'] = os.environ['BCC_PROXY']
            ret = Popen(['aws','sns', 'publish','--topic-arn',os.environ['DAS_SNSTOPIC'],'--message',msg],stdout=PIPE).communicate()[0]

        for var in ['HTTP_PROXY','HTTPS_PROXY']:
            if var not in old_env:
                del os.environ[var]
            else:
                os.environ[var] = old_env[var]

if __name__=="__main__":
    msg = " ".join(sys.argv[1:])
    notify(msg)
