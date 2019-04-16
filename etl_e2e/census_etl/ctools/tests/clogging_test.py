#!/usr/bin/env python3
#
# clogging_test.py:
#
# various test functions for the logging


import sys
import py.test
import os
import os.path
import logging
import time
import platform
import warnings

sys.path.append(os.path.dirname(os.path.dirname(__file__)))
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))

import ctools.clogging as clogging


def test_logging_to_syslog():
    if platform.system()=='Windows' or platform.system()=='Darwin':
        return

    clogging.setup(level='INFO',syslog=True)
    nonce = str(time.time())
    logging.error("Logging at t={}.".format(nonce))
    # Wait a few milliseconds for the nonce to appear in the logfile
    time.sleep(.01)
    # Look for the nonce
    count = 0
    for line in open("/var/log/local1.log"):
        if nonce in line:
            sys.stdout.write(line)
            count += 1
    if count==0:
        warnings.warn("local1 is not logging to /var/log/local1.log")
    assert count in [0,1,2]
    clogging.shutdown()

    



    
