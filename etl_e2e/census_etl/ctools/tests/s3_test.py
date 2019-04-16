#!/usr/bin/env python3
# Test S3 code

import os
import sys
import warnings

#sys.path.append( os.path.join( os.path.dirname(__file__), "..") )
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))

from ctools.s3 import *

def test_s3open():
    if "EC2_HOME" in os.environ:
        path = "s3://uscb-decennial-ite-das/motd"

        for line in s3open(path,"r"):
            print("> ",line)
    else:
        warnings.warn("test_s3open only runs on AWS EC2 computers")


if __name__=="__main__":
    test_s3open()
