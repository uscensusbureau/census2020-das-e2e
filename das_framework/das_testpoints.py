#
# testpoints.py module

import sys
import os
import os.path
import csv
import subprocess
import io

LOCAL_PATH = ["das_framework"]
FULL_PATH = os.path.dirname(__file__)
TESTPOINTS_FILE = "DAS_TESTPOINTS.csv"


def log_testpoint(testpoint, additional=None):
    facility = 'local1.err' if testpoint.endswith("F") else 'local1.info'
    # with io.StringIO(s) as csvfile:
    if os.path.exists(os.path.join(FULL_PATH, TESTPOINTS_FILE)):
        testpoints_file = os.path.join(FULL_PATH, TESTPOINTS_FILE)
    else:
        testpoints_file = os.path.join(*(LOCAL_PATH + [TESTPOINTS_FILE]))
    with open(testpoints_file,"r") as csvfile:
        for row in csv.reader(csvfile, delimiter=','):
            if row and row[0]==testpoint:
                appendage = ":"+additional if additional else ""
                subprocess.check_call(['logger','-p',facility,row[0],row[1]+appendage])
                return
    raise ValueError(f"Unknown testpoint: {testpoint}")
