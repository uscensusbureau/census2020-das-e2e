#
# testpoints.py module

import sys
import os
import os.path
import csv
import subprocess
from argparse import ArgumentParser, ArgumentDefaultsHelpFormatter

TESTPOINTS_FILE=os.path.join( os.path.dirname(__file__), "DAS_TESTPOINTS.csv")

assert os.path.exists(TESTPOINTS_FILE)

def find_testpoint(testpoint):
    with open(TESTPOINTS_FILE,"r") as csvfile:
        for row in csv.reader(csvfile, delimiter=','):
            if row and row[0]==testpoint:
                return row[0],row[1]
    raise ValueError(f"Unknown testpoint: {testpoint}")

def log_testpoint(testpoint):
    """Log the named testpoint to local1.err or local1.info"""
    facility = 'local1.err' if testpoint.endswith("F") else 'local1.info'
    row = find_testpoint(testpoint)
    subprocess.check_call(['logger','-p',facility,row[0],row[1]])
    return

if __name__=="__main__":
    parser = ArgumentParser(formatter_class=ArgumentDefaultsHelpFormatter)
    parser.add_argument("testpoint", help=f"Testpoint code from testpoint file {TESTPOINTS_FILE}")
    args = parser.parse_args()
    print(" ".join(find_testpoint(args.testpoint)))
    log_testpoint(args.testpoint)
