#!/usr/bin/python
# -*- coding: utf-8 -*-
#
# Test the module for reading an IPUMS hierarchical file and turning it into two CSV files.

import os
import sys
from subprocess import call

sys.path.append("..")           # schema is in the parent directory
sys.path.append("../..")        # schema is in the parent directory

# get the full path of all the files in the same directory
MYDIR        = os.path.dirname(__file__)
ETL_MIGRATE_PATH = os.path.join(MYDIR, "../../etl_migrate.py")

CONFIG_FNAME = 'config_ipums.ini'
SAS_FNAME    = 'usa_00005.sas'
H_FNAME      = 'output_H.csv'
P_FNAME      = 'output_P.csv'

def clean():
    for fname in [P_FNAME,H_FNAME]:
        if os.path.exists(fname):
            print("rm {}".format(fname))
            os.unlink(fname)

def test_is_ipums_sas_file():
    from census_spec_scanner import IPUMS_SASParser

    os.chdir( MYDIR )
    clean()
    assert IPUMS_SASParser.is_ipums_sas_file(CONFIG_FNAME)==False
    assert IPUMS_SASParser.is_ipums_sas_file(SAS_FNAME)==True
    assert os.path.exists(ETL_MIGRATE_PATH)
    cmd = [sys.executable,ETL_MIGRATE_PATH,CONFIG_FNAME]
    print(" ".join(cmd))
    assert call(cmd)==0
    for fn in [H_FNAME,P_FNAME]:
        assert os.path.exists(fn)

    # Check the files to make sure their contents are correct
    assert open(H_FNAME,"r").read().count("\n")==3
    assert open(P_FNAME,"r").read().count("\n")==9
