import pytest
import subprocess
import sys
import os

sys.path.append(os.path.join(os.path.dirname(__file__),".."))


from etl_migrate import *
import io

fh_nodup = io.StringIO("""
[section1]
key: value
key2: value2
""")

fh_dups = io.StringIO("""
[section1]
key: value
key: value2
key2: value1
key2: value2
""")

ETL_MIGRATE_PY_PATH  = os.path.join( os.path.dirname(__file__), "../etl_migrate.py")
CONFIG_JOIN_INI_PATH = os.path.join( os.path.dirname(__file__), "test_files/config_join.ini")

def test_configFileHasDuplicateKeys():
    assert configFileHasDuplicateKeys(fh_dups)==True
    assert configFileHasDuplicateKeys(fh_nodup)==False

def test_join():
    subprocess.run([sys.executable,ETL_MIGRATE_PY_PATH, CONFIG_JOIN_INI_PATH])
    
import os
from subprocess import call

CONFIG_FNAME = os.path.join( os.path.dirname(__file__), 'ipums_test/config_ipums.ini')
SAS_FNAME    = os.path.join( os.path.dirname(__file__), 'ipums_test/usa_00005.sas')
H_FNAME      = os.path.join( os.path.dirname(__file__), 'ipums_test/output_H.csv')
P_FNAME      = os.path.join( os.path.dirname(__file__), 'ipums_test/output_P.csv')

def clean():
    for fname in [P_FNAME,H_FNAME]:
        if os.path.exists(fname):
            print("rm {}".format(fname))
            os.unlink(fname)

def test_parse_run_step_command():
    assert parse_run_step_command("A=B()") == ("A","B","")
    assert parse_run_step_command("A=B(C)") == ("A","B","C")
    assert parse_run_step_command("B(C)") == (None,"B","C")
    assert parse_run_step_command("B()") == (None,"B","")

def test_is_ipums_sas_file():
    from census_spec_scanner import IPUMS_SASParser

    clean()
    assert IPUMS_SASParser.is_ipums_sas_file(CONFIG_FNAME)==False
    assert IPUMS_SASParser.is_ipums_sas_file(SAS_FNAME)==True
    cmd = [sys.executable, ETL_MIGRATE_PY_PATH, CONFIG_FNAME]
    print(" ".join(cmd))
    assert call(cmd)==0
    for fn in [H_FNAME,P_FNAME]:
        assert os.path.exists(fn)

    # Check the files to make sure their contents are correct
    assert open(H_FNAME,"r").read().count("\n")==3
    assert open(P_FNAME,"r").read().count("\n")==9

    # And clean the files
    clean()


in_order = io.StringIO("""
[section]
label_1: test
label_2: test
""")

out_of_order = io.StringIO("""
[section]
label_2: test
label_1: test
""")


def test_configFileSectionOutOfOrder():
    assert configFileSectionOutOfOrder(in_order,"section")    == None
    assert configFileSectionOutOfOrder(out_of_order,"section") == ("label_2","label_1")


