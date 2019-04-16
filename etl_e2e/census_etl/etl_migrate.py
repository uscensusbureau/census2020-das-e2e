#!/usr/bin/env python3
#
# etl_migrate.py:
# Migrate an input database file to an output file
# Based on etl_ipums.py
# Note coding around Pandas memory leak...

import gc
import gzip
import json
import logging
import logging.handlers
import os
import os.path
import psutil
import re
import sqlite3
import subprocess
import math
import sys
import time
import gzip
import sqlite3


from schema import Schema,Table,Variable,Recode,TYPE_VARCHAR,TYPE_INTEGER
from configparser import ConfigParser
from census_spec_scanner import CensusSpec

from migrate import Migrate

import ctools.cspark  as cspark
import ctools.dconfig as dconfig
import ctools.clogging as clogging

PROCESS = psutil.Process(os.getpid())

SETUP_SECTION = "setup"
CONFIGFILE_TYPE = "configfile_type"
CONFIGFILE_TYPE_WANTED = "etl_migrate"
OUTDIR = 'outdir'
RANDOM_RECORDS = 'random_records'
LIMIT = 'limit'
DEBUG_OPTION = 'debug'
SPARK_SECTION= 'spark'
SPARK_OPTION = 'spark'
NUM_EXECUTORS = 'num_executors'
NUM_EXECUTORS_DEFAULT = '2'
WRITE_DELIMITER = "write_delimiter"
SETUP_OPTIONS = set([CONFIGFILE_TYPE,CONFIGFILE_TYPE_WANTED,OUTDIR,RANDOM_RECORDS,LIMIT,WRITE_DELIMITER,DEBUG_OPTION])

RECODES_SECTION = "recodes"
RUN_SECTION = "run"
GLOBALS_SECTION = "globals"
GLOBALS = "globals"
COMPLETED_FLAG = '.completed'
BULK_LOAD_REPORTING_INTERVAL=100000

def fatal(msg):
    """Send msg to the error log and then raise a runtime error"""
    logging.error(msg)
    raise RuntimeError(msg)

def squash_floats(d):
    """Transform floats into ints; used in the output routines"""
    for k in d.keys():
        try:
            if math.isnan(d[k]):
                d[k] = ''
                continue
        except TypeError:
            continue

        try:
            if (type(d[k])==float and float(d[k])==int(d[k])):
                d[k]=int(d[k])
        except ValueError:
            pass


def configFileHasDuplicateKeys(f):
    # return true if any section in the config file has multiple keys
    import configparser
    config = ConfigParser()
    try:
        config.readfp(f)
        return False
    except configparser.DuplicateOptionError as e:
        return True

def configFileSectionOutOfOrder(f,section):
    # return true if any of the labels in the section of the config file are out of order
    section_re = re.compile(r"^\[(\S+)\]")
    label_re   = re.compile(r"^([^# \t]\S*):")
    in_section = ''
    last_label = ''
    this_label = ''
    for line in f:
        m = section_re.search(line)
        if m:
            in_section = m.group(1)
            continue
        m = label_re.search(line)
        if m:
            last_label = this_label
            this_label = m.group(1)
            if last_label and this_label < last_label:
                return (last_label,this_label)
    return None
    

def infodb(path):
    conn = sqlite3.connect(path)
    c = conn.cursor()
    print("Database: {} Metadata".format(path))
    print("Key: value")
    print("==========")
    for (key,value) in c.execute("select key,value from metadata"):
        print("{}: {}".format(key,value))
    print("Database: {} Data (limit 10)".format(path))
    print("Key: value")
    print("==========")
    for (key,value) in c.execute("select key,value from data limit 10"):
        print("{}: {}".format(key,value))
    exit(0)
    
def infosas(path):
    s = Schema()
    print("Loading schema from {}".format(path))
    s.load_schema_from_file( dconfig.dopen(path))
    s.dump(path)
    exit(0)

RUN_COMMAND_RE = re.compile(r"\s*((\w+)\s*=)?\s*(\w+)\((.*)\)")
def parse_run_step_command(line):
    m = RUN_COMMAND_RE.search(line.strip())
    if not m:
        fatal("Cannot parse: {}".format(line))
    return m.group(2,3,4)

def process_setup_section(config, sc=None):
    """Process the setup section of the config file. Return the mig variable"""
    setup = config[SETUP_SECTION]

    # Check for configuration file variables that set options
    
    mig = Migrate( outdir=os.path.expandvars(config[SETUP_SECTION][OUTDIR]),
                   dry_run = args.dry_run ,
                   limit   = args.limit,
                   dumpdb  = args.dumpdb,
                   spark_context = sc)

    # Get the input and output schemas from the input and output options
    db = CensusSpec(name='etl_db')
    for (key,val) in config[SETUP_SECTION].items():
        # inputNN: specify the schema of an input table. 
        # For some tables, also specifies the filenames containing the data
        if key.startswith("input"):
            val = os.path.expandvars(val)
            logging.info("{}: {}".format(key,val))
            db.load_schema_from_file( val )

        # outputNN: or formatNN: specify the schema of an output table
        elif key.startswith("output"): 
            val = os.path.expandvars(val)
            logging.info("{}: {}".format(key,val))
            db.load_schema_from_file( val )

        # sqlNN: Specify the schema of a table with SQL
        elif key.startswith("sql"):
            db.add_sql_table(val)

        elif key == RANDOM_RECORDS:
            args.random = int(val)
        elif key == LIMIT:
            args.limit = int(val)
        elif key == WRITE_DELIMITER:
            args.write_delimiter = val

        elif key not in SETUP_OPTIONS: # ignore these
            fatal("Unknown option in secton {}: '{}'".format(SETUP_SECTION,key))

    return (mig,db)

def process_recodes_section(config,mig,db):
    if RECODES_SECTION not in config.sections():
        return
    logging.info("Adding recodes")
    recode_re = re.compile("([^(]+)[(](.+)[)]")
    for option in sorted(config.options(RECODES_SECTION)):
        recode_desc = config[RECODES_SECTION][option]
        m = recode_re.search(option)
        if m:
            (option_without_vtype,vtype) = m.group(1,2)
            vtype = vtype.upper()
            logging.info("{}:  {} ({})".format(option_without_vtype,recode_desc,vtype))
            db.add_recode(option_without_vtype,vtype,recode_desc)
        else:
            fatal("Cannot code recode {}:{}".format(name,recode_desc))
    db.compile_recodes()

def process_run_section(config,mig,db):
    # Process the steps
    for option in sorted(config.options(RUN_SECTION)):
        if option==GLOBALS:     # ignore the globals
            continue
        logging.info("processing [{}] {}".format(RUN_SECTION,option))
        if not option.startswith("step"):
            logging.info("Ignoring option '{}'".format(option))
            continue
        lines = config[RUN_SECTION][option].split("\n")

        (dest,func_name,func_args) = parse_run_step_command(lines[0])
        logging.info("{} dest:{} func_name:{}  func_args:{}".format(option,dest,func_name,func_args))
        if func_args:
            try:
                kvargs = dict([kv.strip().split("=") for kv in func_args.split(",")])
            except ValueError as e:
                raise RuntimeError("Parsing error: option:{} argument:{} missing '=' for key=value assignment".format(
                    option,func_args))
        else:
            kvargs = {}
        # If any of the kvargs are quoted strings, unquote them
        for (key,value) in kvargs.items():
            while (value[0]==value[-1]) and (value[0] in "\"'"):
                value = value[1:-1]
                kvargs[key] = value

        if mig.command(db=db, cmd=func_name, dest=dest, kvargs=kvargs, lines=lines):
            continue
        if func_name=='DUMP':
            db.dump(logging.info)
        elif func_name=="COLLECT_ACCUM":
            mig.compile_and_exec("euid_counts = euid_counts.value")
        else:
            fatal("Unknown command in config file option '{}': func_name={} func_args={} dest={} ".
                               format(option,func_name,func_args,dest))

if __name__=="__main__":
    from argparse import ArgumentParser,ArgumentDefaultsHelpFormatter
    parser = ArgumentParser( formatter_class = ArgumentDefaultsHelpFormatter,
                             description="Migrate an input dataset to an output dataset" )
    parser.add_argument('--debug', action='store_true')
    parser.add_argument('--dump',  action='store_true', help='Dump schema and internal functions; do not run the transfer')
    parser.add_argument('--dry-run', action='store_true', help='Print what would be done only')
    parser.add_argument('--schema', action='store_true', help='display the SQL schema')
    parser.add_argument('--limit', help='Only copy this many records from each file (for testing)', type=int)
    parser.add_argument('--random', action='store_true', help='generate random data')
    parser.add_argument('--loglevel', help='Set logging level',
                        choices=['CRITICAL','ERROR','WARNING','INFO','DEBUG'],
                        default='INFO')    
    parser.add_argument('--write_delimiter', help='delimiter for output csv files',default='|')
    parser.add_argument("--justglobals", help="Just run the globals setup, dump what's in the environment, and then stop",
                        action='store_true')
    parser.add_argument('--infosas',help='Print information about SAS7BDAT file and exit; config is ignored')
    parser.add_argument('--loaddb', action='store_true', help='Only load join databases')
    parser.add_argument('--infodb', help='Print information about the SQlite3 database file and exit; config is ignored')
    parser.add_argument('--dumpdb', help='If specified, dump this many records of the SQLite3 database',type=int)
    parser.add_argument("--detach", action="store_true", help="disconnection from console and write output to logfiles")
    parser.add_argument("--spark",  action="store_true", help="Run under spark")
    parser.add_argument('config', help='config file')

    args   = parser.parse_args()

    # Check the args
    if args.random and not args.limit:
        parser.error("--limit must be specified with --random")

    # See if simple infos are specified
    if args.infodb:  infodb(args.infodb); exit(0)
    if args.infosas: infosas(args.infosas); exit(0)

    ###
    ### VALIDATE AND THEN READ CONFIG FILE
    ###

    if configFileHasDuplicateKeys(open(args.config)):
        fatal("{} has duplicate keys".format(args.config))
    os.environ["CONFIGDIR"] = os.path.split(os.path.abspath(args.config))[0]
    config = dconfig.get_config(pathname=args.config)
    if CONFIGFILE_TYPE not in config[SETUP_SECTION]:
        fatal("{} not in config file {}".format(CONFIGFILE_TYPE,args.config))
    
    if config[SETUP_SECTION][CONFIGFILE_TYPE] != CONFIGFILE_TYPE_WANTED:
        fatal("{} is not {} in config file {}".
                           format(CONFIGFILE_TYPE,CONFIGFILE_TYPE_WANTED,args.config))
    order = configFileSectionOutOfOrder(open(args.config),"run")
    if order:
        fatal("[run] section has steps that are out of order: {} comes before {}".format(order[0],order[1]))

    if config[SETUP_SECTION].get("DEBUG","").lower()[0:1] in ["1","t","y"]:
        args.debug = True

    if SPARK_SECTION in config and config[SPARK_SECTION].get("SPARK","").lower()[0:1] in ["1","t","y"]:
        args.spark = True

    # Detach if requested
    if args.detach:
        print("Detaching...")
        cspark.detach()

    ###
    ### ENABLE SPARK IF REQUESTED
    ###

    sc = None
    if args.spark:
        num_executors = config[SPARK_SECTION].get(NUM_EXECUTORS, NUM_EXECUTORS_DEFAULT)
        sc = cspark.spark_context(num_executors=int(num_executors),
                                  pydirs=[os.path.dirname(__file__),
                                          os.path.join(os.path.dirname(__file__),'ctools'),
                                          os.path.join(os.path.dirname(__file__),'dfxml/python')])

    ###
    ### ENABLE LOGGING.
    ### Logging must be set up before any logging is done
    ### Note that we log to both a file and to syslog facility 1
    ###

    clogging.setup(args.loglevel)
    logging.info("START {} ".format(os.path.abspath(__file__)))

    (mig,db) = process_setup_section(config, sc)
    process_recodes_section(config,mig,db)

    # Dump or print schema, as necessary
    if args.dump:       db.dump()
    if args.schema:     print(db.sql_schema())
    #if args.spark:      mig.add_spark_to_environment()

    # run steps are implemented as compiled code.
    mig.add_tables_to_environment(db)

    # Add globals to environment if we have any
    if GLOBALS in config[RUN_SECTION]:
        fatal("globals must now be  [globals] section")

    if GLOBALS_SECTION in config:
        logging.info("Adding GLOBALs")
        for option in config[GLOBALS_SECTION]:
            logging.info("Adding [{}]{}".format(GLOBALS_SECTION,option))
            lines = config[GLOBALS_SECTION][option]
            mig.compile_and_exec(lines)
    else:
        logging.info("No [{}] section".format(GLOBALS_SECTION))

    #mig.undo_spark()

    if args.justglobals:
        mig.dump_context()
    else:
        process_run_section(config, mig, db)

    logging.info("DONE")
    exit(0)
