#!/usr/bin/env python3
#
"""dconfig.py

Tools for dynamically handling configuration files and paths.

This module helps programs that use configuration files. It provides for the following:
* Specify variables in the config file that are substituted in dopen() with $VAR notation.
* Allows variables to be specific for a hostname. That is, you can have:

[paths]
ROOT=/mnt
ROOT@test1.company.com:/mnt-us

This makes dopen("$ROOT/myfile.txt") open the file /mnt/myfile.txt on
most hosts, but the file /mnt-us/myfile.txt on the host
test1.company.com.

This module also supports opening files on Amazon S3, with the s3:// notation.

"""

from configparser import ConfigParser
from subprocess import run,PIPE,Popen
import os
import os.path
import logging
import logging.handlers
import datetime
import argparse
import csv
import zipfile
import io
import glob
import sys
import atexit
import re
import socket

from ctools.s3 import s3open,s3exists

__author__ = "Simson L. Garfinkel"
__version__ = "0.0.1"


SECTION_PATHS='paths'
SECTION_RUN='run'
OPTION_NAME='NAME'
OPTION_SRC='SRC'

SRC_DIRECTORY = os.path.dirname(__file__)

# For handling the config file
CONFIG_FILENAME = "config.ini"
CONFIG_PATHAME  = os.path.join(SRC_DIRECTORY, CONFIG_FILENAME)    # can be changed
config_file     = None              # will become a ConfiParser object

# Get the config file. We would like to automate getting the config file and setting up logging.
def get_config(pathname=None,filename=None):
    global config_file
    if not config_file:
        if pathname and filename:
            raise RuntimeError("Cannot specify both pathname and filename")
        if not pathname:
            if not filename:
                filename = CONFIG_FILENAME
            pathname = os.path.join(SRC_DIRECTORY, filename ) 
        config_file = ConfigParser(interpolation=None)
        if os.path.exists(pathname):
            config_file.read(pathname)
        else:
            print("dopen: No config file found: {}".format(pathname),file=sys.stderr)
        # Add our source directory to the paths
        if SECTION_PATHS not in config_file:
            config_file.add_section(SECTION_PATHS)
        config_file[SECTION_PATHS][OPTION_SRC] = SRC_DIRECTORY 
    return config_file


# Our generic setup routine
# https://stackoverflow.com/questions/8632354/python-argparse-custom-actions-with-additional-arguments-passed
def setup_logging(*,config,level):
    logfname = "{}-{}-{:06}.log".format(config[SECTION_RUN][OPTION_NAME],datetime.datetime.now().isoformat()[0:19],os.getpid())
    loglevel = logging.getLevelName(level)
    logging.basicConfig(filename=logfname, 
                        format="%(asctime)s.%(msecs)03d %(filename)s:%(lineno)d (%(funcName)s) %(message)s",
                        datefmt='%Y-%m-%dT%H:%M:%S',
                        level=loglevel)
    # Make a second handler that logs to syslog
    handler=logging.handlers.SysLogHandler(address="/dev/log",
                                           facility=logging.handlers.SysLogHandler.LOG_LOCAL1)

    logging.getLogger().addHandler(handler)
    logging.info("START %s %s  log level: %s (%s)",sys.executable, " ".join(sys.argv), loglevel,loglevel)
    atexit.register(logging_exit)
        
def logging_exit():
    if hasattr(sys,'last_value'):
        logging.error(sys.last_value)

    
def argparse_add_logging(parser):
    parser.add_argument('--loglevel', help='Set logging level',
                        choices=['CRITICAL','ERROR','WARNING','INFO','DEBUG'],
                        default='INFO')    


var_re = re.compile(r"(\$[A-Z_0-9]+)")
def dpath_expand(path):
    """Find and replace all of the dollar-sign variables with names drawn from the config file.
    A future version could also look in the environment variables or on the command line, but it is
    not clear which would dominate."""
    config_file = get_config()
    while True:
        m = var_re.search(path)
        if not m:
            break
        varname  = m.group(1)[1:]
        varname_hostname = varname + "@" + socket.gethostname()
        # See if the variable with my hostname is present. If so, use that one
        if varname_hostname in config_file[SECTION_PATHS]:
            varname = varname_hostname
        try:
            path = path.replace(m.group(1), config_file[SECTION_PATHS][varname])
        except KeyError as e:
            print("Unknown variable: {}".format(varname))
            raise e
    return path

def dpath_exists(path):
    path = dpath_expand(path)
    if path[0:5]=='s3://':
        return s3exists(path)
    return os.path.exists(path)


def dopen(path, mode='r', encoding='utf-8'):
    """open data relatively to ROOT. Allows opening UFS files or S3 files."""
    logging.info("dopen: path:{} mode:{} encoding:{}".format(path,mode,encoding))
    path = dpath_expand(path)

    if path[0:5]=='s3://':
        return s3open(path, mode=mode, encoding=encoding)

    if 'b' in mode:
        encoding=None

    # Check for full path name
    logging.info("=>open(path={},mode={},encoding={})".format(path,mode,encoding))

    # If opening mode==r, and the file does not exist, see if it is present in a ZIP file
    if "r" in mode and (not os.path.exists(path)):
        # path does not exist; see if there is a single zip file in the directory
        # If there is, see if the zipfile has the requested file in it
        (dirname,filename) = os.path.split(path)
        zipnames = glob.glob(os.path.join(dirname,"*.zip"))
        if len(zipnames)==1:
            zip_file  = zipfile.ZipFile(zipnames[0])
            zf        = zip_file.open(filename, 'r')
            logging.info("  ({} found in {})".format(filename,zipnames[0]))
            if encoding==None and ("b" not in mode):
                encoding='utf-8'
            return io.TextIOWrapper(zf , encoding=encoding)
    if encoding==None:
        return open(path,mode=mode)
    else:
        return open(path,mode=mode,encoding=encoding)

def dmakedirs(path):
    """Like os.makedirs, but just returns for s3"""
    path = dpath_expand(path)

    # Can't make directories on S3
    if path[0:5]=='s3://':
        return
    logging.info("mkdirs({})".format(path))
    os.makedirs(path,exist_ok=True)


def dsystem(x):
    """Like os.system, but logs the command and throws an error if there is a non-zero return code."""
    logging.info("system({})".format(x))
    print("$ {}".format(x))
    r = os.system(x)
    if r!=0:
        raise RuntimeError("{} RETURNED {}".format(x,r))
    return r
