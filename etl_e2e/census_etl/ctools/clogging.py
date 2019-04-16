#
# 
"""clogging.py - a collection of logging support methods for use at the US Census Bureau.

This module adds support for logging to syslog to the Python logging system. 

All messages sent to the default logger will also go to syslog. We use
this in some production environments to aggregate log messages using
syslog and Splunk.

In the 2020 Disclosure Avoidance System, we will be using local1 as our logging facility. 
Within Amazon's Elastic Map Reduce system, we set up the EMR CORE nodes to send all local1 messages
to the MASTER node, and we set up the MASTER node to send all local1 messages to the Splunk server.

This code also sets up the python logger so that it reports the
filename, line number, and function name associated with all log
messages. That's proven to be useful. You can change that by changing
the module variables after the module is imported.

To use this logging module, you must call clogging.setup().  The call
must be called in every python process in which you want logging to go
to syslog. In an EMR system, this means that you need to call it at
least once in every mapper (because a new Python process may be
started up at any time by the Spark system). Calling clogging.setup()
is fast and idempotent---you can call it as often as you want. It
tracks to see if it has been previously called and immediately returns
if it has been.

This code has been tested on both Apple MAC OSX and on Amazon Linux.

"""

import logging
import logging.handlers
import os
import os.path
import datetime

__author__ = "Simson L. Garfinkel"
__version__ = "0.0.1"


DEVLOG = "/dev/log"
DEVLOG_MAC = "/var/run/syslog"

# Default log formats. 
#

# YEAR is used in callers
YEAR=str(datetime.datetime.now().year)
LOG_FORMAT="%(asctime)s %(filename)s:%(lineno)d (%(funcName)s) %(message)s"
SYSLOG_FORMAT="%(filename)s:%(lineno)d (%(funcName)s) %(message)s"

# Global state variables. Keep track as to whether or not syslog
# handler was added and whether or not the basicConfig was setup.

added_syslog = False
called_basicConfig = False


def applicationId():
    """Return the Yarn applicationID.
    The environment variables are only set if we are running in a Yarn container.
    """
    try:
        return "_".join(['application'] + os.environ['CONTAINER_ID'].split("_")[1:3])
    except KeyError:
        pass

    # Perhaps we are running on the head-end. If so, run a Spark job that finds it.
    try:
        from pyspark     import SparkConf, SparkContext
        sc = SparkContext.getOrCreate()
        appid = sc.parallelize([1]).map(lambda x:applicationId()).collect()
        return appid[0]
    except ImportError:
        pass

    # Ugh. We can't find it.
    return "unknown"

def shutdown():
    """Turn off the logging system."""
    global added_syslog, called_basicConfig
    logging.shutdown()
    added_syslog = False
    called_basicConfig = False

def add_argument(parser):
    """Add the --loglevel argument to the ArgumentParser"""
    parser.add_argument("--loglevel", help="Set logging level",
                        choices=['CRITICAL', 'ERROR', 'WARNING', 'INFO', 'DEBUG'], default='INFO')

def setup_syslog(facility=logging.handlers.SysLogHandler.LOG_LOCAL1,
                 syslog_format = YEAR+" "+SYSLOG_FORMAT):
    global  added_syslog
    if not added_syslog:
        # Make a second handler that logs to syslog
        if os.path.exists(DEVLOG):
            handler = logging.handlers.SysLogHandler(address=DEVLOG, facility=facility)
        elif os.path.exists(DEVLOG_MAC):
            handler = logging.handlers.SysLogHandler(address=DEVLOG_MAC, facility=facility)
        else:
            return              # no dev log
        formatter = logging.Formatter(syslog_format)
        handler.setFormatter(formatter)
        logging.getLogger().addHandler(handler)
        added_syslog = True

def setup(level='INFO',
          syslog=False,
          filename=None,
          facility=logging.handlers.SysLogHandler.LOG_LOCAL1,
          log_format=LOG_FORMAT,
          syslog_format=SYSLOG_FORMAT):
    """Set up logging as specified by ArgumentParse. Checks to see if it was previously called and, if so, does a fast return.
    @param syslog     - if True, also create the syslog handler.
    @param filename   - if provided, log to this file, too.
    @param facility   - use this facility, default LOG_LOCAL1
    @param log_format - log this log format for all but syslog
    @param syslog_format - use this for the syslog format.
    """
    global called_basicConfig
    if not called_basicConfig:
        loglevel = logging.getLevelName(level)
        if filename:
            logging.basicConfig(filename=filename, format=log_format, level=loglevel)
        else:
            logging.basicConfig(format=log_format, level=loglevel)
        called_basedConfig = True

    if syslog:
        setup_syslog(facility=facility,syslog_format=syslog_format)


if __name__=="__main__":
    setup_syslog()
    assert added_syslog==True
    logging.error("By default, error gets logged but info doesn't")
