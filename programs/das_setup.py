# DAS Spark setup module
# William Sexton
# Last modified: 7/12/2018

"""
    This is the setup module for DAS development on Research 2 and on EMR clusters.
    It launches Spark.
"""

import logging
import socket
import os
import sys

try:
    from pyspark import SparkConf, SparkContext
    from pyspark.sql import SparkSession
except ImportError:
    pass

try:
    from das_framework.driver import AbstractDASSetup
except ImportError:
    logging.debug("System path: {}".format(sys.path))
    logging.debug("Working dir: {}".format(os.getcwd()))
    path = os.path.realpath(__file__)
    pathdir = os.path.dirname(path)
    while True:
        if os.path.exists(os.path.join(pathdir, ".git")):
            logging.debug("Found repository root as: {}".format(pathdir))
            break
        pathdir = os.path.dirname(pathdir)
        if os.path.dirname(path) == path:
            raise ImportError("Could not find root of git repository")
    path_list = [os.path.join(root, name) for root, dirs, files in os.walk(pathdir)
                 for name in files if name == "driver.py"]
    logging.debug("driver programs found: {}".format(path_list))
    assert len(path_list) > 0, "Driver not found in git repository"
    assert len(path_list) == 1, "Too many driver programs found"
    sys.path.append(os.path.dirname(path_list[0]))
    from driver import AbstractDASSetup

SPARK = "spark"
NAME = "name"
MASTER = "master"
LOGLEVEL = "loglevel"
ZIPFILE = "ZIPFILE"

# TK - FIXME - Get the spark.eventLog.dir from the config file and set it here, rather than having it set in the bash script
#              Then check to make sure the directory exists.

class setup(AbstractDASSetup):
    """
        DAS setup class for 2018 development on research 2 and emr clusters.
    """
    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def setup_func(self):
        """
            Starts spark up in local mode or client mode for development and testing on Research 2.

            returns: a SparkSession object.
        """

        # Validate the config file. DAS Developers: Add your own!
        assert self.getconfig('queriesfile',section='budget') 

        pyspark_log = logging.getLogger('py4j')
        pyspark_log.setLevel(logging.ERROR)
        conf = SparkConf().setAppName(self.getconfig("{}.{}".format(SPARK, NAME)))
        sc = SparkContext.getOrCreate(conf=conf)

        sc.setLogLevel(self.getconfig("{}.{}".format(SPARK, LOGLEVEL)))
        if sc.getConf().get("{}.{}".format(SPARK, MASTER)) == "yarn":
            # see stackoverflow.com/questions/36461054
            # --py-files sends zip file to workers but does not add it to the pythonpath.
            try:
                sc.addPyFile(os.environ[ZIPFILE])
            except KeyError:
                logging.info("Run script did not set environment variable '%s'.",ZIPFILE)
        return SparkSession(sc)
