#!/usr/bin/env python3
#
"""
Demo code to create RDDs of the 1940 IPUMS file.

"""

import sys
import os
import time

# default values

DAS_S3ROOT='DAS_S3ROOT'
EXT1940USCB_FILE = os.environ['EXT1940USCB']
MDF_UNIT_FILE = None
MDF_PER_FILE = None


COMMENT_CHAR='#'
NUM_EXECUTORS=10
TEN_VACANT='0'                  # ten==0 means vacant


def parse_ipums_unit(line):
    """Read a line and return it as a dictionary. This should be automatically generated..."""
    import ipums_1940_classes
    ipums_unit = ipums_1940_classes.H()
    ipums_unit.parse_position_specified(line)
    return ipums_unit.SparkSQLRow()

def parse_ipums_per(line):
    """Read a line and return it as a dictionary. This should be automatically generated..."""
    import ipums_1940_classes
    ipums_per = ipums_1940_classes.P()
    ipums_per.parse_position_specified(line)
    return ipums_per.SparkSQLRow()

def ipums_rdds(ipumsFile):
    """Return a tuple of (unit,person) dataframes"""
    from pyspark.sql import SparkSession
    spark = SparkSession.builder.getOrCreate()
    textFileRDD = spark.sparkContext.textFile(ipumsFile) # note .persist() gave out-of-memory

    return (textFileRDD.filter(lambda line:line[0]=='H').map(parse_ipums_unit),
            textFileRDD.filter(lambda line:line[0]=='P').map(parse_ipums_per))


if __name__=="__main__":
    from argparse import ArgumentParser,ArgumentDefaultsHelpFormatter
    parser = ArgumentParser( formatter_class = ArgumentDefaultsHelpFormatter,
                             description="Demonstrate that we can create an ipums_unit_rdd and an ipums_person_rdd" )
    parser.add_argument("--num_executors", type=int, default=NUM_EXECUTORS)

    parser.add_argument("--debug", action='store_true', help='print additional debugging info')
    args  = parser.parse_args()
    
    import census_etl.ctools.cspark as cspark
    spark = cspark.spark_session(logLevel='ERROR',
                                 pyfiles = ['ipums_1940/ipums_1940_classes.py' ],
                                 num_executors=NUM_EXECUTORS)

    t0 = time.time()
    (unit_rdd, per_rdd) = ipums_rdds( EXT1940USCB_FILE )
    print("Records in Units RDD: {:,}".format(unit_rdd.count()))
    print("Records in Per RDD: {:,}".format(per_rdd.count()))
    print("Elapsed time: {}".format(time.time()-t0))
    exit(0)
