#!/usr/bin/env python3

# Test program.
#
# sqlite2parquet.py 
# Idea taken from:
# https://gist.github.com/beauzeaux/a68d6f32f4985ed547ce
# https://stackoverflow.com/questions/41254011/sparksql-read-parquet-file-directly

import sqlite3
import os
import argparse
import logging
import sys

try:
    import pyspark
    import pyspark.sql
    from pyspark import StorageLevel
    from pyspark.sql.types import *
    assert "SPARK_HOME" in os.environ
except ImportError:
    print("Running without SPARK")


def get_table_list(conn):
    """Gets the list of tables in the sqlite database given a file path
        to the sqlite database
    """
    cur = conn.cursor()
    res = cur.execute("""SELECT name FROM sqlite_master WHERE type='table'""")
    names = [x[0] for x in res]
    return names


def conv(b):
    """if b is bytes, make it a string, otherwise return as is"""
    if type(b)==bytes: return b.decode('utf-8')
    if type(b)==type(None): return ""
    return b

def get_generator_from_table(conn, table_name):
    rowcounter = 0
    cur = conn.cursor()
    res = cur.execute(""" SELECT * FROM {0} """.format(table_name))
    for row in res:
        if rowcounter % 100000==0:
            logging.info("rowcounter={:,}".format(rowcounter))
        rowcounter += 1
        yield [conv(r) for r in row]

def get_column_names_from_table(conn, table_name):
    cur = conn.cursor()
    res = cur.execute(""" SELECT * FROM {0} """.format(table_name))
    return [x[0] for x in cur.description]


def sqlite2parquet(db_path, output_dir, skip_tables=['sqlite_sequence']):
    if args.spark:

        # load Spark configuration
        conf = pyspark.SparkConf()
        conf.set('spark.executor.memory', '4g')
        #conf.set('spark.sql.parquet.compression.codec', 'gzip')
        # We recommend snappy because it is splittable
        # https://www.cloudera.com/documentation/enterprise/5-3-x/topics/admin_data_compression_performance.html
        conf.set('spark.sql.parquet.compression.codec', 'snappy')
        sc = pyspark.SparkContext("local", conf=conf)

    conn = sqlite3.connect(db_path)
    tables = get_table_list(conn)
    for table in tables:
        if table in skip_tables:
            print("Skipping: {0}".format(table))
            continue
        print("Converting: {0}".format(table))
        logging.info("Converting: {0}".format(table))
        gen    = get_generator_from_table(conn, table)
        schema = get_column_names_from_table(conn, table)
        print("schema: ",schema)

        if args.spark:
            print("converting to data-frame")
            print("column names: {}".format(schema))

            a = sc.parallelize(gen)
            a.persist(StorageLevel.DISK_ONLY)
            sqlContext = pyspark.SQLContext(sc)
            df = sqlContext.createDataFrame(a, schema=schema, samplingRatio=None)
            fname = os.path.join(output_dir, table + '.parquet')
            print("\t saving...")
            df.saveAsParquetFile(fname)
        else:
            print("Running --no-spark")
            print("Here are the first five rows:")
            i = 0
            for row in gen:
                print("#{}: ".format(i),end=" ")
                for x in row:
                    print(x,end=" ")
                print()
                i += 1
                if i>5: break

if __name__ == "__main__":
    import argparse
    import datetime
    import sys
    import os

    # https://stackoverflow.com/questions/15008758/parsing-boolean-values-with-argparse
    parser = argparse.ArgumentParser(description='Convert sqlite database into parquet files')
    parser.add_argument("--spark",dest='spark', action='store_true')
    parser.add_argument("--no-spark",dest='spark', action='store_false')
    parser.add_argument("--loglevel", help="Set logging level",
                        choices=['CRITICAL','ERROR','WARNING','INFO','DEBUG'],
                        default='INFO')
    parser.add_argument('sqlite_db_path')
    args = parser.parse_args()


    ###
    ### Logging must be set up before any logging is done
    ### By default is is in the current directory, but if we run an experiment, put the logfile in that directory
    logfname = "{}-{}-{:06}.log".format(sys.argv[0],datetime.datetime.now().isoformat()[0:19],os.getpid())
    loglevel = logging.getLevelName(args.loglevel)
    logging.basicConfig(filename=logfname, 
                        format="%(asctime)s %(filename)s:%(lineno)d (%(funcName)s) %(message)s", 
                        level=loglevel)
    logging.info("START {}  log level: {} ({})".format(os.path.abspath(__file__), args.loglevel,loglevel) )


    print("args.spark=",args.spark)

    try:
        os.makedirs(args.sqlite_db_path + '.parquets')
    except OSError:
        pass

    sqlite2parquet(args.sqlite_db_path, args.sqlite_db_path + '.parquets',
                   skip_tables=['task', 'sqlite_sequence', 'sqlite_stat1', 'crawl'])


