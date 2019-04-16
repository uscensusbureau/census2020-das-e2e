#!/usr/bin/env python
#
# sas_convert:
# convert a SAS datafile into an sqlite database
#

import re
import sys
import os
import pandas
from schema import Table,Variable
import schema
import sqlite3
import logging
import time

assert sys.version > '3'

CHUNKSIZE = 10000


def process_file(fname,dbfile):

    data = pandas.read_sas(fname,chunksize=1)
    frame = next(data)

    # Make a table
    table = Table(name=os.path.splitext(os.path.split(fname)[1])[0])
    logging.info("Creating table {}".format(table.name))
    for col in frame.columns:
        v = Variable()
        v.set_name(col)
        v.set_vtype(schema.vtype_for_numpy_type(type(frame[col][0])))
        table.add_variable(v)

    conn = sqlite3.connect(dbfile)
    c = conn.cursor()
    cmd = table.sql_schema()
    c.execute(cmd)

    t0 = time.time()
    logging.info("Transferring data...")
    istmt = table.sql_insert()
    print(istmt)
    lines = 0
    for frame in pandas.read_sas(fname,chunksize=CHUNKSIZE):
        c.execute("BEGIN TRANSACTION;")
        for row in frame.itertuples(index=False):
            c.execute(istmt,row)
            lines += 1
            if lines%10000==0:
                t = int(time.time()-t0)
                s = t % 60
                m = (t%3600) // 60
                h = t//3600
                logging.info("time: {}:{:02}:{:02} lines {:,}".format(h,m,s,lines))
        c.execute("END TRANSACTION;")
        

if __name__=="__main__":
    from argparse import ArgumentParser,ArgumentDefaultsHelpFormatter
    parser = ArgumentParser(
        formatter_class = ArgumentDefaultsHelpFormatter)
    parser.add_argument("--loglevel", help="Set logging level",
                        choices=['CRITICAL','ERROR','WARNING','INFO','DEBUG'],
                        default='INFO')
    parser.add_argument("--debug", action="store_true")
    parser.add_argument("infile")
    parser.add_argument("dbfile")

    args = parser.parse_args()

    loglevel = logging.getLevelName(args.loglevel)
    logging.basicConfig(format="%(asctime)s %(filename)s:%(lineno)d (%(funcName)s) %(message)s", 
                        level=loglevel)
    #logging.getLogger().addHandler(logging.StreamHandler(sys.stdout))

    logging.info("START {}  log level: {} ({})".format(os.path.abspath(__file__), args.loglevel,loglevel) )

    process_file(args.infile,args.dbfile)
