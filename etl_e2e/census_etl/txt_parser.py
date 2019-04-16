#!/usr/bin/env python
#
# txt_parser:
# scan text spec file and generate the source code for a C++ program that:
# 1 - Creates an SQLite3 database that matches the spec.
# 2 - Reads a text file that matches the spec.
# 3 - For each record, inserts it into the SQLite3 database
#

import re
import sys
import os
from schema import Table,Variable

title_re    = re.compile("Title: ([^:]*)\s\s\s\s\s")
version_re  = re.compile("Version:\s*([0-9.]+)")
# variable.1 = position
# variable.2 = name
# variable.3 = description
# variable.4 = type
# variable.5 = column
variable_re = re.compile("\s*(\d+)[.] ([A-Z_0-9]+)\s+(.*\S)\s+([A-Z0-9]+\(\d+\))\s*(\d+-\d+)")


def process_file(fname,schemafile):
    table = None
    for line in open(fname):
        if not table:
            m = title_re.search(line)
            if m:
                table = Table(name=m.group(1))
        if table and not table.version:
            m = version_re.search(line)
            if m:
                table.version = m.group(1)
        m = variable_re.search(line)
        if m:
            v = Variable()
            (position,name,desc,vtype,column) = m.group(1,2,3,4,5)
            oname = name
            count = 2
            while name in [v.name for v in table.vars]:
                name = "{}{}".format(oname,count)
                count += 1
            v.define_from_row([position,name,desc,vtype])
            if "-" in column:
                v.column = [int(x) for x in column.split("-")]
            table.add_variable(v)

    schemafile.write('#define SPEC_FILE "{}"\n'.format(fname))
    table.write_sql_scanner(schemafile)
        

if __name__=="__main__":
    from argparse import ArgumentParser,ArgumentDefaultsHelpFormatter
    parser = ArgumentParser(
        formatter_class = ArgumentDefaultsHelpFormatter)
    parser.add_argument("--debug",  action="store_true")
    parser.add_argument("--config", help="Specifies a configuration file")
    parser.add_argument("infile",nargs="?")
    parser.add_argument("--outdir", help="Specifies output directory", default=".")

    args = parser.parse_args()
    schemafile = os.path.join(args.outdir,"schema.h")
    process_file(args.infile,open(schemafile,"w"))
