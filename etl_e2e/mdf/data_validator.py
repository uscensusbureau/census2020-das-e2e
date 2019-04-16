#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
data_validator: given an automatically generated python specification file,
read a datafile and validate it. Can run under Spark.
"""


import sys
import os
import time
import json
import copy


sys.path.append( os.path.join(os.path.dirname(__file__), "../census_etl"))

CRED = '\033[91m'
CGREEN = '\033[92m'
CEND = '\033[0m'
def red(s):
    return CRED + s + CEND
def green(s):
    return CGREEN + s + CEND

#
# SPEC_CLASS_OBJECTS is an array of the validator classes.


from mdf_spec_autogen import SPEC_CLASS_OBJECTS,SPEC_DICT
from ctools.dconfig import dopen

def voname(vo):
    return vo.name().replace(".","_")

class Validator:
    def __init__(self,vo,*,show=False,maxerrors=1,position_delimited=False,pipe_delimited=False):
        assert not (position_delimited==True and pipe_delimited==True)
        assert not (position_delimited==False and pipe_delimited==False)
        self.vo = vo
        self.show = show
        self.maxerrors = maxerrors
        self.position_delimited = position_delimited
        self.pipe_delimited = pipe_delimited

    def badline(self,line):
        if self.show:
            print("".join(f"         {i}" for i in range(1,9)))
            print("1234567890" * 8)
            print(red(line[:-1])+green("[END]"))
            print("_" * 80)


    def validate(self,fname):
        """
        Validate file fname with vo.
        This is a single-threaded validation routine that has a nice GUI display of what doesn't validate
        """
        self.records = 0
        self.errors = 0
        self.t0 = time.time()
        with dopen(fname,"r") as f:
            lines = 0
            for line in f:
                lines += 1
                line = line.rstrip()
                if line[0]=='#':
                    continue    # comment line
                if self.errors > self.maxerrors:
                    print("too many errors",file=sys.stderr)
                    break
                self.records += 1
                try:
                    if self.position_delimited:
                        self.vo.parse_position_delimited(line)
                    elif self.pipe_delimited:
                        self.vo.parse_pipe_delimited(line)
                except ValueError as e:
                    print(f"{fname}:{lines} will not parse: {e}")
                    self.badline(line)
                    self.errors += 1
                    continue
                if not vo.validate():
                    print(f"{fname}:{lines} {vo.validate_reason()}")
                    self.badline(line)
                    self.errors += 1
        self.t1 = time.time()
        if self.errors==0:
            print(f"{fname} validated {self.records} records in {int(self.t1-self.t0)} seconds")
        else:
            print(f"{fname} had at least {self.errors} errors")
        return self.errors==0
        
    
        
if __name__=="__main__":
    from argparse import ArgumentParser,ArgumentDefaultsHelpFormatter
    parser = ArgumentParser( formatter_class = ArgumentDefaultsHelpFormatter,
                             description="Demo program for cspark module" )
    parser.add_argument('--debug',  action='store_true')
    parser.add_argument("--spark",  action="store_true", help="Run a sample program with spark")
    parser.add_argument('--maxerrors', type=int, default=10, help='max errors to report per file')
    parser.add_argument("--show",   action='store_true', help='show lines with errors')
    parser.add_argument("--position_delimited",  action='store_true', help='Parse position-specified file')
    parser.add_argument("--pipe_delimited",  action='store_true', help='Parse pipe-delimited file')
    parser.add_argument("--write_json", help='Write to JSON, using write_json as the base name')

    # Add an option for each file to validate.
    # We add them as options so that the order is not predetermined
    for vo in SPEC_CLASS_OBJECTS:
        parser.add_argument(f"--{voname(vo)}", help=f"{voname(vo)} file")

    args = parser.parse_args()

    file_errors = 0

    for vo in SPEC_CLASS_OBJECTS:
        name = voname(vo)
        fname = getattr(args,name)
        validator = Validator(vo,show=args.show,maxerrors=args.maxerrors,
                              position_delimited=args.position_delimited,pipe_delimited=args.pipe_delimited)
        if args.write_json:
            data = copy.copy(SPEC_DICT['tables'][name])

        if fname:
            if not validator.validate(fname):
                file_errors += 1
            if args.write_json:
                data['records'] = validator.records
                data['errors'] = validator.errors
            
        if args.write_json:
            with open(args.write_json + name +".json","w") as f:
                json.dump( data, f, indent=4, sort_keys=True)
    exit(file_errors)
                    
    
