#!/usr/bin/env python3
#
# migrate.py:
# The migration class

import gc
import gzip
import gzip
import json
import logging
import math
import operator
import os
import os.path
import psutil
import re
import sqlite3
import sqlite3
import subprocess
import sys
import time
from configparser import ConfigParser

# Bring in ETL part
from schema import Schema,Table,Variable,Recode,TYPE_VARCHAR,TYPE_INTEGER
from census_spec_scanner import CensusSpec

# We need to manipulate the path so that this can run under Spark...
sys.path.append( os.path.join( os.path.dirname(__file__), "ctools") )
import cspark
import dconfig
import tytable 
from stopwatch import stopwatch


#can't find pyspark, possible hack add to path
#sys.path.append("need to find spark path")
#from pyspark.accumulators import AccumulatorParam
#class DefaultDictAccumulatorParam(AccumulatorParam):
#    def zero(self, value):
#        return defaultdict(int)
#    def addInPlace(self, thing1, thing2):
#        for key, val in thing2.items():
#            thing1[key] += val
#        return thing1

def require_file(filename):
    if not dconfig.dpath_exists(filename):
        raise RuntimeError("{}: file does not exist".format(filename))

def require_no_file(filename):
    if dconfig.dpath_exists(filename):
        raise RuntimeError("{}: file exists; will not overwrite".format(filename))

def assert_has_table(db,tablename):
    if tablename not in db.table_names():
        raise RuntimeError("Table '{}' not in schema. Available tables: {}".
                           format(tablename,db.table_names()))
def stats_min(a,b):
    if a==None: return b
    if b==None: return a
    return min(a,b)

def stats_max(a,b):
    if a==None: return b
    if b==None: return a
    return max(a,b)

def stats_sum(a,b):
    if a==None: return b
    if b==None: return a
    if type(a)==str or type(b)==str:
        return ""
    return a+b

def stats_variable_names(res):
    """Return the variable names for a stats object"""
    def varname(s):
        pos = s.find(':')
        return s if pos==-1 else s[0:pos]
    return set( [ varname(key) for key in res.keys()] )

def stats_reducer(d1,d2):
    """Given two dicts, d1 and d2, return a dict that can be used as d1 for future stats calculations. Stats are kept in variables that end with a :, so don't use them"""
    res = {}
    for key in stats_variable_names(d1).union(stats_variable_names(d2)):
        res[key + ":min"] = stats_min( d1.get(key, d1.get(key+":min", None)), d2.get(key, d2.get(key+":min", None)))
        res[key + ":max"] = stats_max( d1.get(key, d1.get(key+":max", None)), d2.get(key, d2.get(key+":max", None)))
        res[key + ":sum"] = stats_sum( d1.get(key, d1.get(key+":sum", None)), d2.get(key, d2.get(key+":sum", None)))

    count = 0
    if len(d1): count += d1.get(':count',1)
    if len(d2): count += d2.get(':count',1)
    res[':count'] = count
    return res
    
def print_stats(res,key):
    print("res=",res)
    print("count={}  min={}  sum={}  max={}".format(res[':count'],res[key+":min"],res[key+":sum"],res[key+":max"]))

def census_person_polynominal(rec):
    # Compute the 'polynominal' of person attributes:
    # Typical record:
    # {'RECTYPE': 'P', 'YEAR': 1920, 'DATANUM': 3, 'SERIAL': 1, 'PERNUM': 1, 'PERWT': 100, 'SLWT': 100, 'RELATE': 1, 
    # 'RELATED': 101, 'SEX': 1, 'AGE': 60, 'RACE': 2, 'RACED': 200, 'HISPAN': 0, 'HISPAND': 0, 'HHLINK': 1}
    assert rec['RECTYPE']=='P'
    return rec['YEAR'] + rec['PERNUM']*3 + rec['RELATE']*5 + rec['SEX']*7 + rec['AGE']*11 + rec['RACE']*13

class Migrate:
    """Data Migration Class. Reads data, applies compiled python code, and writes the result to output files."""
    def __init__(self,*,outdir=None,dry_run=False, limit=None, dumpdb=None, spark_context=None):
        self.outdir = outdir
        self.dry_run = dry_run
        self.limit   = limit
        self.dumpdb  = dumpdb
        self.spark_context = spark_context
        logging.info("OUTDIR={}".format(outdir))
        dconfig.dmakedirs(outdir)
        from types import ModuleType
        self.context = ModuleType('context')
    
    def compile_and_exec(self,code):
        logging.info("CODE:")
        for line in code.split("\n"):
            logging.info(line)
        exec( compile( code, '', 'exec'), self.context.__dict__)

    #def add_spark_to_environment(self):
    #    self.context.__dict__['sc'] = self.spark_context
    #    self.context.__dict__["DefaultDictAccumulatorParam"] = DefaultDictAccumulatorParam

    #def undo_spark(self):
    #    self.context.__dict__['sc'] = None

    def add_tables_to_environment(self,db):
    # Add the variables for every table as a string.
    # This way, Table[FOO] = 'bar'  becomes Table['FOO'] = 'bar'
        for table in db.tables():
            logging.info("Adding table {} to the environment".format(table.name))
            for v in table.vardict.values():
                self.context.__dict__[v.name] = v.name

    def create_process_record_function(self,*,source,dest,body):
        # Compile a function to do the step
        func_lines = []
        func_lines.append(    "def process_record({},default_output_record):".format(source))
        if source != dest:
            func_lines.append("    import copy")
            func_lines.append("    {}=copy.deepcopy(default_output_record) # Create a directory for output".format(dest))
        for line in body:
            func_lines.append("    {}        # a line from the config file".format(line))
        func_lines.append(    "    return {}".format(dest))
        func_lines.append("")   # give us a ending \n
        self.compile_and_exec("\n".join(func_lines))
        
    def create_filter_record_function(self,*,dest,body):
        func_lines = []
        func_lines.append(   "def filter_record({}):".format(dest))
        func_lines.append(   "    return {}".format(body))
        self.compile_and_exec("\n".join(func_lines))

    def process_bulkload(self, db, dest, kvargs, lines):
        """BULK_LOAD is used to implement joins, but it can really load any table into an SQLite3 database.
        Compute the filename for the dbm file that maps `key` to dictionaries in `table_source`
        """

        logging.info("process_bulkload db={} dest={} kvargs={}".format(db.name,dest,kvargs))

        if len(lines)>1:
            raise RuntimeError("Continuation lines not allowed in BULK_LOAD statement")

        # We create a function that creates the SQLite3 database
        # We should be able to restart by seeing how many records are in the database and skiping that many
        # records in the input file.

        filename     = os.path.join(self.outdir,dest + ".db")
        logging.info("SQLite3 database {}  exists: {}".format(filename,os.path.exists(filename)))
        if self.dry_run and not os.path.exists(filename):
            logging.info("DRY RUN: Database will be created; Returning")
            return False

        conn = sqlite3.connect(filename)
        c = conn.cursor()
        try:
            c.execute("select value from metadata where key='count'")
            res = c.fetchall()
            if len(res)==0:
                logging.error("Schema present in {} but no count. Database is incomplete. Stopping.".format(filename))
                raise RuntimeError("invalid database file "+filename)
            logging.info("Schema is present. {} records in file".format(res[0][0]))
            if self.dry_run:
                logging.info("DRY RUN: Returning")
                return False
        except sqlite3.OperationalError:
            # Create the database 
            logging.info("Create data, metadata and data_idx")
            if self.dry_run:
                logging.info("DRY RUN: Returning")
                return False

            c.execute("CREATE TABLE data (key varchar,value varchar);")
            c.execute("CREATE INDEX data_idx ON data (key);")
            c.execute("CREATE TABLE metadata (key varchar,value varchar);")

            # Create the key function, compile it into the environment,
            # and use the key function to load up the database

            source = kvargs['table_source']
            key    = kvargs['key']
            func   = "def keyfunc({}):return {}\n".format(source,key)
            self.compile_and_exec(func)

            # Read the input table. Use keyfunc to extract the keys. Put it into the sqlite3 database
            count = 0
            sw = stopwatch()
            sw.start()
            for source_record in db.read_records_as_dicts(tablename=source):
                # Convert any bytes values in source_record to strings
                for key in source_record.keys():
                    if type(source_record[key])==bytes:
                        source_record[key] = source_record[key].decode('utf-8')
                key_str   = self.context.keyfunc(source_record)
                value_str = json.dumps(source_record)
                c.execute("insert into data (key,value) values (?,?);",(key_str,value_str))
                count += 1
                if count % BULK_LOAD_REPORTING_INTERVAL==0:
                    conn.commit()      
                    gc.collect()
                    rate = int(BULK_LOAD_REPORTING_INTERVAL / sw.elapsed())
                    mem     = PROCESS.memory_info().rss
                    logging.info("count={:,}  rate={} records/sec elapsed time= {} pythonMemoryUsed={:,}".
                                 format(count,rate,int(sw.elapsed()), mem))
                if count==self.limit:
                    break
            c.execute("insert into metadata (key,value) values (?,?);",("count",count))
            conn.commit()

        # Create a function that can access the values
        dest_cursor = dest+"_cursor"
        self.context.__dict__[dest_cursor] = conn.cursor()
        if self.limit:
            limit = " LIMIT {} ".format(self.limit)
        else:
            limit = ""

        func = ("def {}(key):\n".format(dest) +
                "    import json;\n" +
                "    cursor={}\n".format(dest_cursor) +
                "    cursor.execute('select value from data where key=?;',(key,))\n" + 
                "    row = cursor.fetchone()\n" +
                "    return json.loads(row[0])\n" +
                "def dumpdb():\n" +
                "    for (key,value) in {}.execute('select key,value from data {}'):\n".format(dest_cursor,limit) +
                "        print(key,'=',value)\n")
        self.compile_and_exec(func)
        if self.dumpdb:
            self.context.__dict__['dumpdb']()
        return True
                
    def output_records(self, db, dest, kvargs, lines):
        # copy records from the input table to the output table
        # using the output format

        logging.info("output_records db:{} dest:{} kvargs:{} ".format(db.name,dest,kvargs))

        input_table_name  = kvargs['input_table']  ; assert_has_table(db,input_table_name)
        output_table_name = kvargs['output_table'] ; assert_has_table(db,output_table_name)
        output_filename   = kvargs['output_filename']
        output_delimiter  = kvargs['output_delimiter']
        write_header      = kvargs.get('write_header',True) # default True
        output_mode       = kvargs.get('output_mode','w') # get the mode, default to 'w'
        filter_expr       = kvargs.get('filter','True')
        random            = kvargs.get('random',False) 
        header            = kvargs.get('header',False)
        context = self.context

        self.create_process_record_function(source=input_table_name,dest=output_table_name,body=lines[1:])
        self.create_filter_record_function(dest=output_table_name,body=filter_expr)

        if self.dry_run:
            logging.info("DRY RUN: Returning")
            return

        # Now run it:
        # 1. Read a line of the source file into a dictionary with the table name
        # 2. Apply the function
        # 3. Write the output line
        # 4. Loop until there are no data left to process

        sw    = stopwatch().start()
        count_input = 0
        count_output = 0
        output_table = db.get_table(output_table_name)
        default_output_record = output_table.default_record()

        ## This is where the Spark code goes, if we are using Spark
        if self.spark_context:
            print("spark_context=",self.spark_context)
            print("****************************")
            input_table = db.get_table(input_table_name)
            print("reading input from ",input_table.filename)
            infile_raw = self.spark_context.textFile( input_table.filename )
            if header:
                the_header = infile_raw.first()
                infile     = infile_raw.filter(lambda line: line!=the_header)
            else:
                infile     = infile_raw

            infile_records = infile.map(input_table.parse_line_to_dict) # every record is a dictionary

            output_records = infile_records.map(lambda f:context.process_record(f, default_output_record)) # every record is a dictionary
            output_records_filtered = output_records.filter(context.filter_record)
            output_record_lines = output_records_filtered.map( lambda f:output_table.unparse_dict_to_line(f,delimiter=output_delimiter ))

            # Cache the items that we will need to compute the sizes of
            infile.persist()
            output_records_filtered.persist()

            # Compute the lines
            count_input = infile.count()
            count_output = output_records_filtered.count()
            
            # TODO: Generate the output
            # output_record_lines.saveAsTextFile(output_filename)

            # TODO: Optionally generate a histogram

        else:
            ## Not using Spark
            output_table.open_csv(fname=os.path.join(self.outdir,output_filename),
                                  delimiter=output_delimiter, mode=output_mode, write_header=write_header)

            for source_record in db.read_records_as_dicts(tablename=input_table_name,
                                                          limit=self.limit,
                                                          random=random):
                # run the compiled code with the record
                count_input += 1
                dest_record = self.context.process_record(source_record,default_output_record) 

                if self.context.filter_record(source_record,dest_record):
                    output_table.write_dict(dest_record)
                    count_output += 1
                if count_input % 100000==0:
                    rate = count_input/sw.elapsed()
                    logging.info("Processed {:,} lines, {:,} lines output, {:,} lines/sec".
                                 format(count_input, count_output,int(rate)))
            ## Dont without Spark
        logging.info("{:,} records read, {:,} records written".format(count_input,count_output))
        return True

    def process_ipums_hierarchical_file(self, db, dest, kvargs, lines):
        """Reads the hierarchical file and outputs a Household and Person file."""

        logging.info("process_ipums_hierarchical_file db:{} dest:{} kvargs:{} ".format(db.name,dest,kvargs))
        if self.dry_run:
            logging.info("DRY RUN: Returning")
            return

        ipums_dat  = os.path.expandvars( kvargs['ipums_dat'] )
        h_table    = kvargs['h_table'] # table name to use for households
        h_filename = os.path.join(self.outdir, kvargs['h_filename'] ) # filename where households get written
        p_table    = kvargs['p_table'] # table name to use for households
        p_filename = os.path.join(self.outdir, kvargs['p_filename'] ) # filename where households get written

        count = dict()

        # Make sure input file exists and output file does not exist
        require_file(ipums_dat)
        require_no_file(h_filename)
        require_no_file(p_filename)

        # Make sure that H and P tables exist (for reading IPUMS tables) and that the output tables exist too
        for name in ['H','P',h_table,p_table]:
            if name not in db.table_names():
                raise RuntimeError("Cannot process IPUMS hierarchial file: "
                                   "table type '{}' does not exist in schema".format(name))

        # Now either run single-threaded or run with Spark
        if self.spark_context:
            print("spark_context=",self.spark_context)
            print("****************************")
            # Calculate and print the number of lines...
            infile = self.spark_context.textFile( dconfig.dpath_expand(ipums_dat)).cache()
            lines = infile.map(lambda a:1).reduce(operator.add)
            print("Input file {} has {:,} lines".format(ipums_dat,lines))

            # Do something!
            raise RuntimeError("FINISH IMPLEMENTING THIS")
        else:
            ### RUNNING WITHOUT SPARK

            # for each rectype,  open the output file
            db.get_table(h_table).open_csv( dconfig.dpath_expand(h_filename), mode='w')
            db.get_table(p_table).open_csv( dconfig.dpath_expand(p_filename), mode='w')

                               
            sw = stopwatch().start()
            rcount = 0
            wcount = {"H":0,"P":0}

            # Read the ipums file, which might be compressed
            with (gzip.open if ipums_dat.endswith(".gz") else open)(ipums_dat,"rt") as f:
                for line in f:
                    # The IPUMS standard is that the record type is in the first column
                    rectype = line[0]    
                    assert rectype=='H' or rectype=='P'
                    table   = db.get_table(rectype)

                    # Note that we got data record for a specific table
                    data = table.parse_line_to_dict(line) # Parse the line into a data array
                    # Process recodes
                    db.recode_load_data(rectype,data)     # Store the raw data for recoding
                    db.recode_execute(rectype,data)       # Process the recodes

                    # Write the data for this record, which will write to the csv file that we created
                    table.write_dict(data)
                    wcount[rectype] += 1
                    rcount +=1
                    if rcount%100000==0:
                        rate = rcount / sw.elapsed()
                        print("Processed {:,} lines, {:,} lines/sec".format(rcount,int(rate)))
                    if rcount==self.limit:
                        print("Limit {} reached.".format(rcount))
                        break
            ### END OF NO SPARK 
        logging.info("input records processed: {}".format(rcount))
        logging.info("output records processed: H:{} P:{}".format(wcount['H'],wcount['P']))
        return True

    def process_overrides(self, db, dest, kvargs, lines):
        """Applies overrides to the schema. keyword=value"""
        logging.info("process_overrides db:{} dest:{} kvargs:{} ".format(db.name,dest,kvargs))
        keyword = kvargs['keyword']
        db.create_overrides(keyword)
        return True

    def apply_defaults(self, db, dest, kvargs, lines):
        """Applies defaults to the database"""
        table        = db.get_table(kvargs['table'])
        default_text = kvargs['default_text']
        table.find_default_from_allowable_range_descriptions(default_text)
        # Log the defaults
        logging.info("Defaults for table: {}".format(table.name))
        for var in table.vars():
            if var.default:
                logging.info("  {}: {}".format(var.name,var.default))
        return True

    def table_stats(self, db, dest, kvargs, lines):
        """The TABLE_STATS(table=table_name) function. Omit table= to get stats on all tables."""
        if 'table' in kvargs:
            tables = [db.get_table(kvargs['table'])]
        else:
            tables = db.tables()
        options = kvargs.get('options','')
        done = False
        for table in db.tables():
            print("======================= {} =======================".format(table.name))
            if 'dump' in options:
                print("schema dump:")
                table.dump()
                print("")
            if 'head' in options:
                print("First 5 records:")
                for source_record in db.read_records_as_dicts(tablename=table.name, limit=5):
                    print(source_record)
                print("")
            # Compute single-variable stats on each of the variables
            sw = stopwatch().start()
            print("Computing statistics...")
            stats = {}
            census_checksum = 0
            
            if self.spark_context:
                print("Using spark to read {} ... assuming first line has headings".format(table.filename))
                sc = self.spark_context
                data = sc.textFile(table.filename)
                header = data.first() # extract the header
                stats = data.filter(lambda row:row!=header).map(table.parse_line_to_dict).reduce(stats_reducer)
            else:
                try:
                    for source_record in db.read_records_as_dicts(tablename=table.name,limit=self.limit):
                        if source_record['RECTYPE']=='P':
                            census_checksum += census_person_polynominal(source_record)
                        stats = stats_reducer(source_record, stats)
                except KeyboardInterrupt as e:
                    print("*** KeyboardInterrupt at count: {}".format(stats[':count']))
                    done = True
            if stats:
                print("total records: {}   speed: {:8.0f} records/sec".format( stats[':count'], stats[':count']/sw.elapsed()))
                tt = tytable.ttable()
                tt.add_head(['variable','min','avg','max'])
                tt.set_col_alignment(1,tytable.ttable.RIGHT)
                tt.set_col_alignment(2,tytable.ttable.RIGHT)
                tt.set_col_alignment(3,tytable.ttable.RIGHT)
                for key in stats_variable_names(stats):
                    try:
                        tt.add_data([key, stats[key+":min"], stats[key+":sum"] / stats[':count'], stats[key+":max"]])
                    except TypeError:
                        tt.add_data([key, stats[key+":min"], "",                                  stats[key+":max"]])
                print(tt.typeset(mode=tytable.TEXT))
            if census_checksum:
                print("Census checksum: {}".format(census_checksum))
            print("")
            if done:
                return True          # had the keyboard abort
        return True

    def dump_context(self):
        logging.info("Migrate Context:")
        for key in self.context.__dict__:
            logging.info("{} = {}".format(key, self.context.__dict__[key]))
        return True

    def command(self,*,db,cmd,dest,kvargs,lines):
        """Process a named command"""

        print("*** cmd=",cmd)

        if cmd=='BULK_LOAD':
            return self.process_bulkload(db, dest, kvargs, lines)
        elif cmd=='OUTPUT_RECORDS':
            return self.output_records(db, dest, kvargs, lines)
        elif cmd=='PROCESS_IPUMS':
            return self.process_ipums_hierarchical_file(db, dest, kvargs, lines)
        elif cmd=='OVERRIDE':
            return self.process_overrides(db, dest, kvargs, lines)
        elif cmd=='APPLY_DEFAULTS':
            return self.apply_defaults(db, dest, kvargs, lines)
        elif cmd=='TABLE_STATS':
            return self.table_stats(db, dest, kvargs, lines)
        else:
            return False
