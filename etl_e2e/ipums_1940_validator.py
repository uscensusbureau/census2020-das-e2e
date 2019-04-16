#!/usr/bin/env python3
#
"""
Validate the 1940 input data file using Spark
Notes: 

1. The ipums_1940 parser classes are built by the ELT system.
2. The only thing validated are the invariants.
3. This module can be imported into pyspark with "import e2e_validator" and then you can 
   use the ipums_df() and the mdf_df() to get dataframes for each.

The five items set as invariant were 
C1: Total population (invariant at the county level for the 2018 E2E)
C2: Voting-age population (population age 18 and older) (eliminated for the 2018 E2E)
C3: Number of housing units (invariant at the block level)
C4: Number of occupied housing units (invariant at the block level) 
C5: Number of group quarters facilities by group quarters type.(invariant at the block level)

"""

import sys
import os

# default values

DAS_S3ROOT='DAS_S3ROOT'
EXT1940USCB_FILE = os.environ['EXT1940USCB']
MDF_UNIT_FILE = None
MDF_PER_FILE = None


COMMENT_CHAR='#'
NUM_EXECUTORS=100
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

def parse_mdf_unit(line):
    import mdf_validator_classes
    mdf_unit = mdf_validator_classes.MDF_Unit()
    mdf_unit.parse_pipe_delimited(line)
    return mdf_unit.SparkSQLRow()

def parse_mdf_per(line):
    import mdf_validator_classes
    mdf_per = mdf_validator_classes.MDF_Person()
    mdf_per.parse_pipe_delimited(line)
    return mdf_per.SparkSQLRow()

registered_tables = set()
def is_not_comment_line(line):
    return line[0][0]!=COMMENT_CHAR

def read_and_register_df( sql_table_name, *, textFile=None, textMapper=None, csvFile = None, csvOptions=None, prefix=None ):
    from pyspark.sql import SparkSession
    spark = SparkSession.builder.getOrCreate()
    if sql_table_name in registered_tables:
        raise RuntimeError(f"{sql_table_name} already registered")
    registered_tables.add( sql_table_name )
    print(f"Loading {sql_table_name}:")
    if textFile and textMapper:
        rdd = spark.sparkContext.textFile(textFile)
        if prefix:
            rdd = rdd.filter(lambda line:line[0] == prefix)
        df  = spark.createDataFrame(  rdd.map( textMapper ), samplingRatio=1.0 )
    elif csvFile and csvOptions:
        df = spark.read.format('csv').options(**csvOptions).load(csvFile)
    else:
        raise RuntimeError("must provide (textFile and textMapper) or (csvFile and csvOptions)")
    df.persist()
    df.registerTempTable( sql_table_name )
    if args.debug:
        print("====== NEW TABLE: {}  =========".format(sql_table_name))
        df.printSchema()
        print("")
    return df

class E2E():
    def __init__(self, ipums_file=EXT1940USCB_FILE, mdf_unit_file=MDF_UNIT_FILE, mdf_per_file=MDF_PER_FILE):
        self.ipums_unit = read_and_register_df( "ipums_unit", textFile=ipums_unit_file, textMapper=parse_ipums_unit, prefix='H' )
        self.ipums_per  = read_and_register_df( "ipums_per",  textFile=ipums_per_file, textMapper=parse_ipums_per, prefix='P' )
        self.mdf_unit = read_and_register_df( "mdf_unit", textFile=mdf_unit_file, textMapper=parse_mdf_unit )
        self.mdf_per  = read_and_register_df( "mdf_per",  textFile=mdf_per_file, textMapper=parse_mdf_per )

def select(stmt):
    """Execute a SQL statement and return the results"""
    from pyspark.sql import SparkSession
    spark = SparkSession.builder.appName('e2e_validator').getOrCreate()
    return spark.sql(stmt).collect()

def select1(stmt):
    """Execute a SQL statement and return the first element of the first row"""
    from pyspark.sql import SparkSession
    spark = SparkSession.builder.appName('e2e_validator').getOrCreate()
    for row in spark.sql(stmt).collect():
        return row[0]

def verify(msg,cond):
    """Print msg. cond should be true. Return number of errors."""
    import inspect
    caller_lineno = inspect.stack()[1].lineno
    print(f"{caller_lineno}: {msg} {'PASS' if cond else 'FAIL'}")
    return 0 if cond else 1

def select_into(sql_table_name,stmt):
    from pyspark.sql import SparkSession
    spark = SparkSession.builder.getOrCreate()
    df = spark.sql(stmt)
    df.registerTempTable(sql_table_name)
    if args.debug:
        print(stmt)
        print("====== NEW TABLE: {}  =========".format(sql_table_name))
        df.show()
        print("")
    return df

if __name__=="__main__":
    from argparse import ArgumentParser,ArgumentDefaultsHelpFormatter
    parser = ArgumentParser( formatter_class = ArgumentDefaultsHelpFormatter,
                             description="Migrate an input dataset to an output dataset" )
    parser.add_argument("IPUMS_EXT1940USCB_FILE", help="Unit file", default=EXT1940USCB_FILE, nargs='?')
    parser.add_argument("MDF_UNIT",         help="Unit file", default='', nargs='?')
    parser.add_argument("MDF_PER",         help="Person file", default='', nargs='?')
    parser.add_argument("--debug", action='store_true', help='print additional debugging info')
    args  = parser.parse_args()
    
    import census_etl.ctools.cspark as cspark
    spark = cspark.spark_session(logLevel='ERROR',
                                 pyfiles = ['ipums_1940/ipums_1940_classes.py',
                                            'mdf/mdf_classes.py',
                                        ], num_executors=NUM_EXECUTORS)
    e2e = E2E(ipums_file=args.IPUMS_EXT1940USCB_FILE,
              mdf_unit_file=args.MDF_UNIT,
              mdf_per_file=args.MDF_PER)

    if args.debug:
        for table in sorted(registered_tables):
            print(f"2 rows from {table}")
            for row in select(f"SELECT * from {table} LIMIT 2"):
                print(row)
            print("Total rows in {}: {}".format(table,select1(f"SELECT COUNT(*) FROM {table}")))

    errors = 0
    failed_invariants = 0

    print("IPUMS FILE VERIFICATION")
    ipums_unit_total       = select1(f"SELECT COUNT(*) from ipums_unit")
    errors += verify(f"Number of IPUMS units: {ipums_unit_total}")

    ipums_per_total = select1(f"SELECT COUNT(*) from ipums_per")
    ipums_per_valid = select1(f"SELECT COUNT(*) from ipums_per where ipums_per.serial in (SELECT serial from ipums_unit)")
    errors += verify(f"Total IPUMS per: {ipums_per_total} number with SERIAL in a UNIT: {ipums_per_valid}",
                     ipums_per_total==ipums_per_valid)

    exit(0)

    print("MDF FILE VERIFICATION")
    mdf_unit_total = select1("SELECT COUNT(*) from mdf_unit")
    errors += verify(f"Total MDF unit: {mdf_unit_total}",
                     ipums_unit_total == mdf_unit_total)

    mdf_per_total = select1("SELECT COUNT(*) from mdf_per")
    errors += verify(f"Total MDF per: {mdf_per_total}",
                    ipums_per_total == mdf_per_total)

    mdf_per_euid_valid = select1(f"SELECT COUNT(*) from mdf_per where mdf_per.euid in (SELECT euid from mdf_unit)")
    errors += verify(f"Number of MDF pers that are placed into a MDF unit: {mdf_per_euid_valid}",
                     mdf_per_total == mdf_per_euid_valid)

    grfc_geocoded = spark.sql("select concat(tabblkst,tabblkcou,tabtractce,tabblkgrpce,tabblk) as geocode,oidtabblk from grfc")
    grfc_geocoded.registerTempTable("grfc_geocoded")
    if args.debug:
        print("grfc_geocoded")
        grfc_geocoded.show()

    ipums_unit_geocoded = spark.sql("SELECT ipums_unit.mafid AS mafid,grfc_geocoded.geocode AS geocode,ten,qgqtyp "
                                  "FROM ipums_unit LEFT JOIN grfc_geocoded ON ipums_unit.oidtb=grfc_geocoded.oidtabblk")
    ipums_unit_geocoded.registerTempTable("ipums_unit_geocoded")
    if args.debug:
        print("ipums_unit_geocoded:")
        ipums_unit_geocoded.show()

    ipums_per_geocode_counts = spark.sql("SELECT geocode,count(*) AS block_count "
                                       "FROM ipums_unit_geocoded LEFT JOIN ipums_per "
                                       "WHERE ipums_unit_geocoded.mafid=ipums_per.mafid GROUP BY geocode")
    ipums_per_geocode_counts.registerTempTable("ipums_per_geocode_counts")
    ipums_per_geocode_counts.persist()
    if args.debug:
        print("ipums_per_geocode_counts:")
        ipums_per_geocode_counts.show()
        print("Number of CEF Geocoded blocks: {}".format(ipums_per_geocode_counts.count()))

    mdf_unit_geocoded = spark.sql("SELECT CONCAT(mdf_unit.tabblkst,mdf_unit.tabblkcou,mdf_unit.tabtractce,mdf_unit.tabblkgrpce,mdf_unit.tabblk) AS geocode,"
                                  "euid,ten,gqtype FROM mdf_unit")
    mdf_unit_geocoded.registerTempTable("mdf_unit_geocoded")
    mdf_unit_geocoded.persist()
    if args.debug:
        print("mdf_geocode_counts:")
        mdf_unit_geocoded.show()
    
    mdf_per_geocode_counts = spark.sql("SELECT mdf_unit_geocoded.geocode AS geocode,count(*) AS block_count "
                                       "FROM mdf_unit_geocoded LEFT JOIN mdf_per WHERE mdf_unit_geocoded.euid = mdf_per.euid GROUP BY geocode")
    mdf_per_geocode_counts.persist()
    mdf_per_geocode_counts.registerTempTable("mdf_per_geocode_counts")
    if args.debug:
        mdf_per_geocode_counts.printSchema()
        mdf_per_geocode_counts.show()
        print("mdf_per_geocode_counts:")
        print("Number of MDF geocoded blocks: {}".format(mdf_per_geocode_counts.count()))

    print("VERIFYING INVARIANTS")
    print("C1: Total Population at the County Level (UPDATE TO DO COUNTIES)")
    failed_invariants += verify(f"CEF TOTAL POPULATION: {ipums_per_total:,}   MDF TOTAL POPULATION: {mdf_per_total:,}",
                     ipums_per_total == mdf_per_total)
    print("")

    
    print("C2: Omitted")
    print("")
    print("C3: Number of housing units (invariant at the block level)")
    select_into("ipums_unit_block_counts","SELECT geocode,count(*) AS ipums_unit_count,SUM(IF(ten=='0',0,1)) AS ipums_occupied_count FROM ipums_unit_geocoded GROUP BY geocode")
    select_into("mdf_unit_block_counts","select geocode,count(*) as mdf_unit_count,sum(if(ten=='0',0,1)) as mdf_occupied_count from mdf_unit_geocoded group by geocode")
    c34_inner = select_into("c34_inner","select ipums_unit_block_counts.geocode as G1,mdf_unit_block_counts.geocode as G2,ipums_unit_count,mdf_unit_count,ipums_occupied_count,mdf_occupied_count from ipums_unit_block_counts FULL OUTER JOIN mdf_unit_block_counts WHERE ipums_unit_block_counts.geocode=mdf_unit_block_counts.geocode")
    print(f"Number of rows in c34_inner table: {c34_inner.count():,}")
    
    c3_invariant_fail = select_into("c3_invariant_fail","select G1,G2,ipums_unit_count,mdf_unit_count from  c34_inner where ipums_unit_count!=mdf_unit_count")
    print("Number of blocks that fail invariant C3: {}".format( c3_invariant_fail.count() ))

    print("C4: Number of occupied housing units")
    c4_invariant_fail = select_into("c4_invariant_fail","select G1,G2,ipums_occupied_count,mdf_occupied_count from c34_inner where ipums_occupied_count!=mdf_occupied_count")
    print("Total of occupied housing units in CEF: {}".format( select1(" select sum(ipums_occupied_count) from c34_inner ")))
    print("Number of blocks that fail invariant C4: {}".format( c4_invariant_fail.count() ) )

    print("C5: Number of group quarters facilities by group quarters type:")
    select_into("ipums_gq_block_counts","SELECT geocode,qgqtyp,count(*) AS ipums_gq_count FROM ipums_unit_geocoded WHERE qgqtyp>='100' GROUP BY geocode,qgqtyp ")
    select_into("mdf_gq_block_counts","select geocode,gqtype,count(*) AS mdf_gq_count FROM mdf_unit_geocoded WHERE gqtype>='100' GROUP by geocode,gqtype ")
    c5_considered = select_into("c5_considered",
                "SELECT ipums_gq_block_counts.geocode AS geocode,ipums_gq_block_counts.qgqtyp AS gqtype, "
                "ipums_gq_count AS ipums_gq_count, mdf_gq_count AS mdf_gq_count "
                "FROM ipums_gq_block_counts FULL OUTER JOIN mdf_gq_block_counts "
                "WHERE ipums_gq_block_counts.geocode=mdf_gq_block_counts.geocode AND ipums_gq_block_counts.qgqtyp=mdf_gq_block_counts.gqtype")

    c5_invariant_fail  = select_into("c5_invariant_fail","SELECT * FROM c5_considered WHERE ipums_gq_count!=mdf_gq_count")
    print("Number of geocode/group_quarters combinations considered for invariant: {}".format( c5_considered.count()))
    print("Number of geocode/group_quarters combinations considered for invariant: {}".format( select1("select count(*) from c5_considered")))
    print("Number of blocks that fail invariant C5: {}".format( c5_invariant_fail.count() ) )
#          
#    
#                               "       
#    
#
#ipums_per_geocode_counts.geocode, ipums_per_geocode_counts.block_count AS ipums_block_count, mdf_per_geocode_counts.block_count AS mdf_block_count, "
#                           "ABS(ipums_per_geocode_counts.block_count - mdf_per_geocode_counts.block_count) AS error "
#                           "FROM ipums_per_geocode_counts FULL OUTER JOIN mdf_per_geocode_counts "
#                           "WHERE ipums_per_geocode_counts.geocode=mdf_per_geocode_counts.geocode AND ipums_per_geocode_counts.block_count != mdf_per_geocode_counts.block_count order by geocode")
#    bad_blocks.printSchema()
#    bad_blocks.show()
#    print("Number of blocks that have the wrong count: {}".format(bad_blocks.count()))

