#!/usr/bin/env spark-submit
#
# Developing a reader for IPUMS using Spark
# 


from pyspark.sql import SparkSession
import os

APP_NAME = "ipums reader"
FNAME = os.environ['IPUMS_1940_FULL_COUNT']


if __name__=="__main__":
    spark = SparkSession.builder.appName(APP_NAME).getOrCreate()
    spark.sparkContext.setLogLevel('WARN')
    sc = spark.sparkContext

    ipums_file = sc.textFile(FNAME)
    print("{} lines: {}".format(FNAME,ipums_file.count()))

    # The format of the file is:
    # H1940<household 1 record follows>
    # P1940<household 1, person1>
    # P1940<household 1, person2>
    # ...
    # H1940<household 2 record follows>
    # P1940<household 2, person2>
    # P1940<household 2, person2>
    # ...

    # Each record has a household serial number, which makes them easy to put together

    # Make the households table:
    df_h = ipums_file.filter(lambda line:line[0]=='H')
    print("Number of H records: {}".format(df_h.count()))
    print("First 10:")
    print("\n".join(df_h.take(10)))

    df_p = ipums_file.filter(lambda line:line[0]=='P')
    print("number of P records:")
    print(df_p.count())
    print("\n".join(df_p.take(10)))
    

