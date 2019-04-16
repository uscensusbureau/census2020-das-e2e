import os
import pytest
import tempfile
import shutil
import sys

from programs.utils import ship_files2spark

if 'SPARK_HOME' not in os.environ:
    os.environ['SPARK_HOME'] = '/usr/lib/spark'


@pytest.fixture(scope="module")
def spark():

    tempdir = tempfile.mkdtemp()

    # Add the directory with pyspark and py4j (in shape of zip file)
    sys.path.append(os.path.join(os.environ['SPARK_HOME'], 'python'))
    sys.path.append(os.path.join(os.environ['SPARK_HOME'], 'python', 'lib', 'py4j-src.zip'))

    # Create local Spark session (so that we don't have to put local files to HDFS)
    from pyspark.sql import SparkSession
    spark = SparkSession.builder.appName('2018 e2e programs module test')\
        .config("spark.submit.deployMode", "client")\
        .config("spark.authenticate.secret", "111")\
        .config("spark.master", "local[*]")\
        .getOrCreate()

    ship_files2spark(spark, tempdir)

    yield spark

    spark.stop()

    shutil.rmtree(tempdir)
