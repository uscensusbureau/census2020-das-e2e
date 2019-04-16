# setup tests
# William Sexton

"""
   Run pytest inside programs dir.
"""

import sys
import os

assert os.path.basename(sys.path[0]) == "das_decennial"

sys.path.append(os.path.join(sys.path[0], "das_framework"))

# # should be able to find pyspark now
sys.path.append("/usr/lib/spark/python")
sys.path.append("/usr/lib/spark/python/lib/py4j-src.zip")



from configparser import ConfigParser


configs = ConfigParser()
configs.add_section("default")
configs.set("default", "name", "DAS")
configs.add_section("setup")
configs.set("setup", "spark.name", "DAS")
# configs.set("setup", "spark.master", "local[4]")
configs.set("setup", "spark.loglevel", "ERROR")
configs.add_section("budget")
configs.set("budget", "queriesfile", "foo")

def test_setup(spark):
    """
        Tests setup modules' __init__ method and setup_func
        __init__:
            check config and name are stored/accessible
        setup_func:
            check SparkSession is returned and configured as expected
    """
    import programs.das_setup as ds_spark
    from pyspark.sql import SparkSession

    setup_obj = ds_spark.setup(config=configs, name="setup")
    assert setup_obj.config == configs
    assert setup_obj.name == "setup"

    setup_data = setup_obj.setup_func()
    assert setup_data.sparkContext == spark.sparkContext
    assert type(setup_data) == SparkSession
    #assert setup_data.conf.get("spark.master") == "yarn"

    setup_data.stop()


