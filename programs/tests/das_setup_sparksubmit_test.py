from configparser import ConfigParser
import os
import tempfile
import shutil

import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))

from programs.utils import ship_files2spark



configs = ConfigParser()
configs.add_section("default")
configs.set("default", "name", "DAS")
configs.add_section("setup")
configs.set("setup", "spark.name", "DAS")
#configs.set("setup", "spark.master", "local[4]")
configs.set("setup", "spark.loglevel", "ERROR")
configs.add_section("budget")
configs.set("budget", "queriesfile", "foo")

def run_test_setup():
    """
        Tests setup modules' __init__ method and setup_func
        __init__:
            check config and name are stored/accessible
        setup_func:
            check SparkSession is returned and configured as expected
    """
    from pyspark.sql import SparkSession
    spark = SparkSession.builder.appName('das_setup test').getOrCreate()

    tempdir = tempfile.mkdtemp()

    ship_files2spark(spark, tempdir)

    import programs.das_setup as ds

    setup_obj = ds.setup(config=configs, name="setup")
    assert setup_obj.config == configs
    assert setup_obj.name == "setup"

    setup_data = setup_obj.setup_func()
    assert type(setup_data) == SparkSession
    assert setup_data.conf.get("spark.master") == "yarn"

    setup_data.stop()

    shutil.rmtree(tempdir)


def test_das_setup_spark():
    if "SPARK_ENV_LOADED" not in os.environ:
        #
        # Re-run this script under Spark.
        import subprocess
        r = subprocess.run(['spark-submit', __file__], timeout=60)
        assert r.returncode == 0

if __name__=="__main__":
    if "SPARK_ENV_LOADED" in os.environ:
        run_test_setup()
    else:
        print("Please run from py.test")