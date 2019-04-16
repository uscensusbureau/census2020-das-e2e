import sys
import os
import py.test
import io

#sys.path.append( os.path.join( os.path.dirname(__file__), "..") )
sys.path.append(os.path.dirname(os.path.dirname(__file__)))
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))

import ctools.cspark as cspark

CSPARK_PATH = os.path.join(os.path.dirname(os.path.dirname(__file__)),"cspark.py")
assert os.path.exists(CSPARK_PATH)

fh_config = """
[spark]
name1.key1=value1
name2.key2: value2
"""

def test_spark_submit_cmd():
    from configparser import ConfigParser
    config = ConfigParser()
    config.read_string(fh_config)
    cmd = cspark.spark_submit_cmd(configdict=config['spark'])
    assert "name1.key1=value1" in cmd
    assert "name2.key2=value2" in cmd
    
TEST_RUN_SPARK_FILENAME='TEST_RUN_SPARK_FILENAME'
def test_run_spark():
    # Run a Spark job and then check to make sure we got the result.
    # To get the result back, we have to save it in a file. But we only want to call
    # NamedTemporaryFile once, so we store the temporary file name in an environment variable.
    # For the same reason, we can't open the file in truncate mode.

    if not cspark.spark_available():
        return                  # don't test if no Spark is available

    if TEST_RUN_SPARK_FILENAME not in os.environ:
        import tempfile
        f = tempfile.NamedTemporaryFile(delete=False, mode='w+')
        os.environ[TEST_RUN_SPARK_FILENAME] = f.name
        f.close()
        
    with open(os.environ[TEST_RUN_SPARK_FILENAME], "w+") as f:
        if cspark.spark_submit(loglevel='error',pyfiles=[CSPARK_PATH], argv=[__file__]):
            from pyspark import SparkContext, SparkConf
            import operator
            conf = SparkConf().setAppName("cspark_test:test_run_spark")
            sc   = SparkContext(conf=conf)
            sc.setLogLevel("ERROR")
            mysum = sc.parallelize(range(1000000)).reduce(operator.add)
            f.truncate(0)
            f.write("{}\n".format(mysum))
            f.close()
            exit(0)             # Spark job is finished
        f.seek(0)
        data = f.read()
        assert data=='499999500000\n'
        print("spark ran successfully")
    os.unlink(os.environ[TEST_RUN_SPARK_FILENAME])


if __name__=="__main__":
    # This is solely so that we can run under py.test
    # Don't remove it! You can also just run this program to see what happens
    # It should print "spark ran successfully."
    test_run_spark()
    
