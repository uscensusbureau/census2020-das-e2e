#
# Test Gurobi, with Spark, using pytest.
# This test just runs Spark as a sub process


import py.test
import os
import gurobipy
import warnings
import subprocess

gurobi_problem = """Maximize
obj: + 0.6 x1 + 0.5 x2
     
Subject To
  c1: + x1 + 2 x2 <= {bounds}
  c2: 3 x1 + x2 <= 2
Bounds
  x1 free
  x2 free
     
End
"""

def gurobi_spark_tester(params):

    # Get the Gurobi license variables and die if we cannot
    GRB_LICENSE_FILE = params['GRB_LICENSE_FILE']
    GRB_LOGFILE      = params['GRB_LOGFILE']
    GRB_ISV_NAME     = params['GRB_ISV_NAME']
    GRB_APP_NAME     = params['GRB_APP_NAME']

    # Get these into the environment
    os.putenv('GRB_LICENSE_FILE',GRB_LICENSE_FILE)
    assert os.path.exists(GRB_LICENSE_FILE)

    # Create a Gurobi LP file and then solve it.
    fname             = "gurobi_example_{}.lp".format(os.getpid())
    with open(fname,'w') as f:
        f.write(gurobi_problem.format(bounds = params['bounds']))

    env = gurobipy.Env.OtherEnv(GRB_LOGFILE, GRB_ISV_NAME, GRB_APP_NAME, 0, "")
    model = gurobipy.read(fname, env=env)
    model.optimize()
    sol_vals = [sol_val.X for sol_val in model.getVars()]
    return sol_vals

def run_gurobi_workers():
    """Test Gurobi under Spark, running an optimization on many different nodes"""

    from pyspark.sql import SparkSession
    NUM_RUNS = 100

    spark         = SparkSession.builder.getOrCreate()
    sc            = spark.sparkContext
    params = {'GRB_LICENSE_FILE':os.environ['GRB_LICENSE_FILE'],
              'GRB_LOGFILE':'gurobi.log', 
              'GRB_ISV_NAME': os.environ.get('GRB_ISV_NAME',''),
              'GRB_APP_NAME': os.environ.get('GRB_APP_NAME','')}


    spark_outputs = sc.parallelize( [ {**params,**{'bounds':bounds}} for bounds in range(NUM_RUNS)] ).map(gurobi_spark_tester)
    trunc_outputs = spark_outputs.collect()
    print("Retrieved {} optimizations from the spark nodes.".format( len( trunc_outputs) ))

    expected_outputs = [
        [0.8, -0.4], [0.6, 0.2], [0.4, 0.8], [0.2, 1.4], [0.0, 2.0], 
        [-0.2, 2.6], [-0.4, 3.2], [-0.6, 3.8], [-0.8, 4.4], [-1.0, 5.0], 
        [-1.2, 5.6], [-1.4, 6.2], [-1.6, 6.8], [-1.8, 7.4], [-2.0, 8.0], 
        [-2.2, 8.6], [-2.4, 9.2], [-2.6, 9.8], [-2.8, 10.4], [-3.0, 11.0], 
        [-3.2, 11.6], [-3.4, 12.2], [-3.6, 12.8], [-3.8, 13.4], [-4.0, 14.0],
        [-4.2, 14.6], [-4.4, 15.2], [-4.6, 15.8], [-4.8, 16.4], [-5.0, 17.0],
        [-5.2, 17.6], [-5.4, 18.2], [-5.6, 18.8], [-5.8, 19.4], [-6.0, 20.0],
        [-6.2, 20.6], [-6.4, 21.2], [-6.6, 21.8], [-6.8, 22.4], [-7.0, 23.0],
        [-7.2, 23.6], [-7.4, 24.2], [-7.6, 24.8], [-7.8, 25.4], [-8.0, 26.0],
        [-8.2, 26.6], [-8.4, 27.2], [-8.6, 27.8], [-8.8, 28.4], 
        [-9.0, 29.0]
    ]    

    # Now make sure that all of the runs produced the correct rules.
    import numpy as np
    import numpy.testing as npt
    print("testing gurobi runs on each worker node for consistency with expected outputs:")
    for index, (spark_output, expected_output) in enumerate(zip(trunc_outputs, expected_outputs)):
        print("Run {}".format(index+1))
        npt.assert_almost_equal(np.array(spark_output).reshape(2,1),
                                np.array(expected_output).reshape(2,1))

# This is run from py.test
def test_gurobi_spark():
    # Run the script with Spark!
    subprocess.check_call(['spark-submit',__file__,'--sparkworkers'])
    

# This is run from Spark
if __name__=="__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--nospark", help="Run the Gurobi test program without spark. Can be used without py.test", action='store_true')
    parser.add_argument("--sparkmaster", help="Run the Gurobi test on the MASTER node under spark.", action='store_true')
    parser.add_argument("--sparkworkers", help="Run on each worker (executor); to be called when we are running under spark.", action='store_true')
    args = parser.parse_args()
    if args.nospark:
        print("testing gurobi without using spark. Will only test on the MASTER node.")
        gurobi_spark_tester(1)
        exit(0)

    if args.sparkworkers:
        if "SPARK_ENV_LOADED" not in os.environ:
            raise RuntimeError("--sparkworkers provided but we are not running under spark.")
        run_gurobi_workers()
        exit(0)

    if args.sparkmaster:
        cmd = ['spark-submit',__file__,'--nospark']
        print("$ "+" ".join(cmd))
        subprocess.check_call(cmd)
        exit(0)
        
    print("This program is most easily run from py.test")

