# -*- coding: utf-8 -*-
#
# import matplotlib
# matplotlib.use('Agg')

from configparser import ConfigParser
#from importlib import import_module
import io
import os
import glob
import tempfile
import pandas as pd
import numpy as np
import pytest
import shutil
import zipfile

try:
    from pyspark.sql import SparkSession, Row
    from pyspark     import SparkFiles
except ImportError:
    pass

import sys
# Simson commented these out, because their action determines on where py.test is run from.
# Instead, we use the mechanism that are now present.
#
#sys.path.append(".")
#sys.path.append( os.getcwd())
sys.path.append( os.path.dirname(__file__) )
sys.path.append( os.path.join( os.path.dirname(__file__), ".."))
sys.path.append( os.path.join( os.path.dirname(__file__), "../.."))


#import bds_das_spark_setup as setup
import spark_sql_das_engine
import spark_sql_aggregator
import noisealgorithms

variosalgos = """
[engine]
epsilon = 1.0
alpha = 0.1

[t_a]
algorithm = NoNoise

[t_b]
algorithm = SmoothLaplace
sensitivity_field = ssmax
alpha = .05
delta = .05
epsilon_fraction = .4

[t_c]
algorithm = SmoothGamma
sensitivity_field = ssmax
epsilon_fraction = .4

[t_d]
algorithm = LogLaplace
sensitivity_field = ssmax
alpha = .05
epsilon_fraction = .4

[t_e]
algorithm = LaplaceDP
sensitivity = 2.0
epsilon_fraction = 2.0

[t_f]
algorithm = Pub1075
emp_field = f
num_returns_field = numret
largest_emp_field = ssmax
second_largest_field = sectop
geolevel = state
p = 0.9
"""

config4engine = """
[variables2addnoise]
t = b,c;d
t2 = a,b

[t2_a]
algorithm = SmoothLaplace
sensitivity_field = ssmax
alpha = .05
delta = .05
epsilon_fraction = .3

[t2_b]
algorithm = SmoothLaplace
sensitivity_field = ssmax
alpha = .05
delta = .05
epsilon_fraction = .4
"""

confstringddict = """
[prenoise_tables]
t = SELECT location,SUM(COUNT) AS sumc FROM inputtable GROUP BY location
t2 = SELECT location,COUNT(*) AS count FROM inputtable GROUP BY location

[out_tables]
outt = SELECT t.sumc, t2.count FROM t LEFT JOIN t2 ON t.location = t2.location

[variables2addnoise]
t = sumc
t2 = count
"""

engineseeded = """
[engine]
engine = spark_sql_das_engine.engine
epsilon = 1.0
seed = 101
"""

engineurandom = """
[engine]
engine = spark_sql_das_engine.engine
epsilon = 1.0
"""

nonoise = """
[t_sumc]
algorithm = NoNoise

[t2_count]
algorithm = NoNoise
    """

laplacedp = """

[t_sumc]
algorithm = LaplaceDP
epsilon_fraction = 1.0

[t2_count]
algorithm = LaplaceDP
epsilon_fraction = 1.0
        """

algreprs = [
        "NoNoiseAlgorithm:{'varname': 'a', 'algorithm': 'NoNoise'}",
        "SmoothLaplaceAlgorithm:{'varname': 'b', 'alpha': 0.05, 'algorithm': 'SmoothLaplace', 'delta': 0.05, 'epsilon': 0.4}",
        "SmoothGammaAlgorithm:{'varname': 'c', 'alpha': 0.1, 'algorithm': 'SmoothGamma', 'epsilon': 0.4}",
        "LogLaplaceAlgorithm:{'varname': 'd', 'algorithm': 'LogLaplace', 'alpha': 0.05, 'epsilon': 0.4}",
        "LaplaceDPAlgorithm:{'varname': 'e', 'algorithm': 'LaplaceDP', 'sensitivity': 2.0, 'epsilon': 2.0}",
        "Pub1075Algorithm:{'varname': 'f', 'algorithm': 'Pub1075', 'p': 0.9}"
]

datadict = {
        'name'   :   list('abcdefghijklmnopqrstuvwxyz'),
        'location':  list('aaaaaaaaabbbbbbbbccccccccd'),
        #'count': [601,250,530,119,828,625,941,64,838,795,673,288,931,740,275,257,876,995,547,477,137,34,319,397,724,269],
       'count': 26*[100],
   }


@pytest.fixture(scope="session")
def df():
    """ Pytest fixture for the test data"""
    spark = SparkSession.builder.getOrCreate()

    submodules = ["ctools", "dfxml/python/dfxml"]

    # Make tempdir for zipfile
    tempdir = tempfile.mkdtemp()

    # Find the submodule files and zip them
    zipf = zipfile.ZipFile(tempdir + '/submodules.zip', 'w', zipfile.ZIP_DEFLATED)
    for submodule in submodules:
        submodule_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), submodule)
        pyfiles = [fn for fn in glob.iglob('{}/**/*.py'.format(submodule_dir), recursive=True) if not "test" in fn]

        for fullname in pyfiles:
            zipf.write(fullname, arcname=submodule.split('/')[-1]+fullname.split(submodule_dir)[1])

    # Add the files of the main module
    for fullname in glob.glob(os.path.dirname(os.path.dirname(__file__))+'/*.py'):
        zipf.write(fullname, arcname=os.path.basename(fullname))

    zipf.close()
    spark.sparkContext.addPyFile(zipf.filename)

    yield spark.sparkContext.parallelize([Row(**row) for index, row in pd.DataFrame(datadict).iterrows()]).toDF()
    shutil.rmtree(tempdir)


def test_noisealg_repr():
    """ Test the __repr__ and noisify functions of the NoiseAlgorithms, and their creation by engine"""

    # Make config
    config = ConfigParser()
    config.read_file(io.StringIO(variosalgos))

    # Make dummy engine
    e = spark_sql_das_engine.engine(config=config)

    # Make all the supported noise algorithms
    algs = [e.create_noise_algorithm_by_name("t",var) for var in list("abcdef")]

    # Compare __repr__ outputs to what they are supposed to be
    for i,alg in enumerate(algs):
        assert algreprs[i] == repr(alg)

    # Test the noisify functions (the answers are calculated by looking at algorithm descriptions, not the code)
    assert algs[0].noisify([{'a': 100}]) == 100
    assert abs(algs[1].noisify([{'b': 100, 'ssmax': 200}, {'b': .816062}]) - 150) < 1e-3
    assert abs(algs[2].noisify([{'c': 100, 'ssmax': 200}, {'c': .10972505}]) - 1406.3203865411335) < 1e-3
    assert abs(algs[3].noisify([{'d': 100, 'ssmax': 200}, {'d': .75}]) - 122.10774911533412) < 1e-3
    assert abs(algs[4].noisify([{'e': 100}, {'e': .816062}]) - 101) < 1e-3
    assert abs(algs[5].noisify([{'f': 100, 'ssmax': 200, 'sectop': 150, 'numret': 50}]) - 0) < 1e-3
    assert abs(algs[5].noisify([{'f': 1000, 'ssmax': 20, 'sectop': 15, 'numret': 50}]) - 1000) < 1e-3

    return

def test_agg_get_prenoise(df):
    """ Tests pre-noise aggregation. Assert correct answers to the SQL queries """

    configstring = """
[prenoise_tables]
t = SELECT location,SUM(COUNT) AS count FROM inputtable GROUP BY location
t2 = SELECT location,COUNT(*) AS count FROM inputtable GROUP BY location
    """

    config = ConfigParser()
    config.read_file(io.StringIO(configstring))
    #
    # # Counts corresponding to the data
    # counts = {'everything': 62, 'byind': 10}

    # Create sql table view for queries
    df.createOrReplaceTempView('inputtable')

    # Create the aggregator and run queries
    a = spark_sql_aggregator.PreNoiseAggregator(config=config).run_queries()

    # Compare results
    assert (a.tables['t'].toPandas().set_index('location').sort_index() - pd.DataFrame(
        {'location': list('abcd'), 'count': [900, 800, 800, 100]}).set_index('location').sort_index()).as_matrix().sum() == 0

    assert (a.tables['t2'].toPandas().set_index('location').sort_index() - pd.DataFrame(
        {'location': list('abcd'), 'count': [9, 8, 8, 1]}).set_index(
        'location').sort_index()).as_matrix().sum() == 0



def test_agg_get_postnoise(df):
    """ Test creation of output tables / conversion to Pandas / adding new variables / dropping extra variables """

    configstring = """
[engine]
engine = temptestenginemodule.engine

[out_tables]
t = SELECT location,SUM(COUNT) AS count FROM inputtable GROUP BY location
t2 = SELECT location,count FROM inputtable

[out_tables_add_vars_pandas]
t = count2
t2 = count3; count2

[out_tables_aggregate_pandas]
t2 = location; sum

[out_tables_drop_vars_pandas]
t = count

[validator]
cell_id_vars = name,location
    """

    config = ConfigParser()
    config.read_file(io.StringIO(configstring))


    # Create engine module file inheriting from spark_sql_das_engine, with functions for adding new vars in Pandas
    tempdir = tempfile.mkdtemp()
    sys.path.append(tempdir)
    with open(tempdir + '/temptestenginemodule.py', 'w') as f:
        f.write("""
import spark_sql_das_engine

class engine(spark_sql_das_engine.engine):
    pass

def count2(df):
    df['count2'] = df['count']*df['count']
    return df
    
def count3(df):
    df['count3'] = df['count']*df['count']*df['count']
    return df
        """)

    import temptestenginemodule

    # Create sql table view for queries
    df.createOrReplaceTempView('inputtable')


    # Create the output (true) tables aggregator and run queries
    true_agg = spark_sql_aggregator.TrueAggregator(config=config).run_queries().make_pandas_out_tables()


    assert sum(true_agg.tables['t'].set_index('location').sort_index().as_matrix().transpose()[0] - [810000,640000,640000,10000]) == 0
    assert sum(true_agg.tables['t2'].as_matrix()[0] - [2600,26000000,6760000]) == 0

    shutil.rmtree(tempdir)

def test_engine_create_noisifiers():
    """ Test noisifiers creation structure and epsilon calculations"""

    config = ConfigParser()
    config.read_file(io.StringIO(variosalgos+config4engine))
    e = spark_sql_das_engine.engine(config=config)
    assert repr(e.noisifiers['t'][0][0]) == algreprs[1]
    assert repr(e.noisifiers['t'][0][1]) == algreprs[2]
    assert repr(e.noisifiers['t'][1][0]) == algreprs[3]
    assert repr(e.noisifiers['t2'][0][0]) == "SmoothLaplaceAlgorithm:{'varname': 'a', 'alpha': 0.05, 'algorithm': 'SmoothLaplace', 'delta': 0.05, 'epsilon': 0.4}"
    assert repr(e.noisifiers['t2'][0][1]) == algreprs[1]

    # Calculate master epsilons:
    # epsilon set in [engine] is 1.0, every variable is assigned fraction of 0.4 (a in t2 set to 0.3, but re-assigned 0.4
    # because it is composable with b)

    # in table 't': b and c are composable so get 0.4 (no sum), another 0.4 for d - 0.8 total
    assert e.table_epsilons['t'] == 0.8

    # in table 't2': a and b are composable, so 0.4
    assert e.table_epsilons['t2'] == 0.4

    # and the total is 0.8 + 0.4 = 1.2
    assert abs(e.epsilon_effective - 1.2) < 1e-5

def test_engine_nonoise(df):
    """ Test the engine run with no noise. Check counts and that true and noisy tables are identical """

    config = ConfigParser()
    config.read_file(io.StringIO(engineurandom + confstringddict + nonoise))

    # Create the engine
    e = spark_sql_das_engine.engine(config=config)
    original_data = {'original_data': {'inputtable':df}}

    # Run the engine
    assert e.willRun()

    private_tables = e.run(original_data)[0]
    true_tables = original_data["true_tables"]

    for tname, table in private_tables.items():
        # Check counts
        assert (table.as_matrix().shape[0] == 4)
        # Check that tables are identical
        assert (true_tables[tname].as_matrix() == private_tables[tname].as_matrix()).all()

def test_engine_reproducible(df):
    """ Test the engine run with reproducible results. Check counts that noisy tables are those that come of the seed """
    config = ConfigParser()
    config.read_file(io.StringIO(engineseeded + confstringddict + laplacedp))
    original_data = {'original_data': {'inputtable': df}}

    # Create the engine
    e = spark_sql_das_engine.engine(config=config)

    # Run the engine
    assert e.willRun()

    np.random.seed(e.getint("seed", default=101, section="engine"))
    private_tables = e.run(original_data)[0]
    for tname, table in private_tables.items():
        # Check counts
        assert (table.as_matrix().shape[0] == 4)
        # Check that tables are identical
        assert sum(sum(abs(private_tables[tname].as_matrix() - np.array(
            [[100.03334714, 1.46291515], [800.15237662, 9.10199919], [797.13439125, 7.51212961],
             [898.93010144, 10.54752546]])))) < 1e-7

def test_engine_urandom(df):
    """ Test the engine run with random results. Check counts and that there is some noise, but not large"""
    config = ConfigParser()
    config.read_file(io.StringIO(engineurandom + confstringddict + laplacedp))

    # Create the engine
    e = spark_sql_das_engine.engine(config=config)
    original_data = {'original_data': {'inputtable': df}}

    # Run the engine
    assert e.willRun()

    private_tables = e.run(original_data)[0]
    true_tables = original_data["true_tables"]

    # Create the engine
    e1 = spark_sql_das_engine.engine(config=config)
    original_data = {'original_data': {'inputtable': df}}

    # Run the engine
    assert e1.willRun()

    private_tables1 = e1.run(original_data)[0]

    for tname, table in private_tables.items():
        # Check counts
        assert (table.as_matrix().shape[0] == 4)
        # Check that tables are identical
        assert sum(sum(abs(private_tables[tname].as_matrix() - true_tables[tname].as_matrix()))) > 0.1
        assert sum(sum(abs(private_tables[tname].as_matrix() - true_tables[tname].as_matrix()))) < 50
        assert sum(sum(abs(private_tables[tname].as_matrix() - private_tables1[tname].as_matrix()))) > 0.1
        assert sum(sum(abs(private_tables[tname].as_matrix() - private_tables1[tname].as_matrix()))) < 50
        assert sum((abs(private_tables[tname].as_matrix()-true_tables[tname].as_matrix())/private_tables[tname].as_matrix())[:,0]) < 0.1



def test_mapll():
    assert spark_sql_das_engine.map_ll(lambda x: x ** 2, [[1, 2], [3, 4], [5], [6, 7, 8]]) == [[1, 4], [9, 16], [25], [36, 49, 64]]

def test_get_set_of_first_items():
    assert spark_sql_aggregator.get_set_of_first_items([(1,2),(1,3),(4,2)]) == {1,4}

def test_uniform2Laplace():
    assert abs(noisealgorithms.uniform2Laplace(.4,.5) + 0.1115717756571048) < 1e-5
    assert abs(noisealgorithms.uniform2Laplace(.816062, 1)) - 1 < 1e-5
    assert abs(noisealgorithms.uniform2Laplace(.7,.5) - 0.25541281188299525) < 1e-5

def test_uniform2Gamma4():
    assert abs(noisealgorithms.uniform2Gamma4(.1) + 1.045219196776023) < 1e-7
    assert abs(noisealgorithms.uniform2Gamma4(.10972505) + 1) < 1e-7
    assert abs(noisealgorithms.uniform2Gamma4(.3) + 0.4478116975318562) < 1e-7
    assert abs(noisealgorithms.uniform2Gamma4(.9) - 1.045219196776023) < 1e-7
