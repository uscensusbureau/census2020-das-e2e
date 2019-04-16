import os
import sys
import numpy as np
from configparser import ConfigParser
import tempfile
import shutil
import pickle
import subprocess

# If there is __init__.py in the directory where this file is, then Python adds das_decennial to sys.path
# automatically. Not sure why and how it works, therefore, keeping the following line as a double
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(__file__)))))

from programs.utils import ship_files2spark

# config = ConfigParser()
# config.read_file(io.StringIO(confstring.format(testdir, fname)))


def run_spark_reader_tests():
    import programs.reader.e2e_reader as e2er
    # Set up Spark
    from pyspark.sql import SparkSession

    spark = SparkSession.builder.appName('2018 e2e reader module test').getOrCreate()

    tempdir = tempfile.mkdtemp()

    ship_files2spark(spark, tempdir)

    datafiles = ['grfctest.txt', 'person.txt', 'unit.txt']
    subprocess.run(['hdfs', 'dfs', '-put'] + list(map(lambda fn: os.path.join(os.path.dirname(__file__), fn), datafiles)) + ['.'])

    # Set up config
    # curdir = os.path.dirname(__file__)
    config = ConfigParser()
    config.add_section('reader')
    # config['reader']['Per_Data.path'] = os.path.join(curdir, 'person.txt')
    # config['reader']['Unit_Data.path'] = os.path.join(curdir, 'unit.txt')
    # config['reader']['GRFC.path'] = os.path.join(curdir, 'grfctest.txt')
    config['reader']['Per_Data.path'] = 'person.txt'
    config['reader']['Unit_Data.path'] = 'unit.txt'
    config['reader']['GRFC.path'] = 'grfctest.txt'
    config['reader']['tables'] = "PersonData UnitData"
    config['reader']['privacy_table'] = "PersonData"
    config['reader']['constraint_tables'] = "UnitData"
    config.add_section('constraints')
    config['constraints']['constraints'] = "programs.reader.constraints.ConstraintsCreatorPL94"
    config['constraints']['invariants'] = "programs.reader.invariants.InvariantsCreatorPL94"
    config['constraints']['theInvariants'] = "tot, va, gqhh_vect, gqhh_tot"
    config['constraints']['theConstraints'] = "total, voting_age, hhgq_va_ub, hhgq_va_lb, hhgq_total_lb, hhgq_total_ub, nurse_nva_0"
    config.add_section('geodict')
    config['geodict']['geolevel_names'] = "Block,Block_Group,Tract,County,State"
    config['geodict']['geolevel_leng'] = "16,12,11,5,1"

    reader_instance = e2er.reader(config=config, setup=spark, name='reader')

# test_reader_init(reader_instance):
    r = reader_instance
    assert r.per_path
    assert r.unit_path
    assert r.grfc_path

    read_data = [reader_instance.load_per(), reader_instance.load_unit(), reader_instance.load_grfc()]

# def test_load_grfc(read_data):
    assert read_data[2].collect()[0] == (2103510917869541, '4400700010111000')
    # assert read_data[2].collect()[0] == ('210351091786954', '4400700010111000')  # 15 or 16 digits in OIDTB?


# def test_load_per(read_data):
    assert read_data[0].collect()[0] == (123456789, (1, 0, 22))


# def test_load_unit(read_data):
    assert read_data[1].collect()[0] == (2103510917869541, (1, 2, 123456789))

    join_data = reader_instance.join_hist_agg(*read_data).collect()[0]

# def test_join_hist_agg(join_data):
    assert np.sum(join_data[1][0]).astype(int) == 2
    assert np.sum(join_data[1][1]).astype(int) == 3


# def test_make_block_node(reader_instance, join_data):
    bn = reader_instance.make_block_node(join_data)
    assert np.array_equal(bn.raw.toDense(), join_data[1][0])
    assert bn.geocode == join_data[0][0]
    assert bn.dp is None
    assert bn.syn is None
    assert bn.syn_unrounded is None
    assert bn.dp_queries is None

# def test_compare_with_saved(reader_instance, join_data):
#     """
#     WILL CHANGE IF TEST DATA  in .txt FILES or CONFIG CHANGES!
#     """
    bn = reader_instance.make_block_node(join_data)
    fname = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))), 'engine', 'unit_tests', 'geounitnode.pickle')
    # with(open(fname,'wb')) as f:
    #     pickle.dump(bn,f)
    sbn = pickle.load(open(fname, 'rb'))
    assert sbn == bn


# def test_read(reader_instance, join_data):
    block_nodes = reader_instance.read()
    assert np.array_equal(block_nodes.collect()[0].raw.toDense(), join_data[1][0])

    subprocess.run(['hdfs', 'dfs', '-rm', *datafiles])

    shutil.rmtree(tempdir)


def test_reader_spark():
    if "SPARK_ENV_LOADED" not in os.environ:
        #
        # Re-run this script under Spark.
        import subprocess
        r = subprocess.run(['spark-submit', __file__], timeout=60)
        assert r.returncode == 0


if __name__ == "__main__":
    if "SPARK_ENV_LOADED" in os.environ:
        run_spark_reader_tests()
    else:
        print("Please run from py.test")
