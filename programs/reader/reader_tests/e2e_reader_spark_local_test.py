import os
import sys
import numpy as np
from configparser import ConfigParser
import pytest
import pickle

# If there is __init__.py in the directory where this file is, then Python adds das_decennial to sys.path
# automatically. Not sure why and how it works, therefore, keeping the following line as a double
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(__file__)))))

#import programs.reader.e2e_reader as e2er

# config = ConfigParser()
# config.read_file(io.StringIO(confstring.format(testdir, fname)))


# @pytest.fixture()
# def datafiles():
#     #### We don't need to put these files to HDFS when running in local mode
#
#     import subprocess
#     datafiles = ['grfctest.txt', 'person.txt', 'unit.txt']
#     subprocess.run(
#         ['hdfs', 'dfs', '-put'] + list(map(lambda fn: os.path.join(os.path.dirname(__file__), fn), datafiles)) + ['.'])
#     yield datafiles
#     subprocess.run(['hdfs', 'dfs', '-rm', *datafiles])


@pytest.fixture(scope="module")
def config():
    curdir = os.path.dirname(__file__)
    config = ConfigParser()
    config.add_section('reader')
    config['reader']['Per_Data.path'] = "file:///" + os.path.join(curdir, 'person.txt')
    config['reader']['Unit_Data.path'] = "file:///" + os.path.join(curdir, 'unit.txt')
    config['reader']['GRFC.path'] = "file:///" + os.path.join(curdir, 'grfctest.txt')
    #### The following are used for when the files are put to HDFS
    # config['reader']['Per_Data.path'] = 'person.txt'
    # config['reader']['Unit_Data.path'] = 'unit.txt'
    # config['reader']['GRFC.path'] = 'grfctest.txt'
    config['reader']['tables'] = "PersonData UnitData"
    config['reader']['privacy_table'] = "PersonData"
    config['reader']['constraint_tables'] = "UnitData"
    config.add_section('constraints')
    config['constraints']['constraints'] = "programs.reader.constraints.ConstraintsCreatorPL94"
    config['constraints']['invariants'] = "programs.reader.invariants.InvariantsCreatorPL94"
    config['constraints']['theInvariants.Block'] = "gqhh_vect,gqhh_tot"
    config['constraints']['theInvariants.County'] = "tot"
    config['constraints']['theConstraints.Block'] = "hhgq_total_lb,hhgq_total_ub,nurse_nva_0"
    config['constraints']['theConstraints.County'] = "total,hhgq_total_lb,hhgq_total_ub"
    config.add_section('geodict')
    config['geodict']['geolevel_names'] = "Block,Block_Group,Tract,County"
    config['geodict']['geolevel_leng'] = "16,12,11,1"
    return config


@pytest.fixture(scope="module")
def reader_instance(spark, config):
    import programs.reader.e2e_reader as e2er_spark
    return e2er_spark.reader(config=config, setup=spark, name='reader')


def test_reader_init(reader_instance):

    r = reader_instance
    assert r.per_path
    assert r.unit_path
    assert r.grfc_path


@pytest.fixture(scope="module")
def read_data(reader_instance):
    return [reader_instance.load_per(), reader_instance.load_unit(), reader_instance.load_grfc()]


def test_load_grfc(read_data):
    assert read_data[2].collect()[0] == (2103510917869541, '4400700010111000')
    #assert read_data[2].collect()[0] == ('210351091786954', '4400700010111000')  # 15 or 16 digits in OIDTB?


def test_load_per(read_data):
    assert read_data[0].collect()[0] == (123456789, (1, 0, 22))


def test_load_unit(read_data):
    assert read_data[1].collect()[0] == (2103510917869541, (1, 2, 123456789))


@pytest.fixture(scope="module")
def join_data(reader_instance, read_data):
    return reader_instance.join_hist_agg(*read_data).collect()[0]


def test_join_hist_agg(join_data):
    assert np.sum(join_data[1][0]).astype(int) == 2
    assert np.sum(join_data[1][1]).astype(int) == 3


def test_make_block_node(reader_instance, join_data):
    bn = reader_instance.make_block_node(join_data)
    assert np.array_equal(bn.raw.toDense(), join_data[1][0])
    assert bn.geocode == join_data[0][0]
    assert bn.dp is None
    assert bn.syn is None
    assert bn.syn_unrounded is None
    assert bn.dp_queries is None

def test_compare_with_saved(reader_instance, join_data):
    """
    WILL CHANGE IF TEST DATA  in .txt FILES or CONFIG CHANGES!
    """
    bn = reader_instance.make_block_node(join_data)
    fname = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))), 'engine','unit_tests','geounitnode.pickle')
    # with(open(fname, 'wb')) as f:
    #     pickle.dump(bn, f)
    sbn = pickle.load(open(fname, 'rb'))
    assert sbn == bn


def test_read(reader_instance, join_data):
    block_nodes = reader_instance.read()
    assert np.array_equal(block_nodes.collect()[0].raw.toDense(), join_data[1][0])
