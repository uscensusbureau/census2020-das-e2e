import os
import sys
import numpy as np
from programs.sparse import multiSparse
from programs.engine.nodes import geounitNode
import pytest
from configparser import ConfigParser

# If there is __init__.py in the directory where this file is, then Python adds das_decennial to sys.path
# automatically. Not sure why and how it works, therefore, keeping the following line as a double
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(__file__)))))

@pytest.fixture
def data(spark):

    geocodeDict = {16: 'Block', 12: 'Block_Group', 11: 'Tract', 5: 'County'}
    bn1 = geounitNode(geocode='4400700010111000',
                      raw=multiSparse(np.array([[1, 2], [3, 4]])),
                      syn=multiSparse(np.array([[1, 1], [0, 7]])),
                      geocodeDict=geocodeDict)

    bn2 = geounitNode(geocode='4400700010111001',
                      raw=multiSparse(np.array([[3, 4], [2, 1]])),
                      syn=multiSparse(np.array([[2, 2], [1, 0]])),
                      geocodeDict=geocodeDict)

    sc = spark.sparkContext

    return sc.parallelize([bn1, bn2])

@pytest.fixture()
def spark_e2ev(spark):
    import programs.validator as e2ev
    return e2ev


def test_calc_L1(data, spark_e2ev):
    true_data = data.map(lambda node: ((node.geocode,), node.raw.toDense()))
    noisy_data = data.map(lambda node: ((node.geocode,), node.syn.toDense()))
    assert true_data.count() == 2
    assert noisy_data.count() == 2
    assert spark_e2ev.calc_total_L1(true_data.join(noisy_data)) == 12

def test_calc_N(data, spark_e2ev):
    true_data = data.map(lambda node: ((node.geocode,), node.raw.toDense()))
    noisy_data = data.map(lambda node: ((node.geocode,), node.syn.toDense()))
    assert true_data.count() == 2
    assert noisy_data.count() == 2
    assert spark_e2ev.calc_N(true_data) == 20

@pytest.fixture(scope="module")
def config():
    config = ConfigParser()

    config.add_section('validator')
    config.set('validator', 'error_privacy_budget', '1000')
    config.set('validator', 'certificate_path', 's3://uscb-decennial-ite-das/tmp/test_cert.pdf')

    config.add_section('budget')
    config.set('budget', 'epsilon_budget_total', '0.1')
    config.set('budget', 'geolevel_budget_prop', '0.2,0.2,0.2,0.2,0.2')
    config.set('budget', 'detailedprop', '1.0')

    config.add_section('geodict')
    config.set('geodict', 'geolevel_names', 'Block,Block_Group,Tract,County,State')

    config.add_section('engine')
    config.set('engine', 'engine', 'programs.engine.topdown_engine.engine')
    return config

def test_dp_L1(spark_e2ev, config, data):
    config.set('validator', 'error_privacy_budget', '10')
    val_inst = spark_e2ev.validator(config=config, name='validator')
    assert val_inst.protected_L1_error(data, data) == 12

    np.random.seed(100)
    config.set('validator', 'error_privacy_budget', '0.1')
    val_inst = spark_e2ev.validator(config=config, name='validator')
    assert val_inst.protected_L1_error(data, data) == 21

    np.random.seed()

def test_validate(spark_e2ev, config, data):
    import subprocess
    val_inst = spark_e2ev.validator(config=config, name='validator')
    val_inst.validate(data, data)
    cmd = ['aws', 's3', 'rm', config['validator']['certificate_path']]
    r = subprocess.run(cmd)