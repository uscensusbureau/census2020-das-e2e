# driver_test.py
#
# Test driver.py
#

# This uses the pandas demo

import sys
import os
import io
import pytest

# Make sure the current directory and the parent directory are part of the path
sys.path.append( os.path.dirname(__file__) )
sys.path.append( os.path.dirname(os.path.dirname(__file__)))
sys.path.append( os.path.dirname(os.path.dirname(os.path.dirname(__file__))))

from das_framework import driver
from ctools.hierarchical_configparser import HierarchicalConfigParser

fake_path = "/path/to/something"

# Create a demo config file that will be used for these test
s_config = """
[DEFAULT]
defopt: defopt

[ENVIRONMENT]
MY_VAR: 1.0x

[engine]
engine: demo_pandas.engine

[reader]
reader: demo_pandas.reader

[writer]
writer: demo_pandas.writer

[temp]
histogram_path: /path/to/something

[setup]
setup: demo_pandas.setup

[takedown]
takedown: demo_pandas.takedown

[validator]
validator: demo_pandas.validator

[values]
A: 1
B: 2.
C: true
D: sssstttrrring
E: 
F:

[lists]
A: a,b , c, d
B: e f  g    h
C: i;j;  k; l
D: m-n-o-p
E: 1,2,3,4
F: 1 2,3;4
"""

config = HierarchicalConfigParser()
config.read_file( io.StringIO( s_config))


class ARGH:
    def __init__(self, d):
        self.setup = d
        self.reader = d
        self.engine = d
        self.writer = d
        self.validator = d
        self.takedown = d

assert config[driver.SETUP][driver.SETUP] == "demo_pandas.setup"
assert config[driver.READER][driver.READER] == "demo_pandas.reader"
assert config[driver.ENGINE][driver.ENGINE] == "demo_pandas.engine"
assert config[driver.WRITER][driver.WRITER] == "demo_pandas.writer"
assert config[driver.VALIDATOR][driver.VALIDATOR] == "demo_pandas.validator"
assert config[driver.TAKEDOWN][driver.TAKEDOWN] == "demo_pandas.takedown"

def test_config_apply_environment():
    import os
    driver.config_apply_environment(config)
    assert os.environ["MY_VAR"] == "1.0x"

def test_DAS():
    f = driver.DAS(config)
    import demo_pandas
    assert type(f) == driver.DAS
    # Note: there is no longer a DAS setup attribute
    assert type(f.reader) == demo_pandas.reader
    assert type(f.engine) == demo_pandas.engine
    assert type(f.writer) == demo_pandas.writer
    assert type(f.validator) == demo_pandas.validator
    assert type(f.takedown) == demo_pandas.takedown

def test_strtobool():
    assert driver.strtobool("true")==True
    assert driver.strtobool("false")==False
    assert driver.strtobool("1")==True
    assert driver.strtobool("0")==False
    assert driver.strtobool("yes")==True
    assert driver.strtobool("no")==False
    assert driver.strtobool("",default=False)==False
    assert driver.strtobool("",default=True)==True

def test_getiter():
    aml = driver.AbstractDASModule(config=config,name="lists")
    assert tuple(aml.getiter('A')) == ('a','b','c','d')
    assert tuple(aml.getiter('B', sep=r"\W+")) == ('e', 'f', 'g', 'h')
    assert tuple(aml.getiter('C', sep=";")) == ('i', 'j', 'k', 'l')
    assert tuple(aml.getiter('D', sep="-")) == ('m', 'n', 'o', 'p')
    assert tuple(aml.getiter_of_ints('E', sep=",")) == (1, 2, 3, 4)
    assert tuple(aml.getiter_of_floats('E', sep=",")) == (1., 2., 3., 4.)
    assert aml.gettuple_of_ints("F",sep=r"[\W\,\;]{1}") == (1, 2, 3, 4)
    assert aml.gettuple_of_floats("F", sep=r"[\W\,\;]{1}") == (1., 2., 3., 4.)
    am = driver.AbstractDASModule(config=config)
    assert am.gettuple_of_ints("F", sep=r"[\W\,\;]{1}", section="lists") == (1, 2, 3, 4)

def test_getconf():
    amv = driver.AbstractDASModule(config=config,name="values")
    assert amv.getint('A') == 1
    assert amv.getconfig('A') == '1'
    assert amv.getfloat('A') == 1.
    assert amv.getboolean('A') == True
    with pytest.raises(ValueError):
        amv.getint('B')
    assert amv.getconfig('B') == '2.'
    assert amv.getfloat('B') == 2.
    with pytest.raises(ValueError):
        amv.getboolean('B')

    assert amv.getboolean('C') == True
    assert amv.getconfig('C') == "true"

    assert amv.getconfig('D') == "sssstttrrring"

    assert amv.getconfig('E') == ""
    with pytest.raises(ValueError):
        amv.getboolean('E')

    assert amv.getboolean('E', default=True) == True
    assert amv.getboolean('E', default=False) == False

def test_getconfitems():
    am = driver.AbstractDASModule(config=config)
    assert am.getconfitems("values") == [('a', '1'), ('b', '2.'), ('c', 'true'), ('d', 'sssstttrrring'), ('e', ''), ('f', '')]

