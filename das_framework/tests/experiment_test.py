# experiment_test.py
#
# This program tests the ../experiment.py module with a contrived, simplified experiment
#
# Note that we don't need to change sys.path because there is a __init__.py in this directory
# and in the parent directory. For info, see:
# https://docs.pytest.org/en/latest/pythonpath.html


import sys,os
from decimal import Decimal

from driver import *
from experiment import *
from argparse import ArgumentParser, ArgumentDefaultsHelpFormatter
from configparser import ConfigParser
import io

# For the test, we just read the config file from the string below.
# That way the test does not depend on the current directory, and there is no need for a config file.
# https://stackoverflow.com/questions/21766451/how-to-read-config-from-string-or-list

s_config = """
[DEFAULT]
root: .
name: test
loglevel: INFO

[experiment]
loop1: FOR demo.x = 1 to 10 step 1
loop2: FOR demo.y = 0.1 to 1.0 step .1
loop3: FOR demo.z = 0.1 to 10 mulstep 3.16
loop4: FOR demo.t IN 0.02,0.05,0.1

[demo]
junk: 10

###
### what follows is largely from test.ini with the exception of the writer, which is a special writer that
### appends everything to a single file as a JSON object with the "x" and "y" values included.
###


[setup]
setup: demo_setup.setup

[reader]
reader: experiment_test.experiment_test_reader

[engine]
engine: demo_engine.engine

[writer]
writer: experiment_test.experiment_test_writer
output_fname: %(root)s/test_output.json

[validator]
validator: experiment_test.dummy_validator

[error_metrics]
error_metrics: experiment_test.dummy_error_metrics

[takedown]
takedown: demo_takedown.takedown
delete_output: False
"""

# Read the config file!
config = ConfigParser()
config.read_file( io.StringIO( s_config))

def test_decode_loop():
    assert decode_loop("FOR main.i=1 to 10 step 0.1")  == ("main","i",Decimal('1'),Decimal('10'),Decimal('0.1'),"ADD")
    assert decode_loop("FOR max.j=1 to 10")            == ("max", "j",Decimal('1'),Decimal('10'),Decimal('1'),"ADD")
    assert decode_loop("FOR max.j=1 to 100 mulstep 3") == ("max", "j",Decimal('1'), Decimal('100'), Decimal('3'), "MUL")
    assert decode_loop("FOR max.j IN 0.1,0.2,0.3")     == ("max", "j",Decimal('0.1'),Decimal('0.3'),[Decimal('0.1'), Decimal('0.2'), Decimal('0.3')],"LIST")
    assert decode_loop("FOR max.j=1") == None


def test_build_loop():
    ret = build_loops(config)
    assert ret == [('demo','x', Decimal('1.0'), Decimal('10.0'), Decimal('1.0'),"ADD"),
                   ('demo','y', Decimal('0.1'), Decimal('1.0'), Decimal('.1'),"ADD"),
                   ('demo','z', Decimal('0.1'), Decimal('10'), Decimal('3.16'), "MUL"),
                    ('demo', 't', Decimal('0.02'), Decimal('0.1'), [Decimal('0.02'), Decimal('0.05'), Decimal('0.1')], "LIST")
                   ]

def test_initial_state():
    state = initial_state( build_loops( config ))
    assert state == (Decimal('1.0'), Decimal('0.1'), Decimal('0.1'), Decimal('0.02'))

def test_increment_state():
    loops = build_loops( config )
    state = initial_state( loops )
    state = increment_state(loops,state)
    assert state == (Decimal('2.0'), Decimal('0.1'), Decimal('0.1'), Decimal('0.02'))
    # loop 9 more times
    for i in range(9):
        state = increment_state(loops,state)
    assert state == (Decimal('1.0'), Decimal('0.2'), Decimal('0.1'), Decimal('0.02'))
    # Loop 89 more times
    for i in range(9,98):
        state = increment_state(loops,state)
    assert state == (Decimal('10.0'), Decimal('1.0'), Decimal('0.1'), Decimal('0.02'))
    # loop 400 more times
    for i in range(99,499):
        state = increment_state(loops, state)
    assert state == (Decimal('10.0'), Decimal('1.0'), Decimal('9.971220736'), Decimal('0.02'))
    # loop 1000 more times
    for i in range(499, 1499):
        state = increment_state(loops, state)
    assert state == (Decimal('10.0'), Decimal('1.0'), Decimal('9.971220736'), Decimal('0.1'))
    #Make sure it ends now
    assert increment_state(loops,state) == None

def test_substitute_config():
    loops = build_loops( config )
    state = initial_state( loops )
    newconfig = substitute_config( loops=loops, state=state, config=config)
    assert newconfig['demo']['junk'] == "10" # make sure that the old section is still there
    assert newconfig['demo']['x']    == "1"  # make sure we have a "1"
    assert newconfig['demo']['y']    == "0.1"  # make sure we have a "0.1"
    assert newconfig['demo']['z']    == "0.1"  # make sure we have a "0.1"
    assert newconfig['demo']['t']    == "0.02"  # make sure we have a "0.02"

    # for giggles, lets try incrementing
    state = increment_state(loops, state)
    newconfig = substitute_config( loops=loops, state=state, config=config)
    assert newconfig['demo']['x']    == "2"  # make sure we have a "1"
    assert newconfig['demo']['y']    == "0.1"  # make sure we have a "0.1"
    assert newconfig['demo']['z']    == "0.1"  # make sure we have a "0.1"
    assert newconfig['demo']['t']    == "0.02"  # make sure we have a "0.02"

# Run the full experiment
def test_run_experiment():
    loops = build_loops( config )
    state = initial_state( loops )
    # To test the callback, we collect all of the (x,y) pairs in the callback
    quadruplets = []
    def callback(config):
        quadruplets.append((config['demo']['x'],config['demo']['y'],config['demo']['z'],config['demo']['t']))

    config[driver.EXPERIMENT][driver.EXPERIMENT_DIR] = '.' # use the current directory
    run_experiment(config=config, callback=callback)
    # I should have 1500 quadruplets
    assert len(quadruplets) == 1500
    # Check for some key values
    assert ('1','0.1','0.1','0.02') in quadruplets
    assert ('5','0.5','3.1554496','0.05') in quadruplets
    assert ('10','1.0','9.971220736','0.1') in quadruplets

        
### 
### this is for testing the experiment with an end-to-end test
###

import json
import driver
import pandas
class experiment_test_reader(driver.AbstractDASReader):
    def read(self):
        sales = {'ID': [1,2,3,4,5],
                 'Data': [10,20,30,40,50]}
        return pandas.DataFrame(sales)

class experiment_test_writer(driver.AbstractDASWriter):
    def write(self,df):
        import copy
        fname = self.getconfig("output_fname")
        assert fname.endswith(".json")
        with open(fname,"a") as f:
            f.write( json.dumps( {"x":self.getfloat("x",section='demo'),
                                  "y":self.getfloat("y",section='demo'),
                                  'sum-ID':df['ID'].sum(),
                                  'sum-Data':df['Data'].sum()
                                  } ))
            f.write("\n")
        return fname            # where to find it
        

class dummy_validator(driver.AbstractDASValidator):
    def validate(self, original_data, written_data_reference):
        return True

class dummy_error_metrics(driver.AbstractDASErrorMetrics):
    def run(self, data):
        return None


if __name__=="__main__":
    test_run_experiment()
