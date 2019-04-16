# experiment_test.py
#
# graph_test.py: for making a graph. That's all it does
#


import sys,os
import math
import driver

class setup(driver.AbstractDASSetup):
    pass

# For reading, we return a single dictionary with an "x" in it
class reader(driver.AbstractDASReader):
    def read(self):
        return {"x":self.getfloat("x")}

# For the engine, we compute sin(x):
class engine(driver.AbstractDASEngine):
    def run(self, original_data):
        return {"x":original_data['x'],
                'y':math.sin(original_data['x']) }

class writer(driver.AbstractDASWriter):
    def write(self, privatized_data):
        return privatized_data  # return the data as its own reference

class validator(driver.AbstractDASValidator):
    def validate(self, original_data, written_data_reference):
        self.x = original_data['x']
        self.y = written_data_reference['y']
        return original_data['x']==written_data_reference['x']

    def get_x(self):
        return self.x

    def get_y(self):
        return self.y

class takedown(driver.AbstractDASTakedown):
    pass


