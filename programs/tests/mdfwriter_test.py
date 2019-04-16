# writer tests
# William Sexton

"""
   Run pytest inside programs dir.
"""

import sys
import os
import numpy as np

assert os.path.basename(sys.path[0]) == "das_decennial"

sys.path.append(os.path.join(sys.path[0], "das_framework"))

print(sys.path)

from configparser import ConfigParser
from programs.mdfwriter import *

configs = ConfigParser()
configs.add_section("default")
configs.set("default", "name", "DAS")
configs.add_section("writer")
configs.set("writer", "writer", "")
configs.set("writer", "output_fname", "s3://uscb-decennial-ite-das/sexto015/pytest")
configs.set("writer", "produce_flag", "1")
configs.set("writer", "overwrite_flag", "1")
configs.set("writer", "filesystem", "hadoop")
configs.set("writer", "split_by_state", "True")
configs.set("writer", "state_codes", "02 01 05 04 06 08 09 11 10 12 13 15 19 16 17 18 20 21 22 25 24 23 26 27 29 28 30 37 38 31 33 34 35 32 36 39 40 41 42 72 44 45 46 47 48 49 51 50 53 55 54 56")


from programs.sparse import multiSparse

def test_to_list_from_sparse():
    spar_obj = multiSparse(np.array([[2,3,0],[0,0,1]]))
    assert to_list_from_sparse(spar_obj) == [((0,0), 2), ((0, 1), 3), ((1, 2), 1)]
    
def test_expand():
    assert expand(((111,), ((0,0), 2))) == [(111, 0, 0), (111, 0, 0)]

def test_to_line():
    tup = (111, 0, 0)
    assert to_line(tup) == "111|0|0"

def test_writer():
    pass