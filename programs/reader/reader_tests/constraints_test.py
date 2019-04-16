# unit tests for constraints
# William Sexton

# Major Modifications Log:
#
# 2018-06-28 wns - created test skeleton
#
import os
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))

# assert os.path.basename(sys.path[0]) == "das_decennial", "incorrect file structure, no __init__.py files inside packages"
#
# prj_root = sys.path[0]
# sys.path.insert(0, os.path.join(prj_root, "das_framework"))
# sys.path.insert(0, os.getcwd())

import programs.reader.constraints as constraints

# def test_creater_init():
#     cons = constraints.ConstraintsCreatorPL94(hist_shape=(1,1),invariants=None, constraint_names=None)
