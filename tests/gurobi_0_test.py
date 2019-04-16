#
# Test Gurobi environment and Python modules
#


import py.test
import os


def test_gurobi_variables():
    assert "GRB_LICENSE_FILE" in os.environ
    assert os.path.exists( os.environ["GRB_LICENSE_FILE"] )
    # grbfile = open( os.environ["GRB_LICENSE_FILE"], "r").read()
    # assert "PORT=" in grbfile
    # assert "TOKENSERVER=" in grbfile

def test_gurobi_module():
    """Verify that the gurobi module exists"""
    import gurobipy


if __name__=="__main__":
    test_gurobi_variables()
    test_gurobi_module()
