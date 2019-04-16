import os
import sys

# If there is __init__.py in the directory where this file is, then Python adds das_decennial directory to sys.path
# automatically. Not sure why and how it works, therefore, keeping the following line as a double
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(__file__)))))

try:
    import cenquery
except ImportError as e:
    import programs.engine.cenquery as cenquery


def test_Query_subsetNone():
    # test to see that subset = None gives the correct subset
    array_dims = (1, 4, 5)
    subset = None
    add_over_margins = (2,)
    X = cenquery.Query(array_dims=array_dims, subset=subset, add_over_margins=add_over_margins)
    assert X.subset_input == (range(1), range(4), range(5))
