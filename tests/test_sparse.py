import os
import sys
import pytest
import numpy as np

# If there is __init__.py in the directory where this file is, then Python adds das_decennial directory to sys.path
# automatically. Not sure why and how it works, therefore, keeping the following line as a double
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(__file__)))))

import programs.sparse as sparse

@pytest.fixture
def dataint():
    shape = (6, 4, 5)
    size = np.prod(shape)
    return np.arange(1, size + 1).reshape(shape)

@pytest.fixture
def datafloat():
    shape = (6, 4, 5)
    size = np.prod(shape)
    return np.arange(1, size + 1).reshape(shape) + 0.3


def test_build(dataint):
    a = sparse.multiSparse(dataint)
    assert np.array_equal(a.toDense(), dataint)

def test_equal_int(dataint):
    assert sparse.multiSparse(dataint) == sparse.multiSparse(dataint)

def test_equal_float(datafloat):
    assert sparse.multiSparse(datafloat) == sparse.multiSparse(datafloat + 1e-6)
    shape = (6, 4, 5)
    size = np.prod(shape)
    a = np.arange(1, size + 1).reshape(shape).astype(np.float)
    a[0,0,0] = np.nan
    b = a + 1e-6
    assert sparse.multiSparse(a) == sparse.multiSparse(b)

def test_add(dataint, datafloat):
    assert sparse.multiSparse(dataint) + sparse.multiSparse(dataint) == sparse.multiSparse(dataint*2)
    assert sparse.multiSparse(datafloat) + sparse.multiSparse(datafloat) == sparse.multiSparse(datafloat * 2)

def test_sub(dataint, datafloat):
    assert sparse.multiSparse(dataint) - sparse.multiSparse(dataint) == sparse.multiSparse(dataint*0)
    assert sparse.multiSparse(datafloat) - sparse.multiSparse(datafloat) == sparse.multiSparse(datafloat * 0)

def test_abs(dataint, datafloat):
    assert sparse.multiSparse(dataint).abs() == sparse.multiSparse(np.abs(dataint))
    assert sparse.multiSparse(datafloat).abs() == sparse.multiSparse(np.abs(datafloat))

def test_sqrt(dataint, datafloat):
    assert sparse.multiSparse(dataint).sqrt() == sparse.multiSparse(np.sqrt(dataint))
    assert sparse.multiSparse(datafloat).sqrt() == sparse.multiSparse(np.sqrt(datafloat))

def test_square(dataint, datafloat):
    assert sparse.multiSparse(dataint).square() == sparse.multiSparse(np.square(dataint))
    assert sparse.multiSparse(datafloat).square() == sparse.multiSparse(np.square(datafloat))

def test_sum(dataint, datafloat):
    assert sparse.multiSparse(dataint).sum() == np.sum(dataint)
    assert np.isclose(sparse.multiSparse(datafloat).sum(), np.sum(datafloat))
    assert np.array_equal(sparse.multiSparse(dataint).sum(dims = (1,2)), dataint.sum((1,2)))
    assert np.isclose(sparse.multiSparse(datafloat).sum(dims=(1, 2)), datafloat.sum((1, 2))).all()



