import sys
import os

assert os.path.basename(sys.path[0]) == "das_decennial"

sys.path.append(os.path.join(sys.path[0], "das_framework"))

from programs.sparse import *

def test_init():
    good_array = np.array([[0, 1, 2], [2, 0, 4]])
    bad_obj = {"a":"bad", "dict":"obj"}
    spar = multiSparse(good_array)
    assert spar.shape == (2, 3)
    assert isinstance(spar.sparse_array,  ss.csr_matrix)
    assert spar.sparse_array.count_nonzero() == 4

    try:
        bad_spar = multiSparse(bad_obj)
        assert False
    except TypeError:
        assert True

def test_toDense():
    good_array = np.array([[[0, 1], [2, 0], [0, 3]],
                           [[4, 0], [0, 5], [6, 0]]])
    spar = multiSparse(good_array)
    assert spar.shape == (2, 3, 2)
    undo = spar.toDense()
    assert (undo == good_array).all()

def test_add():
    good_array_A = np.array([[0, 1], [2, 3]])
    good_array_B = np.array([[3, 2], [1, 0]])
    bad_array_C = np.array([[2, 4, 4]])

    spar_A = multiSparse(good_array_A)
    spar_B = multiSparse(good_array_B)
    spar_C = multiSparse(bad_array_C)

    spar = spar_A + spar_B
    assert spar.shape == (2, 2)
    assert spar.sparse_array.count_nonzero() == 4
    assert (spar.toDense() != spar_A.toDense()).any()
    assert (spar.toDense() == np.array([[3, 3], [3, 3]])).all()

    try:
        bad_spar = spar_A + spar_B
        assert False
    except AssertionError:
        assert True
