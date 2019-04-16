import sys
import pytest
import numpy
sys.path.append('../code/')
sys.path.append('code/')

import programs.engine.cenquery as cenquery

@pytest.fixture
def data():
    """ automatically allows test functions to have a data argument """
    shape = (6,4,5)
    size = numpy.prod(shape)
    return numpy.arange(1, size + 1).reshape(shape) + 0.3


def test_answer1(data, tol=0.00000001):
    """ tests query answers without add_over_margins """
    array_dims = data.shape
    subset = (slice(1,3), slice(0, 2), slice(2,4))
    query_no_sumover = cenquery.Query(array_dims, subset, None)
    query_empty_sumover = cenquery.Query(array_dims, subset, ())
    true_answer = data[subset]
    assert numpy.abs(query_no_sumover.answer(data)-true_answer).sum() <= tol
    assert numpy.abs(query_empty_sumover.answer(data)-true_answer).sum() <= tol
    
    
def test_answer2(data, tol=0.00000001):
    """ tests query answers with add_over_margins """
    array_dims = data.shape
    subset = (slice(1,3), slice(0, 2), slice(2,4))
    axes = [(1,), (0,2),  (0,1,2)]
    true_answers = [data[subset].sum(axis=ax) for ax in axes]
    queries = [cenquery.Query(array_dims, subset, ax) for ax in axes]
    answers = [q.answer(data) for q in queries]
    errors = [numpy.abs(a - ta).sum() for (a, ta) in zip(answers, true_answers)]
    assert max(errors) < tol
    
def test_kron_rep1(data, tol=0.00000001):
    """ tests query answers from kron representation without add_over_margins """
    array_dims = data.shape
    flat_data = data.flatten()
    subset = (slice(1,3), slice(0, 2), slice(2,4))
    query_no_sumover = cenquery.Query(array_dims, subset, None)
    query_empty_sumover = cenquery.Query(array_dims, subset, ())
    true_answer = data[subset].flatten()
    assert numpy.abs(query_no_sumover.answer(flat_data)-true_answer).sum() <= tol
    assert numpy.abs(query_empty_sumover.answer(flat_data)-true_answer).sum() <= tol
    
    
def test_kron_rep2(data, tol=0.00000001):
    """ tests query answers from kron representation with add_over_margins """
    array_dims = data.shape
    flat_data = data.flatten()
    subset = (slice(1,3), slice(0, 2), slice(2,4))
    axes = [(1,), (0,2),  (0,1,2)]
    true_answers = [data[subset].sum(axis=ax).flatten() for ax in axes]
    queries = [cenquery.Query(array_dims, subset, ax) for ax in axes]
    answers = [q.answer(flat_data) for q in queries]
    errors = [numpy.abs(a - ta).sum() for (a, ta) in zip(answers, true_answers)]
    assert max(errors) < tol
    
def test_kron_consistency(data, tol=0.00000001):
    """ checks whether kron rep is consistent with flattened answers """
    array_dims = data.shape
    subset = (slice(1,3), slice(0, 2), slice(2,4))
    axes = [(1,), (0,2),  (0,1,2)]
    flattened_data = data.flatten()
    queries = [cenquery.Query(array_dims, subset, ax) for ax in axes]
    flatten_answers = [q.answer(data, flatten=True) for q in queries]
    kron_answers = [q.answer(flattened_data) for q in queries]
    errors = [numpy.abs(fa - ka).sum() for (fa, ka) in zip(flatten_answers, kron_answers)]
    assert max(errors) < tol
