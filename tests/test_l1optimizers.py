import sys
import numpy
import pytest

# 10/24/2018 - SLG - This test is no longer used
# It should be removed. Right now test is changed to toast_ to prevent the test from being run

def toast_gurobi():
    import opt
    import opt.gbopt as gbopt
    import cenquery
    array_dims = (6,4,5)
    size = numpy.prod(array_dims)
    data = numpy.arange(1, size+1).reshape(array_dims) + 0.3
    #set up queries and answers
    subset = (slice(1,3), slice(0, 2), slice(2,4))
    axes = [(0,2),  (0,1,2)]
    queries = [cenquery.Query(array_dims, subset, ax) for ax in axes]
    answers = [q.answer(data) for q in queries]
    weights = [x for x in range(len(answers))]
    # set up exact queries and answers
    c_subset = None
    c_axes = [(1,), (2,)]
    c_queries = [cenquery.Query(array_dims, subset, ax) for ax in c_axes]
    c_answers = [q.answer(data) for q in c_queries] 
    # set up geq queries and answers
    g_subsets = [(slice(1,3), slice(0, 4), slice(0,5)), (slice(0,2), slice(0, 4), slice(0,5))]
    g_queries = [cenquery.Query(array_dims, s, add_over_margins=None) for s in g_subsets]
    # set up structural zeros
    z_subsets = [(slice(0,1), slice(0, 2), slice(0,1)), (slice(0,1), slice(1, 3), slice(0,1))]
    z_queries = [cenquery.Query(array_dims, s, add_over_margins=None) for s in z_subsets]
    
    answer1 = gbopt.solve_l1(data_shape=array_dims, queries=queries, answers=answers, weights=weights, \
                            const_queries=c_queries, const_answers=c_answers, \
                            geq_queries=g_queries, \
                            zero_queries=z_queries, \
                            nnls=False)
    answer1 = gbopt.solve_l1(data_shape=array_dims, queries=queries, answers=answers, weights=weights, \
                            const_queries=c_queries, const_answers=c_answers, \
                            geq_queries=g_queries, \
                            zero_queries=z_queries, \
                            nnls=True)
    assert True
