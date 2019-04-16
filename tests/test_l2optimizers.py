import sys
import numpy
import pytest

# 10/24/2018 - slg - This code is no longer used. 
# Changed test_ tests below to toast_ so that they will not be called


@pytest.fixture
def queries_answers_weights_sol():
    import opt
    import cenquery
    import opt.cvx as cvx
    shape=(6,4,5)
    q2_subset = (slice(3,5), slice(2,4), slice(0,2))
    q2_answer_shape = (2,2)
    prng = numpy.random.RandomState(seed)
    seed=23
    q1 = cenquery.Query(shape)
    q2 = cenquery.Query(shape, subset=q2_subset, add_over_margins=(0,))
    queries = [q1, q2]
    ans1 = numpy.uniform(low=-0.5, high=0.5, size=shape)
    ans2 = numpy.uniform(low=-0.5, high=0.5, size=q2_answer_shape)
    answers = [ans1, ans2]
    weights = [1.0, 2.0]
    solution = None
    return (queries, answers, weights, solution)


def solve(optimizer, method):
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
    
    answer1 = optimizer.solve_l2(data_shape=array_dims, queries=queries, answers=answers, weights=weights, \
                            const_queries=c_queries, const_answers=c_answers, \
                            geq_queries=g_queries, \
                            zero_queries=z_queries, \
                            nnls=False, method=method)
    answer1 = optimizer.solve_l2(data_shape=array_dims, queries=queries, answers=answers, weights=weights, \
                            const_queries=c_queries, const_answers=c_answers, \
                            geq_queries=g_queries, \
                            zero_queries=z_queries, \
                            nnls=True, method=method)
    
def toast_gurobi():
    import opt.gbopt as gbopt
    solve(gbopt,None)
    assert True
    
def toast_cvxopt_with_cvxpy():
    solve(cvx, "CVXOPT")
    assert True
    
    
def toast_ecos_with_cvxpy():
    solve(cvx, "ECOS")
    assert True    
