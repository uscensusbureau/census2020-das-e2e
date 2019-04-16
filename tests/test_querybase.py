import pytest
import numpy as np
import scipy.sparse as ss
import programs.engine.querybase as cenquery



def compare_arrays(a,b, tol=0.00000001):
    """ compares two arrays by checking that their L1 distance is within a tolerance """
    return a.shape == b.shape and np.abs(a-b).sum() <= tol

@pytest.fixture
def data():
    """ sets up data for testing """
    shape = (3,4,5)
    size = np.prod(shape)
    return np.arange(size).reshape(shape)


def test_sumover_query(data):
    """ Tests that SumoverQuery answers queries correctly """
    s = data.shape

    q1 = cenquery.SumoverQuery(data.shape, add_over_margins=(0,1,2))
    q2 = cenquery.SumoverQuery(data.shape)
    q3 = cenquery.SumoverQuery(data.shape, add_over_margins=(1,))
    subset4 = ([0, s[0]-1], range(0, s[1], 2), [0, s[2]-1])
    q4 = cenquery.SumoverQuery(data.shape, subset=subset4, add_over_margins=(1,))

    # check for (1) correct answer, (2) answer matches the matrix representation 
    assert compare_arrays(q1.answer(data), data.sum().flatten())
    assert compare_arrays(q1.matrixRep() * data.flatten(), data.sum().flatten())
    # check for (1) correct answer, (2) answer matches the matrix representation
    assert compare_arrays(q2.answer(data), data.flatten())
    assert compare_arrays(q2.matrixRep() * data.flatten(), data.flatten())
    # check for (1) correct answer, (2) answer matches the matrix representation
    assert compare_arrays(q3.answer(data), data.sum(axis=(1,)).flatten())
    assert compare_arrays(q3.matrixRep() * data.flatten(), data.sum(axis=(1,)).flatten())
    # check for (1) correct answer, (2) answer matches the matrix representation
    assert compare_arrays(q4.answer(data), data[np.ix_(*subset4)].sum(axis=(1,)).flatten())
    assert compare_arrays(q4.matrixRep() * data.flatten(), data[np.ix_(*subset4)].sum(axis=(1,)).flatten())
    # check that the query sizes are correct
    assert q4.domainSize() == data.size
    assert q4.domainSize() == q4.matrixRep().shape[1]
    assert q4.numAnswers() == q4.answer(data).size, "numAnswers is: {} but should be: {}".format(q4.numAnswers(), q4.answer(data).size)

    # check sensitivities 
    assert q1.sensitivity() == 1
    assert np.abs(q1.matrixRep()).sum(axis=0).max() == q1.sensitivity()
    assert q2.sensitivity() == 1
    assert np.abs(q2.matrixRep()).sum(axis=0).max() == q2.sensitivity()
    assert q3.sensitivity() == 1
    assert np.abs(q3.matrixRep()).sum(axis=0).max() == q3.sensitivity()
    assert q4.sensitivity() == 1
    assert np.abs(q4.matrixRep()).sum(axis=0).max() == q4.sensitivity()
    #make sure it is counting query
    assert q1.isIntegerQuery()
    assert q2.isIntegerQuery()
    assert q3.isIntegerQuery()
    assert q4.isIntegerQuery()


def test_sparsekronquery(data):
    """ test the SparseKronQuery class """
    shape = data.shape
    
    #marginalize over first and last dimensions
    matrices1 = [ss.eye(x) for x in shape]
    matrices1[0] = np.ones(shape[0])
    matrices1[-1] = ss.csr_matrix(np.ones(shape[-1]))
    query1 = cenquery.SparseKronQuery(matrices1)
    query1a = cenquery.QueryFactory.makeKronQuery(matrices1, "hello")

    #marginalize over first and last dimensions, denser kron representation
    matrices2 = matrices1[1:]
    matrices2[0] = ss.kron(matrices1[0], matrices1[1])
    query2 = cenquery.SparseKronQuery(matrices2)
    query2a = cenquery.QueryFactory.makeKronQuery(matrices2, "hello2")

    #marginalize over everything
    matrices3 = [np.ones(x) for x in shape]
    query3 = cenquery.SparseKronQuery(matrices3)
    query3a = cenquery.QueryFactory.makeKronQuery(matrices3, "hello3")
    
    # check for (1) correct answer, (2) answer matches the matrix representation, (3) correct column size, (4) correct row size 
    assert compare_arrays(query1.answer(data), data.sum(axis=(0,len(shape)-1)).flatten())
    assert compare_arrays(query1.matrixRep() * data.flatten(), data.sum(axis=(0, len(shape)-1)).flatten())
    assert compare_arrays(query1.answer(data), query1a.answer(data))
    assert query1.domainSize() == data.size
    assert query1.domainSize() == query1.matrixRep().shape[1]
    assert query1.numAnswers() == query1.matrixRep().shape[0]

    # check for (1) correct answer, (2) answer matches the matrix representation, (3) correct column size, (4) correct row size 
    assert compare_arrays(query2.answer(data), data.sum(axis=(0,len(shape)-1)).flatten())
    assert compare_arrays(query2.matrixRep() * data.flatten(), data.sum(axis=(0, len(shape)-1)).flatten())
    assert query2.domainSize() == data.size
    assert query2.domainSize() == query2.matrixRep().shape[1]
    assert query2.numAnswers() == query2.matrixRep().shape[0]
    assert compare_arrays(query2.answer(data), query2a.answer(data))

    # check for (1) correct answer, (2) answer matches the matrix representation, (3) correct column size, (4) correct row size
    assert compare_arrays(query3.answer(data), data.sum().flatten())
    assert compare_arrays(query3.matrixRep() * data.flatten(), data.sum().flatten())
    assert query3.domainSize() == data.size
    assert query3.domainSize() == query3.matrixRep().shape[1]
    assert query3.numAnswers() == query3.matrixRep().shape[0]
    assert compare_arrays(query3.answer(data), query3a.answer(data))

    #check sensitivity
    assert np.abs(query1.matrixRep()).sum(axis=0).max() == query1.sensitivity()
    assert np.abs(query2.matrixRep()).sum(axis=0).max() == query2.sensitivity()
    assert np.abs(query3.matrixRep()).sum(axis=0).max() == query3.sensitivity()
    #make sure it is counting query
    assert query1.isIntegerQuery()
    assert query2.isIntegerQuery()
    assert query3.isIntegerQuery()

def test_stackedquery1(data):
    """ we will stack a SumoverQuery and a SparseKronQuery """
    q1 = cenquery.SumoverQuery(data.shape, add_over_margins=(1,))
    matrices1 = [ss.eye(x) for x in data.shape]
    matrices1[0] = np.ones(data.shape[0])
    matrices1[-1] = ss.csr_matrix(np.ones(data.shape[-1]))
    q2 = cenquery.SparseKronQuery(matrices1)
    q3 = q1
    #now stack
    q4 = cenquery.StackedQuery([q1, q2, q3])
    answer = q4.answer(data)
    expected = np.concatenate([q1.answer(data), q2.answer(data), q3.answer(data)])
    assert compare_arrays(answer, expected)
    #check sensitivities
    assert np.abs(q1.matrixRep()).sum(axis=0).max() == q1.sensitivity()
    assert np.abs(q2.matrixRep()).sum(axis=0).max() == q2.sensitivity()
    assert np.abs(q3.matrixRep()).sum(axis=0).max() == q3.sensitivity()
    assert np.abs(q4.matrixRep()).sum(axis=0).max() == q4.sensitivity()
    #make sure it is counting query
    assert q1.isIntegerQuery()
    assert q2.isIntegerQuery()
    assert q3.isIntegerQuery()
    assert q4.isIntegerQuery()
    #check shape
    assert q4.domainSize() == q4.matrixRep().shape[1]
    assert q4.domainSize() == data.size
    assert q4.numAnswers() == q4.matrixRep().shape[0]
    assert q4.numAnswers() == q1.numAnswers() + q2.numAnswers() + q3.numAnswers()




def test_stackedquery2():
    """ test the subsetting part of StackedQuery """
    data = np.arange(6)
    q1 = cenquery.SparseKronQuery([
          ss.csr_matrix(np.array([[1,1,1,-1,-1,-1], [1, 1, 1, 1, 1, 1]]))
       ])
    q2 = cenquery.SparseKronQuery([
          ss.csr_matrix(np.array([[1,2,3,0,0,0], [0, 0, 0, 1, 1, 1]]))
       ])
    domain_subset = [1,2, 5]
    q3 = cenquery.StackedQuery([q1, q2], domain_subset=domain_subset)
    answer = q3.answer(data[domain_subset])
    expected = np.array([-2, 8, 8, 5])
    assert compare_arrays(answer, expected)
    #check sensitivities
    assert np.abs(q1.matrixRep()).sum(axis=0).max() == q1.sensitivity()
    assert np.abs(q2.matrixRep()).sum(axis=0).max() == q2.sensitivity()
    assert np.abs(q3.matrixRep()).sum(axis=0).max() == q3.sensitivity()
    #make sure it is counting query
    assert q1.isIntegerQuery()
    assert q2.isIntegerQuery()
    assert q3.isIntegerQuery()
    #check shape
    assert q3.domainSize() == q3.matrixRep().shape[1]
    assert q3.domainSize() == data[domain_subset].size
    assert q3.numAnswers() == q3.matrixRep().shape[0]
    assert q3.numAnswers() == q1.numAnswers() + q2.numAnswers()


def test_queryfactory_without_collapse(data):
    shape = data.shape
    subset = ([0, shape[0]-1], range(0, shape[1], 2), [0, shape[2]-1])
    add_over_margins = (1,)
    q3 = cenquery.QueryFactory.makeTabularQuery(shape, subset=subset, add_over_margins=add_over_margins)
    q4 = cenquery.SumoverQuery(shape, subset=subset, add_over_margins=add_over_margins)
    assert compare_arrays(q3.answer(data), q4.answer(data))
    #check sensitivities
    assert np.abs(q3.matrixRep()).sum(axis=0).max() == q3.sensitivity()
    assert np.abs(q4.matrixRep()).sum(axis=0).max() == q4.sensitivity()
    #make sure it is counting query
    assert q3.isIntegerQuery()
    assert q4.isIntegerQuery()
    #check shape
    assert q3.domainSize() == q3.matrixRep().shape[1]
    assert q3.numAnswers() == q3.answer(data).size
    assert q4.domainSize() == q4.matrixRep().shape[1]
    assert q4.numAnswers() == q4.answer(data).size


#def test_queryfactory_with_collapse1(data):
    #collapse gives the same answer as subset
#    shape = data.shape
#    subset = ( range(shape[0]), range(0, shape[1], 2), range(shape[2]))
#    axis_groupings = [(1, ((0,2)))]
#    add_over_margins = (2,)
#    q5 = cenquery.QueryFactory.makeTabularQuery(shape, subset=subset, add_over_margins=add_over_margins)
#    q6 = cenquery.QueryFactory.makeTabularQuery(shape, subset= None, add_over_margins=add_over_margins, axis_groupings = axis_groupings)
#    assert compare_arrays(q5.answer(data),q6.answer(data)) 
    #check sensitivities
#    assert np.abs(q5.matrixRep()).sum(axis=0).max() == q5.sensitivity()
#    assert np.abs(q6.matrixRep()).sum(axis=0).max() == q6.sensitivity()
    #make sure it is counting query
#    assert q5.isIntegerQuery()
#    assert q6.isIntegerQuery()

#def test_queryfactory_with_collapse2(data):
#    #a "null" collapse
#    shape = data.shape
#    subset = None
#    axis_groupings = [(1, ((0,1,2,3)))]
#    add_over_margins = (2,)
#    q5 = cenquery.QueryFactory.makeTabularQuery(shape, subset=None, add_over_margins=add_over_margins)
#    q6 = cenquery.QueryFactory.makeTabularQuery(shape, subset= None, add_over_margins=add_over_margins, axis_groupings = axis_groupings)
#    assert compare_arrays(q5.answer(data),q6.answer(data)) 
    #check sensitivities
#    assert np.abs(q5.matrixRep()).sum(axis=0).max() == q5.sensitivity()
#    assert np.abs(q6.matrixRep()).sum(axis=0).max() == q6.sensitivity()
    #make sure it is counting query
#    assert q5.isIntegerQuery()
#    assert q6.isIntegerQuery()

def test_counting_query(data):
    """ test if we can detect non-counting query """
    shape = data.shape
    
    #marginalize over first and last dimensions
    matrices1 = [ss.eye(x) for x in shape]
    bad = np.ones(shape[0])
    bad[0] += 0.000000001
    matrices1[0] = bad
    matrices1[-1] = ss.csr_matrix(np.ones(shape[-1]))
    query1 = cenquery.SparseKronQuery(matrices1)
    assert not query1.isIntegerQuery()

def test_sumover_grouped_query(data):
    """ Tests that SumOverGroupedQuery answers queries correctly """
    #these are the same as in test_sumover_query
    s = data.shape

    q1 = cenquery.SumOverGroupedQuery(data.shape, add_over_margins=(0,1,2))
    q2 = cenquery.SumOverGroupedQuery(data.shape)
    q3 = cenquery.SumOverGroupedQuery(data.shape, add_over_margins=(1,))
    subset4 = ([0, s[0]-1], range(0, s[1], 2), [0, s[2]-1])
    q4 = cenquery.SumoverQuery(data.shape, subset=subset4, add_over_margins=(1,))

    # check for (1) correct answer, (2) answer matches the matrix representation 
    assert compare_arrays(q1.answer(data), data.sum().flatten())
    assert compare_arrays(q1.matrixRep() * data.flatten(), data.sum().flatten())
    # check for (1) correct answer, (2) answer matches the matrix representation
    assert compare_arrays(q2.answer(data), data.flatten())
    assert compare_arrays(q2.matrixRep() * data.flatten(), data.flatten())
    # check for (1) correct answer, (2) answer matches the matrix representation
    assert compare_arrays(q3.answer(data), data.sum(axis=(1,)).flatten())
    assert compare_arrays(q3.matrixRep() * data.flatten(), data.sum(axis=(1,)).flatten())
    # check for (1) correct answer, (2) answer matches the matrix representation
    assert compare_arrays(q4.answer(data), data[np.ix_(*subset4)].sum(axis=(1,)).flatten())
    assert compare_arrays(q4.matrixRep() * data.flatten(), data[np.ix_(*subset4)].sum(axis=(1,)).flatten())
    # check that the query sizes are correct
    assert q4.domainSize() == data.size
    assert q4.numAnswers() == q4.answer(data).size, "numAnswers is: {} but should be: {}".format(q4.numAnswers(), q4.answer(data).size)
    
    assert q3.domainSize() == data.size
    assert q3.numAnswers() == q3.answer(data).size, "numAnswers is: {} but should be: {}".format(q3.numAnswers(), q3.answer(data).size)
    
    assert q1.domainSize() == data.size
    assert q1.numAnswers() == q1.answer(data).size, "numAnswers is: {} but should be: {}".format(q1.numAnswers(), q1.answer(data).size)

    # check sensitivities 
    assert q1.sensitivity() == 1
    assert np.abs(q1.matrixRep()).sum(axis=0).max() == q1.sensitivity()
    assert q2.sensitivity() == 1
    assert np.abs(q2.matrixRep()).sum(axis=0).max() == q2.sensitivity()
    assert q3.sensitivity() == 1
    assert np.abs(q3.matrixRep()).sum(axis=0).max() == q3.sensitivity()
    assert q4.sensitivity() == 1
    assert np.abs(q4.matrixRep()).sum(axis=0).max() == q4.sensitivity()
    #make sure it is counting query
    assert q1.isIntegerQuery()
    assert q2.isIntegerQuery()
    assert q3.isIntegerQuery()
    assert q4.isIntegerQuery()

def test_sumover_grouped_query2(data):
    """ Tests that SumOverGroupedQuery answers queries correctly """
    #these are queries that use groupings
    s = data.shape
    
    #the same as a subset
    groupings1 = {0: [[0],]}
    # mutliple groups
    groupings2 = {0: [ [0,1], [2]]}
    #multiple dimensions
    groupings3 = {0: [ [0,1], [2]], 1: [[0],[1]] }
    
    q0 = cenquery.SumOverGroupedQuery(data.shape, add_over_margins=(0,1,2))
    q1 = cenquery.SumOverGroupedQuery(data.shape, groupings = groupings1)
    q2 = cenquery.SumOverGroupedQuery(data.shape, groupings = groupings2)
    q3 = cenquery.SumOverGroupedQuery(data.shape, groupings = groupings3)
    
    q5 = cenquery.SumOverGroupedQuery(data.shape, groupings = groupings3, add_over_margins =(2,) )
    
    # check for (1) correct answer, (2) answer matches the matrix representation 
    assert compare_arrays(q0.answer(data), data.sum().flatten())
    assert compare_arrays(q0.matrixRep() * data.flatten(), data.sum().flatten())
    # check for (1) correct answer, (2) answer matches the matrix representation 
    assert compare_arrays(q1.answer(data), data[0,0:s[1],0:s[2]].flatten())
    assert compare_arrays(q1.matrixRep() * data.flatten(), data[0,0:s[1],0:s[2]].flatten())
    # check for (1) correct answer, (2) answer matches the matrix representation
    right_answer = np.stack([data[0:2,0:s[1],0:s[2]].sum(0,keepdims=False),data[2,0:s[1],0:s[2]]], axis =0).flatten()
    assert compare_arrays(q2.answer(data), right_answer )
    assert compare_arrays(q2.matrixRep() * data.flatten(), right_answer)
    # check for (1) correct answer, (2) answer matches the matrix representation
    right_answer = np.stack([data[0:2,0:2,0:s[2]].sum(0,keepdims=False),data[2,0:2,0:s[2]]], axis =0).flatten()
    assert compare_arrays(q3.answer(data), right_answer)
    assert compare_arrays(q3.matrixRep() * data.flatten(), right_answer)
    # check for (1) correct answer, (2) answer matches the matrix representation
    right_answer = np.stack([data[0:2,0:2,0:s[2]].sum(0,keepdims=False),data[2,0:2,0:s[2]]], axis =0).sum(2).flatten()
    assert compare_arrays(q5.answer(data), right_answer)
    assert compare_arrays(q5.matrixRep() * data.flatten(), right_answer)

    # check that the query sizes are correct
    assert q1.domainSize() == data.size
    assert q1.numAnswers() == q1.answer(data).size, "numAnswers is: {} but should be: {}".format(q1.numAnswers(), q1.answer(data).size)

    #make a query with sensitivty 2
    groupings4 = {0: [ [0,1], [0,1]], 1: [[0],[1]] }
    q4 = cenquery.SumOverGroupedQuery(data.shape, groupings = groupings4)
    
    ## check sensitivities 
    assert q1.sensitivity() == 1
    assert np.abs(q1.matrixRep()).sum(axis=0).max() == q1.sensitivity()
    assert q2.sensitivity() == 1
    assert np.abs(q2.matrixRep()).sum(axis=0).max() == q2.sensitivity()
    assert q3.sensitivity() == 1
    assert np.abs(q3.matrixRep()).sum(axis=0).max() == q3.sensitivity()
    assert q4.sensitivity() == 2
    assert np.abs(q4.matrixRep()).sum(axis=0).max() == q4.sensitivity()
    ##make sure it is counting query
    assert q1.isIntegerQuery()
    assert q2.isIntegerQuery()
    assert q3.isIntegerQuery()


def test_makeTabularGroupQuery(data):
    shape = data.shape
    groupings = {1: [[1],]}
    groupings2 = {1: [range(2)]}
    add_over_margins = (2,)
    q5 = cenquery.QueryFactory.makeTabularGroupQuery(shape, groupings=groupings,  add_over_margins=add_over_margins)
    q6 = cenquery.SumOverGroupedQuery(shape, groupings =groupings, add_over_margins=add_over_margins)
    assert compare_arrays(q5.answer(data),q6.answer(data)) 
    #check sensitivities
    assert np.abs(q5.matrixRep()).sum(axis=0).max() == q5.sensitivity()
    assert np.abs(q6.matrixRep()).sum(axis=0).max() == q6.sensitivity()
    #make sure it is counting query
    assert q5.isIntegerQuery()
    assert q6.isIntegerQuery()
    q7 = cenquery.QueryFactory.makeTabularGroupQuery(shape, groupings=groupings2,  add_over_margins=add_over_margins)
    q8 = cenquery.SumOverGroupedQuery(shape, groupings =groupings2, add_over_margins=add_over_margins)
    assert compare_arrays(q5.answer(data),q6.answer(data)) 
    #check sensitivities
    assert np.abs(q7.matrixRep()).sum(axis=0).max() == q7.sensitivity()
    assert np.abs(q8.matrixRep()).sum(axis=0).max() == q8.sensitivity()
    #make sure it is counting query
    assert q7.isIntegerQuery()
    assert q8.isIntegerQuery()
    #test when it doesn't have a groupings
    q9 = cenquery.QueryFactory.makeTabularGroupQuery(shape, add_over_margins=add_over_margins)
    q10 = cenquery.SumOverGroupedQuery(shape, add_over_margins=add_over_margins)
    assert compare_arrays(q5.answer(data),q6.answer(data)) 
    #check sensitivities
    assert np.abs(q9.matrixRep()).sum(axis=0).max() == q9.sensitivity()
    assert np.abs(q10.matrixRep()).sum(axis=0).max() == q10.sensitivity()
    #make sure it is counting query
    assert q9.isIntegerQuery()
    assert q10.isIntegerQuery()


def test_makeTabularGroupQuery(data):
    import programs.engine.cenquery as cenquery_old
    shape = data.shape
    add_over_margins = (2,)
    subset = (range(3), [1,2], range(5))
    groupings = {1: [[1],[2]]}
    axis_groupings = [ (1, ([0,1],[2])), (2, ([1,3],[0,2] ))]
    groupings2 = {1: [[0,1],[2]], 2: [[1,3],[0,2]]}
    q1 = cenquery_old.Query(shape,  add_over_margins=add_over_margins).convertToQuerybase()
    q2 = cenquery.QueryFactory.makeTabularGroupQuery(shape,  add_over_margins=add_over_margins) 
    assert compare_arrays(q1.answer(data),q2.answer(data)) 
    q3 = cenquery_old.Query(shape,  add_over_margins=add_over_margins, subset=subset).convertToQuerybase()
    q4 = cenquery.QueryFactory.makeTabularGroupQuery(shape,  add_over_margins=add_over_margins, groupings=groupings) 
    assert compare_arrays(q3.answer(data),q4.answer(data)) 
    q5 = cenquery_old.Query(shape,  add_over_margins=(0,), axis_groupings = axis_groupings).convertToQuerybase()
    q6 = cenquery.QueryFactory.makeTabularGroupQuery(shape,  add_over_margins=(0,), groupings=groupings2) 
    assert compare_arrays(q5.answer(data),q6.answer(data)) 