import os
import programs.engine.gurobic as gb
import numpy as np
import scipy.sparse as ss

GUROBI="gurobi"

def l2geoimp(childGeoLen, parent, NoisyChild, DPqueries, config, gurobi_environment, NoisyChild_weight= 1.0, query_weights=None, \
                        constraints = [], \
                        nnls=True, identifier="",
                        use_parent_constraints=True,
                        subsets_zeros = True ):
    """
    This solves the geography imputation problem using the L2 solution from Gurobi.

    Inputs:
        childGeoLen: an integer specifying the number of geographies we impute to
        parent: a numpy array representing the parent geo histogram constraint, (None) if no constraint
        NoisyChild: a numpy array representing the DP query of the detailed child array. Dimensions are data dimensions. The last dimension
            is on geography. If we are doing the national level, parent will evaluate to False and the last dimension of NoisyChild will have size 1
        DPqueries: a list of cenquery.DPquery objects (this may be an empty list)
        NoisyChild_weight: the weight (in the objective function) given to the set of detailed queries
        query_weights: the weight (in the objective function) given to the additional queries
        constraints: a list of cenquery.Constraint objects
        nnls: whether to enforce nonnegativity constraints (True or False)
        subsets_zeros: whether to explicitly remove cells from model in which parent is 0

    Output:
        result: a numpy array with the shape of the parent data plus an additional dimension with size childGeoLen
    """
    ##############################
    # Create gurobi environment  #
    ##############################
    so = "/apps/gurobi752/linux64/lib/libgurobi75.so"
    appname = "DAS"
    isv = "Census"
    logfile = "gb.log"
    env = gb.Environment(logfile, appname, isv, so)
    if not env.isok:
       raise Exception("Cannot create environment: {}".format(env.status))
    #######################################
    # Create model and set its parameters #
    #######################################
    m = gb.Model("persondata",  env)
    if not m.isok:
       raise Exception("Cannot create model: {}".format(m.status))
    m.LogFile = os.path.expandvars(config[GUROBI]['gurobi_logfile_name'])
    m.OutputFlag = int(config[GUROBI]['OutputFlag'])
    m.OptimalityTol = float(config[GUROBI]['OptimalityTol'])
    m.BarConvTol =  float(config[GUROBI]['BarConvTol'])
    m.BarQCPConvTol = float(config[GUROBI]['BarQCPConvTol'])
    m.BarIterLimit = int(config[GUROBI]['BarIterLimit'])
    m.FeasibilityTol = float(config[GUROBI]['FeasibilityTol'])
    m.Threads = int(config[GUROBI]['Threads'])
    m.Presolve = int(config[GUROBI]['Presolve'])
    m.NumericFocus = int(config[GUROBI]['NumericFocus'])

    ##############################################################
    # Step 2: Get rid variables that we know are 0 from the parent
    ##############################################################

    demo_shape = NoisyChild.shape[0:-1] #all but the last dimension are demographics attributes
    demo_size = np.prod(demo_shape) #number of demographics cells
    num_child = NoisyChild.shape[-1]
    assert num_child == childGeoLen
    assert (parent is None) or demo_size == parent.size
    #index of nonzeros in lattened parent
    flat_parent = parent.reshape(parent.size) if parent is not None else None
    if subsets_zeros and parent is not None:
        demo_subindex = np.where(flat_parent > 0)
    else:
        demo_subindex =  slice(0,demo_size) 
    #we will flatten parent and convert child array into (demo_size, num_child) array
    #then we will only work on the indexes that are not restricted to 0
    working_parent = flat_parent[demo_subindex] if parent is not None else None
    working_childs = NoisyChild.reshape((demo_size, num_child))[demo_subindex]
    working_demo_size = demo_subindex.size if subsets_zeros and parent is not None else demo_size
    #############################################################
    # Step 3: add detail counts to the objective function
    ############################################################
    #for the gurobi model, the first set of variables should belong to child 1, the second set to child 2, etc
    tmpcoeffs = working_childs.transpose()
    lincoeffs = tmpcoeffs.reshape(tmpcoeffs.size) * -2.0 * NoisyChild_weight
    qprows = np.arange(lincoeffs.size)
    qpcols = qprows
    qpvals = np.full(lincoeffs.size, NoisyChild_weight)
    m.addVars(lincoeffs)
    m.addQPTerms(qprows, qpcols, qpvals)

    ############################################################
    # Step 4: add remaining queries to the objective function
    ###########################################################
    variable_offsets = [] 
    variable_offsetsnd.append(working_demo_size * numchild)
    for (index, dpq) in enumerate(DPQueries):
        weight =   (query_weights[index] if query_weights is not None else 1)
        tmpcoeffs = dpq.DPanswer
        lincoeffs = tmpcoeffs.reshape(tmpcoeffs.size) * (weight * -2.0)
        qprows = np.arange(lincoeffs.size)
        qpcols = qprows
        qpvals = np.full(lincoeffs.size, weight)
        m.addVars(lincoeffs)
        m.addQPTerms(qprows, qpcols, qpvals)
        variable_offsets.append(variable_offsets[-1] + lincoeffs.size)

    ###########################################################
    # Step 5: add parent sum constraints
    ##########################################################
    m.update()
    if working_parent is not None:
       conmat = ss.kron(ss.csr_matrix(np.ones(num_childs)), ss.eye(working_demo_size, format="csr"), format="csr")
       sense = gb.GRB.GRB_EQUAL
       rhs = working_parent
       m.addConstrs(conmat, sense, rhs)
    ########################################################
    # Step 6: add constraints related to the dp queries
    ########################################################

    #########################################################
    # Step 7: add constraints related to the invariants
    ########################################################

    return (childGeoLen, parent, NoisyChild, DPqueries, config, gurobi_environment, NoisyChild_weight, query_weights, constraints, nnls, use_parent_constraints, subsets_zeros)
