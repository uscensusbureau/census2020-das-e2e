import os
import sys

import optimization.utils as gutils
import numpy as np
import itertools
import socket
import pickle
import logging
import time
import ctools
import ctools.clogging as clogging
from gurobipy import *
import json
import os
import scipy.sparse as ss
import psutil

import programs.engine.primitives as primitives
import programs.engine.cenquery as cenquery
import programs.engine.helper as helper
import programs.engine.checkpoint as checkpoint

GUROBI = "gurobi"
GUROBI_LIC="gurobi_lic"
ENGINE = "engine"
WRITER = "writer"

def l2geoimp(parent, NoisyChild, DPqueries, config,gurobi_environment,
             NoisyChild_weight= 1.0, query_weights=None,
             constraints = None, nnls=True, identifier="",
             backup_feas = False, min_schema_add_over_dims = None):
    """
    This solves the geography imputation problem using the L2 solution from Gurobi.
    
    Inputs:
        parent: a numpy array representing the parent geo histogram constraint, (None) if no constraint 
        NoisyChild: a numpy array representing the DP query of the detailed child array
        DPqueries: a list of cenquery.DPquery objects (this may be an empty list)
        config: the configuration object
        gurobi_environment: the gurobi environment
        NoisyChild_weight: an number (int or float) representing the weight (in the objective function) given to the set of detailed queries
        query_weights: a list of number (ints or floats) representing the weight (in the objective function) given to the additional queries
        constraints: a list of cenquery.Constraint objects
        nnls: whether to enforce the nonnegativity constraints (True or False)
        identifier: a string to identify the solve
        backup_feas: if ture, invoke the backup feasibility
        min_schema_add_over_dims: a tuple e.g. (1,2) the dimensions of the minimal schema that we will use to add over
    
    Output:
        result: a numpy array with the shape of the parent data plus an additional dimension with size childGeoLen
        mStatus: a model status variable - gurobi's model status codes
    """
    #Some preliminaries
    #the number of children is the last dimension
    childGeoLen = NoisyChild.shape[-1]
    #logic for backup feasibility, zero subsetting, and parent constraints
    use_parent_constraints = True
    if parent is None:
        use_parent_constraints=False
    if backup_feas:
        use_parent_constraints=False

    ip = helper.getIP()

    pmem0 = helper.memStats(os.getpid())
    print(f"Parent constraints = {use_parent_constraints}. On identifier {identifier} in node {ip} before l2geoimp model is built: {pmem0}. # children: {NoisyChild.shape[-1]}")

    ###############################
    # Step 1: Set solver parameters
    ###############################
    m = gutils.newModel(name="persondata", env=gurobi_environment, config=config)
    
    ##############################################################
    # Step 2: Get rid variables that we know are 0 from the parent
    ##############################################################
    
    if parent is None:
        parent_shape = NoisyChild.shape[0:-1]
        parent_temp = np.ones(parent_shape, dtype=bool)
        parent_ind = np.where(parent_temp >0)
        parent_ind2 = parent_temp.flatten() >0
        #n is the number of cells in the parent we are using for the solve
        n= np.prod(parent_temp.shape)
    else:
        parent_shape = parent.shape
        if backup_feas==False:
            parent_ind = np.where(parent >0)
            parent_sub = parent[parent_ind]
            parent_ind2 = parent.flatten() >0 
        elif min_schema_add_over_dims:
            temp = parent.sum(min_schema_add_over_dims) >  0
            for dim in min_schema_add_over_dims:
                temp=np.repeat(np.expand_dims(temp,dim),parent_shape[dim],axis=dim)
            parent_ind = np.where(temp==True)
            parent_sub = parent[parent_ind]
            parent_ind2 = temp.flatten() == True
            parent_ind_ms = parent.sum(min_schema_add_over_dims) >  0
        else:
            parent_temp = np.ones(parent_shape, dtype=bool)
            parent_ind = np.where(parent_temp >0)
            parent_sub = parent[parent_ind]
            parent_ind2 = parent_temp.flatten() >0 
        n=np.sum(parent_sub.size)
         
    #subset and reshape to 2 dims
    NoisyChild_sub = NoisyChild[parent_ind].reshape((n,childGeoLen))
    
    childshape=NoisyChild.shape
    temp = np.zeros( childshape, dtype = bool)
    temp[parent_ind] = True
    child_ind_f = temp
    child_ind = temp.flatten()
    
    #################################################
    # Step 3: Prepare the optimizer
    #
    # Note: This does the necessary background matrix
    # work, etc. to prepare for the optimizer.
    #################################################
    
    #total number of cells to solve for
    domain_size = n * childGeoLen
    #for non-negative answers (or not)
    lb = 0 if nnls else -GRB.INFINITY
    
    #the vars is an n by childGeoLen array
    thevars = m.addVars( int(n),int(childGeoLen), vtype=GRB.CONTINUOUS, lb=lb, name = "cells")
    m.update()
    
    ################################################################
    # Step 4: Prepare objective function and constraints for queries
    ################################################################

    # Initialize objective function
    obj_fun = QuadExpr()
    # Add objective function for detailed queries
    for i in range(n):
        for j in range(childGeoLen):
            expr = LinExpr(thevars[i,j] - NoisyChild_sub[i,j])
            obj_fun.add(expr=expr*expr, mult=NoisyChild_weight)
    
    # Add objective function for additional queries, only if we have some dp_queries
    the_weights = [1.0 for _ in DPqueries] if query_weights is None else query_weights
    if DPqueries:
        #find the cells of thevars that they are associated with
        marg=0 #counter
        for (dpq, w) in zip(DPqueries, the_weights):
            if dpq.DPanswer.shape[:-1] == ():
                num_aux_vars = 1
            else:
                num_aux_vars = np.prod(dpq.DPanswer.shape[:-1])
            aux_vars = m.addVars( int(num_aux_vars), int(childGeoLen), vtype=GRB.CONTINUOUS, lb=lb, name= "m" + str(marg))
            q = dpq.query
            a = dpq.DPanswer.reshape(num_aux_vars,childGeoLen)
            tmp = compute_kron(array_dims=q.array_dims[:-1], add_over_margins=q.add_over_margins, subset_input=q.subset_input[:-1], axis_groupings = q.axis_groupings)
            tmp2=tmp[:,parent_ind2]
            
            #make constraints
            w_array_ind = dpq.query.weight_array is not None
            for i in range(int(num_aux_vars)):
                for j in range(childGeoLen):
                    expr = [thevars[x,j] for x in tmp2.tocsr()[i,:].indices.tolist() ]
                    if w_array_ind:
                        multiplier = tmp2.tocsr()[i,:].data.tolist()
                        lin_exp = [a*b for a,b in zip(expr,multiplier)] 
                        m.addConstr( lhs = sum(lin_exp), sense=GRB.EQUAL, rhs =aux_vars[i,j] 
                                                , name = "m_cons"+str(marg) )
                    else:
                        m.addConstr( lhs = sum(expr), sense=GRB.EQUAL, rhs =aux_vars[i,j] 
                                               , name = "m_cons"+str(marg) )

            #objective function
            for i in range(num_aux_vars):
                for j in range(childGeoLen):
                    expr = LinExpr(aux_vars[i,j] - a[i,j])
                    obj_fun.add(expr=expr*expr, mult=w)

            marg+=1
        
    m.update()
    
    #backup feasibility secondary objective function
    if backup_feas:
        rhs = parent_sub.flatten()
        slack = m.addVars( int(n), vtype=GRB.CONTINUOUS, lb=0.0, name= "backup")
        m.update()
        #constraints
        for i in range(n):
           constexpr = LinExpr( -1.0 * thevars.sum(i, '*'))
           constexpr += rhs[i]
           temp=m.addConstr(lhs=slack[i], sense=GRB.GREATER_EQUAL, rhs =constexpr )
           constexpr2 = LinExpr( thevars.sum(i, '*'))
           constexpr2 += -rhs[i]
           temp=m.addConstr(lhs=slack[i], sense=GRB.GREATER_EQUAL, rhs =constexpr2 )
        backup_obj_fun = LinExpr()
        m.update()
        for i in range(n):
            expr1 = LinExpr(slack[i])
            backup_obj_fun.add(expr1)

        #add marginal constraint
        if min_schema_add_over_dims:
            rhs = parent.sum(min_schema_add_over_dims)[parent_ind_ms].flatten()
            n_rhs = len(rhs)
            dims = parent.shape
            subset_input  = tuple(range(dims[x]) for x in range(len(dims)))
            tmp_kron=compute_kron(array_dims=dims, add_over_margins=min_schema_add_over_dims, subset_input=subset_input, axis_groupings=None)
            #only select the non-zero rows from the marginalized parent
            row_ind = parent_ind_ms.flatten()
            tmp_kron2=tmp_kron[:,parent_ind2][row_ind,:]
            parent_back_cons = []
            children_geos = range(childGeoLen)
            for i in range(n_rhs):
                xlist = tmp_kron2.tocsr()[i,:].indices.tolist()
                expr = [thevars[x,j] for x in xlist for j in children_geos]
                m.addConstr( lhs = sum(expr), sense=GRB.EQUAL, rhs =rhs[i], name = "backup_cons")
            m.update()

    #########################################
    # Step 5: Prepare the Various Constraints
    #########################################
    
    if backup_feas == False and parent is not None:
        #add parent geo constraint
        rhs=parent_sub.flatten()
        parent_cons = []
        for i in range(n):
            parent_cons.append( m.addConstr( thevars.sum(i,'*') == rhs[i], name = "cons_p") )
        m.update()

    # Other Constraints
    if constraints is not None:
        for c in constraints:
            addConstraint(c=c, model=m, childGeoLen=childGeoLen, parent_ind2=parent_ind2, thevars=thevars)

        m.update()
    
    ######################################################
    # Step 6: Solve using the Gurobi optimizer
    #
    # Note: This also checks the solution for feasibility.
    ######################################################
    
    pmem1 = helper.memStats(os.getpid())
    print(f"Parent constraints = {use_parent_constraints}. On identifier {identifier} in node {ip}, physical memory usage before l2geoimp model is optimized: {pmem1}. Increase since l2geoimp started: {pmem1.pMem-pmem0.pMem}. # children: {NoisyChild.shape[-1]}")

    if identifier == "nat_to_nat" or identifier == "0":
        print(f"For identifier {identifier}, just before optimization, this was the process list:")
        for p in psutil.process_iter():
            print("proc name: ", p.name(), " has parent : ", p.parent())

    #backup feasibility objective function minimize
    if backup_feas:
        m.setObjective(backup_obj_fun, sense = GRB.MINIMIZE)
        m.optimize()
        obj_val = m.ObjVal
        #make constraint for conditional optimization
        obj_slack = 1.0
        temp=m.addConstr(lhs=backup_obj_fun, sense=GRB.LESS_EQUAL, rhs =obj_val + obj_slack  )

    #primary objective function optimization
    m.update()
    m.setObjective(obj_fun, sense = GRB.MINIMIZE)
    m.optimize()
    mStatus = m.Status
    m.printStats()

    pmem2 = helper.memStats(os.getpid())
    print(f"Parent constraints = {use_parent_constraints}. On identifier {identifier} in node {ip}, physical memory usage after l2geoimp model is optimized: {pmem2}. Increase since l2geoimp started: {pmem2.pMem-pmem0.pMem}. # children: {NoisyChild.shape[-1]}")

    if m.Status != GRB.OPTIMAL:
        result= None
        del m
        disposeDefaultEnv()
        return result, mStatus
                
    else:
        sol = np.array( [[thevars[i,j].X  for j in range(childGeoLen)] for i in range(n) ])
        temp_result = np.zeros((np.prod(parent_shape),childGeoLen))
        temp_result[parent_ind2,:] = sol
        result = temp_result.reshape(childshape)
        if nnls:
            result[result < 1e-7]=0
    #Python: Delete all Model objects, delete the Env object (if used), then call disposeDefaultEnv()
    del m
    disposeDefaultEnv()

    pmem3 = helper.memStats(os.getpid())
    print(f"Parent constraints = {use_parent_constraints}. On identifier {identifier} in node {ip}, physical memory usage after l2geoimp model is disposed: {pmem3}. Increase since l2geoimp started: {pmem3.pMem-pmem0.pMem}. # children: {NoisyChild.shape[-1]}")

    return result, mStatus

def geoimp_round(config, child, parent, gurobi_environment,
                 constraints=None, identifier="",
                 backup_feas = False, min_schema_add_over_dims=None):
                 
    """
        This function solves the integer rounding problem for geography imputation.  
        It takes a non-negative solution and finds a nearby non-negative integer solution to the problem.
    
        Inputs:
            conifg: config object
            child: a numpy array that is a non-negative solution to the problem (meets all defined constraints)
            parent: the numpy parent that is our geography constraint, (None) if no constraint 
            gurobi_environment:
            constraints: a list of cenquery.Constraint objects
            backup_feas: if true invoke the backup feasibility
            min_schema_add_over_dims: a tuple e.g. (1,2) the dimensions of the minimal schema that we will use to add over
        
        Output:
            result: a numpy array with the same shape as child
    """
    ip = helper.getIP()

    #some initial logic for parent constraints, subsetting zeros
    use_parent_constraints = True
    if parent is None:
        use_parent_constraints=False
    if backup_feas:
        use_parent_constraints=False

    pmem0 = helper.memStats(os.getpid())
    print(f"Parent constraints = {use_parent_constraints}. On identifier {identifier} in node {ip}, physical memory usage before geoimp_round model is built: {pmem0}. # children: {child.shape[-1]}")

    ###############################
    # Step 1: Set solver parameters
    ###############################
    m = gutils.newModel(name="int_persondata", env=gurobi_environment, config=config)

    ######################################################
    # Step 2: Prepare the Data
    #
    # Note: This works on the parent and child information
    #       in preparation for the optimizer.
    ######################################################
    childGeoLen = child.shape[-1]
    child_floor = np.floor(child)
    child_leftover = child - child_floor

    if parent is None:
        parent_shape = child.shape[0:-1]
        print("opt parent_shape:", parent_shape)
        parent_temp = np.ones(parent_shape, dtype=bool)
        parent_ind = np.where(parent_temp >0)
        parent_ind2 = parent_temp.flatten() > 0
        n= np.prod(parent_temp.shape)
    else:
        parent_shape = parent.shape
        parent_diff = parent - np.sum(child_floor, (len(child.shape) - 1) )
        if backup_feas==False:
            parent_ind = np.where(parent_diff >0)
            parent_ind2 = parent_diff.flatten() > 0
            parent_diff_sub = parent_diff[parent_ind]
        elif min_schema_add_over_dims:
            temp = parent_diff.sum(min_schema_add_over_dims) >  0
            for dim in min_schema_add_over_dims:
                temp=np.repeat(np.expand_dims(temp,dim),parent_shape[dim],axis=dim)
            parent_ind = np.where(temp==True)
            parent_ind2 = temp.flatten() == True
            parent_diff_sub = parent_diff[parent_ind]
            parent_diff_ind_ms = parent_diff.sum(min_schema_add_over_dims) >  0
        else:
            parent_temp = np.ones(parent_shape, dtype=bool)
            parent_ind = np.where(parent_temp >0)
            parent_ind2 = parent_temp.flatten() > 0
            parent_diff_sub = parent_diff[parent_ind] 

        n = parent_diff_sub.size
        print("n=", n)

    child_ind = np.zeros(child.shape, dtype=bool)
    child_ind[parent_ind] = True
    child_leftover_sub = child_leftover[parent_ind].reshape((n,childGeoLen))

    #################################################
    # Step 3: Prepare the optimizer
    #
    # Note: This does the necessary matrix background
    # work, etc. to prepare for the optimizer.
    #################################################

    #domain_size = n * childGeoLen
    thevars = m.addVars(int(n),int(childGeoLen), vtype=GRB.BINARY, lb=0.0, ub = 1.0, name = "cells")
    m.update()

    ####################################
    # Step 4: Prepare objective function
    ####################################
    obj_fun = QuadExpr()
    for i in range(n):
        for j in range(childGeoLen):
            expr = LinExpr(thevars[i,j])
            obj_fun.add(expr, mult=1-2*child_leftover_sub[i,j])
    #obj_fun = LinExpr([1-2*child_leftover_sub[x] for x in range(child_leftover_sub.size)], [thevars[x] for x in range(len(thevars))])

    #backup feasibility, secondary optimization function
    if backup_feas:
        rhs=parent_diff_sub.flatten()
        slack = m.addVars(range(n), vtype=GRB.INTEGER, lb=0.0, name= "backup")
        for i in range(n):
           constexpr = LinExpr( -1.0 * thevars.sum(i, '*'))
           constexpr += rhs[i]
           temp=m.addConstr(lhs=slack[i], sense=GRB.GREATER_EQUAL, rhs =constexpr )
           constexpr2 = LinExpr( thevars.sum(i, '*'))
           constexpr2 += -rhs[i]
           temp=m.addConstr(lhs=slack[i], sense=GRB.GREATER_EQUAL, rhs =constexpr2 )

        backup_obj_fun = LinExpr()
        m.update()
        for i in range(n):
            expr1 = LinExpr(slack[i])
            backup_obj_fun.add(expr1)

        #add marginal constraint
        if min_schema_add_over_dims:
            rhs = parent_diff.sum(min_schema_add_over_dims)[parent_diff_ind_ms].flatten()
            n_rhs = len(rhs)
            dims = parent.shape
            subset_input  = tuple(range(dims[x]) for x in range(len(dims)))
            tmp_kron=compute_kron(array_dims=dims, add_over_margins=min_schema_add_over_dims, subset_input=subset_input, axis_groupings=None)
            #only select the non-zero rows from the marginalized parent
            row_ind = parent_diff_ind_ms.flatten()
            tmp_kron2=tmp_kron[:,parent_ind2][row_ind,:]
            parent_back_cons = []
            children_geos = range(childGeoLen)
            for i in range(n_rhs):
                xlist = tmp_kron2.tocsr()[i,:].indices.tolist()
                expr = [thevars[x,j] for x in xlist for j in children_geos]
                m.addConstr( lhs = sum(expr), sense=GRB.EQUAL, rhs =rhs[i], name = "backup_cons")
            m.update()

    #########################################
    # Step 5: Prepare the Various Constraints
    #########################################
    
    if backup_feas==False and parent is not None:
        #add parent geo constraint
        rhs=parent_diff_sub.flatten()
        parent_cons = []
        for i in range(n):
            parent_cons.append( m.addConstr( thevars.sum(i,'*') == rhs[i], name = "cons_p") )
        m.update()

    # Other Constraints
    if constraints is not None:
        for c in constraints:
            addConstraint(c=c, model=m, childGeoLen=childGeoLen, parent_ind2=parent_ind2,
                                                thevars=thevars,child_floor=child_floor)
        m.update()

    ######################################################
    # Step 6: Solve using the Gurobi optimizer
    #
    # Note: This also checks the solution for feasibility.
    ######################################################
   
    pmem1 = helper.memStats(os.getpid())
    print(f"Parent constraints = {use_parent_constraints}. On identifier {identifier} in node {ip}, physical memory usage before geoimp_round model is optimized: {pmem1}. Increase since l2geoimp started: {pmem1.pMem-pmem0.pMem}. # children: {child.shape[-1]}")

    #backup feasibility objective function
    if backup_feas:
        m.setObjective(backup_obj_fun, sense=GRB.MINIMIZE)
        m.optimize()
        obj_val = m.ObjVal
        #make constraint for conditional optimization
        obj_slack = 1.0
        temp=m.addConstr(lhs=backup_obj_fun, sense=GRB.LESS_EQUAL, rhs =obj_val + obj_slack  )

    m.update()
    m.setObjective(obj_fun, sense=GRB.MINIMIZE)
    m.optimize()

    pmem2 = helper.memStats(os.getpid())
    print(f"Parent constraints = {use_parent_constraints}. On identifier {identifier} in node {ip}, physical memory usage after geoimp_round model is optimized: {pmem2}. Increase since l2geoimp started: {pmem2.pMem-pmem0.pMem}. # children: {child.shape[-1]}")

    if m.Status != GRB.OPTIMAL:
        mStatus = m.Status
        del m
        disposeDefaultEnv()
        pmem3 = helper.memStats(os.getpid())
        print(f"Parent constraints = {use_parent_constraints}. On identifier {identifier} in node {ip}, physical memory usage after geoimp_round model is disposed: {pmem3}. Increase since l2geoimp started: {pmem3.pMem-pmem0.pMem}. # children: {child.shape[-1]}")
        return None, mStatus
    else:
        sol = np.array( [[thevars[i,j].X  for j in range(childGeoLen)] for i in range(n) ])
        # Reformulate full estimated table
        result = child_floor.reshape((np.prod(parent_shape),childGeoLen))
        if n>0:
            result[parent_ind2,:] = result[parent_ind2,:] + sol
        result = result.reshape(child.shape)
        result = result.astype(int)
        #Python: Delete all Model objects, delete the Env object (if used), then call disposeDefaultEnv()
        mStatus = m.Status
        del m
        disposeDefaultEnv()

        pmem3 = helper.memStats(os.getpid())
        print(f"Parent constraints = {use_parent_constraints}. On identifier {identifier} in node {ip}, physical memory usage after geoimp_round model is disposed: {pmem3}. Increase since l2geoimp started: {pmem3.pMem-pmem0.pMem}. # children: {child.shape[-1]}")

        return result, mStatus

def L2geoimp_wrapper(parent, NoisyChild, config, DPqueries = [], constraints= None, \
                        NoisyChild_weight=1.0,query_weights=None, identifier = "",
                         parent_constraints = None, min_schema_add_over_dims = None):
    """
        This function does a full iteration of the L2 geography imputation algorithm. Given the inputs, it
        generates DP query answers and finds the L2 post-processing solution. It then uses the integer
        algorithm to find a nearby integer solution.
    
        Inputs:
            parent: a numpy array that is the hierarchical constraint
            true_child: a numpy array that is the true data from the child geography. This needs to have
                        the same dimension of the parent plus an extra dimension (on the end) that
                        represents the added geography.
            queries: a list of cenquery.query objects
            noisychild_budget: the DP budget given to the detailed child 
            queries_budget_list: a list of floats that represent the DP budget given to each query
            constraints: a list of cenquery.constraint objects
            NoisyChild_weight: weight (in the objective function) on the detailed query for the L2 optimization
            query_weights: the weight (in the objective function) given to the additional queries 
            min_schema_add_over_dims: a tuple e.g. (1,2) the dimensions of the minimal schema that we will use to add over
        Outputs:
            output: This is a list with
                [0] a numpy array that is the solution to the L2 optimization (no rounding)
                [1] a numpy array that is the solution after the integer rounding
    """
    ctools.clogging.setup(syslog=True)
    logging.info("LIVE FROM SUITLAND AND AWS, ITS L2geoimp_wrapper")

    e = gutils.getGurobiEnvironment(config) 

    #the number of children
    childGeoLen = NoisyChild.shape[-1]

    ######################################
    # Step 1: Perform the L2 Optimization.
    ######################################
    feasibility_status = 0
    l2answer, mStatus = l2geoimp(config= config, parent = parent,
                                            NoisyChild= NoisyChild,DPqueries=DPqueries, 
                                            NoisyChild_weight = NoisyChild_weight, 
                                            query_weights=query_weights, 
                                            constraints= constraints, identifier=identifier,
                                            backup_feas = False,
                                            gurobi_environment = e)

    print(identifier + " gurobi L2 model status: {}".format(mStatus))

    if mStatus !=2:
            print("running backup feasibility for L2," , identifier)
            print("using minimal schema dims:", min_schema_add_over_dims)
            feasibility_status = 1
            l2answer , mStatus = l2geoimp(config= config, parent = parent,
                                            NoisyChild= NoisyChild,DPqueries=DPqueries, 
                                            NoisyChild_weight = NoisyChild_weight, 
                                            query_weights=query_weights, 
                                            constraints= constraints, identifier=identifier,
                                            backup_feas = True,
                                            gurobi_environment = e,
                                            min_schema_add_over_dims = min_schema_add_over_dims)
            if mStatus !=2:
                del e
                raise Exception(identifier + "infeasible L2 solution. Model status was " + str(mStatus))

    ####################################
    # Step 2: Run the integer algorithm.
    ####################################
    if feasibility_status == 0:
        integer_answer, l1mstatus = geoimp_round(config= config,child = l2answer, parent = parent,
                                            constraints = constraints, identifier=identifier, 
                                            backup_feas = False, gurobi_environment = e)
                                            
        del e
    else:
        integer_answer, l1mstatus = geoimp_round(config= config,child = l2answer, parent = parent,
                                    constraints = constraints, identifier=identifier, 
                                    backup_feas = True, gurobi_environment = e,
                                    min_schema_add_over_dims = min_schema_add_over_dims)
        del e
        print("L1 change in parent bc of feasibility: ",np.sum(np.abs(parent-integer_answer.sum((-1,)))))

    if l1mstatus !=2:
        raise Exception(identifier + "infeasible L1 solution. Model status was " + str(l1mstatus))

    output = [l2answer,integer_answer,bool(feasibility_status)]
    return(output)


def compute_kron(array_dims, add_over_margins, subset_input, axis_groupings, weight_array=None):
    """
        This computes the Kronecker representation of the query.
    """
    if axis_groupings:
        collapse_axes = [x[0] for x in axis_groupings]
        all_collapse_groups = [x[1] for x in axis_groupings]
    else:
        collapse_axes = []
    
    out = ss.identity(1, dtype=bool, format="csr")
    for i in range(len(array_dims)):
        if i in add_over_margins:
            x = np.zeros(array_dims[i], dtype=bool)
            x[subset_input[i]] = True
        elif i in collapse_axes:
            ind = [x for x in range(len(collapse_axes)) if collapse_axes[x] == i]
            collapse_groups = all_collapse_groups[ind[0]]
            n_groups = len(collapse_groups)
            x = np.zeros((n_groups,array_dims[i]), dtype=bool)
            for j in range(n_groups):
                subset = collapse_groups[j]
                x[j,subset] = True 
        else:
            x = ss.identity(array_dims[i], dtype=bool, format="csr")[subset_input[i], :]
        out = ss.kron(out, x, format="csr")
    #matrix multiplication by the weight matrix
    if weight_array is not None:
        out = out * ss.csr_matrix(np.diag(weight_array.flatten()))  
    return out


def addConstraint(c, model, childGeoLen, parent_ind2, thevars, child_floor=None):
    """
    """
    tmp = compute_kron(array_dims=c.query.array_dims[:-1], add_over_margins=c.query.add_over_margins,
                                                subset_input=c.query.subset_input[:-1],
                                                axis_groupings=c.query.axis_groupings)
    tmp2=tmp[:,parent_ind2]
    if child_floor is not None:
        a = a = c.rhs - c.query.answer(child_floor)
    else:
        a = c.rhs
    #a should be two dimensional, first combine everything else besides geo, then geo
    dim1=np.prod(a.shape[:-1])
    dim2=a.shape[-1]
    a=a.reshape(int(dim1),int(dim2))
    #make sure a has a dimension in addition to geography
    if len(a.shape) ==1:
        a=a.reshape(1,len(a))
    first_dim = np.prod(a.shape[:-1])

    if c.sign == "=":
        sense = GRB.EQUAL
    elif c.sign == "le":
        sense = GRB.LESS_EQUAL
    else:
        sense = GRB.GREATER_EQUAL
    #only some of geographies may have the constraint (for the case of union constraints)
    if c.query.subset == None:
        children_geos = range(childGeoLen)
    else:
        children_geos = c.query.subset_input[-1]

    for i  in range(int(first_dim)):
        for j in range(len(children_geos)):
            xlist = tmp2.tocsr()[i,:].indices.tolist()
            expr = [thevars[x,children_geos[j]] for x in xlist]
            model.addConstr( lhs = sum(expr), sense=sense, rhs =a[i,j] , name = c.name )

