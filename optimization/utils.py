""" Utility functions for creating and solving optimization problems """

import os
import logging
import time
import das_framework.ctools.clogging as clogging
import gurobipy as gb
import numpy as np
from constants import *


def getGurobiEnvironment(config, retries=10):
    """ Create a new license environment

    Input:
        config: config file.

    Output:
        environment object

    Notes:
        if config["ENVIRONMENT"] is "GAM" it uses the free license.

    """
    clogging.setup(syslog=True)
    logging.info("Creating environment...")
    os.environ[GRB_LICENSE_FILE] = config[GUROBI][GUROBI_LIC]
    cluster = config.get(ENVIRONMENT, CLUSTER_OPTION, fallback=CENSUS_CLUSTER)
    env = None
    rand_wait_base = np.random.uniform(1, 3)
    attempts = 0
    success = False
    while (not success) and attempts < retries:
        try:
            if cluster == GAM_CLUSTER:
                env = gb.Env()
            else:
                logfile = os.path.expandvars(config[GUROBI][GUROBI_LOGFILE_NAME])
                env1 = config[ENVIRONMENT][GRB_ISV_NAME]
                env2 = config[ENVIRONMENT][GRB_APP_NAME]
                env3 = int(config[ENVIRONMENT][GRB_ENV3])
                env4 = config[ENVIRONMENT][GRB_ENV4].strip()
                env = gb.Env.OtherEnv(logfile, env1, env2, env3, env4)
            success = True
        except gb.GurobiError as err:
            attempts += 1
            if attempts == retries:
                raise err
            rand_wait = 1.3**(attempts-1)*rand_wait_base
            time.sleep(rand_wait/1000)
    if cluster == GAM_CLUSTER:
        logging.debug("gurobi environment creation succeeded on attempt %s", attempts)
    else:
        logging.debug("Successfully connected to Gurobi token server on attempt %s", attempts)
    return env


def newModel(*, name, env, config):
    """ Creates a new model

    Inputs:
       name: model name
       env: license environment
       config: config file specifying model parameters

    Output:
       a model object

    """
    model = gb.Model(name, env=env)
    model.Params.LogFile = os.path.expandvars(config[GUROBI][GUROBI_LOGFILE_NAME])
    model.Params.OutputFlag = int(config[GUROBI][OUTPUT_FLAG])
    model.Params.OptimalityTol = float(config[GUROBI][OPTIMALITY_TOL])
    model.Params.BarConvTol = float(config[GUROBI][BAR_CONV_TOL])
    model.Params.BarQCPConvTol = float(config[GUROBI][BAR_QCP_CONV_TOL])
    model.Params.BarIterLimit = int(config[GUROBI][BAR_ITER_LIMIT])
    model.Params.FeasibilityTol = float(config[GUROBI][FEASIBILITY_TOL])
    model.Params.Threads = int(config[GUROBI][THREADS])
    model.Params.Presolve = int(config[GUROBI][PRESOLVE])
    model.Params.NumericFocus = int(config[GUROBI][NUMERIC_FOCUS])
    return model

def convertSense(sense):
    """ Converts an inequality description into a Gurobi sense

    Input:
       sense: one of =, ==, (these are equivalent)
                   <, <=, le, lt,  (these are equivalent)
                  >, >=, gt, ge (these are equivalent)
    Output:
       the appropriate GRB constant
    """
    converter = {"=": gb.GRB.EQUAL, "==": gb.GRB.EQUAL,
                 "<=": gb.GRB.LESS_EQUAL, "lt": gb.GRB.LESS_EQUAL,
                 "le": gb.GRB.LESS_EQUAL, "<": gb.GRB.LESS_EQUAL,
                 "ge": gb.GRB.GREATER_EQUAL, ">": gb.GRB.GREATER_EQUAL,
                 ">=": gb.GRB.GREATER_EQUAL, "gt": gb.GRB.GREATER_EQUAL
                }
    return converter[sense]

def addConstraint(csrmatrix_rep, sense, rhs, model, variables, name):
    """ Adds a constraint to a model

    Input:
       csrmatrix_rep: a matrix representation of the constraint in csr format
       sense: '<', '=', '>' or equivalent (e.g., ==, lt, le, gt, ge)
       rhs: right hand side (constant) of the constraint
       model: the gurobi model
       variables: the base variables of the model involved in the constraint
       name: name of the constraint

    Note: variables is a vector with the semantics that csrmatrix_rep * variables = rhs

    Output: the model is modified, otherwise there is no output
    """
    gurobi_sense = convertSense(sense)
    num_rows = csrmatrix_rep.shape[0]
    for i in range(num_rows):
        expr = [variables[x] for x in csrmatrix_rep[i, :].indices]
        model.addConstr(lhs=sum(expr), sense=gurobi_sense, rhs=rhs[i], name=name)
