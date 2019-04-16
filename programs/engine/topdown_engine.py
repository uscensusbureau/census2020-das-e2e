import logging
import os
import numpy as np
from das_framework.driver import AbstractDASEngine
import gurobipy
from gurobipy import *
from collections import defaultdict
import sys
import programs.engine.cenquery as cenquery
import programs.engine.nodes as nodes
import programs.sparse as sparse
import pickle
import gc
import ctools.clogging
import ctools.geo
import das_utils


BUDGET = "budget"
QUERIESFILE = "queriesfile"
CONSTRAINTS = "constraints"
INVARIANTS = "invariants"
PTABLE ="privacy_table"
CTABLES = "constraint_tables"
GUROBI = "gurobi"
GUROBI_LIC="gurobi_lic"
ENGINE = "engine"
DELETERAW = "delete_raw"
GRB_LICENSE_FILE='GRB_LICENSE_FILE'
THEINVARIANTS = "theInvariants"
THECONSTRAINTS = "theConstraints"

class engine(AbstractDASEngine):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)

        hist_vars = re.split(" ", self.config["reader"]["PersonData.histogram"])
        self.schema = {}
        for i,var in enumerate(hist_vars):
            self.schema[var] = i
        print("schema: ", self.schema)
        if self.config.has_option(CONSTRAINTS, 'minimalSchema'):
            self.minimal_schema = re.split(" ", self.config[CONSTRAINTS]["minimalSchema"])
        else:
            self.minimal_schema = None
        print("minimal schema: ", self.minimal_schema)

    def run(self, block_nodes):
        """
        This is the main function of the topdown engine. It is currently divided into three parts for better integration with simulations/experiments
        The function makeNodesAndAggregate creates the geoNodes without DP data and aggregates.
        The function noisyAnswers creates noisy measurements
        The function topdown implements the topdown algorithm.

        Inputs:
                self: refers to the object of the engine class
                block_nodes: RDDs of nodes objects for each of the lowest level geography 

        Output:
                nodes_dict: a dictionary containing RDDs of node objects for each of the geolevels
                feas_dict: a dictionary containing meta information about optimization
        """
        #make higher level nodes by aggregation
        nodes_dict = self.getNodesAllGeolevels(block_nodes)

        block_nodes.unpersist()
        del block_nodes
        gc.collect()

        nodes_dict = self.noisyAnswers(nodes_dict)
        nodes_dict, feas_dict = self.topdown(nodes_dict)

        return (nodes_dict,feas_dict)

    def noisyAnswers(self, nodes_dict):
        """
        This function is the second half of the topdown engine.
        This function takes the geounitNode objects at each geolevel, adds differential
        privacy measurements

        Inputs:
            nodes_dict:  a dictionary containing RDDs of node objects for each of the geolevels.
                        No DP measurement or privatized data are present on input

        Output:
            nodes_dict: a dictionary containing RDDs of node objects for each of the geolevels
        """
        config = self.config
        
        #bottom to top of tree
        levels = tuple(config["geodict"]["geolevel_names"].split(","))
        n_levels = len(levels)
        #top to bottom of tree
        levels_reversed = list(reversed(levels))
        
        for level in levels:
            nodes_dict[level] = nodes_dict[level].map(lambda node: (make_dp_node(config, node))).persist()
            print(level + " constraints used : " + str(nodes_dict[level].take(1)[0].cons.keys()))
        
        if bool(int(config[ENGINE][DELETERAW])):
            logging.info("Removing True Data Arrays")
            for level in levels:
                #explicitly remove true data "raw"
                nodes_dict[level] = nodes_dict[level].map(lambda node: (deleteTrueArray(node))).persist()
                #check that true data "raw" has been deleted from each, if not will raise an exception
                nodes_dict[level].map(lambda node: (checkTrueArrayIsDeleted(node)))
                
        return nodes_dict

    def topdown(self, nodes_dict):
        """
        This function is the third part of the topdown engine.
        This function and initiates the topdown algorithm.

        Inputs:
            nodes_dict:  a dictionary containing RDDs of node objects for each of the geolevels.
                        No DP measurement or privatized data are present on input

        Output:
            nodes_dict: a dictionary containing RDDs of node objects for each of the geolevels
            feas_dict: a dictionary containing meta information about optimization
        """
        config = self.config
        schema = self.schema
        minimal_schema = self.minimal_schema
        if minimal_schema:
            min_schema_dims = tuple([schema[x] for x in minimal_schema])
        else:
            min_schema_dims = None

        #bottom to top of tree
        levels = tuple(config["geodict"]["geolevel_names"].split(","))
        n_levels = len(levels)
        #top to bottom of tree
        levels_reversed = list(reversed(levels))
        
        #Do Topdown Imputation
        os.environ[GRB_LICENSE_FILE] = config[GUROBI][GUROBI_LIC]
        
        #This does imputation from national to national level.
        nodes_dict[levels_reversed[0]] = nodes_dict[levels_reversed[0]].map(lambda node: geoimp_wrapper_nat(config,node,min_schema_dims)).persist() 
        
        feas_dict = {}

        
        for i in range((n_levels-1)):
            #make accumulator
            feas_dict[levels_reversed[i]] = self.setup.sparkContext.accumulator(0)
            
            parent_rdd = nodes_dict[levels_reversed[i]].map(lambda node: (node.geocode, node))
            child_rdd = nodes_dict[levels_reversed[i+1]].map(lambda node: (node.parentGeocode, node))
            parent_child_rdd = parent_rdd.union(child_rdd).groupByKey()
            
            nodes_dict[levels_reversed[i+1]] = parent_child_rdd.map(lambda nodes: geoimp_wrapper(config,nodes,feas_dict[levels_reversed[i]],min_schema_dims)).flatMap(lambda children: tuple([child for child in children])).persist()
            
        #unpersist all but bottom level
        for i in range((n_levels-1)):
            nodes_dict[levels_reversed[i]].unpersist()
        
        #return dictionary of nodes RDDs
        return nodes_dict, feas_dict

    def getNodesAllGeolevels(self, block_nodes):
        """
        This function takes the block_nodes RDD and aggregates them into higher level nodes.geounitNode RDD objects:

        Inputs:
            block_nodes: RDDs of geounitNode objects aggregated at the block level
        
        Outputs:
            blockgroup_nodes: RDDs of geounitNode objects aggregated at the block group level
            tract_nodes: RDDs of geounitNode objects aggregated at the tract level
            county_nodes: RDDs of geounitNode objects aggregated at the county level
            state_nodes: RDDs of geounitNode objects aggregated at the state level
            national_node: RDDs of geounitNode objects at the national level
        """
        config = self.config
        
        levels = tuple(config["geodict"]["geolevel_names"].split(","))
        n_levels = len(levels)
        nodes_dict={}
        nodes_dict[levels[0]] = block_nodes
        
        for i in range((n_levels - 1)):
            nodes_dict[levels[i+1]] = nodes_dict[levels[i]].map(lambda blockNode: (blockNode.parentGeocode, blockNode)
                            ).reduceByKey(lambda x,y: addGeounitNodes(x,y)
                            ).map(lambda node: shiftGeocodesUp(node)
                            ).map(lambda node: makeAdditionalInvariantsConstraints(node, config)
                            ).persist()
            
        block_nodes.unpersist()
        
        return nodes_dict

def addConstraints(const1, const2):
    """
    For each constraint in the dictionary, this function adds those two constraints together (specifically their right hand sides) and returns a dictionary with the aggregated constraints.
    
    Inputs:
        const1: a dictionary containing the constraint objects from cenquery for a geounit at a specific geolevel
        const2: a dictionary containing the constraint objects from cenquery for a geounit at a specific geolevel
    
    Note: const1 and const2 must be at the same geolevel.

    Output:
        const_sum: a dictionary containing the aggregated constraints from const1 and const2
    
    To do:s3://uscb-decennial-ite-das/ashme001/tmp2
    1. Implement checks that the keys are the same
    2. Implement checks that all values are cenquery.Constraint
    3. Implement checks that the queries and signs in pairs of constraints are equivalent
    """
    
    #get unique keys
    keys = list(set(list(const1.keys()) + list(const2.keys())))
    const_sum = {}
    
    for key in keys:
        addrhs = np.array(np.add(const1[key].rhs, const2[key].rhs))
        const_sum[key] = cenquery.Constraint(query=const1[key].query,
                        rhs=addrhs, sign=const1[key].sign, name =const1[key].name )
    
    return(const_sum)

def addInvariants(invar1, invar2):
    """
    For each invariant in the dictionary, this function adds the two invariant arrays together and returns a dictionary with the aggregated numpy.array invariants.

    Inputs:
        invar1: a dictionary containing the invariant objects from numpy.array for a geounit at a specific geolevel
        invar2: a dictionary containing the invariant objects from numpy.array for a geounit at a specific geolevel

    Note: invar1 and invar2 must be at the same geolevel.

    Output:
        invar_sum: a dictionary containing the aggregated numpy.array invariants from invar1 and invar2
    
    """
    invar_sum= {}
    for key in invar1.keys():
        invar_sum[key] = np.array(np.add(invar1[key], invar2[key]))
        
    return(invar_sum)

def addGeounitNodes(node1, node2):
    """
    This function is both commutative and associative, invoked only by .reduceByKey.
    This function sums geounitNodes together when aggregating to higher geolevels.

    Inputs:
        node1: a node from a geounit containing constraints and invariants
        node2: a node from a geounit containing constraints and invariants

    Output:
        aggregatedNode: a node containing the aggregation, to a higher geolevel, of constraints and invariants from node1 and node2
    
    """
    
    from operator import add
    
    argsDict = {} 
    argsDict["raw"] = node1.raw + node2.raw
    argsDict["raw_housing"] = node1.raw_housing + node2.raw_housing
    if node1.syn and node2.syn:
        argsDict["syn"] = node1.syn + node2.syn
    if node1.cons and node2.cons:
        argsDict["cons"] =  addConstraints(node1.cons,node2.cons)
    if node1.invar and node2.invar:
        argsDict["invar"] = addInvariants(node1.invar,node2.invar)
    argsDict["geocodeDict"] = node1.geocodeDict
    
    aggregatedNode = nodes.geounitNode(node1.geocode, **argsDict)
    
    return aggregatedNode

def shiftGeocodesUp(geocode_geounitNode):
    """
    This sets the parent geocode and the parent geolevel for a specific geounitNode.

    Input:
        geocode_geounitNode: This is a node that stores the geocode, raw data, and DP measurements for a specific geounit.

    Output:
        geounitNode: This is a node that stores the geocode, raw data, and DP measurements for a specific geounit.

    """
    geounitNode = geocode_geounitNode[1]
    geounitNode.geocode = geounitNode.parentGeocode
    geounitNode.setParentGeocode()
    geounitNode.setGeolevel()
    return geounitNode

def deleteTrueArray(node):
    """
    This function explicitly deletes the geounitNode "raw" true data array.

    Input:
        node: a geounitNode object

    Output:
        node: a geounitNode object
    """
    node.raw = None
    node.raw_housing = None
    return(node)

def checkTrueArrayIsDeleted(node):
    """
    This function checks to see if the node.raw is None for a geounitNode object.
    If not it raises an exception.

    Input:
        node: a geounitNode object
    """
    if (node.raw) is not None or (node.raw_housing) is not None:
        raise Exception("The true data array has not been deleted")

def geoimp_wrapper_nat(config, nat_node, min_schema_add_over_dims = None):
    """
    This function performs the Post-Processing Step of National to National level.

    Inputs:
        config: configuration object
        nat_node: a geounitNode object referring to entire nation

    Output:
        nat_node: a geounitNode object referring to entire nation
    """
    import programs.engine.geoimpgbopt as geoimpgbopt
    parent_hist = None
    NoisyChild = np.expand_dims(nat_node.dp.DPanswer, axis=len(nat_node.dp.DPanswer.shape))
    NoisyChild_weight = 1/nat_node.dp.Var
    child_geos =  nat_node.geocode
    parent_geocode = "nat_to_nat"
    #what if DPqueries is empty {}?
    DPqueries = nat_node.dp_queries.values()
    
    if any(nat_node.cons) == False:
        constraints = None
    else:
        constraints = nat_node.cons.values()
    
    query_weights = []
    
    # need to add a dimension for geography to the object
    for x in DPqueries:
        x.query.array_dims = NoisyChild.shape
        x.query.subset_input = tuple(list(x.query.subset_input) + [[0]])
        x.query.subset = np.ix_(*x.query.subset_input)
        if x.query.weight_array is not None:
            x.query.weight_array = np.expand_dims(x.query.weight_array,
                                         axis=len(x.query.weight_array.shape))
        x.DPanswer = np.expand_dims(x.DPanswer, axis=len(x.DPanswer.shape))
        x.check_after_update()
        weight = 1/x.Var
        query_weights.append(weight)
        
    # if no DPqueries, change this to an empty list
    if any(DPqueries)== False:
        DPqueries = []
        query_weights = None
    
    # need to add a dimension for geography to the object
    if constraints is not None:
        for x in constraints:
            x.query.array_dims = NoisyChild.shape
            x.query.subset_input = tuple(list(x.query.subset_input) + [[0]])
            x.query.subset = np.ix_(*x.query.subset_input)
            if x.query.weight_array is not None:
                x.query.weight_array = np.expand_dims(x.query.weight_array,
                            axis=len(x.query.weight_array.shape))
            x.rhs = np.expand_dims(x.rhs, axis=len(x.rhs.shape))
            x.check_after_update()
    
    #this is the actual post-processing optimization step
    l2_answer, int_answer, backup_solve_status = geoimpgbopt.L2geoimp_wrapper(config=config,parent=parent_hist, NoisyChild=NoisyChild, NoisyChild_weight = NoisyChild_weight, DPqueries=DPqueries, query_weights = query_weights, constraints=constraints, identifier="nat_to_nat", min_schema_add_over_dims = min_schema_add_over_dims)
    
    if constraints is not None:
        check = True
        for x in constraints:
            check = bool(np.prod(x.check(int_answer)) * check)
        print("constraints are ", check, "for parent geocode ", parent_geocode)
    
    #get rid of extra dimension
    nat_node.syn = sparse.multiSparse(int_answer.squeeze())
    nat_node.syn_unrounded = sparse.multiSparse(l2_answer.squeeze())
    
    return(nat_node)

def geoimp_wrapper(config, parent_child_node, accum, min_schema_add_over_dims = None):
    """
    This function performs the Post-Processing Step for a generic parent to the Child geography.
    
    Inputs:
        config: configuration object
        parent_child_node: a RDD of geounitNode objects containing one parent and multiple child
        accum: spark accumulator object which tracks the number of solves that use the backup solve

    Output:
        children: a list of Node objects for each of the children, after post-processing
    """

    import programs.engine.geoimpgbopt as geoimpgbopt
    from itertools import compress
    
    parent_child_node = list(parent_child_node)
    parent_geocode = parent_child_node[0] 
    print("parent geocode is", parent_geocode)
    # a list of the node objects
    nodes = list(list(parent_child_node)[1])
    
    #calculate the length of each of the geocodes (to determine which is the parent)
    geocode_lens = [len(node.geocode) for node in nodes]
    #the parent is the shortest geocode
    parent = nodes[np.argmin(geocode_lens)]
    
    #subset the children nodes and sort
    children = nodes[:np.argmin(geocode_lens)] + nodes[np.argmin(geocode_lens)+1:]
    children = sorted(children, key=lambda geocode_data: int(geocode_data.geocode))
    child_geos = [child.geocode for child in children]
    #the number of children
    n_children = len(child_geos)
    
    #######
    #under certain circumstances we can skip the Gurobi optimization
    #######
    
    #Only 1 child
    if n_children == 1:
        children[0].syn = parent.syn
        return children
    if parent.syn.sum() == 0:
        for child in children:
            child.syn=sparse.multiSparse(np.zeros(parent.syn.shape))
        return children
    
    #########
    #resume code for Gurobi optimization
    ########
    
    #stack the dp arrays on top of one another, if only 1 child just expand the axis
    if n_children > 1:
        NoisyChild = np.stack([child.dp.DPanswer for child in children],axis=-1)
    else:
        NoisyChild = np.expand_dims(children[0].dp.DPanswer, axis=len(children[0].dp.DPanswer.shape))
    
    NoisyChild_weight = 1/children[0].dp.Var
    
    #combine DPqueries without geography to combined DPqueries with geography
    #if no DPqueries, change this to an empty list
    query_weights = []
    if any(children[0].dp_queries)== False:
        DPqueries_comb = []
        query_weights = None
    else:
        DPqueries = list(list(child.dp_queries.values()) for child in children)
        n_q= len(DPqueries[0])
        DPqueries_comb = []
        for i in range(n_q):
            subset_input=tuple(list(DPqueries[0][i].query.subset_input) + [range(NoisyChild.shape[-1])])
            axis_groupings = DPqueries[0][i].query.axis_groupings
            # the dimension which will be needed to be added for geography
            if DPqueries[0][i].query.weight_array is not None:
                dims = len(DPqueries[0][i].query.weight_array.shape)
                weight_array = np.repeat(np.expand_dims(DPqueries[0][i].query.weight_array,
                                    dims),n_children, axis = dims )
            else:
                weight_array = None
            query = cenquery.Query(array_dims = NoisyChild.shape, subset=subset_input,
                       add_over_margins=DPqueries[0][i].query.add_over_margins, axis_groupings = axis_groupings, weight_array = weight_array)
            q_answer = np.stack([DPquery[i].DPanswer for DPquery in DPqueries], axis = -1) 
            DP_query = cenquery.DPquery(query = query, DPanswer = q_answer)
            DPqueries_comb.append(DP_query)
            #find the inverse variance
            weight = 1/DPqueries[0][i].Var
            query_weights.append(weight)
    
    #combine cenquery.Constraint objects without geography to combined cenquery.Constraint
    constraints_comb = []
    #children may have different constraints. only combine the ones that match.
    if any(children[0].cons) == False:
        constraints_comb = None
    else:
        all_keys = []
        for child in children:
            all_keys.extend(list(child.cons.keys()))
        #subset to unique names
        constraint_keys = tuple(list(set(all_keys)))
        
        #children is a list of nodes
        for key in constraint_keys:
            #make a list of indiviual constraints for all children who have them
            #find which children have the key
            ind = [key in child.cons.keys() for child in children]
            #children_sub is subset of children with that key
            children_sub = list(compress(children,ind))
            constraints = list(child.cons[key] for child in children_sub)
            
            #get the list of geos that have this constraint
            subset_geos = list(compress(range(NoisyChild.shape[-1]),ind))
            subset_input=tuple(list(constraints[0].query.subset_input) + [subset_geos,])
            axis_groupings = constraints[0].query.axis_groupings
            query = cenquery.Query(array_dims = NoisyChild.shape, subset=subset_input,
                        add_over_margins=constraints[0].query.add_over_margins, axis_groupings=axis_groupings)
            rhs = np.stack([con.rhs for con in constraints], axis = -1)
            constraint = cenquery.Constraint(query = query, rhs=rhs, sign = constraints[0].sign, name = constraints[0].name ) 
            constraints_comb.append(constraint)
            
    parent_hist = parent.syn.toDense()
    parent_geocode = parent.geocode
    parent_constraints = parent.cons  #for checking purposes
    
    #this is the actual post-processing optimization step
    l2_answer, int_answer, backup_solve_status = geoimpgbopt.L2geoimp_wrapper(config=config,parent=parent_hist, NoisyChild=NoisyChild,
    NoisyChild_weight = NoisyChild_weight, DPqueries=DPqueries_comb,query_weights =query_weights,
    constraints=constraints_comb,identifier=parent_geocode, parent_constraints = parent_constraints, min_schema_add_over_dims=min_schema_add_over_dims)
    
    #check constraints
    if constraints_comb is not None:
        check = True
        for x in constraints_comb:
            check = bool(np.prod(x.check(int_answer)) * check)
        print("constraints are ", check, "for parent geocode ", parent_geocode)
    
    #slice off the combined child solution to make separate arrays for each child
    temps = []
    for i in range(len(child_geos)):
        temp = int_answer[tuple( [ slice(0,int_answer.shape[x]) for x in range(len(int_answer.shape) - 1) ] + [slice(i,i+1)] )] #this is really ugly - feel free to improve, trying to subset to each geography
        temp = temp.squeeze() #gets rid of dimensions of size 1
        temps.append(temp)
    
    #do this for unrounded too
    temps2 = []
    for i in range(len(child_geos)):
        temp2 = l2_answer[tuple( [ slice(0,l2_answer.shape[x]) for x in range(len(l2_answer.shape) - 1) ] + [slice(i,i+1)] )] #this is really ugly - feel free to improve, trying to subset to each geography
        temp2 = temp2.squeeze() #gets rid of dimensions of size 1
        temps2.append(temp2) 
    
    #make sparse arrays
    for i, geocode in enumerate(child_geos):
        children[i].syn = sparse.multiSparse(temps[i])
        children[i].syn_unrounded = sparse.multiSparse(temps2[i])
    
    if backup_solve_status ==True:
        accum += 1
    
    return(children)

def agg_func(config,parent_child_node):
    """
    This function takes a set of parent and child nodes, aggregates the children syn histograms and replaces the parent.syn with the aggregation.
    
    Inputs: 
        config: the config object
        parent_child_node: a list of a parent and it's children nodes
    
    Outputs:
        parent: the parent node
    """
    parent_child_node = list(parent_child_node)
    parent_geocode = parent_child_node[0] 
    # a list of the node objects
    nodes = list(list(parent_child_node)[1])
    
    #calculate the length of each of the geocodes (to determine which is the parent)
    geocode_lens = [len(node.geocode) for node in nodes]
    #the parent is the shortest geocode
    parent = nodes[np.argmin(geocode_lens)]
    
    #subset the children nodes
    children = nodes[:np.argmin(geocode_lens)] + nodes[np.argmin(geocode_lens)+1:]
    children = sorted(children, key=lambda geocode_data: int(geocode_data.geocode))
    child_geos = [child.geocode for child in children]
    
    parent.backup_solve = children[0].parent_backup_solve
    syn_agg = sparse.multiSparse(np.zeros(parent.syn.shape))
    
    for child in children:
        syn_agg = syn_agg + child.syn
    parent.syn = syn_agg
    
    return parent

def make_dp_node(config, geounitNode):
    """"
    This function takes a geounitNode with "raw" data and generates noisy DP query answers depending the specifications in the config object.
     
    Inputs: 
        config: configuration object
        geounitNode: a Node object with "raw" data
     
    Outputs:
        DPgeounitNode: a Node object with selected DP measurements 
    """
    import programs.engine.primitives as primitives
    import re
    
    (file,queries_class_name) = config[BUDGET][QUERIESFILE].rsplit(".", 1)
    queries_module = __import__(file,   fromlist=[queries_class_name])
    
    REGEX_CONFIG_DELIM = "^\s+|\s*,\s*|\s+$"
    
    levels = tuple(config["geodict"]["geolevel_names"].split(","))
    levels_reversed = list(reversed(levels))
    #index relative to topdown
    index = levels_reversed.index(geounitNode.geolevel)
    total_budget = float(config["budget"]["epsilon_budget_total"])
    
    #check that geolevel_budget_prop adds to 1, if not raise exception
    geolevel_prop_budgets= [float(x) for x in re.split(REGEX_CONFIG_DELIM, config["budget"]["geolevel_budget_prop"])]
    t_prop_sum = round(sum(geolevel_prop_budgets) , 5)
    if t_prop_sum != 1.0:
        raise Exception("Total Budget Proportion must add to 1.0 ; Current Sum: " .format(t_prop_sum) ) 
    
    dp_budget = total_budget * geolevel_prop_budgets[index]
    
    hist_shape = geounitNode.raw.shape
    
    if config['budget']['DPqueries']:
        dp_query_prop = [float(x) for x in re.split(REGEX_CONFIG_DELIM, config["budget"]["queriesprop"])]
        dp_query_names = re.split(das_utils.DELIM, config[BUDGET]['DPqueries'])
        queries_dict= getattr(queries_module, queries_class_name)(hist_shape, dp_query_names).calculateQueries().queries_dict

    else:
        dp_query_prop = [0.0,0.0]
        dp_query_names = None
        queries_dict = {}
        
    detailed_prop = float(config["budget"]["detailedprop"])
    prop_sum = sum(dp_query_prop) + detailed_prop
    
    #sensitivity specification
    #defaults
    global_sensitivity = 2.0
    detailed_sens =  global_sensitivity
    if dp_query_names:
        dp_query_sens = [global_sensitivity for query in dp_query_names]
    
    #change defaults
    if config.has_option("budget","global_sensitivity"):
        val = config["budget"]["global_sensitivity"]
        if val != "":
            global_sensitivity = float(val)
            detailed_sens = global_sensitivity
            if dp_query_names:
                dp_query_sens = [global_sensitivity for query in dp_query_names]
    elif config.has_option("budget","detailed_sens") & config.has_option("budget","dp_query_sens"):
        val = config["budget"]["detailed_sensitivity"]
        if val != "":
            detailed_sens = float(config["budget"]["detailed_sensitivity"])
            if dp_query_names:
                dp_query_sens = [float(x) for x in re.split(REGEX_CONFIG_DELIM, config["budget"]["queries_sensitivity"])]
    
    #check that budget proportion adds to 1, if not raise exception
    if prop_sum != 1.0:
        raise Exception("Geolevel Budget Proportion must add to 1.0 ; Current Sum: " .format(prop_sum) )

    np.random.seed()
        
    dp_queries = {}
    if config['budget']['DPqueries']:
        n_queries = len(dp_query_names)
        
        for i in range(n_queries):
            name = dp_query_names[i]
            query = queries_dict[name]
            budgetprop = float(dp_query_prop[i])
            sens = float(dp_query_sens[i])
            DPanswer, Var = primitives.geometric_mechanism(true_answer=query.answer(geounitNode.raw.toDense()), budget=budgetprop*dp_budget, sensitivity=sens, prng=np.random)
            dp_query = {name: cenquery.DPquery(query=query, DPanswer=np.array(DPanswer),
                                             epsilon = budgetprop*dp_budget, DPmechanism = "geometric",
                                             Var = Var)}
            dp_queries.update(dp_query)
    
    DPgeounitNode = geounitNode
    DPgeounitNode.dp_queries = dp_queries
    
    DPanswer, Var = primitives.geometric_mechanism(true_answer = geounitNode.raw.toDense(), budget=detailed_prop*dp_budget, sensitivity=detailed_sens, prng=np.random)
    
    query = cenquery.Query(array_dims = geounitNode.raw.shape)
    DPgeounitNode.dp  = cenquery.DPquery(query=query, DPanswer=DPanswer,
                                             epsilon = detailed_prop*dp_budget, DPmechanism = "geometric",
                                             Var = Var)
    return DPgeounitNode

def makeAdditionalInvariantsConstraints(node, config):
    """
    """
    level = node.geolevel
    if config.has_option(CONSTRAINTS, THEINVARIANTS+"."+level):
        #import invariants_module
        (file, invariants_class_name) = config[CONSTRAINTS][INVARIANTS].rsplit(".", 1)
        invariants_module = __import__(file, fromlist=[invariants_class_name])
        invariant_names = tuple(config[CONSTRAINTS][THEINVARIANTS+"."+level].split(","))
        invariants_dict= getattr(invariants_module,invariants_class_name)(raw = node.raw, raw_housing=node.raw_housing, 
                                 invariant_names = invariant_names).calculateInvariants().invariants_dict
        node.invar.update(invariants_dict)
    if config.has_option(CONSTRAINTS, THECONSTRAINTS+"."+level):
        #import constraints_module
        (file,class_name) = config[CONSTRAINTS][CONSTRAINTS].rsplit(".", 1)
        constraints_module = __import__(file,   fromlist=[class_name])

        constraint_names = tuple(config[CONSTRAINTS][THECONSTRAINTS+"."+level].split(","))
        constraints_dict= getattr(constraints_module,class_name)(hist_shape=node.raw.shape,invariants=node.invar, 
                                 constraint_names = constraint_names).calculateConstraints().constraints_dict
        node.cons.update(constraints_dict)

    return node
