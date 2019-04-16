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
import programs.reader.invariants as invariants
import programs.reader.constraints as constraints
import pickle
import gc
import ctools.clogging
import ctools.geo
import das_utils
import re


BUDGET = "budget"
QUERIESFILE = "queriesfile"
CONSTRAINTS = "constraints"
PTABLE ="privacy_table"
CTABLES = "constraint_tables"
GUROBI = "gurobi"
GUROBI_LIC="gurobi_lic"
ENGINE = "engine"
DELETERAW = "delete_raw"
GRB_LICENSE_FILE='GRB_LICENSE_FILE'

GEODICT = "geodict"
GEOLEVEL_NAMES = "geolevel_names"


class engine(AbstractDASEngine):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
    
    def run(self, block_nodes):
        """
        """

        # Phase 1: reduce block_nodes to minimal schema and run top-down on these nodes with their reduced representation
        block_nodes_ms = self.reduceNodesToMinimalSchema(block_nodes)

        #make higher level nodes by aggregation
        nodes_dict_ms = self.getNodesAllGeolevels(block_nodes_ms)

        #get minimal schema measurements
        nodes_dict_ms = self.noisyAnswers(nodes_dict_ms, phase = 1)
        #solve topdown
        nodes_dict_ms,feas_dict_phase1 = self.topdown(nodes_dict_ms, identifier="phase1_")

        print("nodes_dict_ms constructed, Phase 1 of minSchema Engine complete! nodes_dict_ms contains:")
        print("Initiating Phase 2 of minSchema Engine...")

        #bottom to top of tree
        levels = re.split(das_utils.DELIM, self.config[GEODICT][GEOLEVEL_NAMES])

        # Phase 2 Step 1: Replace block_node constraints with Phase 1 solution gqhh counts
        block_nodes = self.getMinSchemaConstraints(block_nodes = block_nodes, block_nodes_ms = nodes_dict_ms[levels[0]])

        for level in levels:
            nodes_dict_ms[level].unpersist()
            del nodes_dict_ms[level]
        del block_nodes_ms

        # Phase 2 Step 2: Aggregate nodes, perform solves
        nodes_dict = self.getNodesAllGeolevels(block_nodes)

        block_nodes.unpersist()
        del block_nodes
        gc.collect()
        
        #take measurements
        nodes_dict = self.noisyAnswers(nodes_dict, phase = 2)
        #solve topdown
        nodes_dict, feas_dict_phase2 = self.topdown(nodes_dict, identifier="phase2_")

        #combine feas dicts
        #for key in feas_dict_phase1.keys():
        #    feas_dict_phase2[key] = feas_dict_phase1[key]
        feas_dict_phase2.update(feas_dict_phase1)

        return (nodes_dict, feas_dict_phase2) # only Phase 1 should ever invoke the failsafe, we report both to double check

    def reduceNodesToMinimalSchema(self, nodes):
        """
        Uses minimal_schema section in config file to take a query that marginalizes the input block nodes
        in order to yield the desired minimal schema.
        """
        config = self.config
        add_over_margins = [int(i) for i in config["minimal_schema"]["minSchema.add_over_margins"].split(',')]
        original_array_dims = nodes.take(1)[0].raw.shape
        print("Mapping over minSchematize to turn full-schema geounit nodes into minimal-schema geounit nodes...")
        minimal_schema_nodes = nodes.map(lambda node: minSchematize(node, original_array_dims, add_over_margins))
        return minimal_schema_nodes

    def noisyAnswers(self, nodes_dict, phase):
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
        
        if phase == 1:
            for level in levels:
                nodes_dict[level] = nodes_dict[level].map(lambda node: (makeDPNode_ms(config, node))).persist()
                print(level + " constraints used : " + str(nodes_dict[level].take(1)[0].cons.keys()))
            #delete certain constraints?
            if config.has_option(CONSTRAINTS,"theConstraints."+level):
                nodes_dict[level] = nodes_dict[level].map(lambda node: (delete_constraints(config, node,
                                                                        level=level))).persist()

        elif phase ==2:
            for level in levels:
                nodes_dict[level] = nodes_dict[level].map(lambda node: (makeDPNode(config, node))).persist()
                print(level + " constraints used : " + str(nodes_dict[level].take(1)[0].cons.keys()))

        if bool(int(config[ENGINE][DELETERAW])):
            logging.info("Removing True Data Arrays")
            for level in levels:
                #explicity remove true data "raw"
                nodes_dict[level] = nodes_dict[level].map(lambda node: (deleteTrueArray(node))).persist()
                #check that true data "raw" has been deleted from each, if not will raise an exception
                nodes_dict[level].map(lambda node: (checkTrueArrayIsDeleted(node)))
                
        return nodes_dict

    def topdown(self, nodes_dict, identifier = ""):
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
        
        #bottom to top of tree
        levels = tuple(config["geodict"]["geolevel_names"].split(","))
        n_levels = len(levels)
        #top to bottom of tree
        levels_reversed = list(reversed(levels))
        
        #Do Topdown Imputation
        os.environ[GRB_LICENSE_FILE] = config[GUROBI][GUROBI_LIC]
        
        #This does imputation from national to national level.
        nodes_dict[levels_reversed[0]] = nodes_dict[levels_reversed[0]].map(lambda node: geoimp_wrapper_nat(config,node)).persist() 
        
        feas_dict = {}
        
        for i in range((n_levels-1)):
            #make accumulator
            feas_dict[identifier + levels_reversed[i]] = self.setup.sparkContext.accumulator(0)
            
            parent_rdd = nodes_dict[levels_reversed[i]].map(lambda node: (node.geocode, node))
            child_rdd = nodes_dict[levels_reversed[i+1]].map(lambda node: (node.parentGeocode, node))
            parent_child_rdd = parent_rdd.union(child_rdd).groupByKey()
            
            nodes_dict[levels_reversed[i+1]] = parent_child_rdd.map(
                                                lambda nodes: geoimp_wrapper(config,nodes,
                                                feas_dict[identifier + levels_reversed[i]])).flatMap(
                                                lambda children: tuple([child for child in children])).persist()
            
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
        config=self.config
        
        levels = tuple(config["geodict"]["geolevel_names"].split(","))
        n_levels = len(levels)
        nodes_dict={}
        nodes_dict[levels[0]] = block_nodes
        
        for i in range((n_levels - 1)):
            nodes_dict[levels[i+1]] = nodes_dict[levels[i]].map(lambda blockNode: (blockNode.parentGeocode, blockNode)
                            ).reduceByKey(lambda x,y: addGeounitNodes(x,y)
                            ).map(lambda node: shiftGeocodesUp(node)
                            ).persist()
            
        block_nodes.unpersist()
        
        return nodes_dict        


    def getMinSchemaConstraints(self, block_nodes, block_nodes_ms):
        config = self.config
        block_nodes = block_nodes.map(lambda node: (node.geocode, node))
        block_nodes_ms = block_nodes_ms.map(lambda node: (node.geocode, node))
        block_nodes_fullMinSchemas_joined = block_nodes.join(block_nodes_ms)
        block_nodes = block_nodes_fullMinSchemas_joined.map(lambda key_node: installPhase1Constraints(node_pair = key_node[1],
                                                                                                        config = config))
        return block_nodes

def installPhase1Constraints(node_pair, config):
    block_node = node_pair[0]
    block_node_ms = node_pair[1]

    add_over_margins = re.split(das_utils.DELIM, config["minimal_schema"]["minSchema.add_over_margins"])
    add_over_margins = tuple([int(x) for x in add_over_margins]) 

    query = cenquery.Query(array_dims=block_node.raw.shape,
                            add_over_margins= add_over_margins,
                            name = "minSchema")

    rhs = block_node_ms.raw.toDense()
    ms_constraint = cenquery.Constraint(query, rhs, sign="=", name = "minSchema")
    cons = {}
    cons["minSchema"] = ms_constraint

    block_node.cons = cons

    return block_node 

def agg_func(config, parent_child_node):
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

def geoimp_wrapper_nat(config, nat_node):
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
            x.rhs = np.expand_dims(x.rhs, axis=len(x.rhs.shape))
            x.check_after_update()
    
    #this is the actual post-processing optimization step
    l2_answer, int_answer, backup_solve_status = geoimpgbopt.L2geoimp_wrapper(config=config,parent=parent_hist, NoisyChild=NoisyChild, NoisyChild_weight = NoisyChild_weight, DPqueries=DPqueries, query_weights = query_weights, constraints=constraints, identifier="nat_to_nat")
    
    if constraints is not None:
        check = True
        for x in constraints:
            check = bool(np.prod(x.check(int_answer)) * check)
        print("constraints are ", check, "for parent geocode ", parent_geocode)
    
    #get rid of extra dimension
    nat_node.syn = sparse.multiSparse(int_answer.squeeze())
    nat_node.syn_unrounded = sparse.multiSparse(l2_answer.squeeze())
    
    return(nat_node)

def geoimp_wrapper(config, parent_child_node, accum):
    """
    This function performs the Post-Processing Step for a generic parent to the Child geography.
    
    Inputs:
        config: configuration object
        parent_child_node: a collection of geounitNode objects containing one parent and multiple child
        accum: spark accumulator object

    Output:
        children: a collection of geounitNode objects for each of the children, after post-processing
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
    
    #subset the children nodes
    children = nodes[:np.argmin(geocode_lens)] + nodes[np.argmin(geocode_lens)+1:]
    children = sorted(children, key=lambda geocode_data: int(geocode_data.geocode))
    child_geos = [child.geocode for child in children]
    n_children = len(child_geos)
    
    #stack the dp arrays on top of one another, if only 1 child just expand the axis
    if n_children > 1:
        NoisyChild = np.stack([child.dp.DPanswer for child in children],axis=-1)
    else:
        NoisyChild = np.expand_dims(children[0].dp.DPanswer, axis=len(children[0].dp.DPanswer.shape))
    
    #combine DPqueries without geography to combined DPqueries with geography
    #if no DPqueries, change this to an empty list
    if any(children[0].dp_queries)== False:
        DPqueries_comb = []
    else:
        DPqueries = list(list(child.dp_queries.values()) for child in children)
        n_q= len(DPqueries[0])
        DPqueries_comb = []
        for i in range(n_q):
            subset_input=tuple(list(DPqueries[0][i].query.subset_input) + [range(NoisyChild.shape[-1])])
            query = cenquery.Query(array_dims = NoisyChild.shape, subset=subset_input, add_over_margins=DPqueries[0][i].query.add_over_margins)
            q_answer = np.stack([DPquery[i].DPanswer for DPquery in DPqueries], axis = -1) 
            DP_query = cenquery.DPquery(query = query, DPanswer = q_answer)
            DPqueries_comb.append(DP_query)
        
    #delete redundant union constraints
    #which gq cat are non-zero
    
    #combine cenquery.Constraint objects without geography to build combined cenquery.Constraint
    constraints_comb = []
    #now children may have different constraints. only combine the ones that match.
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
            #make a list of individual constraints for all children who have them
            #find which children have the key
            ind = [key in child.cons.keys() for child in children]
            #children_sub is subset of children with that key
            children_sub = list(compress(children,ind))
            constraints = list(child.cons[key] for child in children_sub)
            
            #get the list of geos that have this constraint
            subset_geos = list(compress(range(NoisyChild.shape[-1]),ind))
            subset_input=tuple(list(constraints[0].query.subset_input) + [subset_geos,])
            query = cenquery.Query(array_dims = NoisyChild.shape, subset=subset_input, add_over_margins=constraints[0].query.add_over_margins)
            rhs = np.stack([con.rhs for con in constraints], axis = -1)
            constraint = cenquery.Constraint(query = query, rhs=rhs, sign = constraints[0].sign, name = constraints[0].name ) 
            constraints_comb.append(constraint)
            
    parent_hist = parent.syn.toDense()
    parent_geocode = parent.geocode
    parent_constraints = parent.cons  #for checking purposes
    
    #this is the actual post-processing optimization step
    l2_answer, int_answer, backup_solve_status = geoimpgbopt.L2geoimp_wrapper(config=config,parent=parent_hist, NoisyChild=NoisyChild, DPqueries=DPqueries_comb, constraints=constraints_comb, identifier=parent_geocode, parent_constraints = parent_constraints)
    
    #check constraints
    if constraints_comb is not None:
        check = True
        for x in constraints_comb:
            check = bool(np.prod(x.check(int_answer)) * check)
        print("constraints are ", check, "for parent geocode ", parent_geocode)
    
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
    
    for i, geocode in enumerate(child_geos):
        children[i].syn = sparse.multiSparse(temps[i])
        children[i].syn_unrounded = sparse.multiSparse(temps2[i])
    
    if backup_solve_status ==True:
        accum += 1
    
    return(children)

def makeDPNode_ms(config, geounitNode):
    """"
    This function takes a geounitNode with "raw" data and generates noisy DP query answers depending the specifications in the config object.
     
    Inputs: 
        config: configuration object
        geounitNode: a geounitNode with "raw" data
     
    Outputs:
        DPgeounitNode: a geounitNode with differential privacy data and queries from differential privacy.  
    """
    import programs.engine.primitives as primitives
    import re
    
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
    
    assert config.has_option("budget", "minSchema.budget_prop_Phase1"), "config file must specify minSchema.budget_prop_Phase 1 (proportion of budget to be expended on Phase 1, the min schema phase of the MinSchema Engine"
    assert float(config["budget"]["minSchema.budget_prop_Phase1"]) > 0. and float(config["budget"]["minSchema.budget_prop_Phase1"]) < 1., "minSchema.budget_prop_Phase1 must lie in (0,1)."

    #total*level_prop*min_schema_prop
    dp_budget = total_budget * geolevel_prop_budgets[index] * float(config["budget"]["minSchema.budget_prop_Phase1"])

    #sensitivity specification
    #defaults
    global_sensitivity = 2.0
    detailed_sens =  global_sensitivity

    #change defaults
    if config.has_option("budget","global_sensitivity"):
        val = config["budget"]["global_sensitivity"]
        if val != "":
            global_sensitivity = float(val)
            detailed_sens = global_sensitivity
    elif config.has_option("budget","detailed_sens") & config.has_option("budget","dp_query_sens"):
        val = config["budget"]["detailed_sensitivity"]
        if val != "":
            detailed_sens = float(config["budget"]["detailed_sensitivity"])

    DPgeounitNode = geounitNode
    DPgeounitNode.dp_queries = {}

    DPanswer, Var = primitives.geometric_mechanism(true_answer = geounitNode.raw.toDense(),
                                                   budget=dp_budget, sensitivity=detailed_sens,
                                                   prng=np.random)

    query = cenquery.Query(array_dims = geounitNode.raw.shape, subset=None, add_over_margins=None)
    DPgeounitNode.dp  = cenquery.DPquery(query=query, DPanswer=DPanswer,
                                             epsilon = dp_budget, DPmechanism = "geometric",
                                             Var = Var)
    return DPgeounitNode

def makeDPNode(config, geounitNode):
    """"
    This function takes a geounitNode with "raw" data and generates noisy DP query answers depending the specifications in the config object.
     
    Inputs: 
        config: configuration object
        geounitNode: a geounitNode with "raw" data
     
    Outputs:
        DPgeounitNode: a geounitNode with differential privacy data and queries from differential privacy.  
    """
    import programs.engine.primitives as primitives
    import re
    
    REGEX_CONFIG_DELIM = "^\s+|\s*,\s*|\s+$"
    
    (file,queries_class_name) = config[BUDGET][QUERIESFILE].rsplit(".", 1)
    queries_module = __import__(file,   fromlist=[queries_class_name])
    
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
    
    assert config.has_option("budget", "minSchema.budget_prop_Phase1"), "config file must specify minSchema.budget_prop_Phase 1 (proportion of budget to be expended on Phase 1, the min schema phase of the MinSchema Engine"
    assert float(config["budget"]["minSchema.budget_prop_Phase1"]) > 0. and float(config["budget"]["minSchema.budget_prop_Phase1"]) < 1., "minSchema.budget_prop_Phase1 must lie in (0,1)."

    #total * geolevel_proportion * (1- MinSchema_proportion)
    dp_budget = total_budget * geolevel_prop_budgets[index] * (1. - float(config["budget"]["minSchema.budget_prop_Phase1"]))

    if config['budget']['DPqueries']:
        dp_query_prop = [float(x) for x in re.split(REGEX_CONFIG_DELIM, config["budget"]["queriesprop"])]
        dp_query_names = tuple(config["budget"]["DPqueries"].split(","))
    else:
        dp_query_prop = [0.0,0.0]
        dp_query_names = None
    
    dp_queries = {}
    
    detailed_prop = 1.0
    detailed_prop = float(config["budget"]["detailedprop"])
    prop_sum = sum(dp_query_prop) + detailed_prop
    
    #check that budget proportion adds to 1, if not raise exception
    if prop_sum != 1.0:
        raise Exception("Geolevel Budget Proportion must add to 1.0 ; Current Sum: " .format(prop_sum) ) 

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
    
    query = cenquery.Query(array_dims = geounitNode.raw.shape, subset=None, add_over_margins=None)
    DPgeounitNode.dp  = cenquery.DPquery(query=query, DPanswer=DPanswer,
                                             epsilon = detailed_prop*dp_budget, DPmechanism = "geometric",
                                             Var = Var)
    return DPgeounitNode

def deleteTrueArray(node):
    """
    This function explicitly deletes the geounitNode "raw" true data array.

    Input:
        node: a geounitNode object

    Output:
        node: a geounitNode object
    """
    node.raw = None
    return(node)

def checkTrueArrayIsDeleted(node):
    """
    This function checks to see if the node.raw is None for a geounitNode object.
    If not it raises an exception.

    Input:
        node: a geounitNode object
    """
    if(node.raw) is not None:
        raise Exception("The true data array has not been deleted")

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

def addConstraints(const1, const2):
    """
    For each constraint in the dictionary, this function adds those two constraints together (specifically their right hand sides) and returns a dictionary with the aggregated constraints.
    
    Inputs:
        const1: a dictionary containing the constraint objects from cenquery for a geounit at a specific geolevel
        const2: a dictionary containing the constraint objects from cenquery for a geounit at a specific geolevel
    
    Note: const1 and const2 must be at the same geolevel.

    Output:
        const_sum: a dictionary containing the aggregated constraints from const1 and const2
    
    To do:
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
    if node1.syn and node2.syn:
        argsDict["syn"] = node1.syn + node2.syn
    if node1.cons and node2.cons:
        argsDict["cons"] =  addConstraints(node1.cons, node2.cons)
    if node1.invar and node2.invar:
        argsDict["invar"] = addInvariants(node1.invar, node2.invar)
    argsDict["geocodeDict"] = node1.geocodeDict
    
    aggregatedNode = nodes.geounitNode(node1.geocode, **argsDict)
    
    return aggregatedNode

def minSchematize(node, array_dims, add_over_margins):
    minSchemaQuery = cenquery.Query(array_dims=array_dims, subset=None, add_over_margins=add_over_margins)

    node.raw = sparse.multiSparse(minSchemaQuery.answer_original(node.raw.toDense()))
    minSchema_shape = node.raw.shape
    dims_keep = [x for x in set(range(len(array_dims))).difference(set(add_over_margins))]
    
    constraint_keys = node.cons.keys()
    for key in constraint_keys:
        node.cons[key].query.array_dims = minSchema_shape
        node.cons[key].query.add_over_margins = tuple([x for x in set(dims_keep).intersection(set(node.cons[key].query.add_over_margins))])
        node.cons[key].query.subset_input = [node.cons[key].query.subset_input[x] for x in dims_keep]
        node.cons[key].query.subset = np.ix_(*tuple(node.cons[key].query.subset_input))
        #axis_groupings = () ?? currently no axis groupings in constraints
        print(node.cons[key].query)
        node.cons[key].check_after_update()

    return node
