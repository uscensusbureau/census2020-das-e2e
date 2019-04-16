import pickle

WRITER = "writer"

def savel2geoimp(childGeoLen, parent, NoisyChild, DPqueries, config, gurobi_environment, NoisyChild_weight, query_weights, constraints, nnls, identifier, use_parent_constraints, subsets_zeros):
    constraints_info = {}
    constraints_info["childGeoLen"] = childGeoLen
    constraints_info["NoisyChild_weight"] = NoisyChild_weight
    constraints_info["query_weights"] = query_weights
    constraints_info["nnls"] = nnls
    constraints_info["identifier"] = identifier
    constraints_info["use_parent_constraints"] = use_parent_constraints
    constraints_info["subsets_zeros"] = subsets_zeros

    # Should generalize this to take write paths from config file
    pickle.dump(constraints_info, open(f"/mnt/tmp/{identifier}_constraints_info.p",'wb'))
    pickle.dump(list(constraints), open(f"/mnt/tmp/{identifier}_constraints.p",'wb'));
    pickle.dump(list(DPqueries), open(f"/mnt/tmp/{identifier}_dpqueries.p",'wb'))
    pickle.dump(NoisyChild, open(f"/mnt/tmp/{identifier}_NoisyChild.p",'wb'))
    pickle.dump(parent, open(f"/mnt/tmp/{identifier}_parent.p",'wb'))
    config.write(open(f"/mnt/tmp/{identifier}_config.checkpoint",'w'))
