Experiments
=
The experiments section of the code allows for different **algorithm settings** to be run **multiple times**.

### How to Run Experiments ###
To run an experiment we need the following:
1. Make sure there is an **[experiment] section** in the config file.
2. Under the experiment section, include the following attributes:
    * Include `experiment: programs.experiment.experiment.experiment` to load the experiment class.
    * `run_experiment_flag`:  - Use 1 to run / 0 to not run the experiment.
    * `budget_groups`: Provide a comma-separated list of names for the algorithm settings (a.k.a. budget groups).
    * `num_runs`: Expects an integer that states how many times to run each of the algorithm settings groups.
    * `experiment_saveloc`: the directory where the experiment will be saved.
    * Include `save_original_data_flag: 0` and `original_data_saveloc: some_path`. The current version of the analysis code does not expect the original data to be saved separate from the privatized data, so we almost always leave `save_original_data_flag` in the 0 ('off') position.
3. For each of the algorithm settings (i.e. `budget_groups`), specify the following attributes:
    * `group.epsilon_budget_total`: The total budget used for each run in this algorithm settings group.
    * `group.geolevel_budget_prop`: A comma-separated list indicating how to split the total budget across the geolevels. The order of the proportions should follow the order of the geolevels as they were listed in the **[geodict] section**. _The proportions must sum to one._
    * `group.detailedprop`: The proportion of the budget within a geolevel to allocate to the detailed histogram.
    * `group.DPqueries`: A comma-separated list of DPquery names that match zero or more of the _DPqueries_ attribute found listed within the **[budget] section**.
    * `group.queriesprop`: A comma-separated list of proportions. The number of entries must match the number of entries in _group.DPqueries_. _The sum of **these proportions** and the proportion found in **group.detailedprop** must sum to one._
4. In order to run the analysis code on this experiment, make sure the **[writer] section** points at the **block_node_writer**. and contains the **output_fname** attribute, even if it is blank.
5. Run the experiment like normal. (i.e. using `config=configs/config_file_to_use.ini bash run_cluster.sh`)

**Note that attributes in the [budget] section will be overwritten by those included in the [experiment] section.** This will happen dynamically during runtime. To ensure that we know which values were used for each run, a copy of the _modified_ config file is included with each run when written using the *block_node_writer* module.

##### Example Config Snippet for PL94 #####
```ini
[geodict]
#smallest to largest (no spaces)
geolevel_names: Block,Block_Group,Tract,County,State,National

#(largest geocode length to smallest, put 1 for top level) (no spaces)
geolevel_leng: 16,12,11,5,2,1

[budget]
epsilon_budget_total: 4

#budget in topdown order (e.g. National, State, .... , Block)
geolevel_budget_prop: 0.15,0.17,0.17,0.17,0.17,0.17

# detailed query proportion of budget (a float between 0 and 1)
detailedprop: 0.5

# DP queries to create, (or leave blank) (total budget proporiton must add to 1.0)
queriesfile: programs.engine.queries.QueriesCreatorPL94
DPqueries: race, number_of_races, race_ethnicity
queriesprop: 0.1,0.2,0.2

[writer]
# point at the block node writer module
writer: programs.block_node_writer.writer

# the specified path is only used when experiments are turned off (i.e. run_experiment_flag: 0)
# the attribute is needed by the experiment code and the path/value will be overwritten
output_fname: path_to_save_single_runs

[experiment]
experiment: programs.experiment.experiment.experiment

# to run (1) / not run (0) an experiment
run_experiment_flag: 1

experiment_saveloc: path_where_experiment_will_be_saved

# needs to be included, but is oftentimes turned off
save_original_data_flag: 0
original_data_saveloc: s3://uscb-decennial-ite-das/experiments/original_data

# specify the algorithm settings (a.k.a. 'budget groups')
budget_groups: bu1

# number of times to run each algorithm settings group
num_runs: 3

# Budgets follow the order of the geolevels listed in the geodict section
# e.g. Block, Block_Group, Tract, County, State, National
bu1.epsilon_budget_total: 4
bu1.geolevel_budget_prop: 0.15,0.17,0.17,0.17,0.17,0.17
bu1.detailedprop: 0.5
bu1.DPqueries: race, number_of_races, race_ethnicity
bu1.queriesprop: 0.1,0.2,0.2
```


