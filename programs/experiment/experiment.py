# experiment
# Brett Moran
# 07/11/2018

import pickle
import numpy as np
import os.path
import logging
import shutil
import subprocess
import json
import re
import datetime

import gc

from das_framework.driver import AbstractExperiment

ENGINE ="engine"
EXPERIMENT = "experiment"
RUN_EXPERIMENT_FLAG = "run_experiment_flag"

CONFIG_BUDGET = "budget"
EPSILON_BUDGET_TOTAL= "epsilon_budget_total"
GEOLEVEL_BUDGET_PROP = "geolevel_budget_prop"
BUDGET_GROUPS = "budget_groups"
NUM_RUNS = "num_runs"
DP_QUERIES = "DPqueries"
QUERIES_PROP = "queriesprop"
DETAILED_PROP = "detailedprop"
BLANK = ""

GEODICT = "geodict"
GEOLEVEL_NAMES = "geolevel_names"

FILESYSTEM = "filesystem"
OVERWRITE = "overwrite_flag"
SAVE_ORIGINAL_DATA = "save_original_data_flag"
ORIGINAL_DATA_SAVELOC = "original_data_saveloc"

OUTPUT_FNAME = "output_fname"
EXPERIMENT_SAVELOC = "experiment_saveloc"

# removes whitespace and splits by comma
REGEX_CONFIG_DELIM = "^\s+|\s*,\s*|\s+$"

WRITER = "writer"



class experiment(AbstractExperiment):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
    
    def runExperiment(self):
        # get the config file
        config = self.das.config
        
        # load the original data
        original_data = self.das.runReader()
        logging.debug("Finished Reading")
        
        # if the save original data flag is 1 (on), save the original data
        if int(config[EXPERIMENT][SAVE_ORIGINAL_DATA]) == 1:
            original_data.saveAsPickleFile(config[EXPERIMENT][ORIGINAL_DATA_SAVELOC])
        
        # get the experiment's save location
        experiment_saveloc = config[EXPERIMENT][EXPERIMENT_SAVELOC]
        logging.debug("Experiment Save Location: {}".format(experiment_saveloc))
        
        # get the budget groups
        budget_groups = re.split(REGEX_CONFIG_DELIM, config[EXPERIMENT][BUDGET_GROUPS])
        
        # get the number of runs for each budget group
        bg_runs = int(config[EXPERIMENT][NUM_RUNS])
        
        # for each budget group:
        # 1. get the budgets and number of runs
        # 2. alter the config file so it reflects the current budgets
        # 3. run the engine and save the results "number of runs" times in the specified save location
        # e.g. .../experiment_name/budget_group_name/runs/run_* where * ranges from 0 to "number of runs"-1
        for bg in budget_groups:
            epsilon_budget_total = float(config[EXPERIMENT]["{}.{}".format(bg, EPSILON_BUDGET_TOTAL)])
            
            geolevel_budget_prop = [float(b) for b in re.split(REGEX_CONFIG_DELIM, config[EXPERIMENT]["{}.{}".format(bg, GEOLEVEL_BUDGET_PROP)])]
            
            detailedprop = float(config[EXPERIMENT]["{}.{}".format(bg, DETAILED_PROP)])
            if config[EXPERIMENT]["{}.{}".format(bg, DP_QUERIES)]:
                dpqueries = re.split(REGEX_CONFIG_DELIM, config[EXPERIMENT]["{}.{}".format(bg, DP_QUERIES)])
                queriesprop = [float(qp) for qp in re.split(REGEX_CONFIG_DELIM, config[EXPERIMENT]["{}.{}".format(bg, QUERIES_PROP)])]
            else:
                dpqueries = ""
                queriesprop = "" 
            
            for i in range(bg_runs):
                budgetgroup = BudgetGroup(bg,epsilon_budget_total, geolevel_budget_prop, detailedprop, dpqueries, queriesprop)
                logging.debug("Budget Group: {}  | Total Budget: {} | Geolevel Budget Prop: {} | Detailed Prop: {} | DP Queries: {} | Queries Prop: {}".format(budgetgroup.name, budgetgroup.epsilon_budget_total, budgetgroup.geolevel_budget_prop, budgetgroup.detailedprop, budgetgroup.dpqueries, budgetgroup.queriesprop))
                
                ################################################################
                # Altering the config file to include the current run's settings
                # 
                # Note that it's easiest to just copy config to config, but the
                # saved variables still exist in case we want to do something
                # different with them later.
                ################################################################
                # alter the config file to include the current set of budgets
                config[CONFIG_BUDGET][EPSILON_BUDGET_TOTAL] = config[EXPERIMENT]["{}.{}".format(bg, EPSILON_BUDGET_TOTAL)]
                
                config[CONFIG_BUDGET][GEOLEVEL_BUDGET_PROP] = config[EXPERIMENT]["{}.{}".format(bg, GEOLEVEL_BUDGET_PROP)]
                
                # alter the config file to include the current budget group's detailed prop
                config[CONFIG_BUDGET][DETAILED_PROP] = config[EXPERIMENT]["{}.{}".format(bg, DETAILED_PROP)]
                
                # alter the config file to include the current budget group's dpqueries
                config[CONFIG_BUDGET][DP_QUERIES] = config[EXPERIMENT]["{}.{}".format(bg, DP_QUERIES)]
                
                # alter the config file to include the current budget group's queries prop
                config[CONFIG_BUDGET][QUERIES_PROP] = config[EXPERIMENT]["{}.{}".format(bg, QUERIES_PROP)]
                
                logging.debug("Current budget values: {}".format(budgetgroup))
                
                # run the engine
                privatized_data_i, feas_dict_i = self.das.runEngine(original_data)
                
                # save the data via the experiment writer
                # this run's save_loc
                run_save_loc = "{}/{}/run_{}".format(experiment_saveloc, budgetgroup.name, i)
                config[WRITER][OUTPUT_FNAME] = run_save_loc
                written_data_i = self.das.runWriter((privatized_data_i, feas_dict_i))
                
                levels = re.split(REGEX_CONFIG_DELIM, config[GEODICT][GEOLEVEL_NAMES])
                for level in levels:
                    privatized_data_i[level].unpersist()
                del privatized_data_i
                
                written_data_i.unpersist()
                del written_data_i
        
                collected = gc.collect()
                print("Garbage collector: collected objects.", collected)
        #experiment_metadata = { "Timestamp": datetime.datetime.now().isoformat()[0:19],
                                #"Runs": {} }
        
        return None



class BudgetGroup():
    def __init__(self, name, epsilon_budget_total, geolevel_budget_prop, detailedprop, dpqueries, queriesprop):
        self.name = name
        self.epsilon_budget_total = epsilon_budget_total
        self.geolevel_budget_prop = geolevel_budget_prop
        self.detailedprop = detailedprop
        self.dpqueries = dpqueries
        self.queriesprop = queriesprop
    
    def getBudgetsAsString(self):
        return ", ".join([str(b) for b in self.geolevel_budget_prop])
    
    def __repr__(self):
        """
        """
        output = ""
        output += "name: " + str(self.name) + "\n"
        output += "epsilon_budget_total: " + str(self.epsilon_budget_total) + "\n"
        output += "geolevel_budget_prop: " + self.getBudgetsAsString() + "\n"
        output += "detailedprop: " + str(self.detailedprop) + "\n"
        output += "dpqueries: " + str(self.dpqueries) + "\n"
        output += "queriesprop: " + str(self.queriesprop) + "\n"

        return output



