#######################################################
# Block Node Writer Notes
# Updates: 
#   16 August 2018 - bam
# 
# How to use block_node_writer in the config file:
#
# Use in the [writer] section of the config file
# writer: programs.block_node_writer.writer
#
#
# Other attributes and options: 
# write_type        : indicates whether to save rdd elements as json objects or as node objects
#                     if json object, then use 'write_type: json'
#                     otherwise, ignore and just use either 'write_type: ' or 'write_type: node'
#
# json_keepAttrs    : this only works if the write_type is json
#                     this is where object attributes would be specified from the nodes (e.g. geocode, geolevel);
#                     specified attributes will be encoded into json and then saved
#
# output_fname      : the output path where the data will be stored
#                     automatically detects if s3 path or not
#
# produce_flag      : whether or not to write the data to file
#                     Use 1 to save / 0 to not save the data
#
# pickle_batch_size : changes the batchSize attribute of the RDD.saveAsPickleFile function
#                     only include this if a different batchSize is needed; default is 10
#                     leaving it blank also sets the value to 10
#
# minimize_nodes    : whether or not to call the node's stripForSave function to minimize the memory impact of each node
#                     Use 1 to minimize / 0 to not minimize the nodes
#                     default is 0
#
# num_parts         : indicates how to repartition the rdd for faster saving
#                     default = 100
#
#######################################################
# For quick copying:
#
# [writer]
# writer:
# write_type:
# output_fname:
# produce_flag:
# pickle_batch_size:
# minimize_nodes:
# num_parts:
#
#######################################################

import pickle
import numpy as np
import os.path
import logging
import subprocess
import re
import gc
import json
import time

import configparser
from configparser import ConfigParser

import das_framework.driver as driver
import das_utils


GEODICT = "geodict"
GEOLEVEL_NAMES = "geolevel_names"

WRITER = "writer"
OUTPUT_FNAME = "output_fname"
WRITE_TYPE = "write_type"
JSON = "json"
WRITE_JSON_KEEP_ATTRS = "json_keepAttrs"
PICKLE_BATCH_SIZE = "pickle_batch_size"
PRODUCE = "produce_flag"
MINIMIZE_NODES = "minimize_nodes"
NUM_PARTS = "num_parts"
BLANK = ""

class writer(driver.AbstractDASWriter):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
    
    def write(self, input_tuple):
        
        engine_dict, feas_dict = input_tuple
        
        config = self.config
        
        # get relevant config attributes
        levels = re.split(das_utils.DELIM, config[GEODICT][GEOLEVEL_NAMES])
        batchSize = self.getint(PICKLE_BATCH_SIZE, default=10)
        produce = self.getboolean(PRODUCE)
        saveloc = self.getconfig(OUTPUT_FNAME)
        write_type = self.getconfig(WRITE_TYPE)
        minimize_nodes = self.getboolean(MINIMIZE_NODES, default=False)
        num_parts = self.getint(NUM_PARTS, default=100)
        
        rdd = engine_dict[levels[0]]
        
        # keep the original rdd so the validator can still check that the constraints have been met
        original_rdd = rdd
        
        if write_type == JSON:
            rdd = transformNodesToJSON(config, rdd)
        else:
            if minimize_nodes:
                rdd = rdd.map(lambda node: node.stripForSave()).persist()
        
        # free up memory
        deleteEngineDict(engine_dict)
        
        if produce:
            try:
                print("Time before rdd.count (just used to isolate repartition+saveRunData time): ", time.time())    
                cnt = rdd.count()
                startTime = time.time()
                rdd = rdd.repartition(num_parts)
                print(f"With num_parts {num_parts}, time for repartition (transformation, should be ~zero): ", time.time() - startTime)
                saveRunData(saveloc, config=config, feas_dict=feas_dict, rdd=rdd, batchSize=batchSize)
                print(f"With num_parts {num_parts}, time for saveRunData (action, should be >>0): ", time.time() - startTime)
            except KeyError:
                logging.debug("Cannot save the pickled RDD due to a KeyError.")
        
        return original_rdd

def deleteEngineDict(engine_dict):
    """
    unpersists the engine dictionary (dict of rdds) and removes the engine dictionary from memory
    Notes:
        used to save memory since the rdds can be quite large in size
    """
    for level in engine_dict.keys():
        engine_dict[level].unpersist()
    del engine_dict
    gc.collect()

def transformNodesToJSON(config, rdd):
    logging.debug("Transforming Node information to JSON format")
    
    if config[WRITER][WRITE_JSON_KEEP_ATTRS] is BLANK:
        keepAttrs = None
    else:
        keepAttrs = re.split(das_utils.DELIM, config[WRITER][WRITE_JSON_KEEP_ATTRS])
    
    logging.debug("Keeping the following Node attributes: {}".format(keepAttrs))
    rdd = rdd.map(lambda node: node.toJSON(keepAttrs=keepAttrs, addClassName=True))
    
    return rdd

def saveRunData(path, config=None, feas_dict=None, rdd=None, batchSize=10):
    if path[-1] == '/':
        path = path[0:-1]
    
    # needed when not an s3 path, as the with open context assumes the folder already exists
    if not das_utils.isS3Path(path):
        das_utils.makePath(path)
    
    if config is not None:
        config_path = path + "/config.ini"
        logging.debug("Saving config to directory: {}".format(config_path))
        das_utils.saveConfigFile(config_path, config)
    
    if rdd is not None:
        logging.debug("Pickle Batch Size: {}".format(batchSize))
        data_path = path + "/data"
        logging.debug("Saving data to directory: {}".format(data_path))
        das_utils.savePickledRDD(data_path, rdd, batchSize=batchSize)
    
    if feas_dict is not None:
        for key in feas_dict.keys():
            feas_dict[key] = feas_dict[key].value   #this seems redundant, but is actually needed for the accumulator
        logging.info("Feasibility dictionary: {}".format(feas_dict))
        feas_path = path + "/feas_dict.json"
        logging.debug("Saving feas_dict to directory: {}".format(feas_path))
        das_utils.saveJSONFile(feas_path, feas_dict)
    

