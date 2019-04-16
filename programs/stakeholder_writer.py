# stakeholder writer
# Brett Moran
# 06/26/2018

import pickle
import numpy as np
import os.path
import logging

from driver import AbstractDASWriter
class writer(AbstractDASWriter):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
    
    def write(self, engine_dict):
        try:
            fname = self.getconfig("nat_saveloc")
            rdd = engine_dict["National"]
            rdd.saveAsPickleFile(fname)
            
            fname = self.getconfig("state_saveloc")
            rdd = engine_dict["State"]
            rdd.saveAsPickleFile(fname)
            
            fname = self.getconfig("county_saveloc")
            rdd = engine_dict["County"]
            rdd.saveAsPickleFile(fname)
            
            fname = self.getconfig("tract_saveloc")
            rdd = engine_dict["Tract"]
            rdd.saveAsPickleFile(fname)
            
            fname = self.getconfig("blockgroup_saveloc")
            rdd = engine_dict["Block_Group"]
            rdd.saveAsPickleFile(fname)
            
            fname = self.getconfig("block_saveloc")
            rdd = engine_dict["Block"]
            rdd.saveAsPickleFile(fname)
        
        except KeyError:
            logging.debug("Cannot save the pickled RDD due to a KeyError.")
        
        return engine_dict["Block"]
