
import csv
import numpy as np
import pyspark.rdd
import os.path
import logging
import shutil
import subprocess
from collections import defaultdict

import das_framework.driver as driver

BLOCK = "Block"
OUTPUT_FNAME = "output_fname"
PRODUCE_FLAG = "produce_flag"
SPLIT_BY_STATE = "split_by_state"
STATE_CODES = "state_codes"

def to_list(hist):
    tmp = []
    for idx, val in np.ndenumerate(hist):
        if int(val) != 0:
            tmp.append((idx, val))
    return tmp

def to_list_from_sparse(spar_obj):
    spar = spar_obj.sparse_array
    dim = spar_obj.shape
    idx = [(np.unravel_index(i, dim), int(spar[0, i])) for i in list(spar.indices)]
    return idx

def expand(blk_idx_freq):
    blk = blk_idx_freq[0]
    idx = blk_idx_freq[1][0]
    freq = int(blk_idx_freq[1][1])
    tmp = blk + idx
    return [tmp]*freq

def to_line_comma(tup):
    return ",".join(tuple(map(str,tup)))


class writer(driver.AbstractDASWriter):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def write(self, engine_dict):
        config=self.config
        levels = tuple(config["geodict"]["geolevel_names"].split(","))
        block_nodes = engine_dict[levels[0]]
        if int(self.getconfig(PRODUCE_FLAG)) == 1:
            output_filedir = self.getconfig(OUTPUT_FNAME)
            mdf = block_nodes.map(lambda node: ((node.geocode,), node.syn)).persist()
            tmp = defaultdict(int)
            if self.getconfig("split_by_state") == "True":
                states = self.getconfig(STATE_CODES).split(" ")
                for state in states:
                    tmp[state] = mdf.filter(lambda pair: pair[0][0].startswith(state)).persist()
            else:
                tmp["ALL"] = mdf                
            # mdf.persist()
            # shutil.rmtree(output_filedir, ignore_errors=True)
            for key, rdd in tmp.items():
                rdd.flatMapValues(to_list_from_sparse).flatMap(expand).map(to_line_comma).saveAsTextFile(output_filedir+"/"+key)
                rdd.unpersist()
                del rdd
            del tmp
        return block_nodes
