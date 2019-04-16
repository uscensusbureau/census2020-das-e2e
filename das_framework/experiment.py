#!/usr/bin/env python3
#
# experiment runner module
# Meant to be called from driver.py, can be independently tested.
#
#
# Simson L. Garfinkel
#
# Major Modification log:
#
#  2017-11-19  slg - created
#
# Notice that we use decimal math to avoid the 0.1 + 0.1 problem

import os
import sys
import re
import matplotlib
matplotlib.use("Agg")           # do not use X server

import matplotlib.pyplot as plt
import numpy as np
import math
import json
import logging
from decimal import *

sys.path.insert(0, os.path.basename(__file__))

import driver

loop_re = re.compile(r"loop(\d+)")
for3m_re  = re.compile(r"FOR (\w+)[.](\w+)\s*=\s*([0-9.]+)\s*TO\s*([0-9.]+)\s*MULSTEP\s*([0-9.]+)",re.I)
for3_re  = re.compile(r"FOR (\w+)[.](\w+)\s*=\s*([0-9.]+)\s*TO\s*([0-9.]+)\s*STEP\s*([0-9.]+)",re.I)
for2_re  = re.compile(r"FOR (\w+)[.](\w+)\s*=\s*([0-9.]+)\s*TO\s*([0-9.]+)\s*",re.I)
forlist_re = re.compile(r"FOR (\w+)[.](\w+)\s*IN(\s*[0-9.,\s]+\s*)", re.I)


def decode_loop(s):
    """If string s is a FOR loop, return (section,variable,start,stop,step), otherwise return None."""
    m = for3m_re.search(s)
    if m:
        return (m.group(1), m.group(2), Decimal(m.group(3)), Decimal(m.group(4)), Decimal(m.group(5)), "MUL")
    m = for3_re.search(s)
    if m:
        return (m.group(1),m.group(2),Decimal(m.group(3)), Decimal(m.group(4)), Decimal(m.group(5)), "ADD" )
    m = for2_re.search(s)
    if m:
        return (m.group(1),m.group(2),Decimal(m.group(3)), Decimal(m.group(4)), 1, "ADD")
    m = forlist_re.search(s)
    # import pdb
    # pdb.set_trace()
    if m:
        l = list(map(lambda s: Decimal(s.strip()), m.group(3).split(',')))
        return(m.group(1), m.group(2),l[0], l[-1], l, "LIST")
    return None

def build_loops(config):
    """Scan the config file and return an array that contains the decode_loop structure for each of the loops"""
    loops = []
    for var in config[driver.EXPERIMENT]:
        m = loop_re.search(var)
        if m:
            vars = decode_loop(config[driver.EXPERIMENT][var])
            if not vars:
                raise RuntimeError("Cannot decode loop definition {}:{}".format(var,config[driver.EXPERIMENT][var]))
            loops.append(vars)
    return loops
    

def initial_state(loops):
    """Given a set of loops, create the initial counters"""
    return tuple(row[2] for row in loops)

def increment_state(loops,state_):
    """Given a set of loops and a state, increment the state to the next position, handling roll-over.
    Return the next state. If we are finished, return None.
    Remember, loops is loops[rank][section,variable,start,stop,step]
    Notice that we perform decimal arithmetic to avoid the 0.1 + 0.1 problem
    
    """
    state = list(state_)        # convert to a list that can be modified
    rank = 0
    while rank < len(state):
        if loops[rank][5] == "MUL":
            state[rank] *= loops[rank][4]
        elif loops[rank][5] == "ADD":
            state[rank] += loops[rank][4]
        elif loops[rank][5] == "LIST" :
            state[rank] = loops[rank][4][loops[rank][4].index(state[rank])+1] if state[rank]<loops[rank][3] else state[rank]+1000

        if state[rank] <= loops[rank][3]:
            return tuple(state)          # found a new state
        # Reset this rank to the starting position and go to the next rank
        state[rank] = loops[rank][2] # reset to start
        rank += 1                    # go to next rank
    # Ran out
    return None

def substitute_config(*,config,loops,state):
    """Generate a new config given a current config and a state of the loops.
    """
    # copy.deepcopy cannot copy a ConfigParser, so we need to do this hack.
    # See https://stackoverflow.com/questions/23416370/manually-building-a-deep-copy-of-a-configparser-in-python-2-7
    import io
    import configparser

    config_string = io.StringIO() # give me a string buffer
    config.write(config_string)         # write the config to the string buffer
    config_string.seek(0)               # seek string buffer to beginning
    newconfig = configparser.ConfigParser()
    newconfig.read_file(config_string) # create a new config from the string buffer.

    for rank in range(len(loops)):
        section = loops[rank][0]
        var     = loops[rank][1]
        newconfig[section][var] = str(state[rank])
    return newconfig
        

def str_to_bool(x):
    return x.lower()[0:1] in ['t','y']

def averagex(datax,datay):
    from collections import defaultdict
    import statistics
    data  = defaultdict(list)
    for i in range(len( datax)):
        data[datax[i]].append(datay[i])
    # now recompute datax and datay
    datax = []
    datay = []
    for (x,ylist) in data.items():
        datax.append( x )
        datay.append( statistics.mean( ylist ) )
    return (datax,datay)
    

def draw_graph(*,config,datax,datay):
    # See if the values need to be averaged. If so, create a dictionary of lists of the X values,
    # then average them

    logging.info("draw_graph(config={}, datax={}, datay={}".format(config,datax,datay))

    txt = config[driver.EXPERIMENT].get(driver.EXPERIMENT_YLABEL,'')

    datapoints = len(datax)

    if str_to_bool(config[driver.EXPERIMENT].get(driver.EXPERIMENT_AVERAGEX,'')):
        (datax,datay) = averagex(datax,datay)
        txt = "average {} over {} trials".format(txt,int(datapoints / len(datax)))
    # Now graph, using info in config.
    plt.plot(datax,datay)
    plt.xlabel( config[driver.EXPERIMENT].get(driver.EXPERIMENT_XLABEL,'') )
    plt.ylabel( config[driver.EXPERIMENT].get(driver.EXPERIMENT_YLABEL,'') )
    plt.title(  config[driver.EXPERIMENT].get(driver.EXPERIMENT_TITLE,'') )
    if str_to_bool( config[driver.EXPERIMENT].get(driver.EXPERIMENT_GRID,'')):
        plt.grid( True )

    if str_to_bool( config[driver.EXPERIMENT].get(driver.EXPERIMENT_DRAW_LEGEND, '')):
        plt.legend([txt],loc='best')
    plt.savefig( os.path.join(config[driver.EXPERIMENT][driver.EXPERIMENT_DIR],
                              config[driver.EXPERIMENT].get(driver.EXPERIMENT_GRAPH_FNAME,'graph.png') ))

def graph_data(*,config):
    """Just draw the graph from data already collected"""
    datax = []
    datay = []
    with open( fname(config, driver.EXPERIMENT_GRAPH_DATA_FNAME,'graph_data.json'), "r") as f:
        data = json.load(f)
        datax = data['datax']
        datay = data['datay']
        draw_graph(config=config,datax=datax,datay=datay)
        
def fname(config,var,default):
    """Given a config and a variable, return a full pathname for a filename in the experiment directory"""
    return os.path.join(config[driver.EXPERIMENT].get(driver.EXPERIMENT_DIR,"."), config[driver.EXPERIMENT].get(var,default))


def run_experiment(*,config,callback):
    """Find the loop variables in the config file, create the loops, create a config for each one, and call the callback with each config file."""

    # Before the loops are built, start the experiment
    scaffold = driver.Scaffolding(config=config)
    scaffold.experimentSetup()

    #
    # First find the loop variables
    loops = build_loops(config)
    state = initial_state(loops)
    datax = []                  # all X values
    datay = []                  # all Y values
    while state != None:
        conf = substitute_config( config=config, loops=loops, state=state)
        logging.info("conf={}".format(conf))
        d = callback( conf )
        logging.info("d={}".format(d))
        
        # If d contains data to be graphed, append it to the array of data that will be graphed.
        try:
            datax.append(d[driver.EXPERIMENT_GRAPHX])
        except (TypeError,KeyError) as e:
            pass

        try:
            datay.append(d[driver.EXPERIMENT_GRAPHY])
        except (TypeError,KeyError) as e:
            pass

        state = increment_state(loops, state)

        # If we are using our graphing system, get the variables graphx & graphy and save them
        # TODO: Add a switch to enable/disable our graphing system.
        if d:
            datax.append(d['graphx'])
            datay.append(d['graphy'])

    # All experiments are done.
    # Draw the graph
    with open(fname(config, driver.EXPERIMENT_GRAPH_DATA_FNAME,'graph_data.json'), "w") as f:
        json.dump( {"datax":datax, "datay":datay}, f)

    draw_graph(config=config,datax=datax,datay=datay)


    # Run the experiment Takedown

    scaffold.experimentTakedown()

        
