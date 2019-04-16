#
#
# Needs to be expanded to accommodate the common occurrence of sparse.multiSparse objects in the geounitNode class vs pure numpy arrays
#
#

import os
import sys
# If there is __init__.py in the directory where this file is, then Python adds das_decennial directory to sys.path
# automatically. Not sure why and how it works, therefore, keeping the following line as a double
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(__file__)))))


import programs.engine.nodes as nodes
import numpy as np


def buildTestNode():
    
    raw = np.array([0,1,2,3,4])
    syn = np.array([6,7,8,9,5])
    geocode = "0"
    geolevel = "National"
    
    node = nodes.geounitNode(geocode, geolevel=geolevel, raw=raw, syn=syn,  geocodeDict={16: "Block", 12: "Block_Group", 11: "Tract", 5: "County", 2: "State", 1: "National"})
    return node

# NODES NO LONGER HAVE toJSON, as this is not needed
# def test_slotsToJSON():
#     node = buildTestNode()
#
#     jsonStr = node.toJSON()
#     assert jsonStr == '{"geocode": "0", "geocodeDict": {"16": "Block", "12": "Block_Group", "11": "Tract", "5": "County", "2": "State", "1": "National"}, "geolevel": "National", "parentGeocode": "0", "raw": [0, 1, 2, 3, 4], "dp": null, "syn": [6, 7, 8, 9, 5], "syn_unrounded": null, "cons": null, "invar": null, "dp_queries": null, "congDistGeocode": null, "sldlGeocode": null, "slduGeocode": null, "minimalSchemaArray": null, "grbVars": null, "grbPenaltyVarsPos": null, "grbPenaltyVarsNeg": null, "ancestorsDP": null, "ancestorsRaw": null}'
#
#     jsonStr = node.toJSON(keepAttrs=["raw", "syn"])
#     assert jsonStr == '{"raw": [0, 1, 2, 3, 4], "syn": [6, 7, 8, 9, 5]}'
#
#     jsontuple = node.toJSON(addClassName=True)
#     assert jsontuple == ('geounitNode', '{"geocode": "0", "geocodeDict": {"16": "Block", "12": "Block_Group", "11": "Tract", "5": "County", "2": "State", "1": "National"}, "geolevel": "National", "parentGeocode": "0", "raw": [0, 1, 2, 3, 4], "dp": null, "syn": [6, 7, 8, 9, 5], "syn_unrounded": null, "cons": null, "invar": null, "dp_queries": null, "congDistGeocode": null, "sldlGeocode": null, "slduGeocode": null, "minimalSchemaArray": null, "grbVars": null, "grbPenaltyVarsPos": null, "grbPenaltyVarsNeg": null, "ancestorsDP": null, "ancestorsRaw": null}')
#
#     classname, jsonStr = jsontuple
#     assert classname == 'geounitNode'
#     assert jsonStr == '{"geocode": "0", "geocodeDict": {"16": "Block", "12": "Block_Group", "11": "Tract", "5": "County", "2": "State", "1": "National"}, "geolevel": "National", "parentGeocode": "0", "raw": [0, 1, 2, 3, 4], "dp": null, "syn": [6, 7, 8, 9, 5], "syn_unrounded": null, "cons": null, "invar": null, "dp_queries": null, "congDistGeocode": null, "sldlGeocode": null, "slduGeocode": null, "minimalSchemaArray": null, "grbVars": null, "grbPenaltyVarsPos": null, "grbPenaltyVarsNeg": null, "ancestorsDP": null, "ancestorsRaw": null}'


