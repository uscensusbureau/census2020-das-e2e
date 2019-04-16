# Useful AWS CLI commands
# 
# aws s3 ls s3://uscb-decennial-ite-das/
#
# aws s3 rm {path} --recursive --exclude * --include {regex/wildcard/name}
#
# aws s3 cp {from_path} {to_path} --recursive --exclude * --include {regex/wildcard/name} --quiet
#

#####################
# Possibly useful notes:
# https://stackoverflow.com/questions/36994839/i-can-pickle-local-objects-if-i-use-a-derived-class
#####################

import json
import numpy as np
import programs.sparse as sparse
import das_framework.ctools.s3 as s3
import pickle
import os
import sys
import subprocess
import re
import logging

import numpy as np
import pandas as pd

from configparser import ConfigParser


# removes whitespace characters and splits by comma
# use with the re module
# re.split(DELIM, string_to_split)
# "   apple  , hat, fork,spoon,    pineapple" => ['apple', 'hat', 'fork', 'spoon', 'pineapple']
DELIM = "^\s+|\s*,\s*|\s+$"
BLANK = ""
S3_PREFIX = "s3://"

NATIONAL_GEOCODE = "0"
NATIONAL_GEOLEVEL = "National"

def int_wlog(s, name):
    """
    Convert a string to integer, wrapped so that it issues error to the logging module, with the variable name
    :param s: string to convert
    :param name: variable name, to indicate in the error message
    :return: integer obtained as conversion of s
    """
    try:
        ans = int(s)
    except ValueError as err:
        error_msg = "{} value \"{}\" is not numeric, conversion attempt returned \"{}\""\
            .format(name, s, str(err.args[0]))
        logging.error(error_msg)
        raise ValueError(error_msg)
    return ans


def pretty(item, indent=4, join_with='\n'):
    if type(item) == dict:
        return json.dumps(item, indent=indent)
    else:
        item = aslist(item)
        return join_with.join(item)
    

def aslist(item):
    """
    aslist wraps a single value in a list, or just returns the list
    """
    return item if type(item) == list else [item]

def tolist(item):
    """
    tolist wraps a single value in a list, or converts the item to a list
    """
    return item if type(item) == list else list(item)

def quickRepr(item, join='\n'):
    items = ["{}: {}".format(attr, value) for attr, value in item.__dict__.items()]
    if join is None:
        return items
    else:
        return join.join(items)

def getObjectName(object):
    return object.__qualname__


def printList(thelist):
    print('\n\n'.join([str(x) for x in thelist]))


def flattenList(nested_list):
    nested_list = nested_list.copy()
    flattened_list = []
    while nested_list != []:
        element = nested_list.pop()
        if type(element) == list:
            nested_list += element
        else:
            flattened_list.append(element)
    
    flattened_list.reverse()
    return flattened_list

def getConfigItem(config, section, item, name=None, dtype=None):
    """
    Extracts items from a config object
    
    Inputs:
        config: ConfigParser object
        section: the name of the section where the item is found
        item: the name of the item
        name: used for accessing name.item attributes
        dtype: a function used to cast the item(s) to different types
        
    Notes:
        dtype only works (at the moment) with the following casting functions:
            str
            int
            float
    """
    # Used for accessing name.item attributes
    if name is not None:
        item = "{}.{}".format(name, item)
    
    # Split the string
    clist = re.split(das_utils.DELIM, config[section][item])
    
    # Cast the string as other types
    if dtype is not None and dtype in [str, int, float]:
        clist = [dtype(x) for x in clist]
    
    if len(clist) == 1:
        clist = clist[0]
    
    return clist

#################
# Utility functions
# I/O
#################

def loadConfigFile(path):
    config = ConfigParser()
    if isS3Path(path):
        config_file = s3.s3open(path=path, mode="r")
        config.readfp(config_file)
        config_file.close()
    else:
        with open(path, 'r') as config_file:
            config.readfp(config_file)
    
    return config

def getGeoDict(config):
    assert 'geodict' in config, "This config file doesn't contain a 'geodict' section."
    keys = ['geolevel_names', 'geolevel_leng']
    geodict = {}
    for k in keys:
        geodict[k] = config['geodict'][k]
    return geodict

def makePath(path):
    if not os.path.exists(path):
        os.makedirs(path)


def loadPickleFile(path):
    contents = None
    if isS3Path(path):
        contents = loadPickleS3(path)
    else:
        with open(path, 'rb') as f:
            contents = pickle.load(f)
    
    return contents

def loadJSONFile(path):
    contents = None
    if isS3Path(path):
        contents = loadJSONS3(path)
    else:
        with open(path, 'r') as f:
            contents = json.load(path)
        
    return contents

def loadFromS3(path):
    ext = path.split('.')[1]
    if ext == "json":
        contents = loadJSONS3(path)
    else:
        contents = loadPickleS3(path)
    
    return contents

def loadJSONS3(path):
    jsonfile = s3.s3open(path=path, mode='r')
    contents = json.load(jsonfile)
    return contents

def loadPickleS3(path):
    loadfile = s3.s3open(path=path, mode="rb")
    contents = pickle.load(loadfile)
    return contents

def isS3Path(path):
    """
    isS3Path does a simple check to see if the path looks like an s3 path or not
    
    Notes:
        it checks to see if the path string has the standard s3 prefix "s3://"
        at the beginning
    """
    return path.startswith(S3_PREFIX)

def saveNestedListAsTextFile(path, thelist):
    thelist = flattenList(thelist)
    saveListAsTextFile(path, thelist)

def saveListAsTextFile(path, thelist):
    if isS3Path(path):
        savefile = s3.s3open(path=path, mode='w')
        for item in thelist:
            savefile.write("{}\n".format(item))
        savefile.close()
    else:
        with open(path, 'w') as f:
            for item in thelist:
                f.write("{}\n".format(item))
    

def saveConfigFile(path, config):
    if isS3Path(path):
        savefile = s3.s3open(path=path, mode="w")
        config.write(savefile)
        savefile.close()
    else:
        with open(path, 'w') as f:
            config.write(f)
    

def saveJSONFile(path, data, indent=None):
    if isS3Path(path):
        savefile = s3.s3open(path=path, mode="w")
        json.dump(data, savefile, indent=indent)
        savefile.close()
    else:
        with open(path, 'w') as f:
            json.dump(data, f, indent=indent)
    

def savePickledRDD(path, rdd, batchSize=10):
    if isS3Path(path):
        subprocess.call("aws s3 rm {} --recursive --quiet".format(path).split())
    else:
        subprocess.call("hadoop fs -rm -r {}".format(path).split())
    
    rdd.saveAsPickleFile(path, batchSize=batchSize)


def savePickleFile(path, data):
    if isS3Path(path):
        savefile = s3.s3open(path=path, mode='wb')
        pickle.dump(data, savefile)
        savefile.close()
    else:
        with open(path, 'wb') as f:
            pickle.dump(data, f)
    

def getS3Paths(path, fullpath=True):
    """
    0. use subprocess to extract the AWS s3 ls contents as a string
    1. remove the "b'" at the beginning
    2. remove the "'" at the end
    3. remove the escape characters ('\n')
    4. remove all white space characters
    5. split by the PRE string that appears before every filename
    """
    s3ls = str(subprocess.check_output("aws s3 ls {} --human-readable".format(path).split()))
    s2 = re.sub(r"b'", "", s3ls)
    s2 = re.sub(r"'", "", s2)
    s2 = re.sub(r"\\n", "", s2)
    s2 = re.sub("^\s+|\s*\s*|\s+$", "", s2)
    groups = re.split(r"PRE", s2)[1:]
    
    if fullpath:
        groups = ["{}{}".format(path, g) for g in groups]
    
    return groups


#################
# Utility functions
# Custom JSON Encoder; miscellaneous
#################
class NodeEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, np.integer):
            return int(obj)
        elif isinstance(obj, np.floating):
            return float(obj)
        elif isinstance(obj, np.ndarray):
            return obj.tolist()
        elif isinstance(obj, sparse.multiSparse):
            return obj.toDense().tolist()
        else:
            return super(NodeEncoder, self).default(obj)



class JSONify(object):
    """
    JSONify is a class that adds a toJSON method for easily converting
    the properties of a class/object into a JSON string
    
    Notes:
        the encoding only extends the JSON encoder's functionality to handle some of
        numpy's objects and types
        
        this only provides the toJSON encoding function, not the decoding function;
        that will need to be implemented by any class inheriting from this one
        
        there are different methods for implementing the decoding procedure, but the
        recommended way is to add a static method to the class that looks like this:
        
        @staticmethod
        def fromJSON(jsonStr):
            # use json.loads(jsonStr) to turn the JSON string into a dict
            # and then use the dict to reconstruct this class
            # most likely by using the constructor to instantiate it with the dict properties
        
    """
    __slots__ = ()
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
    
    def toJSON(self, keepAttrs=None, encoder=NodeEncoder, addClassName=False):
        # set the jsondict to be the __dict__ attribute
        if hasattr(self, "__dict__"):
            mydict = self.__dict__
        else:
            # No __dict__, so try to see if this is because of a __slots__ attribute
            mydict = self.getDictFromSlots()

        # create a keep dictionary that only keeps the attributes specified in the keepAttrs argument
        if keepAttrs is None:
            keep = mydict
        else:
            keep = {}
            for key,val in mydict.items():
                if key in keepAttrs:
                    keep[key] = val

        # encode the keep dictionary using an encoder class
        jsonStr = json.dumps(keep, cls=encoder)

        # if the name of the class is desired, make a tuple of the class name and JSON string
        if addClassName:
            jsonStr = (type(self).__name__, jsonStr)

        return jsonStr
    
    def getDictFromSlots(self):
        """
        getDictFromSlots is run when an object/class has __slots__ set up
        this tends to make the __dict__ attribute empty '{}', so we need to use the
        __slots__ attribute to generate a dict object
        """
        noSuchAttrDefault = "no slots exist"
        slots_dict = {}

        if getattr(self, "__slots__", noSuchAttrDefault) is not noSuchAttrDefault:
            for attr in self.__slots__:
                slots_dict[attr] = getattr(self, attr, None)

        return slots_dict
        


class SanityChecker():
    def __init__(self, config):
        self.config = config
    
    def validateInputs(self):
        
        pass
    
    def validateOutputs(self):
        pass
    



