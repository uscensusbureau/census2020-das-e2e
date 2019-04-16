import programs.sparse as sparse
import das_utils
import logging
import json
import numpy as np

class geounitNode():
    """"
    This is a simple class to better organize geounit node data.
    
    Geounit node is constructed from (geocode, **argsDict) pair, where argsDict contains at least the keys:
        geocode: the geocode of the node
        geolevel: the geocode level (national, state, etc.)
        raw: np.array of the true values
        dp: np.array of the DP detailed count values
        syn: np.array of the post-imputation synthetic detailed counts
        syn_unrounded : np.array of the mid-imputation pre-rounded synthetic detailed counts
        cons = ["const_name1":const1, "const_name2":const2] where const1 and const2 are cenquery.Constraint objects
        invar = {"name1":invariant1, "name2":invariant2} where invariant1 and invariant2 are numpy arrays 
        dp_queries = {"dp_name1":dp1, "dp_name2":dp2} where dp1 and dp2 are cenquery.DPquery objects
    
    Methods:
        setGeolevel
        setParentGeocode
        setCongDistGeocode
        setSLDLGeocode
        setSLDUGeocode
    
    """

    __slots__ = ["geocode","geocodeDict","geolevel","parentGeocode","raw","raw_housing","dp","syn","syn_unrounded","cons","invar","dp_queries","congDistGeocode","sldlGeocode","slduGeocode"]

    def __init__(self,geocode,geolevel=None,parentGeocode=None,raw=None,raw_housing=None,dp=None,syn=None,syn_unrounded=None,cons=None,invar=None,dp_queries=None,geocodeDict=None):
        #assert geocode is not None and len(geocode) >= 2, 'construction of geounit node requires >=2-digit geocode string'
        self.geocode = geocode
        
        self.raw = raw
        self.raw_housing = raw_housing
        self.dp = dp
        self.syn = syn
        self.syn_unrounded = syn_unrounded
        
        self.cons = cons
        self.invar= invar
        self.dp_queries= dp_queries

        self.congDistGeocode = None
        self.sldlGeocode = None
        self.slduGeocode = None
        
        if geocodeDict:
            self.geocodeDict = geocodeDict
        # elif config:
        #     self.setgeoDict(config=config)
        else:
            err_msg = "geocodeDict not provided for creation of geoUnitNode"
            logging.error(err_msg)
            raise AttributeError(err_msg)
        
        self.setParentGeocode()
        self.setGeolevel()

    def __repr__(self):
        """
            printable str representation of a geounitNode
        """
        output = ""
        output += "--- geounit Node ---\n"
        output += "geocode: " + str(self.geocode) + ", geolevel " + str(self.geolevel) + "\n"
        output += "parent geocode: " + str(self.parentGeocode) + "\n"
        if self.congDistGeocode:
            output += "congressional districts geocode: " + str(self.congDistGeocode) + "\n"
        if self.sldlGeocode:
            output += "state lower chambers geocode: " + str(self.sldlGeocode) + "\n"
        if self.slduGeocode:
            output += "state upper chambers geocode: " + str(self.slduGeocode) + "\n"
        if self.raw is not None:
            output += "raw.shape: " + str(self.raw.toDense().shape) + "\n"
            output += "raw: " + str(self.raw.toDense()) + "\n"
        else:
            output += "raw: None\n"
        if self.raw_housing is not None:
            output += "raw_housing" + str(self.raw_housing.toDense()) + "\n"
        else:
            output += "raw_housing: None\n"
        output += "dp: " + str(self.dp) + "\n"
        output += "cons: " + str(self.cons) + "\n"
        output += "invar: " + str(self.invar) + "\n"
        output += "syn: " + str(self.syn) + "\n"
        output += "syn_unrounded: " + str(self.syn_unrounded) + "\n"
        output += "dp_queries: " + str(self.dp_queries) + "\n"
        return output

    def __eq__(self, other):
        """
        Two nodes are equal if all attributes from the list are equal
        """
        eq = True
        for attr in ['geocode',
                     'geocodeDict',
                     'geolevel',
                     'parentGeocode',
                     'raw',
                     'raw_housing',
                     'dp',
                     'syn',
                     'syn_unrounded',
                     # 'cons',
                     # 'invar',
                     # 'dp_queries',
                     # 'congDistGeocode',
                     # 'sldlGeocode',
                     # 'slduGeocode',
                     ]:

            eq = eq and self.__getattribute__(attr) == other.__getattribute__(attr)

        #eq = eq and (np.array_equal(self.raw.toDense(), other.raw.toDense()))
        return eq

    def setParentGeocode(self):
        """
        Takes the node's geocode and determines its parent's geocode
        """
        mykeys = [key for key in self.geocodeDict.keys()]

        Dict = {child:parent for child, parent in zip(mykeys[:-1], mykeys[1:])}

        # Dict={}
        # for i in range(len(mykeys)-1):
        #     Dict[mykeys[i]] =  mykeys[i+1]

        #Dict = {16:12,12:11,11:5,5:2}

        geocodeLen = len(self.geocode)
        parentGeocodeLen = Dict[geocodeLen] if geocodeLen > 2 else ''
        self.parentGeocode = self.geocode[:parentGeocodeLen] if geocodeLen > 2 else '0'

    def setGeolevel(self):
        """
        Takes the node's geocode and determines its geolevel
        """
        #geocodeDict = {16:"Block",12:"Block_Group",11:"Tract",5:"County",2:"State",1:"National"}
        geocodeLen = len(self.geocode)
        try:
            self.geolevel = self.geocodeDict[geocodeLen]
        except KeyError:
            error_msg = "No GeoLevel name for geocode of length {} (geocode:{}) in geocode dictionary \"{}\""\
                .format(geocodeLen, self.geocode, self.geocodeDict)
            logging.error(error_msg)
            raise KeyError(error_msg)

    def setCongDistGeocode(self, congDistGeocode):
        self.congDistGeocode = congDistGeocode

    def setSLDLGeocode(self, sldlGeocode):
        self.sldlGeocode = sldlGeocode

    def setSLDUGeocode(self, slduGeocode):
        self.slduGeocode = slduGeocode
    
    # def setgeoDict(self, config):
    #
    #     dict = {}
    #     geolevel_names = tuple(config["geodict"]["geolevel_names"].split(","))
    #     geolevel_leng =  tuple(config["geodict"]["geolevel_leng"].split(","))
    #     for i in range(len(geolevel_names)):
    #         dict[int(geolevel_leng[i])] = str(geolevel_names[i])
    #
    #     self.geocodeDict = dict
        
    def stripForSave(self):
        self.dp = None
        self.syn_unrounded = None
        self.cons = None
        self.invar = None
        self.dp_queries = None
        # self.parent_backup_solve = None
        # self.backup_solve = None
        
        return self
