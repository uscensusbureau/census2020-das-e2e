""" Module docstring """

import logging
from operator import add
import os

import numpy as np

from das_framework.driver import AbstractDASReader
import programs.engine.nodes as nodes
import programs.sparse as sparse
from das_utils import int_wlog
from das_framework.das_testpoints import log_testpoint

try:
    from pyspark.sql import SparkSession
except ImportError as e:
    pass

READER = "reader"
PATH = "path"
TABLES = "tables"
PTABLE = "privacy_table"
CTABLES = "constraint_tables"
CONSTRAINTS = "constraints"
THE_CONSTRAINTS = "theConstraints"
INVARIANTS = "invariants"
THE_INVARIANTS = "theInvariants"
DATA = "data"
SUBSET = "subset"
MARGINS = "add_over_margins"
COMPUTE = "compute"
SIGN = "sign"
IPUMS_FILE = 'ipums_file'

BLOCK_NODE_PARTITIONS=100



def map_to_hhgq(gqtype):
    """
        args:
            gqtype - 3 char str encoding gqtype
        returns: hhgq for 2018 tab of P42.
    """
    assert isinstance(gqtype, str), "GQTYPE is not a str"
    

    gqtype = int_wlog(gqtype, "gqtype")

    # For the 1940 data, gqtypes 1,5 do not appear. Only 0,2,3,4,6,7,8,9. Values are shifted down to 0-7.
    
    if gqtype in [2,3,4]:
        gqtype = gqtype - 1
    elif gqtype in [6,7,8,9]:
        gqtype = gqtype - 2
    return gqtype

def per_keyed_by_joinkey(cef_per):
    """
        args:
            cef_per - sql row that includes serial, age, hispan, race.
        returns: tuple keyed by MAFID.
    """
    joinkey = int_wlog(cef_per.serial, "serial")

    age = int(int_wlog(cef_per.age, "age") >= 18)

    hisp = int(int_wlog(cef_per.hispan, "hispan") > 0)
    if hisp > 1 or hisp < 0:
        err_msg = f"hispan is invalid ({cef_per.hispan})"
        logging.error(err_msg)
        raise ValueError(err_msg)

    race = int_wlog(cef_per.race, "race") - 1
    if race > 5 or race < 0:
        err_msg = f"race is outside [1,6] range ({cef_per.race})"
        logging.error(err_msg)
        raise ValueError(err_msg)

    return (joinkey, (age, hisp, race))

#TODO: FIXME
def unit_keyed_by_joinkey(cef_unit):
    """
        args:
            cef_unit - sql row that includes serial, statefip, county, enumdist, gqtype, gq
        returns: tuple keyed by MAFID.
    """
    # TODO: Change to 1940 spec
    # TODO: check if GQ could be vacant in 1940.
    # use numprec for vacant check? or use GQ.

    joinkey = int_wlog(cef_unit.serial, "serial")

    ten = int_wlog(cef_unit.gq, "gq")
    if ten > 6 or ten < 0:
        err_msg = f"TEN is outside legal values range [0,6] ({cef_unit.gq})"
        logging.error(err_msg)
        raise ValueError(err_msg)
    # For 2008 End-to-end only "Unoccupied"(0)/"Occupied"(1) are used for tenure variable TEN
    ten = int(bool(ten))
    gqtype = map_to_hhgq(cef_unit.gqtype)
    geo = cef_unit.statefip + cef_unit.county + cef_unit.enumdist
    return (joinkey, (ten, gqtype, geo))


def per_to_agg(row):
    """
        args:
            row - should look like (age, hisp, race, ten, hhgq, geo)
        returns tuple with cnt appended
    """
    assert len(row) == 6, f"Person row tuple {row} is not of length 6"
    age, hisp, race, _, hhgq, geo = row
    return ((geo, hhgq, age, hisp, race), 1)

def per_keyed_by_geo(row):
    """
        args:
            row - should look like ((geo, hhgq, age, hisp, race), cnt)
        returns tuple keyed by geo
    """
    assert len(row) == 2, f"Person row tuple {row} is not of length 2"
    (geo, hhgq, age, hisp, race), cnt = row
    return ((geo,), ((hhgq, age, hisp, race), cnt))

def unit_to_agg(row):
    """
        args:
            row - should look like (joinkey, (ten, hhgq, geo))
        returns tuple with cnt appended
    """
    assert len(row) == 2, f"Unit row tuple {row} is not of length 2"
    _, (ten, hhgq, geo) = row
    return ((geo, 8), 1) if hhgq == 0 and ten == 0 else ((geo, hhgq), 1)  # adds new hhgq value=8 for vacant housing units.

def unit_keyed_by_geo(row):
    """
        args:
            row - should look like ((geo, hhgq), cnt)
        returns: tuple keyed by geo, joinkey is dropped
    """
    assert len(row) == 2, f"Unit row tuple {row} is not of length 2"
    (geo, hhgq), cnt = row
    return ((geo,), ((hhgq,), cnt))


def to_ndarray(row, dim):
    """
        args:
            row - list of (idx, cnt) pairs, where idx consists of (hhgq, age, hispan, race)
            dim - tuple of hist dimensions, i.e. number of values each of the variables within idx takes, e.g. (8, 2, 2, 6)

            This function performs the following process:
                (1) Initialize the ndarray.
                (2) Iterate through the list assigning cnt to
                    idx in ndarray except for error idx.
        returns: a ndarray, which is the histogram of detail counts
    """
    hist = np.zeros(dim)

    for idx_cnt in row:

        # Check that list element is a pair
        if len(idx_cnt)!=2:
            err_msg = f"Element {idx_cnt} is not a pair"
            logging.error(err_msg)
            raise ValueError(err_msg)

        idx, cnt = idx_cnt

        # Check that dimensions of the index tuple are correct
        if len(hist.shape) != len(idx):
            err_msg = "Element ({},{}) has different index dimensions ({}) than the histogram index dimensions ({}; {})"\
                .format(idx, cnt, len(idx), len(hist.shape), dim)
            logging.error(err_msg)
            raise IndexError(err_msg)

        # Check that indices are within bounds
        if np.any(np.array(idx) >= np.array(dim)):
            err_msg = "One or more indices in element ({},{}) is out the histogram index bounds {}"\
                .format(idx, cnt, hist.shape)
            logging.error(err_msg)
            raise IndexError(err_msg)

        if cnt is None or np.isnan(cnt):
            logging.warning("Count in histogram element ({},{}) is not a number".format(idx, cnt))
            # TODO: Is this how we want to treat these cases?

        hist[idx] = cnt

    return hist


class reader(AbstractDASReader):
    """ reader class for e2e with 1940 data """
    #TODO: FIXME
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.setup = None
        assert self.config
        #self.per_path  = os.path.expandvars(self.getconfig("Per_Data.path"))
        #self.unit_path = os.path.expandvars(self.getconfig("Unit_Data.path"))

        # How many indices in histogram and how many values each one takes
        # For 1940 it's 4 indices: (hhgq, age, hispan, race)
        self.person_hist_dimensions = (8, 2, 2, 6)

        self.privacy_table_name = self.getconfig(PTABLE)
        self.housing_table_name = self.getconfig(CTABLES)

        # Get the names of the input tables (PersonData and UnitData For 1940 End To End)
        self.data_names = [self.privacy_table_name] + list(self.getiter(CTABLES))

        # import invariants_module
        (file, invariants_class_name) = self.config[CONSTRAINTS][INVARIANTS].rsplit(".", 1)
        invariants_module = __import__(file, fromlist=[invariants_class_name])
        self.InvariantsCreator = getattr(invariants_module, invariants_class_name)

        # import constraints_module
        (file, constraints_class_name) = self.config[CONSTRAINTS][CONSTRAINTS].rsplit(".", 1)
        constraints_module = __import__(file, fromlist=[constraints_class_name])
        self.ConstraintsCreator = getattr(constraints_module, constraints_class_name)

        # Make geocodeDict
        dict = {}
        geolevel_names = tuple(self.getiter("geolevel_names",section="geodict"))
        geolevel_leng = tuple(self.getiter("geolevel_leng",section="geodict"))
        for i in range(len(geolevel_names)):
            dict[int(geolevel_leng[i])] = str(geolevel_names[i])

        self.geocodeDict = dict

    def load_ipums_1940(self):
        """ loads unit and person data using etl system """
        import programs.reader.ipums_1940_rdd as ipums_1940_rdd
        return ipums_1940_rdd.ipums_rdds( os.path.expandvars(self.config[READER][IPUMS_FILE]))
    
    #TODO: FIXME
    def join_hist_agg(self, per_data, unit_data):
        """
        Performs manupulations converting joined person and unit data to histograms and aggregating
        :param per_data:
        :param unit_data:
        :return:
        """
        
        unit_data = unit_data.map(unit_keyed_by_joinkey)
        per_data = per_data.map(per_keyed_by_joinkey)

        # row after join looks like (serial, [(age, hisp, race), (ten, hhgq, geo)])
        per_data = per_data.join(unit_data).map(lambda row: row[1][0] + row[1][1]).map(per_to_agg).reduceByKey(add).map(
            per_keyed_by_geo) # This will drop people that aren't assigned a house or GQ, which shouldn't be possible but maybe we should test for it. Also drops vacant houses which is fine.
        
        unit_hhgq = unit_data.map(unit_to_agg).reduceByKey(add).map(unit_keyed_by_geo)
        
        # to ndarrays
        per_dim = self.person_hist_dimensions
        unit_dim = (9,)  # added one level to hhgq for vacant units

        per_data = per_data.groupByKey().mapValues(lambda row: to_ndarray(row, per_dim))
        unit_hhgq = unit_hhgq.groupByKey().mapValues(lambda row: to_ndarray(row, unit_dim))
        join_data = per_data.rightOuterJoin(unit_hhgq) # Modified join to not drop blocks with only vacant houses which is allowed.

        return join_data

    def read(self):
        """ main reader function """

        # load ipums unit/per rdds
        log_testpoint("T02-004S")
        try:
            unit_data, per_data = self.load_ipums_1940()
        except Exception as e:
            log_testpoint("T02-005F", additional="Person File")
            raise e

        log_testpoint("T02-005S")


        join_data = self.join_hist_agg(per_data, unit_data)
        # print(join_data.first())

        block_nodes = join_data.map(self.make_block_node).coalesce(BLOCK_NODE_PARTITIONS).persist()

        # print(block_nodes.first())
        # join_data.unpersist()
        return block_nodes

    #TODO: FIXME
    def make_block_node(self, person_unit_arrays):
        """
            This function makes block nodes from person unit arrays for a given geocode.

            args:
                person_unit_arrays - a key, value pair of (geocode, arrays),
                                    where arrays are the histograms defined in the config

            returns: block_node - a nodes.geounitNode object for the given geocode
        """
        geocode, arrays = person_unit_arrays
        arrays = list(arrays)
        gqhhvacs = arrays[1].astype(int)
        arrays[1] = arrays[1][:-1]

        # Assign arrays to table names in a dictionary and fill in with zeros if array is non-existent
        assert len(arrays) == len(self.data_names)
        data_dict = {n: a.astype(int) if a is not None else np.zeros(self.person_hist_dimensions).astype(int) for n,a in zip(self.data_names, arrays)}
        
        # geocode is a tuple where the [1] entry is empty. We only want the [0] entry.
        geocode = geocode[0]
        logging.info("creating geocode: %s" % geocode)

        housing_table_name = self.housing_table_name
        privacy_table_name = self.privacy_table_name

        raw = sparse.multiSparse(data_dict[privacy_table_name].astype(int))
        raw_housing = sparse.multiSparse(data_dict[housing_table_name].astype(int))
        levels = tuple(self.config["geodict"]["geolevel_names"].split(","))

        invar_names = tuple(self.config[CONSTRAINTS][THE_INVARIANTS+"."+levels[0]].split(","))
        if invar_names == ("",):
            invariants_dict = {}
        else:
            invariants_dict = self.InvariantsCreator(raw=raw, raw_housing=raw_housing, invariant_names=invar_names).calculateInvariants().invariants_dict
        invariants_dict["gqhhvacs_vect"] = gqhhvacs #not used for constraints, but must be passed through. don't need to add hhvacs to node signature anymore this way.

        cons_names = tuple(self.config[CONSTRAINTS][THE_CONSTRAINTS+"."+levels[0]].split(","))
        

        # Make Constraints
        if cons_names == ("",):
            constraints_dict = {}
        else:
            constraints_dict = self.ConstraintsCreator(hist_shape=data_dict[self.privacy_table_name].shape,
                                                   invariants=invariants_dict,
                                                       constraint_names=cons_names)\
            .calculateConstraints().constraints_dict

        #raw = data_dict[self.privacy_table_name].astype(int)
        
        block_node = nodes.geounitNode(geocode=geocode, geocodeDict=self.geocodeDict, raw=raw, raw_housing=raw_housing,
                                       cons=constraints_dict, invar=invariants_dict)
        return block_node
