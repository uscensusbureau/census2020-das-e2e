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


def map_to_hhgq(gqtype):
    """
        args:
            gqtype - 3 char str encoding gqtype
        returns: hhgq for 2018 tab of P42.
    """
    assert isinstance(gqtype, str), "GQTYPE is not a str"
    assert len(gqtype) == 3, "GQTYPE is not a str of length 3"

    # Empty/NULL fields stands for Housing Unit, not GQ
    if gqtype.strip() == "":
        return 0

    gq = int_wlog(gqtype, "GQTYPE")

    # For the 2018 End_to_end, not all GQ codes are used, and those that are used are collapsed to a 101,201,301 etc,
    # and mapped to a single digit within the DAS
    # 700s,800s,900s, all mapped to 7
    if gq < 700:
        return gq // 100
    # if 101 <= gq <= 106: return 1
    # if 201 <= gq <= 203: return 2
    # if gq == 301: return 3
    # if 401 <= gq <= 405: return 4
    # if 501 <= gq <= 502: return 5
    # if 601 <= gq <= 602: return 6
    if gq >= 700:
        return 7


def per_keyed_by_mafid(cef_per):
    """
        args:
            cef_per - CEF_PER obj, spec TBD but assuming MAFID, QAGE, CENHISP, CENRACE attr.
        returns: tuple keyed by MAFID.
    """

    maf = int_wlog(cef_per.MAFID, "MAFID")

    age = int(int_wlog(cef_per.QAGE, "QAGE") >= 18)

    hisp = int_wlog(cef_per.CENHISP, "CENHISP") - 1
    if hisp > 1 or hisp < 0:
        err_msg = f"CENHISP is neither 1, nor 2 ({cef_per.CENHISP})"
        logging.error(err_msg)
        raise ValueError(err_msg)

    race = int_wlog(cef_per.CENRACE, "CENRACE") - 1
    if race > 62 or race < 0:
        err_msg = f"CENRACE is outside [1,63] range ({cef_per.CENHISP})"
        logging.error(err_msg)
        raise ValueError(err_msg)

    return (maf, (age, hisp, race))


def unit_keyed_by_mafid(cef_unit):
    """
        args:
            cef_unit - CEF_UNIT obj, spec TBD but assuming MAFID, TEN, QGQTYP, OIDTB attr.
        returns: tuple keyed by MAFID.
    """
    maf = int_wlog(cef_unit.MAFID, "MAFID")

    ten = int_wlog(cef_unit.TEN, "TEN")
    if ten > 4 or ten < 0:
        err_msg = f"TEN is outside legal values range [0,4] ({cef_unit.TEN})"
        logging.error(err_msg)
        raise ValueError(err_msg)
    # For 2008 End-to-end only "Unoccupied"(0)/"Occupied"(1) are used for tenure variable TEN
    ten = int(bool(ten))
    gqtype = map_to_hhgq(cef_unit.QGQTYP)
    geo = cef_unit.OIDTB
    return (maf, (ten, gqtype, geo))


def unit_keyed_by_oidtb(cef_unit):
    """
        args:
            cef_unit - CEF_UNIT obj, spec TBD but assuming MAFID, TEN, QGQTYP, OIDTB attr.
        returns: tuple keyed by OIDTB.
    """
    maf = int_wlog(cef_unit.MAFID, "MAFID")

    ten = int_wlog(cef_unit.TEN, "TEN")
    if ten > 4 or ten < 0:
        err_msg = f"TEN is outside legal values range [0,4] ({cef_unit.TEN})"
        logging.error(err_msg)
        raise ValueError(err_msg)
    # For 2008 End-to-end only "Unoccupied"(0)/"Occupied"(1) are used for tenure variable TEN
    ten = int(bool(ten))
    gqtype = map_to_hhgq(cef_unit.QGQTYP)
    oidtb = int_wlog(cef_unit.OIDTB, "OIDTB")
    return (oidtb, (ten, gqtype, maf))


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
            row - should look like (maf, (ten, hhgq, geo))
        returns tuple with cnt appended
    """
    assert len(row) == 2, f"Unit row tuple {row} is not of length 2"
    _, (ten, hhgq, geo) = row
    return ((geo, 8), 1) if hhgq == 0 and ten == 0 else ((geo, hhgq), 1)  # adds new hhgq value=8 for vacant housing units.


def unit_keyed_by_geo(row):
    """
        args:
            row - should look like ((geo, hhgq), cnt)
        returns: tuple keyed by geo, mafid is dropped
    """
    assert len(row) == 2, f"Unit row tuple {row} is not of length 2"
    (geo, hhgq), cnt = row
    return ((geo,), ((hhgq,), cnt))


def grfc_map_to_oidtb_geocode(row):
    """
    :param row: Spark Row
    :return:
    """
    geocode = row.TABBLKST + row.TABBLKCOU + row.TABTRACTCE + row.TABBLKGRPCE + row.TABBLK
    return (int_wlog(row.OIDTABBLK,"OIDTABBLK"), geocode) # new test data should work as excepted.


def to_ndarray(row, dim):
    """
        args:
            row - list of (idx, cnt) pairs, where idx consists of (qrel, qage, qsex, cenrace)
            dim - tuple of hist dimensions, i.e. number of values each of the variables within idx takes, e.g. (8, 2, 2, 63)

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
    """ reader class for e2e test """
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.setup = None
        assert self.config
        self.per_path  = os.path.expandvars(self.getconfig("Per_Data.path"))
        self.unit_path = os.path.expandvars(self.getconfig("Unit_Data.path"))
        self.grfc_path = os.path.expandvars(self.getconfig("GRFC.path"))
        self.mafx_path = None

        # How many indices in histogram and how many values each one takes
        # For 2018 End To End it's 4 indices: (hhgq, qage, qsex, cenrace)
        self.person_hist_dimensions = (8, 2, 2, 63)

        self.privacy_table_name = self.getconfig(PTABLE)
        self.housing_table_name = self.getconfig(CTABLES)

        # Get the names of the input tables (PersonData and UnitData For 2018 End To End)
        self.data_names = [self.privacy_table_name] + list(self.getiter(CTABLES))

        #self.invar_names = tuple(self.getiter(THE_INVARIANTS, section=CONSTRAINTS))
        #self.cons_names = tuple(self.getiter(THE_CONSTRAINTS, section=CONSTRAINTS))

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


    # def load_mafx(self):
    #     """ Needs to return an rdd keyed by mafid of tabst, tabcou, etc concatenated into geocode so like (mafid, geocode) """
    #     spark = SparkSession.builder.getOrCreate()
    #     return spark.read.json().rdd.map()

    def load_grfc(self):
        """ Needs to return ad rdd keyed by oidtabblk of tabst, tabcou, etc concatenated into geocode so like (oidtb, geocode) """
        spark = SparkSession.builder.getOrCreate()
        return spark.read.csv(self.grfc_path, sep='|', header=True).rdd\
            .map(grfc_map_to_oidtb_geocode).filter(lambda row: str(row[1]).startswith("44")) # test data includes blocks not in RI which shouldn't be possible.

    def load_per(self):
        """ loads person data into rdd, does some transformations """
        # TODO: verify variables, import simson's CEF_PER class
        spark = SparkSession.builder.getOrCreate()
        return spark.sparkContext.textFile(self.per_path).map(CEF_PER).map(per_keyed_by_mafid)

    def load_unit(self):
        """ loads unit data into rdd, does some transformations """
        # TODO: verify variables, import simson's CEF_UNIT class, textFile might only work with single file not directory. needs testing.
        spark = SparkSession.builder.getOrCreate()
        return spark.sparkContext.textFile(self.unit_path).map(CEF_UNIT).map(unit_keyed_by_oidtb)

    # def load_geo(self):
    #     spark = SparkSession.builder.getOrCreate()
    #     return spark.sparkContext.textFile(self.geo_path).map(GEO_UNIT).map(geo_keyed_by_OIDTB)

    def join_hist_agg(self, per_data, unit_data, geo_data):
        """
        Performs manupulations converting joined person and unit data to histograms and aggregating
        :param per_data:
        :param unit_data:
        :param geo_data:
        :return:
        """
        # need to rekey unit by oidtb then join will work. should return (mafid, (ten, hhgq, geo)), need to edit unit_keyed_by_mafid.
        # row[1][0][2] is mafid, row[1][1] is geocode
        unit_data = unit_data.join(geo_data).map(lambda row: (row[1][0][2], row[1][0][:2] + (row[1][1],))) # this will drop houses that don't have geographies, which shouldn't but it happens in the test data. Also drops blocks with no houses/gqs which is fine.

        # TODO: verify row order, needs to be updated if above changes
        # row after join looks like (maf, [(age, hisp, race), (ten, hhgq, geo)])
        per_data = per_data.join(unit_data).map(lambda row: row[1][0] + row[1][1]).map(per_to_agg).reduceByKey(add).map(
            per_keyed_by_geo) # This will drop people that aren't assigned a house or GQ, which shouldn't be possible but maybe we should test for it. Also drops vacant houses which is fine.
        # TODO: verify row order, update if above changes
        unit_hhgq = unit_data.map(unit_to_agg).reduceByKey(add).map(unit_keyed_by_geo)
        # unit_ov = unit_data.map(lambda row: unit_ov(row))

        # to ndarrays
        per_dim = self.person_hist_dimensions
        unit_dim = (9,)  # added one level for hhgq

        per_data = per_data.groupByKey().mapValues(lambda row: to_ndarray(row, per_dim))
        unit_hhgq = unit_hhgq.groupByKey().mapValues(lambda row: to_ndarray(row, unit_dim))
        join_data = per_data.rightOuterJoin(unit_hhgq) # Modified join to not drop blocks with only vacant houses which is allowed.

        return join_data

    def read(self):
        """ main reader function """

        # load person data
        log_testpoint("T02-004S")
        try:
            per_data = self.load_per()
        except Exception as e:
            log_testpoint("T02-005F", additional="Person File")
            raise e

        # load unit data
        try:
            unit_data = self.load_unit()
        except Exception as e:
            log_testpoint("T02-005F", additional="Unit File")
            raise e

        log_testpoint("T02-005S")

        # load GRFC
        geo_data = self.load_grfc()

        join_data = self.join_hist_agg(per_data, unit_data, geo_data)
        # print(join_data.first())

        block_nodes = join_data.map(self.make_block_node).persist()

        # print(block_nodes.first())
        # join_data.unpersist()
        return block_nodes

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
        #for name in data_dict:
        #if data_dict[self.privacy_table_name] is None:
        #    data_dict[self.privacy_table_name] = np.zeros(self.person_hist_dimensions).astype(int)

        # data_dict = {}
        # for i in range(len(arrays)):
        #     data_dict[self.data_names[i]] = arrays[i].astype(int) if arrays[i] is not None else np.zeros(self.person_hist_dimensions).astype(int)

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

class CEF_UNIT:
    """ CEF unit row parser """
    __slots__ = ['RTYPE', 'MAFID', 'FINAL_POP', 'HHLDRAGE', 'TEN', 'TEN_A', 'TEN_R', 'VACS', 'QGQTYP', 'OIDTB']

    def __repr__(self):
        return 'CEF_UNIT<RTYPE:{},MAFID:{},FINAL_POP:{},HHLDRAGE:{},TEN:{},TEN_A:{},TEN_R:{},VACS:{},QGQTYP:{},OIDTB:{}>'.format(self.RTYPE, self.MAFID, self.FINAL_POP, self.HHLDRAGE, self.TEN, self.TEN_A, self.TEN_R, self.VACS, self.QGQTYP, self.OIDTB)

    def __init__(self, line=None):
        """ CEF unit parser of a txt line """
        if line:
            self.RTYPE = line[0:1]
            self.MAFID = line[1:10]
            self.FINAL_POP = line[10:15]
            self.HHLDRAGE = line[15:18]
            self.TEN = line[18:19]
            self.TEN_A = line[19:20]
            self.TEN_R = line[20:21]
            self.VACS = line[21:22]
            self.QGQTYP = line[22:25]
            self.OIDTB = line[25:47] # spec updated, number(22) ie max length of 22. left padded with whitespace.


class CEF_PER:
    """ CEF person row parser """
    __slots__ = ['RTYPE', 'MAFID', 'CUF_PNC', 'QSEX', 'QAGE', 'QDB', 'QDOB_MONTH', 'QDOB_DAY', 'QDOB_YEAR', 'QSPAN1', 'QSPAN2', 'QSPAN3', 'QSPAN4', 'QSPAN5', 'QSPAN6', 'QSPAN7', 'QSPAN8', 'QSPANX', 'CENHISP', 'QRACE1', 'QRACE2', 'QRACE3', 'QRACE4', 'QRACE5', 'QRACE6', 'QRACE7', 'QRACE8', 'QRACEX', 'CENRACE', 'IMPRACE', 'IMPRACE2', 'RACE2010', 'FRACE2010', 'QREL']

    def __repr__(self):
        return 'CEF_PER<RTYPE:{},MAFID:{},CUF_PNC:{},QSEX:{},QAGE:{},QDB:{},QDOB_MONTH:{},QDOB_DAY:{},QDOB_YEAR:{},QSPAN1:{},QSPAN2:{},QSPAN3:{},QSPAN4:{},QSPAN5:{},QSPAN6:{},QSPAN7:{},QSPAN8:{},QSPANX:{},CENHISP:{},QRACE1:{},QRACE2:{},QRACE3:{},QRACE4:{},QRACE5:{},QRACE6:{},QRACE7:{},QRACE8:{},QRACEX:{},CENRACE:{},IMPRACE:{},IMPRACE2:{},RACE2010:{},FRACE2010:{},QREL:{}>'.format(self.RTYPE, self.MAFID, self.CUF_PNC, self.QSEX, self.QAGE, self.QDB, self.QDOB_MONTH, self.QDOB_DAY, self.QDOB_YEAR, self.QSPAN1, self.QSPAN2, self.QSPAN3, self.QSPAN4, self.QSPAN5, self.QSPAN6, self.QSPAN7, self.QSPAN8, self.QSPANX, self.CENHISP, self.QRACE1, self.QRACE2, self.QRACE3, self.QRACE4, self.QRACE5, self.QRACE6, self.QRACE7, self.QRACE8, self.QRACEX, self.CENRACE, self.IMPRACE, self.IMPRACE2, self.RACE2010, self.FRACE2010, self.QREL)

    def __init__(self, line=None):
        """ CEF person parser of a txt line """
        if line:
            self.RTYPE = line[0:1]
            self.MAFID = line[1:10]
            self.CUF_PNC = line[10:15]
            self.QSEX = line[15:16]
            self.QAGE = line[16:19]
            self.QDB = line[19:27]
            self.QDOB_MONTH = line[27:29]
            self.QDOB_DAY = line[29:31]
            self.QDOB_YEAR = line[31:35]
            self.QSPAN1 = line[35:39]
            self.QSPAN2 = line[39:43]
            self.QSPAN3 = line[43:47]
            self.QSPAN4 = line[47:51]
            self.QSPAN5 = line[51:55]
            self.QSPAN6 = line[55:59]
            self.QSPAN7 = line[59:63]
            self.QSPAN8 = line[63:67]
            self.QSPANX = line[67:68]
            self.CENHISP = line[68:69]
            self.QRACE1 = line[69:73]
            self.QRACE2 = line[73:77]
            self.QRACE3 = line[77:81]
            self.QRACE4 = line[81:85]
            self.QRACE5 = line[85:89]
            self.QRACE6 = line[89:93]
            self.QRACE7 = line[93:97]
            self.QRACE8 = line[97:101]
            self.QRACEX = line[101:102]
            self.CENRACE = line[102:104]
            self.IMPRACE = line[104:106]
            self.IMPRACE2 = line[106:108]
            self.RACE2010 = line[108:110]
            self.FRACE2010 = line[110:112]
            self.QREL = line[112:114]
