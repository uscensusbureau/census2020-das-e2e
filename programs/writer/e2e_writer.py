from operator import add
import os
import numpy as np
import logging
import sys
import psutil
import pwd
import datetime

from das_framework.driver import AbstractDASWriter
from das_utils import int_wlog
from das_framework.das_testpoints import log_testpoint

from constants import *
import ctools
import ctools.s3


PER_PATH = "per_path"
UNIT_PATH = "unit_path"

def to_per_line(record):
    # TODO: confirm line order or stick in config.
    # assume record is tuple (geocode, hhgq, va, hisp, race)

    # Check that the record has appropriate number of fields
    exp_len = 7
    act_len = len(record)
    assert act_len == exp_len, f"Record {record} has {act_len} instead of {exp_len} fields"

    geocode, euid, epnum, hhgq, va, hisp, race = record

    SCHEMA_TYPE_CODE = "MPD"
    SCHEMA_BUILD_ID = "3.1.4"

    # Check that geocode length is 16
    assert isinstance(geocode, str), f"Geocode ({geocode}) in record ({record}) is not in str format"
    assert len(geocode) == 16, "Geocode of record {} has length {} instead of 16".format(record, len(geocode))
    TABBLKST = geocode[:2]
    TABBLKCOU = geocode[2:5]
    TABTRACTCE = geocode[5:11]
    TABBLKGRPCE = geocode[11:12]
    TABBLK = geocode[12:16]

    EUID = int_wlog(euid, "EUID")
    EPNUM = int_wlog(epnum, "EPNUM") + 1

    t = type(hhgq)
    assert np.issubdtype(t, int), f"HHGQ ({hhgq}) in record ({record}) is not in integer format has type {t}"
    t = type(va)
    assert np.issubdtype(t, int), f"VAGE ({va}) in record ({record}) is not in integer format has type {t}"

    RTYPE = "3" if hhgq == 0 else "5"
    QREL = "99"
    QSEX = "9"
    QAGE = "17" if va == 0 else "18"

    CENHISP = str(int_wlog(hisp, "CENHISP") + 1)
    CENRACE = str(int_wlog(race, "CENRACE") + 1).zfill(2)

    if hisp > 1 or hisp < 0:
        err_msg = f"CENHISP is neither 1, nor 2 ({CENHISP})"
        logging.error(err_msg)
        raise ValueError(err_msg)

    if race > 62 or race < 0:
        err_msg = f"CENRACE is outside [1,63] range ({CENHISP})"
        logging.error(err_msg)
        raise ValueError(err_msg)

    QSPANX = "9999"
    QRACE1 = "9999"
    QRACE2 = "9999"
    QRACE3 = "9999"
    QRACE4 = "9999"
    QRACE5 = "9999"
    QRACE6 = "9999"
    QRACE7 = "9999"
    QRACE8 = "9999"
    CIT = "9"
    line = [SCHEMA_TYPE_CODE, SCHEMA_BUILD_ID, TABBLKST, TABBLKCOU, TABTRACTCE, TABBLKGRPCE, TABBLK,
            EUID, EPNUM, RTYPE, QREL, QSEX, QAGE, CENHISP, CENRACE, QSPANX, QRACE1, QRACE2, QRACE3,
            QRACE4, QRACE5, QRACE6, QRACE7, QRACE8, CIT]
    return "|".join([str(x) for x in line])


def to_unit_line(record):
    # TODO: confirm line order or stick in config.
    # assume record is tuple (geocode, global_id, final_pop, hhgq)
    geocode, euid, final_pop, hhgq = record
    SCHEMA_TYPE_CODE = "MUD"
    SCHEMA_BUILD_ID = "3.1.4"
    TABBLKST = geocode[:2]
    TABBLKCOU = geocode[2:5]
    TABTRACTCE = geocode[5:11]
    TABBLKGRPCE = geocode[11:12]
    TABBLK = geocode[12:16]
    EUID = int(euid)
    RTYPE = "2" if int(hhgq) == 0 or int(hhgq) == 8 else "4"
    GQTYPE = "000" if RTYPE == "2" else str(int(hhgq) * 100 + 1)
    TEN = "9" if RTYPE == "2" and int(final_pop) > 0 else "0"
    VACS = "9" if RTYPE == "2" and int(final_pop) == 0 else "0"
    FINAL_POP = int(final_pop)
    HHT = "9"
    HHT2 = "99"
    NPF = "99"
    CPLT = "9"
    UPART = "9"
    MULTG = "9"
    HHLDRAGE = "999"
    HHSPAN = "9"
    HHRACE = "99"
    PAOC = "9"
    P18 = "99"
    P60 = "99"
    P65 = "99"
    P75 = "99"
    line = [SCHEMA_TYPE_CODE, SCHEMA_BUILD_ID, TABBLKST, TABBLKCOU, TABTRACTCE, TABBLKGRPCE, TABBLK,
            EUID, RTYPE, GQTYPE, TEN, VACS, FINAL_POP, HHT, HHT2, NPF, CPLT, UPART, MULTG, HHLDRAGE, HHSPAN,
            HHRACE, PAOC, P18, P60, P65, P75]
    return "|".join([str(x) for x in line])


def unit_to_list(vect):
    """ vect should be a np array with dim =(9,), vect is the unit histogram eg vect[0] is the count of occupied housing units,
        vect[8] is the count of vacant housing units.
        returns: list of tuples (idx, val, vect) where vect[idx] = val and val > 0. 
    """
    tmp = []
    for idx, val in np.ndenumerate(vect):
        if int(val) != 0:
            tmp.append((idx, val, vect))
    return tmp


def expand_unit(group):
    """ group looks like (geocode, (idx, val, unit_vect)) where geocode and idx are singleton tuples
        returns: list of tuples (geocode, geo_id, rtype) """
    (blk,), ((idx,), val, unit) = group
    val = int(val)
    start_id = sum(unit[:idx])
    end_id = start_id + val
    return [(blk,) + (str(blk)+str(i),)+(idx,) for i in range(start_id, end_id)]
    

def to_micro_from_sparse(spar_invar):
    spar_obj, invar = spar_invar
    invar = invar[:-1]  # drop vacs.
    assert len(invar) == 8
    spar = spar_obj.sparse_array
    dim = spar_obj.shape
    hist = [(np.unravel_index(i, dim), int(spar[0, i])) for i in list(spar.indices)]
    microper = []
    for (i,), hhgq in np.ndenumerate(invar):
        if hhgq == 0:
            continue
        start_id = sum(invar[:i])
        end_id = start_id + hhgq
        subhist = [[rtype]*cnt for rtype, cnt in hist if rtype[0] == i]
        exphist = [rtype for sublist in subhist for rtype in sublist]
        assert len(exphist) >= hhgq, "not enough people for hhgq: {}".format(i)
        # if i == 0: assert len(exphist)/100 <= hhgq, "too many people in housing units"
        # if i > 0: assert len(exphist)/100000 <= hhgq, "too many people in hhgq {}".format(i)
        rounds = int(np.ceil(len(exphist) / hhgq))
        ids = [(euid, pnum) for pnum in range(rounds) for euid in range(start_id, end_id)]
        ids = ids[:len(exphist)]
        microper.extend(list(zip(exphist, ids)))
    return microper


def rekey_person(geo_rtype_ids):
    blk, (rtype, (tmp_id, epnum)) = geo_rtype_ids
    record = blk + (epnum,) + rtype
    tmp_id = str(blk[0]) + str(tmp_id)
    return (tmp_id, record)


def to_per_input(row):
    """ think row should be (geo_id, [(geo, epnum, hhgq, age, hisp, cenrace),(geo, global_id, hhgqvacs)]"""
    # unpack
    _, [per_tuple, unit_tuple] = row
    assert per_tuple[0] == unit_tuple[0]  # blk code should match
    assert int(per_tuple[2]) == int(unit_tuple[2]), "person had type: {}, unit had type: {}".format(per_tuple[2], unit_tuple[2])  # hhgq type should match
    geo, epnum, hhgq, age, hisp, cenrace = per_tuple
    _, global_id, _ = unit_tuple
    return (geo, global_id, epnum, hhgq, age, hisp, cenrace) 


def to_unit_input(row):
    global_id, [unit_tuple, final_pop] = row
    geocode, hhgqvacs = unit_tuple
    final_pop = final_pop if final_pop is not None else 0
    return (geocode, global_id, final_pop, hhgqvacs) 


class writer(AbstractDASWriter):
    """DAS Writer for the E2E test"""

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def write(self, input_tuple):
        nodes_dict, feas_dict = input_tuple
        config = self.config
        levels = tuple(config["geodict"]["geolevel_names"].split(","))
        block_nodes = nodes_dict[levels[0]]

        per_data = block_nodes.map(lambda node: ((node.geocode,), (node.syn, node.invar["gqhhvacs_vect"])))
        unit_data = block_nodes.map(lambda node: ((node.geocode,), node.invar["gqhhvacs_vect"]))

        # map flow
        # (geocode, unit_vect) -> (geocode, (rtype, cnt, unit_vector)) -> (geocode, geo_id, rtype) -> (('','',''), zip_id) -> (geo_id, (geocode, zip_id, rtype))
        #
        unit_data = unit_data.flatMapValues(unit_to_list).flatMap(expand_unit).zipWithIndex().map(lambda row: (row[0][1], (row[0][0], row[1], row[0][2]))).persist()

        # map flow
        # (geocode, person_array, unit_vect) -> () -> ()
        per_data = per_data.flatMapValues(to_micro_from_sparse).map(rekey_person)

        # join unit to person transfer over global_id, drop geo_id, keep geo and rtype, convert to MDF_Per
        per_data = per_data.join(unit_data).map(to_per_input).persist()

        per_output = per_data.map(to_per_line).persist()

        # need to compute final pop
        final_pop = per_data.map(lambda x: (x[1], 1)).reduceByKey(add)

        # row[1][1] is id, row[1][0] is geocode, row[1][2] is hhgqvacs
        unit_output = unit_data.map(lambda row: (row[1][1], (row[1][0], row[1][2]))).leftOuterJoin(final_pop).map(to_unit_input).map(to_unit_line).persist()

        unit_path = os.path.expandvars(self.getconfig(UNIT_PATH))
        per_path  = os.path.expandvars(self.getconfig(PER_PATH))

        # Note that we assume that the output files do not exist.
        # Recursive remove removed from here, because it is too dangerous.
        # It is now in run_cluster.sh

        log_testpoint("T02-001S")

        unit_output.saveAsTextFile(unit_path)
        per_output.saveAsTextFile(per_path)

        def dump_metadata( path, now, count):
            logging.info(f"writing to {path} {now} count={count}")
            f = ctools.s3.s3open(path, "w")
            cui = 'C' + 'U' + 'I' # because we scan for the string
            f.write("# {}//CENS - Title 13 protected data\n".format(cui))
            f.write("# Created: {}\n".format( now))
            f.write("# Records: {}\n".format( count))
            f.write("# Command line: {}\n".format( sys.executable + " ".join(sys.argv)))
            f.write("# uid: {}\n".format( os.getuid()))
            f.write("# username: {}\n".format( pwd.getpwuid(os.getuid())[0]))
            f.write("# Boot Time: {}\n".format( datetime.datetime.fromtimestamp( psutil.boot_time()).isoformat() ))
            f.write("# Start Time: {}\n".format( datetime.datetime.fromtimestamp( self.t0).isoformat() ))
            uname = os.uname()
            uname_fields = ['os_sysname','host','os_release','os_version','arch']
            for i in range(len(uname_fields)):
                f.write("# {}: {}\n".format(uname_fields[i], uname[i]))
            f.close()

        # Save the metadata for each
        now = datetime.datetime.now().isoformat()
        dump_metadata( unit_path+"/"+METADATA_SUFFIX, now, unit_output.count())
        dump_metadata( per_path+"/"+METADATA_SUFFIX, now, per_output.count())

        if feas_dict is not None:
            for key in feas_dict.keys():
                feas_dict[key] = feas_dict[key].value   # this seems redundant, but is actually needed for the accumulator
            logging.info("Feasibility dictionary: {}".format(feas_dict))
            # feas_path = path + "/feas_dict.json"
            # logging.debug("Saving feas_dict to directory: {}".format(feas_path))
            # das_utils.saveJSONFile(feas_path, feas_dict)

        log_testpoint("T02-003S")

        return block_nodes
