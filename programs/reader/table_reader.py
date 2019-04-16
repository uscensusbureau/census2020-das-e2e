# CEF reader
# William Sexton
# Last Modified: 4/24/17
# Documentation strengthened by Steve Clark on 5/22/18.

"""
    This is the reader module for the DAS-2018 instance of the DAS-framework.
    It contains a reader class that is a subclass of AbstractDASReader.
    The class must contain a method called read.
    It also defines an AbstractDASTable class for tracking metadata.
"""

import os
import os.path
import logging
from collections import defaultdict
from configparser import ConfigParser

import numpy as np
from pyspark.sql.types import StructType, StructField
from pyspark.sql.types import StringType, IntegerType

from das_framework.driver import AbstractDASReader
import programs.engine.cenquery as cenquery
import programs.engine.nodes as nodes
import programs.sparse as sparse

READER = "reader"
PATH = "path"
TABLES = "tables"
TABLE_CLASS = "class"
VARS = "variables"
RECODE_VARS = "recode_variables"
PRE_RECODER = "recoder"
LEGAL = "legal"
VAR_TYPE = "type"
HEADER = "header"
DELIMITER = "delimiter"
LINKAGE = "linkage"
CONSTRAINTS = "constraints"
PTABLE = "privacy_table"
CTABLES = "constraint_tables"
CONSTRAINTS = "constraints"
THE_CONSTRAINTS = "theConstraints"
INVARIANTS = "Invariants"
THEINVARIANTS = "theInvariants"
DATA = "data"
SUBSET = "subset"
MARGINS = "add_over_margins"
COMPUTE = "compute"
SIGN = "sign"

def config_reader_validate(config):
    """
        Description:
            This function will throw an error if the config file is missing any
            expected sections or options.

        Input:
            config: a ConfigParser object

        Output:
            Any RunTimeError messages

    """
    logging.info("validating config section {}".format(READER))
    if not config.has_option(READER, TABLES):
        logging.error("missing option {}".format(TABLES))
        raise RuntimeError("missing option {}".format(TABLES))

    for table in config[READER][TABLES].split(" "):
        for option in [PATH, TABLE_CLASS, VARS]:
            if not config.has_option(READER, "{}.{}".format(table, option)):
                logging.error("missing option {}.{}".format(table, option))
                raise RuntimeError("missing option {}.{}".format(table, option))
        for var_name in config[READER]["{}.{}".format(table, VARS)].split(" "):
            if not (config.has_option(READER, "{}.{}".format(var_name, LEGAL))\
                    and config.has_option(READER, "{}.{}".format(var_name, VAR_TYPE))):
                logging.error("missing variable {} specifications".format(var_name))
                raise RuntimeError("missing variable {} specifications".format(var_name))
        try:
            for var_name in config[READER]["{}.{}".format(table, RECODE_VARS)].split(" "):
                if not (config.has_option(READER, "{}.{}".format(var_name, LEGAL))
                        and config.has_option(READER, "{}.{}".format(var_name, VAR_TYPE))):
                    logging.error("missing variable {} specifications".format(var_name))
                    raise RuntimeError("missing variable {} specifications".format(var_name))
        except KeyError:
            logging.info("table {} has no recode variables specified".format(table))

class Interval(tuple):
    """
        Description:
            This class does the following:
                It creates a custom tuple for closed interval.
                Left and right endpoints included.
                Overloads contains the method.
                The tuple must be of length 2.
                Constructs a new Interval with the NewRange method.
    """
    @staticmethod
    def NewRange(left, right):
        """
            Description:
                This method creates an interval object.

            Inputs:
                left (and also right): They must be either an integer,
                                       a string representation of integer,
                                       or a single character as a string.
                Note: left and right must each be the same type.

            Output:
                Interval object
        """
        assert left <= right
        assert type(left) == type(right)
        return Interval((left, right))

    def __init__(self, tup):
        """
            This will assert the length of the tuple is exactly 2.
        """
        assert len(tup) == 2
        self.left, self.right = tup

    def __contains__(self, item):
        """
            This checks if something is in a specific interval.
        """
        # TODO: "1BC" in Interval(("000","255")) returns true but we might want false.
        # some legal values include a mix of alphanumeric characters so maybe leave it as is.
        try:
            tmp = str(item)
        except TypeError:
            return False
        try:
            length_check = len(self.left) <= len(item) <= len(self.right)
        except TypeError:
            length_check = True
        return True if (self.left <= item <= self.right) and length_check else False

    def __len__(self):
        """
            This returns the length of an interval.
        """
        try:
            return ord(self.right) - ord(self.left) + 1
        except TypeError:
            return int(self.right) - int(self.left) + 1

class LegalList(list):
    """
        This is a subclass of list. It is intended to hold only Interval objects.
    """
    def __contains__(self, item):
        for interval in self:
            if item in interval:
                return True
        return False

    def __len__(self):
        return sum([len(interval) for interval in self])

    def __min__(self):
        return min([interval.left for interval in self])

class TableVariable:
    """
        This is a Variable metadata holder.
        It stores information about each variable in the table, including:
        type, SQL type, legal values, and 2018 end-to-end specific values.
    """
    def __init__(self, name, vtype=None, legal=None):
        self.name = name
        self.vtype = vtype
        self.sql_type = self.get_sql_type()
        self.size = None
        self.end_to_end_value = None # value for 2018
        self.legal_values = legal

    def make_from_config(self, config):
        """
            This will set the attributes reading from the config file.
        """
        self.set_vtype(config["reader"]["{}.type".format(self.name)])
        self.set_legal_values(config["reader"]["{}.legal".format(self.name)])
        return self

    def __str__(self):
        return self.name

    def __repr__(self):
        """
            This will return the representation of a Variable.
        """
        return "TableVariable(name:{} vtype:{})".format(self.name, self.vtype)

    def set_vtype(self, var_type):
        """
            This defines the string representation of a variable type.

            Input:
                var_type: supports "str" or "int"
        """
        assert var_type in ["int", "str"]
        self.vtype = var_type
        self.sql_type = self.get_sql_type() # Keeps types in sync.

    def get_sql_type(self):
        """
            This returns the SparkSQL type of a variable.
        """
        return IntegerType() if self.vtype == "int" else\
               StringType() if self.vtype == "str" else None

    def set_legal_values(self, legal_string):
        """
            This sets the "legal values."

            Input:
                legal_string: a string of one of the following forms:
                    3
                    2,3,4
                    2-4
                    3-8,4,5,9-12,
                    etc
        """
        ranges = legal_string.strip().split(",")
        legal = []
        for split_string in ranges:
            endpoints = split_string.strip().split("-")
            left = endpoints[0].strip()
            right = endpoints[-1].strip()
            if self.sql_type == IntegerType():
                legal.append(Interval.NewRange(int(left), int(right)))
            else:
                legal.append(Interval.NewRange(left, right))
        self.legal_values = LegalList(legal)

    def set_end_to_end_value(self, value):
        """
            This sets the 2018 value of a variable.

            Input:
                value: a default value for the 2018 End-to-End (E2E) Test.
        """
        self.end_to_end_value = value

    def __eq__(self, other):
        return self.name == other.name

class AbstractTable:
    """
        This class is a support object for storing table layout information.
    """
    def __init__(self, name, config):
        self.name = name
        assert isinstance(config, ConfigParser)
        self.location = os.path.expandvars(config[READER]["{}.{}".format(self.name, PATH)])
        self.variables = [TableVariable(var_name).make_from_config(config)
                          for var_name in config[READER]["{}.{}".format(self.name, VARS)].split(" ")]
        try:
            self.recode_variables = [TableVariable(var_name).make_from_config(config)
                                     for var_name in config[READER]
                                     ["{}.{}".format(self.name, RECODE_VARS)]
                                     .split(" ")]
        except KeyError:
            self.recode_variables = None
        self.csv_file_format = {"schema":self.set_schema(),
                                "header":config[READER][HEADER].lower() == "true",
                                "sep":config[READER][DELIMITER]}

    def add_variable(self, new_var):
        """
            This adds/stores information for a new variable to a self object.

            Input:
                new_var: a TableVariable object
        """
        assert type(new_var) == TableVariable
        if new_var in self.variables:
            raise RuntimeError("TableVariable {} already in table".format(new_var))
        self.variables.append(new_var)
        self.csv_file_format["schema"] = self.set_schema()

    def get_variable(self, name):
        """
            This will get the name of a variable, if possible.

            Input:
                name: the name of variable to return

            Output: the variable with name "name" or not if no such variable
        """
        for var in self.variables:
            if var.name == name:
                return var
        return None

    def set_schema(self):
        """
            This will return the SQL schema/structure type for a self object.
        """
        return StructType([StructField(v.name, v.sql_type) for v in self.variables])

    def load(self, spark):
        """
            This loads the records from a csv into a Spark dataframe.

            Input:
                spark: the SparkSession object

            Output: the Spark dataframe object
        """
        return spark.read.csv(self.location, **self.csv_file_format)

    def recode_meta_update(self):
        """
            This adds the recode variables to the list of table variables.
        """
        try:
            for v in self.recode_variables:
                self.add_variable(v)
        except TypeError:
            pass

    def pre_recode(self, data, config):
        """
            This applies predisclosure avoidance recodes.

            Inputs:
                data: a Spark dataframe (df)
                config: a ConfigParser where the recodes are specified

            Output:
                a Spark dataframe (df) with the recode columns added
        """
        try:
            (recoder_file, recoder_class_name) = config[READER]["{}.{}".format(self.name, PRE_RECODER)].rsplit(".", 1)
        except KeyError:
            return data
        try:
            recoder_module = __import__(recoder_file, fromlist=[recoder_class_name])
        except ImportError as e:
            print("Module import failde.")
            print("current directory: {}".format(os.getcwd()))
            print("__file__: {}".format(__file__))
            raise e
        args = [config[READER][var.name].split(" ") for var in self.recode_variables]
        recoder = getattr(recoder_module, recoder_class_name)(*args)
        return data.rdd.map(recoder.recode).toDF()
        
    # def __eq__(self, other):
    #    #TODO: should check variables match also but this works for now.
    #    # including __eq__ means I also need a __hash__ method. will work on this later.
    #    return self.name == other.name

    def process(self, data_structure):
        """
            This function processes raw input, which must be implemented in child classes.

            Inputs:
                data_structure: a data object (eg: spark, df, or ordd)

            Output:
                defaults to the identity function
        """
        return data_structure

    def verify(self, data):
        """
            This is a quick pass over the data to ensure that all values are as expected.

            Input:
                data - spark df
            Output:
                Either:
                (a) "True" if data is valid,
                otherwise,
                (b) an assertion error.
        """
        def check_row(row):
            """
                Input:
                    row: the row of the Spark dataframe

                Output:
                    Either:
                    (a) "True" if row is valid,
                    otherwise,
                    (b) an assertion error.
            """
            for var in self.variables:
                assert var.name in row, "{} is not in row: {}".format(var.name, row)
                assert row[var.name] in var.legal_values, "{} is not a legal value for {}, the bad row was {}".format(row[var.name], var.name, row)
            return True
        return data.rdd.filter(lambda row: check_row(row) == 0).isEmpty()

class reader(AbstractDASReader):
    """
        The CEF reader object loads microdata and metadata into tables
        and then converts them into a usable form.
    """
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        assert self.config
        config_reader_validate(self.config)
        logging.info("building table infrastructure")
        logging.debug("table names %s", self.getconfig(TABLES))
        logging.debug("Reading table module and class names from config")
        table_names = self.getconfig(TABLES).split(" ")
        tmp = []
        for name in table_names:
            (table_file, table_class_name) = \
             self.config[READER]["{}.{}".format(name, TABLE_CLASS)].rsplit(".", 1)
            try:
                table_module = __import__(table_file, fromlist=[table_class_name])
            except ImportError:
                print("Module import failed.")
                print("current directory: {}".format(os.getcwd()))
                print("__file__: {}".format(__file__))
                raise ImportError
            tmp.append(getattr(table_module, table_class_name)(name, self.config))
        self.tables = tmp

    def read(self):
        """
            This function performs the following steps:

            (1) Load input file into spark dataframe.
            (2) Convert dataframe into a rdd of ndarrays
                (one ndarray for each block).
        """

        logging.info("loading the data")
        tmp = defaultdict()
        for table in self.tables:
            logging.info("loading table {}".format(table.name))
            table_df = table.load(self.setup)
            #if not table.verify(table_df):
            #    print("table contains invalid records")
            #    raise RuntimeError
            logging.info("recode meta")
            table.recode_meta_update()
            table.set_shape()
            logging.info("recodes")
            table_df = table.pre_recode(table_df, self.config)
            logging.info("dict")
            tmp[table.name] = (table, table.process(table_df))
        logging.info("processing the input")
        tables = tmp.keys
        table_df_dict = tmp
        
        privacy_table_name = self.getconfig(PTABLE).strip()
        constraint_table_name = self.getconfig(CTABLES).strip()
        privacy_table = table_df_dict[privacy_table_name][0]
        constraint_table = table_df_dict[constraint_table_name][0]
        
        join_data = table_df_dict[privacy_table_name][1].rightOuterJoin(table_df_dict[constraint_table_name][1]).persist()
        config=self.config
    
        #make block nodes
        dim = table_df_dict[privacy_table_name][0].data_shape
        block_nodes = join_data.map(lambda node: (make_block_node(config, node, dim))).persist()
        join_data.unpersist()
        
        return block_nodes

def make_block_node(config, person_unit_arrays, dim):
    """
        This function makes block nodes from person unit arrays for a given geocode.

        Inputs:
            config: a configuration object
            person_unit_arrays: a RDD of (geocode, arrays), where arrays are the tables defined in the config

        Output:
            block_node: a nodes.geounitNode object for the given geocode
    """

    #import invariants_module
    (file,invariants_class_name) = config[CONSTRAINTS][INVARIANTS].rsplit(".", 1)
    invariants_module = __import__(file,   fromlist=[invariants_class_name])

    #import constraints_module
    (file,class_name) = config[CONSTRAINTS][CONSTRAINTS].rsplit(".", 1)
    constraints_module = __import__(file,   fromlist=[class_name])

    # Get the names of tables in person_unit_arrays.
    data_names = [config[READER][PTABLE]] + config[READER][CTABLES].split(",")
    geocode, arrays = person_unit_arrays

    data_dict = {}
    for i in range(len(arrays)):
        data_dict[data_names[i]] = arrays[i].astype(int) if arrays[i] is not None else np.zeros(dim).astype(int)

    # geocode is a tuple where the [1] entry is empty. We only want the [0] entry.
    geocode = geocode[0]
    logging.info("creating geocode: %s" % geocode)

    # Make Invariants
    invar_names = tuple(config[CONSTRAINTS][THEINVARIANTS].split(","))

    invariants_dict= getattr(invariants_module,        invariants_class_name)(data_dict = data_dict, 
                                 invariant_names = invar_names).calculateInvariants().invariants_dict

    #invariants = {}
    #for name in invar_names:
        #dataset_name = config[CONSTRAINTS]["{}.{}".format(name, DATA)]
        #subset = eval(config[CONSTRAINTS]["{}.{}".format(name, SUBSET)])
        #if config[CONSTRAINTS]["{}.{}".format(name, MARGINS)] == "None":
            #add_over_margins = None
        #else:
            #add_over_margins = tuple(
                #int(x) for x in config[CONSTRAINTS]["{}.{}".format(name, MARGINS)].split(","))
        #query = cenquery.Query(array_dims=data_dict[dataset_name].shape,
                               #subset=subset, add_over_margins=add_over_margins)
        #invariants[name] = np.array(query.answer(data_dict[dataset_name])).astype(int)


    # Make Constraints
    privacy_table_name = config[READER][PTABLE]
    cons_names = tuple(config[CONSTRAINTS]["theConstraints"].split(","))
    hist_shape = data_dict[privacy_table_name].shape
    constraints_dict= getattr(constraints_module,        class_name)(hist_shape=hist_shape,
                            invariants=invariants_dict,
                            constraint_names=cons_names).calculateConstraints().constraints_dict

    raw=data_dict[privacy_table_name].astype(int)

    block_node = nodes.geounitNode(geocode=geocode, config=config, raw=sparse.multiSparse(raw),
                                   cons=constraints_dict, invar=invariants_dict)

    return block_node
