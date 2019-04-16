# -*- coding: utf-8 -*-


import logging
import numpy as np
import os
import sys

sys.path.append(os.path.dirname(__file__))

import driver

try:
    from pyspark.sql import SparkSession
    from pyspark.sql.utils import *
    import pyspark.storagelevel
except ImportError:
    pass

def get_set_of_first_items(iter_of_tuples):
    return set([first for first, second in iter_of_tuples])


class aggregator(driver.AbstractDASModule):
    def __init__(self, **kwargs):
        """ Just to define self.tables, self.configsection and self.addnoisy2name, for propriety """
        super().__init__(**kwargs)
        self.tables = {}
        self.configsection = 'SET_NAME_OF_SECTION_IN_THE_SUBCLASS'
        self.addnoisy2name = False

    def run_queries(self):
        """ Runs SQL queries performing the aggregations"""
        spark = SparkSession.builder.appName('BDS-DAS Spark ' + self.name).getOrCreate()

        # logging.debug(spark.sparkContext._conf.getAll())

        # Go over the corresponding (prenoise or output) queries in config
        for (key, query) in self.getconfitems(self.configsection):

            logging.info("Aggregating table \"{}\"".format(key))

            # If aggregating noisy tables, add "noisy_" to their SQL-names (SQL-views) within the query
            if self.addnoisy2name:
                for tname in self.config['prenoise_tables']:
                    query = query.replace(" "+tname, " noisy_{}".format(tname))


                # If aggregating straight from input table, add prefix to the input table itself
                #input_table_name = self.getconfig('input_table_name', section='engine')
                for input_table_name in self.getiter('input_table_name', section='engine'):
                    query = query.replace(" "+input_table_name, " noisy_{}".format(input_table_name))

            # Run the query, add the resulting table to the dict, create new SQL-view for it
            #print("\n".join(spark.sql("SHOW TABLES").rdd.map(lambda r: r.asDict()['tableName']).collect())+"\n")

            #logging.info("With query {}".format(query))

            try:
                t = spark.sql(query)
                t.count()
            except (AnalysisException, ParseException, StreamingQueryException, QueryExecutionException, IllegalArgumentException) as err:
                #err = sys.exc_info()[0]
                error_msg = "Query \"\n {}\n\" failed, Spark SQL returned\n {}".format(query, str(err.args[0]))
                logging.error(error_msg)
                raise err

            self.tables[key] = t
            t.createOrReplaceTempView(key)

        return self

    def persist_tables(self):
        for table in self.tables.values():
            table.persist(pyspark.storagelevel.StorageLevel.MEMORY_ONLY)
            table.count()

    def drop_aux_prenoise_tables(self):
        self.tables = {tname: table for tname, table in self.tables.items() if
                       tname in get_set_of_first_items(self.getconfitems("variables2addnoise"))}
        return self

    def make_pandas_out_tables(self):
        """ Converts the tables to Pandas from Spark, adds other variables if needed, drops unneeded variables """
        if self.getboolean("pandas_output",section="engine",default=True):
            self.convert2pandas().add_vars_and_aggregate_pandas().drop_vars_pandas()
        return self

    def convert2pandas(self):
        """ Converts the tables to Pandas from Spark """
        spark = SparkSession.builder.appName('BDS-DAS Spark Engine Apply Noise').getOrCreate()
        logging.info("Converting tables to Pandas")
        pandas_tables = {}
        for tname, table in self.tables.items():
            logging.info("Converting table {} to Pandas".format(tname))
            pandas_tables[tname] = table.toPandas()

        for tname, table in self.tables.items():
            table.unpersist()
            spark.catalog.uncacheTable(tname)
            spark.catalog.dropTempView(tname)

        self.tables = pandas_tables
        return self

    def drop_vars_pandas(self):
        """ Removes variables listed in config file from the Pandas dataframes """
        for tname, svarlist in self.getconfitems('out_tables_drop_vars_pandas'):
            # Filter for variables that exist in the dataframe
            vars2drop = filter(lambda s: s in self.tables[tname], map(lambda s: s.strip(), svarlist.split(',')))
            self.tables[tname] = self.tables[tname].drop(vars2drop, axis=len(self.tables[tname].shape)-1)
        return self

    def add_vars_and_aggregate_pandas(self):
        """ Calculates new variables listed in config file from the Pandas dataframes, defined by functions of the same names """

        # Engine module name
        eng_mod_name = self.getconfig("engine", section="engine").rsplit(".", 1)[0]
        for tname, svarlist in self.getconfitems('out_tables_add_vars_pandas'):

            # Pre-aggregation and post-aggregation vars are split by semicolon
            pre_post_agg = svarlist.split(";")
            pre_agg_add = pre_post_agg[0]

            # Add vars before aggregation
            for var in map(lambda s: s.strip(), pre_agg_add.split(',')):

                self.tables[tname] = getattr(sys.modules[eng_mod_name], var)(self.tables[tname])

            # Aggregate
            aggstring = self.getconfig(tname, section="out_tables_aggregate_pandas", default="").split(";")

            if len(aggstring)>1:

                # How to aggregate: sum, mean etc.
                agg_op = aggstring[1].strip()

                # Which vars are aggregated out
                aggregate_over_vars = set(map(lambda s: s.strip(),aggstring[0].split(',')))

                vars_in_table = set(self.tables[tname].columns)
                cell_id_vars = set(self.getiter("cell_id_vars", section="validator"))

                # Table indices (that remain)
                indices = list(vars_in_table.intersection(cell_id_vars) - aggregate_over_vars)

                if len(aggregate_over_vars) > 0:

                    # Remove None/Nan/NA indices, replacing them with 'NA'
                    for ind in indices:
                        self.tables[tname].loc[self.tables[tname][ind].isnull(), ind] = 'NA'

                    # Perform aggregation
                    group_by_vars = indices if len(indices)>0 else lambda d: True # To allow for the "Total" table
                    grouped = self.tables[tname].groupby(group_by_vars, as_index=False)
                    self.tables[tname] = getattr(grouped,agg_op)()

            # Add vars after aggregation (mainly "rates")
            if len(pre_post_agg) > 1:
                post_agg_add = pre_post_agg[1]
                for var in map(lambda s: s.strip(), post_agg_add.split(',')):
                    self.tables[tname] = getattr(sys.modules[eng_mod_name], var)(self.tables[tname])

        return self

    def set_pandas_tables_to_NaN(self):
        """ Set all the cell values / measure variables to NaN """
        cell_id_vars = tuple(self.getiter("cell_id_vars", section="validator"))

        for tname, table in self.tables.items():
            measure_cols = tuple(filter(lambda k: k not in cell_id_vars, list(table)))
            table[measure_cols] = np.NaN

        return self


class PreNoiseAggregator(aggregator):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.configsection = 'prenoise_tables'
        self.addnoisy2name = False
        self.name = "Pre-Noise Aggregator"


class NoisyAggregator(aggregator):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.configsection = 'out_tables'
        self.addnoisy2name = True
        self.name = "Post-Noise Aggregator"


class TrueAggregator(aggregator):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.configsection = 'out_tables'
        self.addnoisy2name = False
        self.name = "True Aggregator"


class NoNoiseDAAggregator(aggregator):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.configsection = 'NoNoiseDA'
        self.addnoisy2name = False
        self.name = "No Noise Disclosure Avoidance Aggregator"
