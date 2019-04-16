# -*- coding: utf-8 -*-

import logging
import numpy as np
import os
import sys
sys.path.append(os.path.dirname(__file__))



import driver
import experiment

import noisealgorithms
import singleton_scaffold

# Bring in Spark if we can

try:
    from pyspark.sql import Row
    from pyspark.sql import SparkSession
    import pyspark.storagelevel
    from spark_sql_aggregator import *
except ImportError:
    pass


def map_ll(func, list_of_lists):
    """
    Recursive (two-level) mapping for list of lists. Applies func to every element, keeping the list structure
    :param func: function to apply
    :param list_of_lists: list of lists
    :return: list of lists with function func applied to each element
    """
    return list(map(lambda sublist: list(map(func, sublist)), list_of_lists))


def apply_noise_to_partition_iter(noisifiers, index, iter):
    """
    Performs the noise algorithm on an iterator of cells in the table, residing in a Spark partition
    (by "cell" here we mean the combination of categorical variables, but since there may be more than one measure
     variable, and things like smooth sensitivity or maximal and second-to-maximal values for top-coding, defined for
     the same cell, it is actually a row in the dataset / DataFrame / RDD)

    Applies to noise to all the measure-variables-to-be-noisified so that we go over each row only once.

    :param noisifiers: List of NoiseAlgorithm objects to be applied.
    :param iter: iterator of the rows of true table
    :param index: partition index (so that the random state of that partition could be set / saved / loaded)
    :return: the row of the noisy table
    """
    np.random.seed()
    retit = []
    for row in iter:
        data = row.asDict()
        rand = {nalg.varname: np.random.random() for nalg in noisifiers}
        for nalg in noisifiers:
            #if nalg.varname in data:
            data[nalg.varname] = nalg.noisify([row, rand]) if nalg.privacy_guarantee else float('NaN')
        retit.append(Row(*data.keys())(*data.values()))
    return retit


def apply_noise_to_cell(noisifiers, row):
    """
    Performs the noise algorithm on a cell in the table
    (by "cell" here we mean the combination of categorical variables, but since there may be more than one measure
     variable, and things like smooth sensitivity or maximal and second-to-maximal values for top-coding, defined for
     the same cell, it is actually a row in the dataset / DataFrame / RDD)

    Applies to noise to all the measure-variables-to-be-noisified so that we go over each row only once.

    :param noisifiers: List of NoiseAlgorithm objects to be applied.
    :param row: the row of true table joined with row of randomness.
      The way we add unirandRDD to the data, row[0] has the data and row[1] has randomness
    :return: the row of the noisy table
    """
    data = row[0].asDict()
    for nalg in noisifiers:
        data[nalg.varname] = nalg.noisify(row) if nalg.privacy_guarantee else float('NaN')
    return Row(*data.keys())(*data.values())
    # return Row(**data) # This re-orders keys in alphabetic order


class engine(driver.AbstractDASEngine):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)

        # This is to change config file, like, add SQL queries producing tables. Should be implemented in
        # subclasses

        self.elaborate_config()

        # Register all the UDF used in SQL queries (run by aggregator). Should be implemented in subclasses
        self.setup_udf()


        # Create dictionary of noise algorithm lists. Key is table name. Value is list of variable sublists.
        # Variables within each sublist are composable/uncorrelated. Each element is NoiseAlgorithm.
        # E.g.
        # {'everything':
        #   [
        #       [
        #           SmoothLaplaceAlgorithm: epsilon=0.6,{'varname': 'empcy', 'alpha': 0.2, 'algorithm': 'SmoothLaplace', 'delta': 0.05},
        #           SmoothLaplaceAlgorithm: epsilon=0.6,{'varname': 'emppy', 'alpha': 0.2, 'algorithm': 'SmoothLaplace', 'delta': 0.05}
        #       ],
        #       [
        #           LogLaplaceAlgorithm: epsilon=0.4,{'varname': 'estabs', 'algorithm': 'LogLaplace', 'alpha': 0.05}
        #       ]
        #    ]}
        # where [SmoothLaplaceAlgorithm: epsilon=0.6,{'varname': 'empcy'...}, SmoothLaplaceAlgorithm: epsilon=0.6,{'varname': 'emppy'...]
        # are applied to composable variables (both get the same epsilon without adding) and
        # [LogLaplaceAlgorithm: epsilon=0.4,{'varname': 'estabs'...] is another group of composable variables (just 1 variable here),
        # with epsilon being added to that of the first group. Here the epsilons are [[0.6,0.6],[0.4]] adding up to 1.0.

        self.noisifiers = {tname: map_ll(lambda var: self.create_noise_algorithm_by_name(tname, var),
                                         map(lambda s: s.split(','), svarlist.split(";"))) for tname, svarlist in
                           self.getconfitems("variables2addnoise")}
       

        # Whether any differentially private algorithms are used, so that output of epsilons is required

        self.DP = all([isinstance(nalg, noisealgorithms.DPNoiseAlgorithm) for nalg in self.flat_noisifiers()])
        self.all_privacy_guaranteed = True

        if not self.DP:
            logging.warning("Not all algorithms used are Differentially private. No effective epsilon calculated")
            return

        for tname, compgroups in self.get_epsilons().items():
            # Check that epsilon is the same within each composable group
            if list(map(len, map(set, compgroups))) != [1] * len(compgroups):
                logging.warning(
                    "WARNING: Unequal epsilons within a composable/uncorrelated group of variables in table \"{}\"! Choosing maximal one.".format(
                        tname))

        # Setting epsilons in groups equal to the maximal one
        for tname, compgroups in self.noisifiers.items():
            for compgroup in compgroups:
                max_epsilon = max(map(lambda nalg: nalg.epsilon, compgroup))
                for nalg in compgroup:
                    nalg.epsilon = max_epsilon

        # Epsilon for a table is the sum of epsilons of each composable group (which is a maximal of epsilons within)
        self.table_epsilons = {tname: sum(map(max, compgroups)) for tname, compgroups in self.get_epsilons().items()}

        # Total epsilon is the sum of all table epsilons
        self.epsilon_effective = sum(self.table_epsilons.values())

        # Check that the parameters comply with noisifiers privacy guarantees
        self.all_privacy_guaranteed = all(map(lambda nalg: nalg.willRun(), self.flat_noisifiers()))

    def willRun(self):

        # Check that all tables are directed to pass through some noisifier
        for tablename in get_set_of_first_items(self.getconfitems("prenoise_tables")):
            if tablename not in get_set_of_first_items(self.getconfitems("variables2addnoise")):
                logging.warning("WARNING: Table {} passes through with no noise applied!".format(tablename))
                # return False

        # Check that effective epsilon is equal to the master epsilon set in the [engine] section
        if self.DP:
            if abs(self.getfloat('epsilon', section='engine') - self.epsilon_effective) > 1e-5:
                logging.warning(
                    "WARNING: Effective epsilon {} is not equal to the desired epsilon {} set in config!".format(
                        self.epsilon_effective, self.getfloat('epsilon', section='engine')))

        # if not self.all_privacy_guaranteed:
        #     return False

        return True

    def run(self, original_data):
        """
        Main engine running routine: Create the table views, aggregators, call aggregators and noise algorithms

        Args: original data (dictionary, with a Spark DataFrame under "original_data" key)
        Returns: true tables, noisy tables (dictionary of Pandas dataframes)
        """

        # Create sql table view for the input table to queries
        # input_table_name = self.getconfig('input_table_name', section='engine', default='bdsgc')
        self.config['engine']['input_table_name'] = ",".join(original_data["original_data"].keys())

        for input_table_name, input_table in original_data["original_data"].items():
            input_table.createOrReplaceTempView(input_table_name)

        # Scaffold is a Singleton that keeps data from previous runs if any (in "experiment" regime)
        scaffold = singleton_scaffold.Scaffold()

        # Create and run the pre-noise aggregator or get the one kept in the scaffold
        if 'prenoise_agg' in scaffold.val:
            prenoise_agg = scaffold.val['prenoise_agg']
        else:
            logging.info("Aggregating the data before noise")
            prenoise_agg = PreNoiseAggregator(config=self.config).run_queries().drop_aux_prenoise_tables()
            prenoise_agg.persist_tables()
            scaffold.val['prenoise_agg'] = prenoise_agg

        # Create and run the after-noise aggregator for true tables, run pandas conversion and modifications
        # (or get from the scaffold)
        if 'true_agg' in scaffold.val:
            true_agg = scaffold.val['true_agg']
        else:
            logging.info("Aggregating the true tables")
            true_agg = TrueAggregator(config=self.config).run_queries().make_pandas_out_tables()
            scaffold.val['true_agg'] = true_agg

        # Seeding of the numpy random generator for reproducible results. Should be done in the setup module
        # Uncomment and test if for some reason (e.g. to not depend on setup module) desired to move here
        #
        # if 'seeded' not in scaffold.val or not scaffold.val['seeded'] and self.getconfig("seed", default="urandom", section="engine") != "urandom":
        #     try:
        #         seed = self.getint("seed", section="engine")
        #         logging.info("Seeding with seed={}".format(seed))
        #     except ValueError:
        #         seed = 100
        #         logging.warning("Can't read random seed, setting the default seed={}".format(seed))
        #     np.random.seed(seed)
        #     scaffold.val['seeded'] = True

        logging.info("Applying noise")

        if self.DP:
            logging.info(self.print_algorithms_epsilons())

        # If there are disclosure avoidance queries not involving noise, apply them first (instead of pre-agg)
        tables2noisify = NoNoiseDAAggregator(config=self.config)\
            .run_queries().drop_aux_prenoise_tables()\
            .tables \
            if self.config.has_section("NoNoiseDA") \
            else prenoise_agg.tables

        # If there are no prenoise queries, just use the input tables
        if len(tables2noisify) < 1:
            tables2noisify = original_data["original_data"] #{input_table_name: original_data["original_data"]}


        # Add noise using RDDs
        noisy_tables = self.apply_noise(tables2noisify)  # if self.all_privacy_guaranteed else tables2noisify

        # Convert RDDs back to DataFrames forcing the original schema
        # and create views for the noisy tables, with the same name with "noisy_" added
        # for tname, table in noisy_tables.items():
        #     # schema = tables2noisify[tname].schema
        #     # noisy_tables[tname] = spark.createDataFrame(table,schema)
        #     # noisy_tables[tname] = table.toDF()  # if self.all_privacy_guaranteed else table
        #     noisy_tables[tname].createOrReplaceTempView('noisy_{}'.format(tname))
        #     noisy_tables[tname].persist(pyspark.storagelevel.StorageLevel.MEMORY_ONLY)
        #     noisy_tables[tname].count()

        # Create and run the after-noise aggregator for noisy tables, run pandas conversion and modifications
        logging.info("Aggregating the noisy tables")
        noisy_agg = NoisyAggregator(config=self.config).run_queries().make_pandas_out_tables()

        spark =  SparkSession.builder.appName('BDS-DAS Spark Engine Apply Noise').getOrCreate()
        for tname,table in noisy_tables.items():
            table.unpersist()
            spark.catalog.uncacheTable('noisy_{}'.format(tname))
            spark.catalog.dropTempView('noisy_{}'.format(tname))


        # # This can be used to save time and not go over the tables if parameters for a privacy algorithm are wrong
        # if not self.all_privacy_guaranteed:
        #     noisy_agg = noisy_agg.set_pandas_tables_to_NaN()

        original_data["true_tables"] = true_agg.tables
        original_data["noisy_tables"] = noisy_agg.tables

        original_data["params"] = {tname: map_ll(lambda nalg: nalg.get_params(), nalgs) for tname, nalgs in
                                   self.noisifiers.items()}

        original_data["expvars"] = {}
        if self.getboolean("run_experiment_flag", section="experiment", default=False):
            try:
                original_data["expvars"] = {loop[1]: self.getfloat(loop[1], section=loop[0]) for loop in
                                            experiment.build_loops(self.config)}
            except KeyError:
                logging.warning("[experiment] section not found, not experimental variable values output")


        if self.DP:
            original_data["epsilon"] = self.epsilon_effective

            # The next line is to have epsilon_effective in the file name rather then the one set in the experimental loop
            # Will be reconciled later, when we figure out the most intuitive logic of treating several independent
            # epsilon budgets in the config file (TODO)

            # original_data["expvars"]["epsilon"] = self.epsilon_effective

            original_data["table_epsilons"] = self.table_epsilons

        return noisy_agg.tables, original_data

    def apply_noise(self, tables):

        spark = SparkSession.builder.appName('BDS-DAS Spark Engine Apply Noise').getOrCreate()

        noisy_tables = {}

        for (tname, table) in tables.items():

            if self.getconfig("seed", default="urandom", section="engine") == "urandom":
                # Generate random numbers on the go, by the executors within the mapping function. Each
                # partition / executor will seed from /dev/urandom or clock (this is faster)

                noisy_table = table.rdd.mapPartitionsWithIndex(
                    lambda index, iter: apply_noise_to_partition_iter(self.flat_noisifiers_table(tname), index, iter)
                )

            else:
                # If we want reproducible results, generate all the random numbers for the current table in the driver,
                # and join this RDD to the current table (this is slower, since RDD needs to be joined across partitions)

                random_numbers_4_table = np.random.random((table.count(), self.get_number_of_unirand_for_table(tname)))

                random_df = spark.sparkContext\
                    .parallelize(random_numbers_4_table, table.rdd.getNumPartitions()) \
                    .zipWithIndex() \
                    .map(lambda d: (d[1], Row(**{nalg.varname: float(d[0][i]) for i, nalg in enumerate(self.flat_noisifiers_table(tname))})))

                noisy_table = table.rdd\
                    .zipWithIndex().map(lambda d: (d[1], d[0])) \
                    .join(random_df)\
                    .map(lambda d: apply_noise_to_cell(self.flat_noisifiers_table(tname), d[1]))

            # Convert RDD back to DataFrame forcing the original schema
            # and create view for the noisy table, with the same name with "noisy_" added

            # noisy_tables[tname] = spark.createDataFrame(noisy_table,table.schema)
            noisy_tables[tname] = noisy_table.toDF()  # if self.all_privacy_guaranteed else table
            noisy_tables[tname].createOrReplaceTempView('noisy_{}'.format(tname))
            noisy_tables[tname].persist(pyspark.storagelevel.StorageLevel.MEMORY_ONLY)
            noisy_tables[tname].count()

        return noisy_tables

    def create_noise_algorithm_by_name(self, tname, varname):
        """
        Given the table name and the variable name, create the appropriate NoiseAlgorithm object, reading the relevant
        information form the config file

        :param tname: Table name
        :param varname: Variable name
        :return: NoiseAlgorithm object
        """
        var = varname.strip()
        section = '{}_{}'.format(tname, var)
        return getattr(sys.modules['noisealgorithms'], self.getconfig('algorithm', section=section) + "Algorithm")(
            config=self.config, section=section, varname=var)

    def get_epsilons(self):
        """
        :return: Dictionary of epsilons for each table as {"TableName": [list of epsilons]}
        """
        return {tname: map_ll(lambda nalg: nalg.epsilon, compgroups) for tname, compgroups in self.noisifiers.items()}

    def print_algorithms_epsilons(self):
        """
        Form a string for output / log file stating all the noise algorithms+epsilons to be used on which variables
        :return:
        """
        s = ''
        for tname, compgroups in self.noisifiers.items():
            s = s + "Table \"{}\": ".format(tname)
            for compgroup in compgroups:
                s = s + ', '.join(
                    ["{} by {} with epsilon = {}".format(nalg.varname, nalg.get_params(), nalg.epsilon) for nalg
                        in compgroup])
            s = s + "\n"
        return s

    def flat_noisifiers_table(self, tname):
        """
        :param tname: Table name
        :return: List of NoiseAlgorithm objects to be used on the table
        """
        return [nalg for compgroup in self.noisifiers[tname] for nalg in compgroup]

    def flat_noisifiers(self):
        """
        :return: List of all NoiseAlgorithm objects to be used in the current run
        """
        return [nalg for tname, compgroups in self.noisifiers.items() for compgroup in compgroups for nalg in compgroup]

    def get_number_of_unirand_for_table(self, tname):
        """
        How many independent uniformly distributed random numbers needed per one row of a table.
        For all the current implementations, it is 1 (or zero, for non-DP algorithms) per algorithm, so the total is
        equal to the number of variables-to-be-noisified in the table
        """
        return sum(map(lambda nalg: nalg.unirandnum, self.flat_noisifiers_table(tname)))

    def elaborate_config(self):
        pass

    def setup_udf(self):
        pass
