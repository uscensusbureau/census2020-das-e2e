# Table.py
# William Sexton
# Last Modified: 9/10/18
"""
    This is the table module for the DAS-2018 instance of the DAS-framework.
    It contains a table classes that inherits from the AbstractDASReader.
    The classes must contain a method called process.
"""

from operator import add
import numpy as np
from programs.reader.table_reader import AbstractTable

READER = "reader"
GEOGRAPHY = "geography"
HISTOGRAM = "histogram"
UNIQUE = "unique"
COUNT = "count"


class PersonTable(AbstractTable):
    """
        This class stores the microdata and metadata for the CEF person records.
    """
    def __init__(self, *args):
        super().__init__(*args)
        _, config = args
        self.geography_variables = config[READER]["{}.{}".format(self.name, GEOGRAPHY)].split(" ")
        self.histogram_variables = config[READER]["{}.{}".format(self.name, HISTOGRAM)].split(" ")
        self.data_shape = None

    def set_shape(self):
        """ Call after recodes to ensure vars have all been added to table """
        self.data_shape = tuple([len(self.get_variable(var).legal_values) for var in self.histogram_variables])


    def process(self, data):
        """
            args:
                a Spark dataframe containing CEF person records

            This function performs the following process:
                (1) Convert the data to a RDD.
                (2) row -> (geo_histogram,1)) (Map the row to a geo_histogram, a tuple of the
                    geography variables plus the histogram variables.)
                (3) Reduce by key.
                (4) (geo_histogram,cnt) -> (geo, (histogram, cnt))
                (5) groupbykey: creates (geo, list of (histogram, cnt))
                (6) (geo, list of (histogram, cnt)) -> (geo, ndarray)

            returns: an rdd of (geo,numpy ndarray) pairs.
        """
        def create_key_value_pair(row):
            """
                This creates a key value pair for geography and histogram variables.
                This function needs to check the validity of the row record.

                Input:
                    a row of data from the CEF

                Output:
                    (geo_histogram_idx, 1), where geo_histogram is a tuple
                        of the geography variable values and
                        the histogram variable values.
            """
            blk_idx = (tuple([row[var] for var in self.geography_variables])
                       + tuple([int(row[var]) for var in self.histogram_variables]))

            return (blk_idx, 1)

        def to_ndarray(prehist_list):
            """
                Input:
                    list of (idx, cnt) pairs, where idx consists of (qrel, qage, qsex, cenrace)

                This function performs the following process:
                    (1) Initialize the ndarray.
                    (2) Iterate through the list assigning cnt to
                        idx in ndarray except for error idx.

                Output:
                    a ndarray, which is the histogram of detail counts
            """
            hist = np.zeros(self.data_shape)
            for idx, cnt in prehist_list:
                hist[idx] = cnt
            return hist

        return (data.rdd.map(create_key_value_pair)
                        .reduceByKey(add)
                        .map(lambda key_value:
                             (key_value[0][:len(self.geography_variables)],
                              (key_value[0][len(self.geography_variables):],
                               key_value[1])))
                        .groupByKey()
                        .mapValues(to_ndarray))

class UnitTable(AbstractTable):
    def __init__(self, *args):
        super().__init__(*args)
        _, config = args
        self.geography_variables = config[READER]["{}.{}".format(self.name, GEOGRAPHY)].split(" ")
        self.histogram_variables = config[READER]["{}.{}".format(self.name, HISTOGRAM)].split(" ")
        self.uniqueID = config[READER]["{}.{}".format(self.name, UNIQUE)].split(" ")
        self.data_shape = None

    def set_shape(self):
        """ Call after recodes to ensure vars have all been added to table """
        self.data_shape = tuple([len(self.get_variable(var).legal_values) for var in self.histogram_variables])

    def process(self, data):
        """
            Input:
                 data: a Spark dataframe (df)

            Output:
                a RDD with block by block counts of housing units and gqs by type
        """
        # Note: We need to do a recode to use dev table1. This shouldn't be necessary for real deal.
        def to_ndarray(gqtype_count_list):
            hist = np.zeros(self.data_shape)
            for idx, cnt in gqtype_count_list:
                hist[idx] = cnt
            return hist
        return (data.select(*([var for var in self.geography_variables] + [var for var in self.histogram_variables] + [var for var in self.uniqueID]))
                    .distinct()
                    .groupBy(*([var for var in self.geography_variables] + [var for var in self.histogram_variables]))
                    .count().rdd
                    .map(lambda row: (tuple([row[var] for var in self.geography_variables]), (tuple([row[var] for var in self.histogram_variables]), row[COUNT])))
                    .groupByKey()
                    .mapValues(to_ndarray))
