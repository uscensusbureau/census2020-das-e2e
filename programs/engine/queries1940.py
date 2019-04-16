import numpy as np
import programs.engine.cenquery as cenquery


class QueriesCreator1940():
    """
    1940 queries that correspond to those used in the 2018 e2e test.
    """
    def __init__(self, hist_shape, query_names):
        self.query_names = query_names
        self.hist_shape = hist_shape

        self.query_funcs_dict = {
            "hhgq"                       : self.hhgq,
            "va_hisp_race"               : self.va_hisp_race
        }
        self.queries_dict = {}

    def calculateQueries(self):
        for name in self.query_names:
            assert name in self.query_names, "Provided query name '{}' not found.".format(name)
            self.query_funcs_dict[name]()
        return self

    # 1 way marginals
    def hhgq(self, name="hhgq", subset=None):
        add_over_margins = [1,2,3]
        self.queries_dict[name] = cenquery.Query(self.hist_shape, subset=subset, add_over_margins=add_over_margins,
                                                 name=name)
    # 3 way marginals
    def va_hisp_race(self):
        add_over_margins = [0]
        self.queries_dict["va_hisp_race"] = cenquery.Query(self.hist_shape, add_over_margins=add_over_margins, name="va_hisp_race")
