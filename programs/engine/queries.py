import numpy as np
import programs.engine.cenquery as cenquery


class QueriesCreatorPL94():
    """
    
    """
    def __init__(self, hist_shape, query_names):
        self.query_names = query_names
        self.hist_shape = hist_shape

        self.query_funcs_dict = {
            "race"                       : self.race,
            "number_of_races"            : self.number_of_races,
            "race_ethnicity"             : self.race_ethnicity,
            "total_pop"                  : self.total_pop,
            "hhgq"                       : self.hhgq,
            "va_hisp_race"               : self.va_hisp_race,
            "hisp"                       : self.hisp,
            "voting_age"                 : self.voting_age,
            "weight_test"                : self.weight_test,
            "p1"                         : self.p1,
            "p2"                         : self.p2,
            "p3"                         : self.p3,
            "p4"                         : self.p4,
            "p42"                        : self.p42
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

    def voting_age(self):
        add_over_margins = [0,2,3]
        self.queries_dict["voting_age"] = cenquery.Query(self.hist_shape, add_over_margins=add_over_margins,
                     name="voting_age")

    def hisp(self, name="hisp", subset=None):
        add_over_margins = [0, 1, 3]
        self.queries_dict[name] = cenquery.Query(self.hist_shape, subset=subset, add_over_margins=add_over_margins, name=name)

    def race(self, name="race", subset=None):
        add_over_margins = [0,1,2]
        self.queries_dict[name] = cenquery.Query(self.hist_shape, add_over_margins=add_over_margins, subset=subset, name=name)

    def all_1_marginals(self):
        self.hhgq()
        self.voting_age()
        self.hisp()
        self.race()

    # 2 way marginals
    def hhgq_va(self):
        add_over_margins = [2, 3]
        self.queries_dict["hhgq_va"] = cenquery.Query(self.hist_shape, add_over_margins=add_over_margins, name="hhgq_va")

    def hhgq_hisp(self):
        add_over_margins = [1, 3]
        self.queries_dict["hhgq_hisp"] = cenquery.Query(self.hist_shape, add_over_margins=add_over_margins, name="hhgq_hisp")

    def hhgq_race(self):
        add_over_margins = [1, 2]
        self.queries_dict["hhgq_race"] = cenquery.Query(self.hist_shape, add_over_margins=add_over_margins, name="hhgq_race")

    def va_hisp(self):
        add_over_margins = [0, 3]
        self.queries_dict["va_hisp"] = cenquery.Query(self.hist_shape, add_over_margins=add_over_margins, name="va_hisp")

    def va_race(self):
        add_over_margins = [0, 2]
        self.queries_dict["va_race"] = cenquery.Query(self.hist_shape, add_over_margins=add_over_margins, name="va_race")

    def race_ethnicity(self):
        add_over_margins = [0, 1]
        self.queries_dict["race_ethnicity"] = cenquery.Query(self.hist_shape, add_over_margins=add_over_margins,
                     name="race_ethnicity")

    def all_2_marginals(self):
        self.hhgq_va()
        self.hhgq_hisp()
        self.hhgq_race()
        self.va_hisp()
        self.va_race()
        self.race_ethnicity()

    # 3 way marginals
    def hhgq_va_hisp(self):
        add_over_margins = [3]
        self.queries_dict["hhgq_va_hisp"] = cenquery.Query(self.hist_shape, add_over_margins=add_over_margins, name="hhgq_va_hisp")

    def hhgq_va_race(self):
        add_over_margins = [2]
        self.queries_dict["hhgq_va_race"] = cenquery.Query(self.hist_shape, add_over_margins=add_over_margins, name="hhgq_va_race")

    def hhgq_hisp_race(self):
        add_over_margins = [1]
        self.queries_dict["hhgq_hisp_race"] = cenquery.Query(self.hist_shape, add_over_margins=add_over_margins, name="hhgq_hisp_race")

    def va_hisp_race(self):
        add_over_margins = [0]
        self.queries_dict["va_hisp_race"] = cenquery.Query(self.hist_shape, add_over_margins=add_over_margins, name="va_hisp_race")

    def all_3_marginals(self):
        self.hhgq_va_hisp()
        self.hhgq_va_race()
        self.hhgq_hisp_race()
        self.va_hisp_race()

    # 4 way marg
    def total_pop(self, name="total_pop", subset=None):
        add_over_margins = [0,1,2,3]
        self.queries_dict[name] = cenquery.Query(self.hist_shape, add_over_margins=add_over_margins, subset=subset,
                                                 name=name)

    # all marginals
    def all_marginals(self):
        self.all_1_marginals()
        self.all_2_marginals()
        self.all_3_marginals()
        self.total_pop()

    # Table P1
    def p1(self):
        self.total_pop()
        self.two_or_more_races()
        self.number_of_races()
        self.race()

    # Table P2
    def p2(self):
        self.total_pop()
        self.hisp()
        self.number_of_races(name="p2_num_of_races", subset=(range(8), range(2), [0], range(63)))
        self.two_or_more_races(name="p2_two_or_more", subset=(range(8), range(2), [0], range(63)))
        self.race(name="p2_races", subset=(range(8), range(2), [0], range(63)))

    # Table P3
    def p3(self):
        self.total_pop(name="va_total", subset=(range(8), [1], range(2), range(63)))
        self.number_of_races(name="p3_num_of_races", subset=(range(8), [1], range(2), range(63)))
        self.two_or_more_races(name="p3_two_or_more", subset=(range(8), [1], range(2), range(63)))
        self.race(name="p3_races", subset=(range(8), [1], range(2), range(63)))

    # Table P4
    def p4(self):
        self.total_pop(name="va_total", subset=(range(8), [1], range(2), range(63)))
        self.hisp(name="p4_hisp", subset=(range(8), [1], range(2), range(63)))
        self.number_of_races(name="p4_num_of_races", subset=(range(8), [1], [0], range(63)))
        self.two_or_more_races(name="p4_two_or_more", subset=(range(8), [1], [0], range(63)))
        self.race(name="p4_races", subset=(range(8), [1], [0], range(63)))
        
    # Table P42
    def p42(self):
        self.total_pop(name="gq_total", subset=(range(1,8), range(2), range(2), range(63)))
        self.hhgq(name="p42_hhgq", subset=(range(1,8), range(2), range(2), range(63)))
        self.inst_noninst()


    def pl94(self):
        self.p1()
        self.p2()
        self.p3()
        self.p4()
        self.p42()

    # other
    def inst_noninst(self):
        add_over_margins = [1,2,3]
        axis_groupings = [(0, (range(1,5), range(5, 8)))]
        self.queries_dict["inst_noninst"] = cenquery.Query(self.hist_shape, add_over_margins=add_over_margins, axis_groupings=axis_groupings, name="inst_noninst")        


    def number_of_races(self, name="number_of_races", subset=None):
        add_over_margins = [0,1,2]
        axis_groupings = [ (3, (range(0,7),
                                range(7,22),
                                range(22,42),
                                range(42,57),
                                range(57,63) 
                                            ))]
        self.queries_dict[name] = cenquery.Query(self.hist_shape, add_over_margins=add_over_margins, subset=subset,
                                                 axis_groupings=axis_groupings, name=name)

    def two_or_more_races(self, name="two_or_more_races", subset=(range(8), range(2), range(2), range(7,63))):
        add_over_margins = [0,1,2,3]
        subset[3] = range(7,63)
        self.queries_dict[name] = cenquery.Query(self.hist_shape, add_over_margins=add_over_margins, subset=subset, name=name)
    
    def white_in_combo(self):
        add_over_margins = [0, 1, 2]
        axis_groupings = [(3, ([0], range(6,11), range(21, 31), range(41, 51), range(56, 61), [63]))]
        self.queries_dict["white_in_combo"] = cenquery.Query(self.hist_shape, add_over_margins=add_over_margins, axis_groupings=axis_groupings, name="white_in_combo")

    def black_in_combo(self):
        add_over_margins = [0, 1, 2, 3]
        axis_groupings = [(3, ([1], [6], range(11, 15), range(21, 25), range(31, 37), range(41, 47), range(51, 55), range(56, 60), range(61,63)))]
        self.queries_dict["black_in_combo"] = cenquery.Query(self.hist_shape, add_over_margins=add_over_margins, axis_groupings=axis_groupings, name="black_in_combo")

    def aian_in_combo(self):
        add_over_margins = [0, 1, 2, 3]
        axis_groupings = [(3, ([2], [7], [11], range(15, 18), [21], range(25, 28), range(31, 34), range(37, 40), range(41, 44), range(47, 50), range(51, 54), range(55, 59), range(60, 63)))] 
        self.queries_dict["aian_in_combo"] = cenquery.Query(self.hist_shape, add_over_margins=add_over_margins, axis_groupings=axis_groupings, name="aian_in_combo")

    def asian_in_combo(self):
        add_over_margins = [0, 1, 2, 3]
        axis_groupings = [(3, ([3],[8],[12],[16], range(18,20), [23],[25], range(28,30), [31], range(34,36), range(37,39), range(40, 42), range(44, 46), range(47, 49), range(50, 53), range(54, 58), range(59,63)))]
        self.queries_dict["asian_in_combo"] = cenquery.Query(self.hist_shape, add_over_margins=add_over_margins, axis_groupings=axis_groupings, name="asian_in_combo")

    def nhpi_in_combo(self):
        add_over_margins = [0, 1, 2, 3]
        axis_groupings = [(3, ([4], [9], [13], [16], [18], [20], [23], [26], [28], [30], [32], [34], range(36, 38), range(39, 41), [42], [44], range(46, 48), range(49, 52), range(53, 57), range(58, 63)))] 
        self.queries_dict["nhpi_in_combo"] = cenquery.Query(self.hist_shape, add_over_margins=add_over_margins, axis_groupings=axis_groupings, name="nhpi_in_combo")




    def weight_test(self):
        add_over_margins = [0,2,3]
        #weight_array = np.random.rand(self.hist_shape)
        weight_array = np.full(self.hist_shape, 1.5)
        self.queries_dict["weight_test"] = cenquery.Query(self.hist_shape, add_over_margins=add_over_margins,
                        weight_array = weight_array, name="weight_test")
        

class QueriesCreator1940():
    """

    """
    def __init__(self, hist_shape, query_names):
        self.query_names = query_names
        self.hist_shape = hist_shape

        self.query_funcs_dict = {
            "race"                       : self.race,
            "racecomb"                   : self.racecomb

        }
        self.queries_dict = {}
       
    def calculateQueries(self):
        for name in self.query_names:
            assert name in self.query_names, "Provided query name '{}' not found.".format(name)
            self.query_funcs_dict[name]()
        return self

    def race(self):
        add_over_margins = [0,1,2,3]
        self.queries_dict["race"] = cenquery.Query(self.hist_shape, add_over_margins=add_over_margins, name="race")
        
    def racecomb(self):
        add_over_margins = [0,1,2,3]
        axis_groupings = [ (4, ([0,1,2],[3,4,5]))]
        self.queries_dict["racecomb"] = cenquery.Query(self.hist_shape, add_over_margins=add_over_margins,
                  axis_groupings=axis_groupings, name="racecomb")
