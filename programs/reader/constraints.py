# constraints.py
# Robert Ashmead
# Last updated: 6/28/18

# Major modification log:
#
#  2018-06-01 ra - created module
#

import math

# import copy - pylint say unused, test if rming breaks anything.
import itertools
import numpy as np
import programs.engine.cenquery as cenquery

class AbstractConstraintsCreator:
    """ New super class for constraint creators
        This class is what is used to create constraint objects from what is listed in the config file
    Inputs:
        hist_shape: the shape of the underlying histogram
        invariants: the list of invariants (already created)
        constraint_names:  the names of the constraints to be created (from the list below)
    """
    def __init__(self, hist_shape, invariants, constraint_names):
        self.invariants = invariants
        self.constraint_names = constraint_names
        self.hist_shape = hist_shape
        self.constraints_dict = {}
        self.constraint_funcs_dict = {}

    def calculateConstraints(self):
        for name in self.constraint_names:
            assert name in self.constraint_names, "Provided constraint name '{}' not found.".format(name)
            self.constraint_funcs_dict[name]()
        return self



class ConstraintsCreatorPL94(AbstractConstraintsCreator):
    """
    This class is what is used to create constraint objects from what is listed in the config file
    Inputs:
        hist_shape: the shape of the underlying histogram
        invariants: the list of invariants (already created)
        constraint_names: the names of the constraints to be created (from the list below)
    """
    def __init__(self, **kwargs):
        super().__init__(**kwargs)

        self.hhgq_cap = np.repeat(99999, 8)
        self.hhgq_cap[0] = 99

        self.constraint_funcs_dict = {
            "total"                     : self.total,
            "hhgq_total_lb"             : self.hhgq_total_lb,
            "hhgq_total_ub"             : self.hhgq_total_ub,
            "union_hhgq_lb"             : self.union_hhgq_lb,
            "union_hhgq_ub"             : self.union_hhgq_ub,
            "nurse_nva_0"               : self.nurse_nva_0
        }

        self.cats = np.where(self.invariants["gqhh_vect"] > 0)[0] # indices with >0 facilities of type
        self.num_cats = sum(self.invariants["gqhh_vect"] > 0)     # # distinct gqhh types w/ >0 facilities of type
       
        self.all_cat_combs = []
        for j in range(2, 8):
            temp = list(itertools.combinations(range(0, 8), j))
            temp = [list(x) for x in temp]
            self.all_cat_combs = self.all_cat_combs + temp
    
    # Structural zero: no minors in nursing facilities (GQ code 301)
    def nurse_nva_0(self):
        subset = ([3], [0], range(2), range(63) )
        add_over_margins = (0,1,2,3)
        query = cenquery.Query(array_dims=self.hist_shape,subset=subset, add_over_margins=add_over_margins)
        rhs = np.array(0)
        sign = "="
        self.constraints_dict["nurse_nva_0"] = cenquery.Constraint(query=query, rhs=rhs, sign=sign, name="nurse_nva_0")

    # Total population per geounit must remain invariant
    def total(self):
        subset = None
        add_over_margins = (0,1,2,3)
        query = cenquery.Query(array_dims=self.hist_shape,subset=subset, add_over_margins=add_over_margins)
        rhs = self.invariants["tot"].astype(int)
        sign = "="
        self.constraints_dict["total"] = cenquery.Constraint(query=query, rhs=rhs, sign=sign, name="total")


    # # of ppl in each gqhh type is >= # facilities of that type, or is everyone if it is the only facility type
    def hhgq_total_lb(self):
        subset = None
        add_over_margins = (1, 2, 3)
        query = cenquery.Query(array_dims=self.hist_shape, subset=subset, add_over_margins=add_over_margins)

        # gq_hh in other cats
        other_gqs = self.invariants["gqhh_tot"] - self.invariants["gqhh_vect"]
        if "tot" in self.invariants.keys():
            total = np.broadcast_to(np.array(self.invariants["tot"]), (1,))
            total_ext = np.broadcast_to(total, (8,))
            rhs = np.where(other_gqs > 0, self.invariants["gqhh_vect"], total_ext)
        else:
            rhs = self.invariants["gqhh_vect"]
        rhs = rhs.astype(int)
        sign = "ge"
        self.constraints_dict["hhgq_total_lb"] = cenquery.Constraint(query=query, rhs=rhs, sign=sign, name="hhgq_total_lb")
   
    # # of ppl in each gqhh type is <= tot-(#other gq facilities), or has no one if there are no facilities of type
    def hhgq_total_ub(self):
        subset = None
        add_over_margins = (1, 2, 3)
        query = cenquery.Query(array_dims=self.hist_shape, subset=subset, add_over_margins=add_over_margins)
        other_gqs = self.invariants["gqhh_tot"] - self.invariants["gqhh_vect"]
        if "tot" in self.invariants.keys():
            total = np.broadcast_to(np.array( self.invariants["tot"] ), (1,) )
            total_ext= np.broadcast_to(total, (8,))
            rhs = np.where(self.invariants["gqhh_vect"]>0, total_ext-other_gqs, np.zeros(8))
        else:
            rhs = self.invariants["gqhh_vect"]*self.hhgq_cap
        rhs = rhs.astype(int)
        sign = "le"
        self.constraints_dict["hhgq_total_ub"] = cenquery.Constraint(query=query, rhs=rhs, sign=sign, name="hhgq_total_ub")

    def union_hhgq_lb(self):
        cats = self.cats
        all_cat_combs = self.all_cat_combs
        for cat_comb in all_cat_combs:
            name = "union_hhgq_lb." + ".".join([str(x) for x in cat_comb])
            subset = (cat_comb, range(2), range(2), range(63))
            add_over_margins = (0, 1, 2, 3)
            if set(cats).issubset(set(cat_comb)):
                rhs = self.invariants["tot"]
            else:
                rhs = self.constraints_dict["hhgq_total_lb"].rhs[cat_comb].sum()
            sign = "ge"
            query = cenquery.Query(array_dims=self.hist_shape,
                                    subset=subset, add_over_margins=add_over_margins)
            self.constraints_dict[name] = cenquery.Constraint(query=query, rhs=np.array(rhs).astype(int),
                                                                sign=sign, name = name, union =True,
                                                                union_type="union_hhgq_lb")
                                                                
        
    def union_hhgq_ub(self):
        cats = self.cats
        all_cat_combs = self.all_cat_combs
        for cat_comb in all_cat_combs:
            other_cats = list(set(range(8)).difference(set(cat_comb)))
            name = "union_hhgq_ub." + ".".join([str(x) for x in cat_comb])
            subset = (cat_comb, range(2), range(2), range(63))
            add_over_margins = (0, 1, 2, 3)
            if set(cats).issubset(set(cat_comb)):
                rhs = self.invariants["tot"]
            else:
                gq_min_ext = self.invariants["gqhh_vect"]
                gq_min_other = gq_min_ext[other_cats].sum(0)
                rhs = self.invariants["tot"] - gq_min_other
            sign = "le"
            query = cenquery.Query(array_dims=self.hist_shape,
                                    subset=subset, add_over_margins=add_over_margins)
            self.constraints_dict[name] = cenquery.Constraint(query=query, rhs=np.array(rhs).astype(int),
                                                                sign=sign, name = name, union =True,
                                                                union_type="union_hhgq_ub")

