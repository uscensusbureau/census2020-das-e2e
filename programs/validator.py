# mdf to mdf test
#
# Compare input to output

import logging
import os
import numpy as np
from operator import add
import datetime
import subprocess
from pathlib import Path

from das_framework.driver import AbstractDASValidator
import programs.engine.primitives as primitives
import das_framework.certificate_printer.certificate as certificate
from das_framework.certificate_printer.latex_tools import latex_escape
from das_framework.certificate_printer.tytable import ttable, LONGTABLE


def calc_total_L1(r_s_rdd):
    """
    Calculate sum of absolute values of histogram differences.
    In the anonymous lambda function datum d corresponds to (geocode, (raw_histogram, syn_histogram)), since
    it is obtained by join of two (geocode, histogram) RDDs
    Thus d[1][0] is true histogram, d[1][1] is noisy histogram, and the mapping function returns the L1
    for the block node, which are then added together in reduce operation
    :param r_s_rdd: (geocode, (raw_histogram, syn_histogram)) RDD
    :return: Total L1 error, np.float64
    """
    return r_s_rdd.map(lambda d: np.sum(np.abs(d[1][0] - d[1][1]))).reduce(add)


def calc_N(r_rdd):
    """
    Sum people in each blocknode, by summing its histogram, then add together through reduce
    :param r_rdd: (geocode, histogram) RDD
    :return:
    """
    return r_rdd.map(lambda d: np.sum(d[1])).reduce(add)


class validator(AbstractDASValidator):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)

        # String to report protected L1 error with confidence interval (goes to print, log and certificate)
        self.L1string: str = ""

        # Budgets for geolevels
        self.level_budgets: dict = {}

        # Total privacy budget except for the tiny part spent on L1
        self.total_budget: float = 0.

    def validate(self, original_data, written_data, **kwargs):

        self.protected_L1_error(original_data, written_data)
        self.get_privacy_parameters()
        self.make_certificate()
        return True

    def protected_L1_error(self, original_data, written_data):
        """
        Changing one record changes 2 counts in the histogram, thus changing the sum of histogram by 2. The L1 error
        would also be changed at maximum 2. Thus, the sensitivity for total L1 query is 2.
        We have post-processing with optimization of histograms, but due to linear budget budget expenditure
        accumulation, the sensitivity remains 2:
                                              ___
                                              ╲
                                              ╱  Pr(│D_j - c│ in S)⋅ Pr[MDF(D_j) = c]
                                              ‾‾‾‾‾
         Pr(│D_j - MDF(D_j)│ in S)            c in R
        ────────────────────────────────    =   ──────────────────────────────────────────────────────────────                (1)
        Pr(│D'_j - MDF(D'_j)│ in S)          ___
                                             ╲
                                             ╱  Pr(│D'_j - c)│ in S)⋅ Pr[MDF(D'_j) = c]
                                             ‾‾‾‾‾
                                            c in R

        where MDF stands for Master Data File, is the output of the protection algorithm + post-processing

        If we have used ε_main to protect MDF(D) (in the engine), and ε_L1 to protect D as we access it again
        for the L1 error calculation, we have

                      Pr[MDF(D_j) = c]        ε_main          Pr[|D_j - c| in S]         ε_L1
                      ────────────────────  ≤  e         and    ────────────────────────   ≤  e
                      Pr[MDF(D'_j) = c]                       Pr[|D'_j - c| in S]

        and substituting these in (1) we get

                    Pr(│D_j - MDF(D_j)│ in S)        ε_main + ε_L1
                   ────────────────────────────────  ≤  e
                   Pr(│D'_j - MDF(D'_j)│ in S)

        Thus the sensitivity is 2 (maximal change in sum of histogram counts, contributing to L1), and the ε used in the
        Laplace mechanism is equal the budget we want to spend on L1 measure protection, and just adds up to the total
        budget for the release.
        :return:
        """

        true_data = original_data.map(lambda node: ((node.geocode,), node.raw.toDense()))
        noisy_data = written_data.map(lambda node: ((node.geocode,), node.syn.toDense()))
        blk_error_rdd = true_data.join(noisy_data)

        total_L1 = calc_total_L1(blk_error_rdd)
        N = calc_N(true_data)

        # Using the budget indicated in config file
        try:
            epsilon_mechanism = self.getfloat("error_privacy_budget")
        except KeyError:
            err_msg = "No budget for L1 error protection indicated in the config file"
            logging.error(err_msg)
            raise RuntimeError(err_msg)

        # Creating and applying Laplace mechanism
        dp_mechanism = primitives.GeometricMechanism(epsilon=epsilon_mechanism, sensitivity=2.0)

        dp_L1 = dp_mechanism.protect(true_answer=total_L1, prng=np.random)

        # Let's find confidence intervals
        ci_level = self.getfloat("error_dp_confidence_level", default=0.9)

        # For one-sided bottom confidence interval (we do that because we only care about privacy here, so want to
        # make sure that the error is higher than zero, i.e. that the system has run. We don't really care about the
        # top bound of the interval which matters for accuracy but not for privacy).
        L1ci = dp_mechanism.inverse_CDF(1. - ci_level, dp_L1) - 1.

        # We want relative errors. Also, the maximal L1 error at fixed N is 2N, since every person assigned to a wrong
        # cell adds 2 to the error, so getting everyone in the wrong cell corresponds to 2N. Normalization below maps
        # the realtive L1 into [0,1] segment
        rL1 = dp_L1 / 2./ N
        rL1ci = L1ci / 2./ N

        self.L1string = \
            f"The protected relative total L1 error is {rL1:.2f} (above {rL1ci:.2f} with {ci_level:.0%} confidence)"

        print(self.L1string)
        logging.info(self.L1string)
        return dp_L1

    def get_privacy_parameters(self):

        # THESE CALCULATIONS ARE COPIED FROM TOPDOWN ENGINE FOR NOW, BUT SHOULD BE CHANGED TO BE THE RESULT
        # OF THOSE CALCULATIONS

        import re
        REGEX_CONFIG_DELIM = "^\s+|\s*,\s*|\s+$"

        levels = tuple(self.config["geodict"]["geolevel_names"].split(","))
        # check that geolevel_budget_prop adds to 1, if not raise exception
        geolevel_prop_budgets = [float(x) for x in
                                 re.split(REGEX_CONFIG_DELIM, self.config["budget"]["geolevel_budget_prop"])]

        self.total_budget = float(self.config["budget"]["epsilon_budget_total"])
        detailed_prop = float(self.config["budget"]["detailedprop"])

        self.level_budgets = {lname: self.total_budget * geolevel_prop_budgets[i] * detailed_prop for i, lname in
                              enumerate(levels)}

    def make_certificate(self):
        """Generate the certificate"""
        engine_name = latex_escape(self.getconfig("engine", section="engine"))
        dpstring = f"Engine {engine_name} is used\n\n\n"

        dpstring = dpstring + "The following privacy budgets ($\\varepsilon$) were used for geographical levels:\n\n"
        tt = ttable()
        tt.latex_colspec = "lr"
        tt.add_head(["GeoLevel", "$\\varepsilon$"])
        for lname, leps in self.level_budgets.items():
            tt.add_data([latex_escape(lname), f"{leps:.5f}"])

        tt.add_data(ttable.HR)
        tt.add_data(["Total", f"{self.total_budget:5f}"])

        dpstring = dpstring + tt.typeset(mode='latex') + "\n\n"

        dpstring = dpstring + latex_escape(self.L1string) + "\n\n"

        dpstring = dpstring + "The following values were set in config file: \n\n"
        tt = ttable()
        tt.latex_colspec = "llr"
        tt.add_head(["Section", "Option", "Value"])
        for sec in self.config.sections():
            if sec in ['reader', 'geodict', 'budget', 'engine', 'constraints', 'writer', 'validator']:
                for item in self.getconfitems(sec):
                    tt.add_data([latex_escape(sec), latex_escape(item[0]), latex_escape(item[1])])

        dpstring = dpstring + tt.typeset(mode='latex') + "\n\n"

        # if "epsilon" in param_dict:
        #     dpstring = dpstring + "The protection mechanism is formally private.\n\n"
        # else:
        #     dpstring = dpstring + "The protection mechanism IS NOT FORMALLY PRIVATE.\n\n"
        #
        # if print_params:
        #     if "epsilon" in param_dict:
        #         dpstring = dpstring + "The release $\\varepsilon={}$.\n\n".format(param_dict["epsilon"])
        #         if "table_epsilons" in param_dict:
        #             tt = ttable()
        #             tt.latex_colspec = "lr"
        #             tt.add_head(["Table Name", "$\\varepsilon$"])
        #             for t, e in param_dict["table_epsilons"].items():
        #                 tt.add_data([t, e])
        #             tt.add_data(ttable.HR)
        #             dpstring = dpstring + "\n\nThe values of $\\varepsilon$ for each table {}.\n\n".format(
        #                 tt.typeset(mode='latex'))
        #
        #     dpstring = dpstring + "Individual mechanism $\\varepsilon$ and other parameters:\n\n"
        #
        #     tt = ttable()
        #     tt.latex_colspec = "lllr"
        #     tt.add_head(["Table", "Composable\ngroup", "Parameter", "Value"])
        #     tt.add_data(ttable.HR)
        #     for itable, (tname, t_param_set) in enumerate(param_dict["params"].items()):
        #         for icg, compgroup in enumerate(t_param_set):
        #             for ivp, var_params in enumerate(compgroup):
        #                 for iv,(p,v) in enumerate(var_params.items()):
        #                     tnameprint = tname if iv == 0 and ivp == 0 else ''
        #                     compgroupprint = ", ".join(list(map(lambda vp: vp['varname'],compgroup))) if iv == 0 else ''
        #                     pprint = p
        #                     vprint = "{}".format(str(v))
        #                     if p == 'epsilon':
        #                         pprint = '$\\varepsilon$'
        #                         vprint = '\\textbf{'+vprint+'}'
        #                     if p == 'alpha':
        #                         pprint = '$\\alpha$'
        #                     if p == 'delta':
        #                         pprint = '$\delta$'
        #                     tt.add_data([tnameprint,compgroupprint,pprint,vprint])
        #                 tt.add_data(ttable.HR)
        #             tt.add_data(ttable.HR)
        #     dpstring = dpstring + tt.typeset(mode='latex') + "\n\n"
        #
        # dpstring = dpstring + "Files with protected data and error calculations:\n\n"
        # tt = ttable()
        # tt.add_option(LONGTABLE)
        # tt.add_head(['Filename', 'lines', 'bytes', 'sha-1'])
        # tt.latex_colspec="lrrl"
        # for fname in written_data:
        #     if not os.path.isdir(fname):
        #         tt.add_data([latex_escape(fname)] + list(certificate.file_stats(fname)))
        # tt.add_data(ttable.HR)
        # for fname in self.written_error_files:
        #     tt.add_data([latex_escape(fname)] + list(certificate.file_stats(fname)))
        # tt.add_data(ttable.HR)
        # dpstring = dpstring + tt.typeset(mode='latex') + "\n\n"

        c = certificate.CertificatePrinter(title="BDS-DAS Certificate")
        c.add_params(
            {"@@NAME@@": "Decennial DAS 2018 End-To-End Test", "@@DATE@@": datetime.datetime.now().isoformat()[0:19],
             "@@PERSON1@@": "Ben Bitdiddle", "@@TITLE1@@": "Novice Programmer", "@@PERSON2@@": "Alyssa P. Hacker",
             "@@TITLE2@@": "Supervisor", "%%DP%%": dpstring})

        s3name = os.path.expandvars(self.getconfig("certificate_path", default="$MDF_CERT"))
        cert_fname = "certificate.pdf"
        c.typeset(os.path.join(cert_fname))
        cmd = ['aws', 's3', 'cp', cert_fname, s3name]
        if Path(s3name).parent.is_dir():
            # If path is local, just a simple `cp` will do
            cmd = ['cp', cert_fname, s3name]
        print(" ".join(cmd))
        subprocess.check_call(cmd)
        cmd = ['aws', 's3', 'ls', s3name]
        if Path(s3name).parent.is_dir():
            # `cp` if local
            cmd = ['ls', s3name]
        print(" ".join(cmd))
        subprocess.check_call(cmd)
