# -*- coding: utf-8 -*-

import logging
import math
from scipy.optimize import fsolve

import os
import sys
sys.path.append(os.path.dirname(__file__))

import driver


def uniform2Laplace(x, b):
    """
    Transform uniformly distributed [0,1) random variable to Laplace distributed
    t = x - .5 for symmetry, since Laplace is symmetrical (even)
    p.d.f. t ~ const

                 1      -│z│
    p.d.f. z ~ ───── exp ────
                2b       b

    Let's consider z>0; (z<0)
    dt = 1/2b exp(-z/b)dz; (dt = 1/2b exp(z/b)dz)
    t + const = -1/2 exp(-z/b); (t + const = 1/2 exp(z/b))
    -2t + const = exp(-z/b); (2t + const = exp(z/b)
    z = -b⋅log(const1 - 2t); (and z = b⋅log(const2 + 2t) for z<0)

    Mapping [-.5,0) to [0,oo) or to [0,-oo) and [0,.5) to [0,-oo) or [0,oo), we get (at zero):
    -b⋅log(const1 + 1) = 0 = b⋅log(const2) or -b⋅log(const1) = 0 = b⋅log(const2 - 1)
    const1 + 1 = 1 = const2 or const1 = 1 = const2 - 1
    Either const1 = 0 and const2 = 1 or const1 = 1 and const2 = 2. Choosing the first option,
    z = -b⋅log(-2t) for z > 0, so t > 0 and
    t in [0,.5) maps to [b⋅log(1),-b⋅log(+0)) -> [0,+oo)
    z = b⋅log(1 + 2t) for z < 0, so t < 0 and
    t in [-.5,0) maps to [b⋅log(0+),b⋅log(1)) -> (-oo,0)

    or z = -b⋅log(2 - 2x) for z > 0 and x >= .5
    and z = b⋅log(2x) for z < 0 and x < .5

    The same formula is used in numpy implementation
    https://github.com/numpy/numpy/blob/master/numpy/random/mtrand/distributions.c
    """
    return b * math.log(x + x) if x < .5 else -b * math.log(2. - x - x)


def uniform2Gamma4(x):
    """
    Transform uniformly distributed [0,1) random variable to z distributed as ~ 1/1+z^4

    p.d.f.  x ~ const
                    √2
    p.d.f.  z ~ ──────────
                   ⎛  4 ⎞
                 π⋅⎝1+z ⎠

    Integrating dx = sqrt(2)/pi*dz/(1+z^4)

    we get

         ⎛ 2           ⎞        ⎛ 2           ⎞
      log⎝z  - √2⋅z + 1⎠     log⎝z  + √2⋅z + 1⎠     atan(√2⋅z - 1)    atan(√2⋅z + 1)
    - ───────────────────── + ───────────────────── + ───────────────── + ───────────────── = x
         4                       4                     2                   2

    or

    x = .25*log((z**2+z*sqrt(2)+1)/(z**2-z*sqrt(2)+1))+.5*(arctan(z*sqrt(2)+1)-arctan(1-z*sqrt(2)))

    x -> -π/2 when z->-oo
    x = 0 when z = 0
    x -> π/2 when z->oo

    so the [-π/2, π/2] maps to [-oo,oo].
    Hence if x = π⋅(uniform[0,1)-1/2), it is uniformly distributed in [-π/2, π/2), then
    z(x) found as inverse of x(z) above is distributed as  sqrt(2)/pi/(1+z^4)

    We numerically solve for z(x) using fsolve from  scipy.optimize

    """
    sq2 = math.sqrt(2)
    return fsolve(lambda z: .25 * math.log((z ** 2 + z * sq2 + 1) / (z ** 2 - z * sq2 + 1)) + .5 * (
            math.atan(z * sq2 + 1) - math.atan(1 - z * sq2)) - math.pi * (x - .5), 0)[0]


class NoiseAlgorithm(driver.AbstractDASModule):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.varname = kwargs['varname']
        self.params = {'varname': self.varname}
        self.unirandnum = 0
        self.privacy_guarantee = True

    def __repr__(self):
        return "{}:{}".format(self.__class__.__name__, self.params)

    def get_params(self):
        return self.params

    def setvar(self, varname, section):
        """
        Look for the algorithm parameter in the engine section and in the table_variable section. Take the latter
        if exists and log warning. Take the former otherwise. Needed for convenient loops, to set parameters for
        all variables simultaneously
        :param varname: name of the algorithm parameter, like 'alpha' or 'delta' (epsilons are taken care of separately)
        :param section: table_variable, where details of noise algorithm are set
        :return: the value of the variable
        """
        if self.config.has_option(section, varname):
            local_alpha = self.getfloat(varname, section=section)
            var_value = local_alpha
            if self.config.has_option('engine', varname):
                engine_alpha = self.getfloat(varname, section='engine')
                logging.info(
                    "Superseding {0}={1} with {0}={2} for {3}".format(varname, engine_alpha, local_alpha, section))
        else:
            var_value = self.getfloat(varname, section='engine')
        return var_value

    def willRun(self):
        """ For checking that the parameters comply with privacy guarantee"""
        return True

    def noisify(self, row):
        """
        Function implementing the noise algorithm. Returns the true value in the prototype
        By specification:
            row[0] contains true values of the variables
            row[1] contains uniform random noise [0,1)
        (e.g., row is a tuple of dicts, this is how spark_sql_das_engine module uses it)
        The implementations of noisify in this module use this specification.
        Different implementations can be used, they will not break this noisealgorithms.py module,
        but will not be compatible with spark_sql_das_engine which uses this specification
        """
        return float(row[0][self.varname])


class NoNoiseAlgorithm(NoiseAlgorithm):
    """ Algorithm that doesn't add noise (DAS0). Could have just used the prototype, but want to give it NoNoise name"""

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.params['algorithm'] = "NoNoise"


class DPNoiseAlgorithm(NoiseAlgorithm):
    """ Differentially Private noise algorithms. Have an epsilon. """

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.epsilon = self.getfloat('epsilon_fraction', section=kwargs['section']) * self.getfloat('epsilon',
                                                                                                    section='engine')
    def __repr__(self):
        self.params = self.get_params()
        return  super().__repr__()

    def get_params(self):
        params = self.params
        params['epsilon'] = self.epsilon
        return params


class SmoothSensitivityAlgorithm(DPNoiseAlgorithm):
    """ DP algorithms, using smooth sensitivity. Have a local sensitivity value for each cell and an alpha. """

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.alpha = self.setvar('alpha', kwargs['section'])
        self.sensitivity_field = self.getconfig('sensitivity_field', section=kwargs['section'])
        self.params['alpha'] = self.alpha


class SmoothLaplaceAlgorithm(SmoothSensitivityAlgorithm):
    """
    From Haney et al.,SIGMOD '17 Proceedings of the 2017 ACM International Conference on Management of Data
    Pages 1339-1354
    https://dl.acm.org/citation.cfm?doid=3035918.3035940
    Section 9 "Approximating Privacy", Algorithm 3
    """

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.delta = self.setvar('delta', kwargs['section'])
        self.ignore_privacy_guarantee_check = self.getboolean('ignore_privacy_guarantee_check', section='engine', default=False)
        self.params['algorithm'] = "SmoothLaplace"
        self.params['delta'] = self.delta
        self.unirandnum = 1
        self.privacy_guarantee = self.willRun()

    def willRun(self):
        if 2 * math.log(1. / self.delta) * math.log(self.alpha + 1) > self.epsilon:
            logging.error(
                "SmoothLaplace smooth sensitivity is unbounded at with (alpha,epsilon,delta)=({},{},{})!".format(
                    self.alpha, self.epsilon, self.delta))
            return self.ignore_privacy_guarantee_check
        else:
            return True

    def noisify(self, row):
        return float(row[0][self.varname]) + (max(float(row[0][self.sensitivity_field]) * self.alpha, 1) * 2. / self.epsilon * uniform2Laplace(row[1][self.varname], 1))


class SmoothGammaAlgorithm(SmoothSensitivityAlgorithm):
    """
    From Haney et al.,SIGMOD '17 Proceedings of the 2017 ACM International Conference on Management of Data
    Pages 1339-1354
    https://dl.acm.org/citation.cfm?doid=3035918.3035940
    Section 8, Algorithm 2
    """
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.params['algorithm'] = "SmoothGamma"
        self.unirandnum = 1
        epsilon2 = 5 * math.log(self.alpha + 1)
        self.epsilon1 = self.epsilon - epsilon2
        self.privacy_guarantee = self.willRun()

    def willRun(self):

        if self.epsilon1 < 0:
            logging.error(
                "SmoothGamma smooth sensitivity is unbounded at with (alpha,epsilon)=({},{})!".format(self.alpha,
                                                                                                      self.epsilon))
            return False
        else:
            return True

    def noisify(self, row):
        return float(row[0][self.varname]) + float(max(float(row[0][self.sensitivity_field]) * self.alpha, 1) * 5. / self.epsilon1 * uniform2Gamma4(row[1][self.varname]))


class LogLaplaceAlgorithm(DPNoiseAlgorithm):
    """
    From Haney et al.,SIGMOD '17 Proceedings of the 2017 ACM International Conference on Management of Data
    Pages 1339-1354
    https://dl.acm.org/citation.cfm?doid=3035918.3035940
    Section 8, Algorithm 1
    """
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.params['algorithm'] = "LogLaplace"
        self.alpha = self.setvar('alpha', kwargs['section'])
        self.unirandnum = 1
        self.params['alpha'] = self.alpha
        self.privacy_guarantee = self.willRun()

    def noisify(self, row):
        gamma = 1. / self.alpha
        l = math.log(float(row[0][self.varname]) + gamma)
        return math.exp(l + uniform2Laplace(row[1][self.varname], 2. * math.log(1 + self.alpha) / self.epsilon)) - gamma


class LaplaceDPAlgorithm(DPNoiseAlgorithm):
    """ Laplace differential privacy, adding Laplace noise with sensitivity parameter"""
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.params['algorithm'] = "LaplaceDP"
        try:
            self.sensitivity = self.setvar('sensitivity', kwargs['section'])
        except KeyError:
            self.sensitivity = 1.
        self.params['sensitivity'] = self.sensitivity
        self.unirandnum = 1

    def noisify(self, row):
        return float(row[0][self.varname]) + uniform2Laplace(row[1][self.varname], self.sensitivity/self.epsilon)


class Pub1075Algorithm(NoiseAlgorithm):
    """
    Rules from IRS Publication 1075.

    For each requested table, suppress a cell if:

        1. If the geography is national \emph{and} the cell has less than 5 establishments.
        2. If the geography is state \emph{and} the cell has less than 10 establishments.
        3. If the geography is Metro/Nonmetro or CBSA \emph{and} the cell has less than 20 establishments.
        4. If the employment distribution among the establishments in the cell fails the $p\%$ rule, where $p=90\%$:
            Suppress if $R<p*L$ where $L$ is the employment of the largest establishment and $R$ is the employment of
            the cell when the employment for the top \emph{two} establishments has been removed. In this case,
            we use ``suppress'' to mean replace the cell count with a ``missing value'' - whatever make sense for the
            data structure - that can then be interpreted as a zero (usually) when the error calculations are performed.
    """

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.emp_field = self.getconfig('emp_field', section=kwargs['section'], default='empcy')
        self.num_returns_field = self.getconfig('num_returns_field', section=kwargs['section'], default='num_estab')
        self.largest_emp_field = self.getconfig('largest_emp_field', section=kwargs['section'], default='ssmax')
        self.second_largest_field = self.getconfig('second_largest_field', section=kwargs['section'], default='secondmax')
        self.geolevel = self.getconfig('geolevel', section=kwargs['section']).lower()
        self.p = self.setvar('p', kwargs['section'])
        self.params['algorithm'] = "Pub1075"
        self.params['p'] = self.p
        self.suppressed_value = 0.

    def noisify(self, row):
        if self.geolevel == "national" and row[0][self.num_returns_field] < 5:
            return self.suppressed_value
        elif self.geolevel == "state" and row[0][self.num_returns_field] < 10:
            return self.suppressed_value
        elif self.geolevel == "metro" and row[0][self.num_returns_field] < 20:
            return self.suppressed_value
        elif row[0][self.emp_field] - row[0][self.largest_emp_field] - row[0][self.second_largest_field] < self.p * row[0][self.largest_emp_field]:
            return self.suppressed_value
        else:
            return float(row[0][self.varname])
