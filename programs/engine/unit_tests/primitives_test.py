import numpy as np

import os
import sys
import scipy.stats


# If there is __init__.py in the directory where this file is, then Python adds das_decennial directory to sys.path
# automatically. Not sure why and how it works, therefore, keeping the following line as a double
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(__file__)))))

try:
    import primitives
except ImportError as e:
    import programs.engine.primitives as primitives

def test_primitives():
    """
    Tests to see if the geometric mechanism is adding noise.
    """

    true_answer = np.array([5.5, 6.5, 7.2, 8.1])
    budget = .01
    sensitivity = 5
    prng = np.random.RandomState(137899)

    X = primitives.geometric_mechanism(true_answer, budget, sensitivity, prng)[0]

    assert all(abs(X - true_answer))


def test_geometric_pmf():
    """
    The 2sided geometric pmf should be 1/(2-p) multiplier of 
    the 1sided geometric pmf
    """
    for epsilon in [1.0,0.1,0.025]:
        p=1-np.exp(-epsilon)
        for x in [0, 4, 6,7]:
            a = scipy.stats.geom.pmf(x+1,p)
            b=primitives.GeometricMechanism(epsilon,1).pmf(x)
            assert(round(b*(2-p),10) == round(a,10))


def test_geometric_cdf():
    """
    The geometric cdf function should be equivalent to 
    adding up the pdf
    P(X<=x) should be >= quantile
    P(X<=x-1) should be < quantile
    """
    for quantile in [0.01,0.3,0.5,0.7,0.95]:
        for location in [0,-5,10]:
            for epsilon in [1.0,0.1,0.025]:
                mech =primitives.GeometricMechanism(epsilon,1)
                x = mech.inverse_CDF(quantile, location)
                sum=0
                for i in range(-99999,int(x),1):
                    sum+=mech.pmf(i,location)
                assert(sum>=quantile)
                sum=0
                for i in range(-99999,int(x)-1,1):
                    sum+=mech.pmf(i,location)
                assert(sum<quantile)
