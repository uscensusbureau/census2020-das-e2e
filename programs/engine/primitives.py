"""
    This module contains primitive operations used in differential privacy.
"""
__all__ = ['GeometricMechanism', 'LaplaceMechanism']
import numpy as np
import scipy.stats

def geometric_mechanism(true_answer, budget, sensitivity, prng):
    """
    This is a wrap function to temporarily keep compatibility with all the places where it's
    called. Should be replaced in all those places with creating the object and calling its
    method(s) and/or field(s) as below
    """
    mech = GeometricMechanism(budget, sensitivity)
    return mech.protect(true_answer, prng), mech.variance

def geometric_lrng(p, shape, prng):
    """
        Wrapper to replace numpy.random.geometric with loop that re-seeds from /dev/urandom (by calling numpy.random.seed)
        in between each random scalar drawn. Avoids ever using more than 1 iterate of numpy's MersenneTwister implementation,
        so PRNG security should be equivalent to that of the Linux RNG or "lrng" (if not quite equivalent to that of the Intel
        Digital RNG's entropy source + CSPRNG).
    """
    assert isinstance(p, float), "geometric_lrng expects scalar float for p parameter."
    noise = np.zeros(shape=shape)
    for index, value in np.ndenumerate(noise):
        prng.seed()
        noise[index] = prng.geometric(p)
    return noise

def laplace_lrng(loc, scale, shape, prng):
    """
        Wrapper to replace numpy.random.laplace with loop that re-seeds from /dev/urandom (by calling numpy.random.seed)
        in between each random scalar drawn. Avoids ever using more than 1 iterate of numpy's MersenneTwister implementation,
        so PRNG security should be equivalent to that of the Linux RNG or "lrng" (if not quite equivalent to that of the Intel
        Digital RNG's entropy source + CSPRNG).
    """
    assert isinstance(scale, float), "laplace_lrng expects scalar float for scale parameter."
    noise = np.zeros(shape=shape)
    for index, value in np.ndenumerate(noise):
        prng.seed()
        true_value = loc[index] if np.array(loc).shape != (1,) else loc
        noise[index] = prng.laplace(loc=true_value, scale=scale)
    return noise

class GeometricMechanism:
    """
    Implementation of the Geometric Mechanism for differential privacy
    Adding noise from the (two-sided) Geometric distribution, with p.d.f (of integer random variable k):

                      p          -│k│       1 - α    -│k│
                   ──────── (1 - p)     or  ──────── α
                    2 - p                   1 + α

    where p is the parameter, or α = 1 - p in different conventional notation

    """
    def __init__(self, epsilon, sensitivity):
        """

        :param epsilon (float): the privacy budget to use
        :param sensitivity (int): the sensitivity of the query. Addition or deletion of one
                                   person from the database must change the query answer vector
                                   by an integer amount
        """
        # Parameter p of the Geometric distribution as per class docstring
        self.p = 1 - np.exp(-epsilon / float(sensitivity))

        # Variance is twice that of the one-sided (1-p)/p^2
        self.variance = 2 * (1 - self.p) / self.p**2

    def protect(self, true_answer, prng):
        """
        Inputs:
                true_answer (float or numpy array): the true answer
                prng: a numpy random number generator

        Output:
            The result of the geometric mechanism (x-y + true_answer).

        numpy.random.geometric draws from p.d.f. f(k) = (1 - p)^{k - 1} p   (k=1,2,3...)

                                                        p          -│k│
        The difference of two draws has the p.d.f    ──────── (1 - p)
                                                      2 - p
        """
        shape = np.shape(true_answer)
        x = geometric_lrng(self.p, shape, prng)
        y = geometric_lrng(self.p, shape, prng)

        return x - y + true_answer

    def pmf(self, x, location=0):
        """
        f(x) = p/(2-p) (1-p)^(abs(x))
        """
        centered_x = np.abs(x-location)
        centered_int_x = int(centered_x)
        if np.abs(centered_x - centered_int_x) > 1e-5:
            return 0
        else:
            return (self.p/(2-self.p)) * ( (1-self.p)**(centered_int_x) )

    def inverse_CDF(self, quantile, location=0.0):
        """
        To be used to create CIs
        quantile: float between 0 and 1
        location: center of distribution

        for all geometric points {0, ... inf} the pdf for the two-sided
        geometric distribution is 1/(2-p) times the pdf for the one-sided
        geometric distribution with the same parameterization. Therefore,
        the area in the rhs tail (>0) for the two-sided cdf for a given
        x value should be 1/(2-p) that of the one-sided geometric.
        
        Say we want the x s.t P(X<=x) = 0.9 for the two-sided geometric.  Then if we find
        the P(X<=x) = 1 - (0.1*(2-p)), the x value should be the same.
        
        Because this is a discrete distribution, cdf(quantile) will give
        the smallest x s.t P(X>=x) >= quantile and P(P>=x-1) < quantile
        """
        tail_prob=1.0-quantile if quantile > 0.5 else quantile 
        one_sided_geom_tail_prob = tail_prob*(2-self.p) 
        
        answer = scipy.stats.geom.ppf(q=1-one_sided_geom_tail_prob,p=self.p,loc=-1) #b/c we want to start at 1 not 0
        answer = location-answer+1 if quantile < 0.5 else location+answer+1
        
        return answer

class LaplaceMechanism:
    """
    Implements Laplace mechanism for differential privacy.
    Adding noise from the Laplace distribution, with p.d.f

                          1      -│x-a│
                        ───── exp ───────,
                         2b         b
    where a is known as the location, and b as the scale of Laplace distribution
    """

    def __init__(self, epsilon, sensitivity):
        """

        :param epsilon (float): the privacy budget to use
        :param sensitivity (float): the sensitivity of the query
        """
        self.epsilon = epsilon

        # Scale of the Laplace distribution, denoted as b in the docstring above
        self.scale = float(sensitivity) / self.epsilon

        # Variance is 2b^2
        self.variance = 2 * (self.scale ** 2)

    def protect(self, true_answer, prng):
        """
        Add noise according to Laplace mechanism and return noisy values
        :param true_answer: true_answer (float or numpy array): the true answer
        :param prng: a numpy random number generator
        :return: protected values
        """
        shape = np.shape(true_answer)
        return laplace_lrng(true_answer, self.scale, shape, prng)

    def inverse_CDF(self, quantile, location):
        """
        To be used to create CIs
        quantile: float between 0 and 1
        location: center of distribution
        """
        return scipy.stats.laplace.ppf(q=quantile, loc=location, scale=self.scale)
