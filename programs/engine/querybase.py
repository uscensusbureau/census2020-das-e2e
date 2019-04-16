"""This module contains classes for the representation of linear queries.

Linear queries should subclass AbstractLinearQueries. Use this module with
the following design principles:

1) Subclasses of AbstractLinearQuery should be simple. The only functionality
    they should provide is a more efficient version of some base class functions
2) Classes should not be instantiated directly. Instead, the QueryFactory should
    provide functions for instantiating the classes. Any complicated construction
    code should be moved from the class constructors and into a QueryFactory function

"""

import functools
from abc import ABCMeta, abstractmethod
import numpy as np
import scipy.sparse as ss

class AbstractLinearQuery(metaclass=ABCMeta):
    """ This class describes the api for queries

    There are two fields:
        self.name: the name of the query. Do not use
            directly. Call getName()
        self.matrix_rep: either none or a cached
           copy of the matrix representation. Do not
           use directly. Call matrixRep()
    """
    def __init__(self, name):
        self.matrix_rep = None
        self.name = name

    def getName(self):
        """ Returns the name of the query. """
        return self.name

    def answer(self, data):
        """ Returns the query answer over data

        Inputs:
            data: a multidimensional numpy array
        Output:
            a vector (not multi-d array) of answers
        Note: the default implementation might not be efficient
        """
        mydata = data.reshape(data.size)
        return self.matrixRep() * mydata

    @abstractmethod
    def kronFactors(self):
        """ Returns a list of matrices whose kron product is the matrix representation """
        pass

    def matrixRep(self):
        """ returns a cached version of the matrix representation if available """
        if self.matrix_rep is None:
            self.matrix_rep = self._matrixRep()
        return self.matrix_rep

    def _matrixRep(self):
        """ Should compute and return matrix representation of the query is csr format

        Note: when the data is flattened as a vector x, this function should return a
             matrix Q such that Qx is the answer to the query
        """
        rep = functools.reduce(lambda x, y: ss.kron(x, y, format="csr"), self.kronFactors())
        # Note: ss.kron differs from np.kron because in the case of kron of 1d vectors, ss.kron
        # returns a 1xc matrix  but np.kron returns a 1d array
        return rep


    def sensitivity(self):
        """ returns the sensitivity of this query set in the UNBOUNDED version of DP

        The sensitivity is the maximum l1 norm of the columns in the matrix representation.
        It can be efficiently computed as the product of the sensitivities of the kron factors.
        Override this  method for query classes where this can be computed faster.
        """
        sen = 1.0
        for mat in self.kronFactors():
            #verify that the kron matrices are either matrices or arrays
            assert len(mat.shape) in [2, 1]
            if len(mat.shape) == 2:
                minisen = np.abs(mat).sum(axis=0).max()
            else:
                minisen = np.abs(mat).max()
            sen = sen * minisen
        return sen


    def isIntegerQuery(self):
        """ Returns true if this is a counting query, e.g., coefficients are integers """
        result = True
        for mat in self.kronFactors():
            if np.abs(mat - np.round(mat)).sum() != 0:
                result = False
        return result

    def domainSize(self):
        """ returns the expected size (number of cells) in the data histogram.

        Note: This should equal the number of columns in the matrix representation
        The default implementation is slow. override this function
        """
        dims = [x.shape[1] if len(x.shape) == 2 else x.shape[0] for x in self.kronFactors()]
        result = np.prod(dims)
        return result


    def numAnswers(self):
        """ returns the number of cells to expect  in the answer

        Note: this should equal the number of rows in the matrix representation
        The default implementation is slow. Override this function
        """
        result = np.prod([x.shape[0] if len(x.shape) == 2 else 1 for x in self.kronFactors()])
        return result


class SparseKronQuery(AbstractLinearQuery):
    """ This class represents queries as a Kron product of sparse matrices """


    def __init__(self, matrices, name=""):
        """ Constructor for SparseKronQuery

        Input:
            matrices: a python list of sparse matrices (preferably in CSR format)
               The query is interpreted as the kron product of the matrices in this list
            name: an optional name for the query
        """
        super().__init__(name)
        self.matrices = matrices
        self.num_columns = np.prod(
            [x.shape[1] if len(x.shape) == 2 else x.shape[0] for x in matrices]
        )
        self.num_rows = np.prod([x.shape[0] if len(x.shape) == 2 else 1 for x in matrices])

    def kronFactors(self):
        return self.matrices

    def domainSize(self):
        return self.num_columns

    def numAnswers(self):
        return self.num_rows


class SumoverQuery(AbstractLinearQuery):
    """ This class provides a more efficient way of answering subset-marginal queries """

    def __init__(self, array_dims, subset=(), add_over_margins=(), name=""):
        """
            This initializes the necessary matrices/arrays.

            Inputs:
                array_dims: the natural dimensions of the data as a multidimensional histogram
                subset: a tuple of lists or iterators (e.g., range() objects)  that should match
                    the number of array dimensions that represent what subset of each dimension
                    we should use to construct the queries e.g ( [0] , range(0,2,1), [1,52,77],
                    range(0,2,1), range(0,63,1), range(0,52,1)).
                add_over_margins: the list of dimensions we will be summing over. None or empty
                    list or empty tuple imply no summation
        """
        super().__init__(name)
        self.array_dims = tuple(array_dims)
        if subset in (None, (), [], False):
            self.subset = np.ix_(*(range(self.array_dims[x]) for x in range(len(self.array_dims))))
            self.subset_input = tuple(
                range(self.array_dims[x]) for x in range(len(self.array_dims))
            )
        else:
            self.subset = np.ix_(*tuple(subset))
            self.subset_input = tuple(subset)
        if add_over_margins in ([], None):
            self.add_over_margins = ()
        else:
            self.add_over_margins = tuple(add_over_margins)
        self.num_columns = np.prod(self.array_dims)
        self.num_rows = np.prod(
            [len(x) for (i, x) in enumerate(self.subset_input) if i not in self.add_over_margins]
        )

    def domainSize(self):
        return self.num_columns


    def numAnswers(self):
        return self.num_rows

    def kronFactors(self):
        """
            This computes the Kronecker representation of the query.
        """
        matrices = [None] * len(self.array_dims)
        for i in range(len(self.array_dims)):
            if i in self.add_over_margins:
                x = np.zeros(self.array_dims[i], dtype=bool)
                x[self.subset_input[i]] = True
            else:
                full = ss.identity(self.array_dims[i], dtype=bool, format="csr")
                x = full[self.subset_input[i], :]
            matrices[i] = x
        return matrices

    def answer(self, data):
        dsize = self.domainSize()
        assert data.size == dsize, "Data does not match except cell count of {}".format(dsize)
        return data[self.subset].sum(axis=self.add_over_margins, keepdims=False).flatten()

class SumOverGroupedQuery(AbstractLinearQuery):
    """ This class provides a more efficient way of answering grouped/collapsed-marginal queries """

    def __init__(self, array_dims, groupings=None, add_over_margins=(), name=""):
        """
            This initializes the necessary matrices/arrays.

            Inputs:
                array_dims: the natural dimensions of the data as a multidimensional histogram

                groupings: this replaces the functionality of subsetting and additionally allows
                            to collapse levels of a dimension into one or more groupings.
                            The argument "groupings" is a dictionary in which the keys are the
                            dimensions (or axes) over which levels should be grouped and
                            the values are each a list containing 1 or more possible groupings
                            which themselves are represented as a list.  For example:

                            groupings = {
                            1: [[1],],
                            3: [range(0, 6), range(6, 21), range(21, 41),
                                range(41, 56), range(56, 62), range(62,63)]
                            }

                            Assume array_dims in this case is (8,2,2,63). This groupings argument
                            would take dimension/axis 1 and form a grouping with single level (1),
                            which is equivalent to subseting this dimension. In the dimension/axis
                            3 it will create 6 groupings that collapse the 63 levels into the
                            corresponding 6 groups by the range arguments listed.

                add_over_margins: the list of dimensions we will be summing over. None or empty
                    list or empty tuple imply no summation
        """
        super().__init__(name)
        self.array_dims = tuple(array_dims)
        self.groupings = groupings if isinstance(groupings, dict) else {}
        self.collapse_axes = list(self.groupings.keys())
        if add_over_margins in ([], None):
            self.add_over_margins = ()
        else:
            self.add_over_margins = tuple(add_over_margins)
        self.num_columns = np.prod(self.array_dims)
        nun_rows_temp = []
        for i in range(len(self.array_dims)):
            if i in self.add_over_margins:
                x = 1
            elif i in self.collapse_axes:
                x = len(self.groupings[i])
            else:
                x = self.array_dims[i]
            nun_rows_temp.append(x)
        self.num_rows = np.prod(nun_rows_temp)

    def domainSize(self):
        return self.num_columns

    def numAnswers(self):
        return self.num_rows

    def kronFactors(self):
        """
            This computes the Kronecker representation of the query.
        """
        matrices = [None] * len(self.array_dims)
        for i in range(len(self.array_dims)):
            if i in self.add_over_margins:
                x = np.ones(self.array_dims[i], dtype=bool)
            elif i in self.collapse_axes:
                n_groups = len(self.groupings[i])
                x = np.zeros((n_groups, self.array_dims[i]), dtype=bool)
                for j in range(n_groups):
                    subset = self.groupings[i][j]
                    x[j, subset] = True
            else:
                x = ss.identity(self.array_dims[i], dtype=bool, format="csr")
            matrices[i] = x
        return matrices

    def collapseLevels(self, data):
        """
        This function is used to iteratively collapse a single axis at a time
        in order to compute the answer to a query

        inputs:
                data: a numpy multi-array
        outputs:
                a numpy multi-array
        """
        #if there is no collapse_axes, just return the array
        result = None
        if self.collapse_axes == []:
            result = data
        else:
            x = data
            #iteratively call the collapse function for each grouping
            for i in self.collapse_axes:
                x = self.collapse(x, i, self.groupings[i])
            result = x
        return result

    @staticmethod
    def collapse(array, axis=None, groups=None):
        """
        This function collapses (adds over) groups of levels in a
        particular dimension of a multi-array

        Inputs:
            array: a numpy multiarray
            axis: the dimension/axis to be grouped (int)
            groups: a list of lists pertaining to the levles to be grouped together
        Output:
            a numpy multiarray
        """
        result = None
        if axis in (None, []) or groups in (None, []):
            assert False, "axis {} {}".format(axis, groups)
            result = array
        else:
            array_dims = array.shape
            n_groups = len(groups)
            #what are all the levels for the particular dimension
            subset = [range(x) for x in array_dims]
            #take a subset for each grouped
            #and append them to a list
            mylist = []
            for i in range(n_groups):
                subset[axis] = groups[i]
                mylist.append(array[np.ix_(*tuple(subset))].sum(axis=axis, keepdims=False))
            #finally stack the sub-arrays
            out = np.stack(mylist, axis=axis)
            result = out
        return result

    def answer(self, data):
        dsize = self.domainSize()
        assert data.size == dsize, "Data does not match except cell count of {}".format(dsize)
        return self.collapseLevels(data).sum(axis=self.add_over_margins, keepdims=False).flatten()

class StackedQuery(AbstractLinearQuery):
    """ This class stacks queries together """

    def __init__(self, queries, domain_subset=None, name=""):
        """ Constructs a stacked query

        Inputs:
            queries: a python list of queries with the same domain size
            domain_subset: a list of indices within the range of domain size
               or None if we want to keep the original domain
            name: an optional name for the query

        Note: this creates a stacked version of the queries over the specified
            Suppose we are stacking two queries with matrix representations A
            and B with dimensions nxd and mxd respectively. Then the new query
            has an (n+m)xd  matrix representation where the first n rows come
            from A then the next m come from B. If domain_subset is specified,
            it reduces the number of columns. For example if domain_subset=[1,3]
            then the matrix representation becomes (n+m)x2 and the top nx2 part of
            the matrix consists of A[:, [1,3]] (i.e. columns 1 and 3) and the bottom
            part consists of B[:, [1,3]]
        """
        super().__init__(name)
        if domain_subset is None:
            self.matrix_rep = ss.vstack([q.matrixRep() for q in queries], format="csr")
        else:
            self.matrix_rep = ss.vstack(
                [x.matrixRep()[:, domain_subset] for x in queries],
                format="csr")

    def kronFactors(self):
        return [self.matrix_rep]


class QueryFactory:
    """Class for instantiaing queries.

    Subclasses of AbstractLinearQuery should be constructed only by functions in this class.
    The job of the QueryFactory is to decide which class to use to represent a desired query

    """

    @staticmethod
    def makeKronQuery(kron_factors, name=""):
        """ Create a kron query """
        query = SparseKronQuery(kron_factors, name)
        return query

    @staticmethod
    def makeTabularQuery(array_dims, subset=(), add_over_margins=(), name=""):
        """ Creates a linear marginal query """
        query = SumoverQuery(array_dims, subset, add_over_margins, name)
        return query

    @staticmethod
    def makeTabularGroupQuery(array_dims, groupings=None, add_over_margins=(), name=""):
        """ Creates a linear query """
        myarray_dims = tuple(array_dims)
        for x in myarray_dims:
            assert isinstance(x, int), "array_dims {} must be ints".format(myarray_dims)
        if add_over_margins in ([], None):
            myadd_over_margins = ()
        else:
            for x in add_over_margins:
                #check that x is int and in range
                assert x in range(len(array_dims)), (
                    "add_over_margins {} must be take values "
                    " in {}").format(add_over_margins, range(len(array_dims)))
            #check that we don't get any duplicate dims
            assert len(add_over_margins) == len(set(add_over_margins)), (
                "add_over_margins {} has duplicate dims").format(add_over_margins)
            #passed checks, set myadd_over_margins
            myadd_over_margins = tuple(add_over_margins)
        if groupings in (None, (), [], False, {}):
            query = SumOverGroupedQuery(array_dims=myarray_dims,
                                        add_over_margins=myadd_over_margins, name=name)
        else:
            assert isinstance(groupings, dict), "groupings must be a dictionary"
            keys = list(groupings.keys())
            for x in keys:
                assert isinstance(x, int), "groupings keys must be integers"
            #check that the intersection is empty
            assert not set(keys).intersection(set(myadd_over_margins)), (
                "add_over_margins {} and groupings {} overlap").format(myadd_over_margins, keys)
            # if so set groupings
            mygroupings = groupings
            query = SumOverGroupedQuery(array_dims=myarray_dims, groupings=mygroupings,
                                        add_over_margins=myadd_over_margins, name=name)
        return query
