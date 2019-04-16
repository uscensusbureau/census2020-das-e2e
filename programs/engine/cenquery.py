import numpy as np
import scipy.sparse as ss
import programs.engine.querybase as querybase

class Query:
    """ 
        This Class is intended to be used to generally describe any query or set of queries.
        It takes up to six arguments:
   
        Inputs:
            array_dims: a tuple that has the dimensions of the array we are describing e.g (17,2,116,2,63), (3,2,255)
            
            subset: a tuple of lists or range() objects that should match the number of array dimensions that represent
                    what subset of the array we should use to construct the queries e.g ( [0] , range(0,2,1), [1,52,77],
                    range(0,2,1), range(0,63,1), range(0,52,1)). This will be turned into a an np.ix_ object.
                    
            axis_groupings: If instead of subseting certain levels of an array dimension, instead we want to collapse(sum) them,
                    we can do this using axis_groupings which is a list of tuples consisting of two objects.
                    First the axis for which to collapse and second the groupings.  For example 
                    say that we had an array with shape (2,5,23,3). The following axis_groupings
                    would combine the 0th and first levels of the first dimension,
                    the second, third, and fourth level of the first dimension,
                    the 0:19th levles of the 2nd dimension and the 20:22 levels of the 2nd dimension:
                    
                    axis_groupings = [ (1, ([0,1],[2,3,4])), (2, (range(20),range(20,23) ))]
                     
                    Only use for a dimension in which we do not subset (shares some similar functionality with subset)
                    
            add_over_margins: which margins (dimensions) should we sum over to construct the query/constraint(s) e.g. (0,1,2)
                    It also has attribute self.subset_input which preserves what is input as the subset (rather
                    than the np.ix_object).
                    
            weight_array: a numpy array that weights the cells of the data for the query.  The array must have dimensions
                          equal to array_dims.  The weight will be incorporated into the query answer in both the
                          answer_original and answer_kron functions.  The later is by incorporating the weight_array into
                          the kronecker representation of the query.  By default (weight_array = None), the array consists
                          of all 1's
                    
            name: give your query a name if you want
    
        Function Calls:
            It also has 4 functions which can be called:
                    answer_original: Given a dataset of the correct dimension, this computes the query answer over the dataset
                                     using the numpy representation.
                                     
                    compute_kron: This computes the Kronecker representation of the query.
                    
                    answer_kron: Given a dataset of the correct dimension (and flattened), this computes the query answer
                                 using the Kronecker representation.
                                 
                    answer: This answers the query using the original or Kronecker representation depending on the shape of the input data.
    """
    
    def __init__(self, array_dims, subset=(), axis_groupings = (), add_over_margins= (), weight_array = None, name = ""):
        """
            This initializes the necessary matrices/arrays.

            Inputs:
                self: a self object
                array_dims: dimensions needed to set up the array
                subset: information needed to subset the necessary matrices
                axis_groupings:
                add_over_margins: information needed to figure out which margins to add over
        """
        self.name = name
        self.array_dims = tuple(array_dims)
        if subset in (None, (), [], False) :
            self.subset = np.ix_(*(range(self.array_dims[x]) for x in range(len(self.array_dims))) )
            self.subset_input = tuple(range(self.array_dims[x]) for x in range(len(self.array_dims)))
        else:
            self.subset = np.ix_(*tuple(subset))
            self.subset_input = tuple(subset)
        self.kron_rep = None
        if add_over_margins in ([] , None):
            self.add_over_margins = ()
        else:
            self.add_over_margins = tuple(add_over_margins)
        if axis_groupings in (None, (), [], False):
            self.axis_groupings = []
        else:
            self.axis_groupings = list(axis_groupings)
            
        if weight_array is None:
            self.weight_array = None
        else:
            if weight_array.shape == self.array_dims:
                self.weight_array = weight_array
            else:
                raise Exception("weight_array must have the same shape as array_dims.  weight_array.shape {} array_dims {}".format(weight_array.shape, self.array_dims))
            
    def __repr__(self):
        """
            This gives a representation of the self object, the only input.
            Output consists of query information (array_dims, subset, add_over_margins).
        """
        
        output = ''
        output += "------------- query looks like -----------------" + '\n'
        output += "name: " + str(self.name) + '\n'
        output += "array_dims: " + str(self.array_dims) + '\n'
        output += "subset: " + str(self.subset_input) + '\n'
        output += "add_over_margins: " + str(self.add_over_margins) + '\n'
        output += "axis_groupings: " + str(self.axis_groupings) + '\n'
        output += "-----------------------------------------------------" + '\n'
        return output
        
    
    def answer(self, data, flatten=False):
        """
            This answers the query using the original or Kronecker representation
            depending on the shape of the input data.
        """
        
        if data.shape == self.array_dims: # Use original representation.
            result = self.answer_original(data)
            return result.flatten() if flatten else result
        elif np.prod(self.array_dims) == data.size: # Use Kronecker representation.
            return self.answer_kron(data)
        else:
            raise Exception("Query shape: {}, data shape: {}".format(self.array_dims, data.shape))
        
    def answer_original(self, numpy_data):
        """
            Given a dataset of the correct dimension, this computes the query answer over the dataset
            using the numpy representation.
        """
        if self.weight_array is None:
            if self.add_over_margins is ():
                return collapseLevels(numpy_data[self.subset], self.axis_groupings)
            else:
                return collapseLevels(numpy_data[self.subset], self.axis_groupings).sum(axis = self.add_over_margins)
        else:
            if self.add_over_margins is ():
                return collapseLevels(np.multiply(numpy_data, self.weight_array)[self.subset], self.axis_groupings)
            else:
                return collapseLevels(np.multiply(numpy_data, self.weight_array)[self.subset], self.axis_groupings).sum(axis = self.add_over_margins)
    
    def compute_kron(self):
        """
            This computes the Kronecker representation of the query.
        """
        
        collapse_axes = [x[0] for x in self.axis_groupings]
        all_collapse_groups = [x[1] for x in self.axis_groupings]
        
        if self.kron_rep is None:
            out = ss.identity(1, dtype=bool, format="csr")
            for i in range(len(self.array_dims)):
                if i in self.add_over_margins:
                    x = np.zeros(self.array_dims[i], dtype=bool)
                    x[self.subset_input[i]] = True
                elif i in collapse_axes:
                    ind = [x for x in range(len(collapse_axes)) if collapse_axes[x] == i]
                    collapse_groups = all_collapse_groups[ind[0]]
                    n_groups = len(collapse_groups)
                    x = np.zeros((n_groups,self.array_dims[i]), dtype=bool)
                    for j in range(n_groups):
                        subset = collapse_groups[j]
                        x[j,subset] = True 
                else:
                    x = ss.identity(self.array_dims[i], dtype=bool, format="csr")[self.subset_input[i], :]
                out = ss.kron(out, x, format="csr")
            #matrix multiplication by the weight matrix
            if self.weight_array is not None:
                out = out * ss.csr_matrix(np.diag(self.weight_array.flatten()))  
            self.kron_rep = out
    
    def answer_kron(self, data):
        """
            Given a dataset of the correct dimension (and flattened), this computes the query answer
            using the Kronecker representation.
        """
        
        self.compute_kron()
        return self.kron_rep.dot(data)

    def convertToQuerybase(self):
        """
        """
        groupings = {}
        if self.axis_groupings:
            collapse_axes = [x[0] for x in self.axis_groupings]
            all_collapse_groups = [x[1] for x in self.axis_groupings]
            for i in range(len(collapse_axes)):
                groupings[collapse_axes[i]] = all_collapse_groups[i]
        
        sub_all = tuple(range(self.array_dims[x]) for x in range(len(self.array_dims)))
        ind = [self.subset_input[i] == sub_all[i] for i in range(len(sub_all))] 
        if np.prod(ind) != 1:
            f_index = [index for index,value in enumerate(ind) if value==False]
            for i in f_index:
                groupings[i] = [[x] for x in self.subset_input[i]]
        lin_query = querybase.QueryFactory.makeTabularGroupQuery(array_dims=self.array_dims, groupings=groupings,
                             add_over_margins=self.add_over_margins, name=self.name)
        return lin_query

class Constraint:
    """
    This class combines the Query class with a right hand side (rhs) and a sign input to create a constraint.
    
    rhs must be a numpy array with shape/size that corresponds to the query.
    sign must be `=`, `ge`, or `le`. All constraints in a given instance must have the same sign.
    
    """

    def __init__(self, query, rhs, sign, name = "", union = False, union_type = None): 
        """
            This initializes the necessary matrices/arrays.

            Inputs:
                self: a self object
                query: a class object of the form cenquery.query
                rhs: a numpy array with the shape/size corresponding to the query
                sign: must be of form mentioned above and all constraints must have this same sign.
                name: string
        """

        if isinstance(query, Query):
            self.query = query
        else:
            raise Exception("Query must be of class cenquery.Query")
        
        answershape=self.query.answer(np.zeros(self.query.array_dims,dtype=bool)).shape
        
        if isinstance(rhs, np.ndarray):
            if rhs.shape == answershape:
                self.rhs = rhs
            else:
                raise Exception("rhs shape must be the same as the query answer, Query shape: {}, data shape: {}".format(rhs.shape, answershape))
        else: 
            raise Exception("rhs must be of class numpy.ndarray")
        
        if sign in ["=","ge","le"]:
            self.sign = sign
        else: 
            raise Exception("sign must be `=`,`ge`, or `le` ")
    
        self.name = name
        self.union = union
        self.union_type = union_type
        
    def check_after_update(self):
        """
            This checks the self object after an update.
        """

        if isinstance(self.rhs, np.ndarray):
            answershape=self.query.answer(np.zeros(self.query.array_dims,dtype=bool)).shape
            if self.rhs.shape == answershape:
                self.rhs = self.rhs
            else:
                raise Exception("rhs shape must be the same as the query answer, Query shape: {}, data shape: {}".format(self.rhs.shape, answershape))
        else: 
            raise Exception("rhs must be of class numpy.ndarray")
        
        if self.sign in ["=","ge","le"]:
            self.sign = self.sign
        else: 
            raise Exception("sign must be `=`,`ge`, or `le` ")
    
    def __repr__(self):
        """
            This checks the representation of the self object.
        """

        output = ''
        output += "------------- constraint looks like -----------------" + '\n'
        output += "Constraint:" + str(self.name) + '\n'
        output += "Constraint contains query: " + str(self.query) + '\n'
        output += "Constraint rhsshape is:" + str(self.rhs.shape) +'\n'
        output += "Constraint contains rhs: " + str(self.rhs) + '\n'
        output += "rhs data type: " + str(self.rhs.dtype) + '\n'
        output += "Constraint contains sign: " + str(self.sign) + '\n'
        output += "-----------------------------------------------------" + '\n'
        return output

    def check(self, data, tol = 0.001):
        """
            This checks that the sign has been properly implemented.
        """

        if self.sign == "=":
            temp = abs(self.query.answer(data) - self.rhs) <= tol
        elif self.sign == "ge":
            temp = self.query.answer(data) + tol >= self.rhs
        elif self.sign == "le":
            temp = self.query.answer(data) - tol <= self.rhs
        else:
            raise ValueError("Sign of the constraint is not 'le' nor 'ge' nor '='.")
        return(temp)

class DPquery:
    """
        This class combines the Query class with a DP answer to the query to be used in future algorithms.
        DPanswer must be a numpy array with shape/size that corresponds to the query.
    """

    def __init__(self, query, DPanswer, epsilon=None, DPmechanism=None, Var=None): 
        """
            This initializes the necessary matrices/arrays.

            Inputs:
                self: a self object
                query: a class object of the form cenquery.query
                DPanswer: the answer to the DP query
                DPmechanism: which mechanism was used (laplace, geometric etc.)
                Var: what is the theoretical variance of the measurement
        """

        if isinstance(query, Query):
            self.query = query
        else:
            raise Exception("Query must be of class cenquery.Query")
        
        answershape=self.query.answer(np.zeros(self.query.array_dims,dtype=bool)).shape
        if isinstance(DPanswer, np.ndarray):
            if DPanswer.shape == answershape:
                self.DPanswer = DPanswer
            else:
                raise Exception("DPanswer shape must be the same as the query answer")
        else: 
            raise Exception("rhs must be of class numpy.ndarray")
        
        self.epsilon = epsilon
        self.DPmechanism = DPmechanism
        self.Var = Var
    
    def check_after_update(self):
        """
            This checks the self object after an update.
        """

        if isinstance(self.query, Query):
            self.query = self.query
        else:
            raise Exception("Query must be of class cenqury.Query")
        
        answershape=self.query.answer(np.zeros(self.query.array_dims,dtype=bool)).shape
        if isinstance(self.DPanswer, np.ndarray):
            if self.DPanswer.shape == answershape:
                self.DPanswer = self.DPanswer
            else:
                raise Exception("DPanswer shape must be the same as the query answer")
        else: 
            raise Exception("rhs must be of class numpy.ndarray")
    
    def __repr__(self):
        """
            This checks the representation of the self object.
        """

        output = ''
        output += "------------- DPquery looks like -----------------" + '\n'
        output += "query: " + str(self.query) + '\n'
        output += "DPanswer shape is:" + str(self.DPanswer.shape) +'\n'
        output += "DPanswer: " + str(self.DPanswer) + '\n'
        output += "epsilon: " + str(self.epsilon) + '\n'
        output += "DPmechanism: " + str(self.DPmechanism) + '\n'
        output += "Variance: " + str(self.Var) + '\n'
        output += "-----------------------------------------------------" + '\n'
        return output


###################
#Helper functions
###################

def collapseLevels(array, axis_groupings = []):
    """
    Description Here
    """
    
    def collapse(array,axis=(),groups=()):
        
        if axis == () or groups == ():
            return array
        else:
            array_dims = array.shape
            n_groups = len(groups)
            subset = [range(x) for x in array_dims]
            
            mylist = []
            for i in range(n_groups):
                subset[axis] = groups[i]
                mylist.append( array[np.ix_(*tuple(subset))].sum(axis=axis, keepdims=False ))
            
            out = np.stack(mylist, axis = axis)
            return out
            
    if axis_groupings == () :
        return array
    else:
        n = len(axis_groupings)
        X = array
        for i in range(n):
            X = collapse(X, axis_groupings[i][0], axis_groupings[i][1])
        
        return X
