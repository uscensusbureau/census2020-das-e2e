## File: gurobic.py
## This is the one place where Gurobi is called

import ctypes
import numpy as np

def setGsoReturnTypes(gso):
   gso.GRBgetenv.restype = ctypes.c_void_p
   gso.GRBgeterrormsg.restype = ctypes.c_char_p

class Environment:
   """ This class is used to manage environments for the c Gurobi wrapper

   Invariants that must be maintained:
     1) if self.env is null then self.isok should return False
   """

   def __init__(self, logfilename, appname, isv, so):
      """ Creates a Gurobi environment 

      params: 
        logfilename: name of log file to give to Gurobi
        appname: app name for Gurobi environment
        isv: a string containing isv for Gurobi environment
        so: a string that is the path to the Gurobi library shared object

      notes:
        sets 2 instance variables
        self.gso: the ctypes object corresponding to the Gurobi c library
        self.env: a pointer to c environment
        after creation, the properties self.status and self.isok are available
      """

      assert type(logfilename) == str
      assert type(appname) == str
      assert type(isv) == str
      self._isok = False
      #store shared object
      self.gso = ctypes.cdll.LoadLibrary(so)
      setGsoReturnTypes(self.gso)
      #put arguments into c types
      logname_c = ctypes.c_char_p(logfilename.encode('utf-8'))
      isv_c = ctypes.c_char_p(isv.encode("utf-8"))
      appname_c = ctypes.c_char_p(appname.encode("utf-8"))
      other_c=ctypes.c_char_p(b"")
      #pointer that will hold the environment
      self.env = ctypes.c_void_p()
      self._status = self.gso.GRBisqp(ctypes.byref(self.env),logname_c,isv_c,appname_c,ctypes.c_int(0),other_c)
      if self.env and self.status == 0:
          self._isok = True

   def free(self):
      """ frees the gGurobi environment """
      if hasattr(self, "env") and self.env:
         self.gso.GRBfreeenv(self.env)
         self._isok = False 
         self.env = None

   @property
   def status(self):
      """ Gurobi return status """
      return self._status

   @property
   def isok(self):
      """ returns True if environment has been successfully created """
      return self._isok and bool(self.env)

   def __del__(self):
      """ frees the Gurobi environment on garbage collection """
      self.free()


class Model:
   def __init__(self, name, env):
      """ Initializes Model class from Environment
      
      Params:
          name: name of the model
          env: the Environment object

      Notes:
          creates a self.env object which is a pointer to the c Gurobi environment
      """
      assert type(name) == str
      assert type(env) == Environment
      # even though a model gets its own copy of the Gurobi environment (per Gurobi docs)
      #   we will get a warning message if the original is garbage collected.
      self._originalenv = env 
      self.strtype = ctypes.c_char * (GRB.GRB_MAX_STRLEN + 1)
      self.gso = env.gso
      name_c = ctypes.c_char_p(name.encode("utf-8"))
      self.model_c  = ctypes.c_void_p()
      self._status  = env.gso.GRBnewmodel(env.env,ctypes.byref(self.model_c), name_c , ctypes.c_int(0), None, None, None, None, None)
      self._isok = (self._status == 0)
      if self.isok:
         self.env = ctypes.c_void_p(self.gso.GRBgetenv(self.model_c))
         if not self.env:
            raise Exception("Error getting model's local copy of environment")

   def getErrorMessage(self):
     mess_c = ctypes.c_char_p(self.gso.GRBgeterrormsg(self.env))
     return str(mess_c.value) 

   @property
   def isok(self):
      """ true if model has not encountered errors during creation """
      return self._isok

   @property
   def status(self):
      """ gurobi status code obtained from model creation """
      return self._status


   def free(self):
      """ frees underlying resources. will be called automatically during garbage collection """
      if self.isok:
         self.gso.GRBfreemodel(self.model_c)
         self._isok = False

   def __del__(self):
      self.free()

   def addVars(self, coefficients, lb=None, ub=None, vtype=None):
      """ adds variables to the model 

      Params:
         coefficients: a numpy array of doubles specifying coefficients of the linear terms of the new variables
         lb: a lower bound on all of them, None will make them 0.0, another example is -GRB.GRB_INFINITY
         ub: an upper bound on all of them, None will make them infinite
         vtype: their type. None will default to GRB.GRB_CONTINUOUS. Other choices are:
            GRB_BINARY and GRB_INTEGER
         
      """
      assert type(coefficients) == np.ndarray
      numvars = coefficients.size
      mycoeff = self.recastAsDoubleArray(coefficients)
      many_doubles_type = ctypes.c_double * numvars
      many_chars_type = ctypes.c_char * numvars
      if lb is None or lb == 0.0:
         lower = None
      else:
         assert type(lower) in [float, int]
         lower = many_doubles_type(lb)
      if ub is None or ub == GRB.GRB_INFINITY:
         upper = None
      else:
         assert type(upper) in [float, int]
         upper = many_doubles_type(ub)
      if vtype is None or vtype == GRB.GRB_CONTINUOUS:
         vartype = None
      else:
         assert vtype in [GRB.GRB_BINARY, GRB.GRB_INTEGER, GRB.GRB_CONTINUOUS]
         vartype = many_char_types(vtype) 
      status = self.gso.GRBaddvars(self.model_c, ctypes.c_int(numvars), ctypes.c_int(0), None, None, None, mycoeff.ctypes, lower, upper, vartype, None)
      if status != 0:
         raise Exception("Cannot add vars: {}".format(status))

   def addConstr(self, ind, coefficients, sense, rhs,name=None):
      """ adds constraint to a model 
    
      Params:
         ind: a numpy array of indexes of variables that appear in the constraint
         coefficients: the coefficients of those variables
         sense: one of the following: GRB.GRB_EQUAL, GRB.GRB_GREATER_EQUAL, GRB.GRB_LESS_EQUAL
         rhs: the right hand side of the constraint
         name: name of the constraint
      """
      numvars = ind.size
      myind = self.recastNPIntArray(ind)
      mycoeff = self.recastAsDoubleArray(coefficients)
      myname = name if name is None else ctypes.c_char_p(name.encode("utf-8"))
      status = self.gso.GRBaddconstr(self.model_c, ctypes.c_int(numvars), myind.ctypes, mycoeff.ctypes, sense, ctypes.c_double(rhs), myname)
      if status != 0:
         raise Exception("Cannot add constraint: {}. Status code={}".format(name, status))

   def addConstrs(self, csr, sense, rhsides):
      """ adds multiple constraints to the model 
     
      params:
         csr: the constraint matrix in csr format
         sense: One of GRB.GRB_EQUAL, GRB.GRB_GREATER_EQUAL, GRB.GRB_LESS_EQUAL
         rhsides: the right hand sides of the constraints

      note that all constraints will share the same sense
      """
      numvars = csr.nnz
      numconstr = csr.shape[0]
      cbeg = self.recastNPIntArray(csr.indptr)
      cind = self.recastNPIntArray(csr.indices)
      cval = self.recastAsDoubleArray(csr.data)
      sensearray = ctypes.c_char_p(sense.value * numconstr) #create a string of senses
      rhs = self.recastAsDoubleArray(rhsides)
      status = self.gso.GRBaddconstrs(self.model_c, ctypes.c_int(numconstr), ctypes.c_int(numvars), cbeg.ctypes, cind.ctypes, cval.ctypes, sensearray, rhs.ctypes, None)
      if status != 0:
         raise Exception("Cannot add constraints. Status code: {}. {}".format(status, self.getErrorMessage()))

   def intType(self):
      """ returns the numpy int type having same size as the c int type """
      s = ctypes.sizeof(ctypes.c_int)
      if s == 8:
         thetype = np.int64
      elif s == 4:
         thetype = np.int32
      else:
         raise Exception("Unsupported int size: {}".format(s))
      return thetype 

   def recastNPIntArray(self, arr):
      """ Converts np arrays to the int size that is used for c """
      thetype  = self.intType()
      result = arr if arr.dtype == thetype else arr.astype(thetype)
      return result

   def recastAsDoubleArray(self, arr):
      """ converts an array into a double array unless it already is one """
      result = arr if arr.dtype == np.float64 else arr.astype(np.float64)
      return result

   def addQPTerms(self, rows, cols, values):
      """ adds quadratic programming terms to the objective 
 
      Each term in the qp matrix is specified by a row, column, and value.
      If a term exists in this row and column, the value is added to the current
      coefficient. Row i col j is equal to row j col i.
     
      Params:
        rows: an ndarray of ints
        cols: an ndarray of ints
        values: an ndarray with dtype float64
        
      """
      assert type(rows) == np.ndarray and type(cols) == np.ndarray and type(values) == np.ndarray
      rightrows = self.recastNPIntArray(rows)
      rightcols = self.recastNPIntArray(cols)
      myrows = rightrows.ctypes
      mycols = rightcols.ctypes
      rightvalues = self.recastAsDoubleArray(values)
      myvalues = rightvalues.ctypes
      numnz = ctypes.c_int(values.size)
      status = self.gso.GRBaddqpterms(self.model_c, numnz,  myrows, mycols, myvalues)
      if status != 0:
         raise Exception("Cannot add qp terms: {}".format(status))

   def optimize(self):
      """ starts the optimization """
      status = self.gso.GRBoptimize(self.model_c)
      if status != 0:
         raise Exception("Problem during optimization: {}".format(status))

   def update(self):
      """ updates the model gue to Gurobi lazy  updates """
      status = self.gso.GRBupdatemodel(self.model_c)
      if status != 0:
         raise Exception("Error updating model: {}".format(status))

   def write(self, filename):
      """ writes out the gurobi model """
      status = self.gso.GRBwrite(self.model_c, ctypes.c_char_p(filename.encode("utf-8")))
      if status != 0:
         raise Exception("Cannot save model in file:{} code: {}".format(filename,status))

   def X(self, start, length):
      """ returns the solution array starting from start """
      return self._getDblAttrArray(GRB.GRB_DBL_ATTR_X, start, length)

   def objVal(self):
      """ returns the objective value """
      return self._getDblAttr(GRB.GRB_DBL_ATTR_OBJVAL)

   def optStatus(self):
      """ returns the optimization status """
      return self._getIntAttr(GRB.GRB_INT_ATTR_STATUS)

   def isOptimal(self):
      "returns true if optimal value was found """
      return self.optStatus() == GRB.GRB_OPTIMAL

   #######################################################
   ### Helper functions for getting/setting attributes ###
   #######################################################
   def _getIntAttr(self, name):
      """ gets an int attribute for model. DO NOT CALL IT YOURSELF 
      
      Params:
         name: a ctypes.c_char_p
      """
      result = ctypes.c_int()
      status = self.gso.GRBgetintattr(self.model_c, name, ctypes.byref(result))
      if status != 0:
         raise Exception("Error code {} when reading GRB attr {}".format(status, name.value))
      return int(result.value)


   def _setIntAttr(self, name, value):
      """ sets an int attribute for model. DO NOT CALL IT YOURSELF
      
      Params
         name: a ctypes.c_char_p
         value: an int
      """
      value_c = ctypes.c_int(value)
      status = self.gso.GRBsetintattr(self.model_c, name, value_c)
      if status != 0:
         raise Exception("Error {}. Could not set Gurobi attr {} to {}".format(status, name.value, value))


   def _getIntAttrArray(self, name, start, length):
      """ gets an int attribute array for model. DO NOT CALL IT YOURSELF 
      
      Params:
         name: a ctypes.c_char_p
         start: starting index
         length: how many elements to get starting from start
      """
      myarray = np.zeros(length, dtype=self.intType())
      result = myarray.ctypes
      status = self.gso.GRBgetintattr(self.model_c, name, ctypes.c_int(start), ctypes.c_int(length), result)
      if status != 0:
         raise Exception("Error code {} when reading GRB attr {} array(start:{}, len:{})".format(status, name.value ,start, length))
      return myarray


   def _getDblAttr(self, name):
      """ gets a double attribute for model. DO NOT CALL IT YOURSELF 
      
      Params:
         name: a ctypes.c_char_p
      """
      result = ctypes.c_double()
      status = self.gso.GRBgetdblattr(self.model_c, name, ctypes.byref(result))
      if status != 0:
         raise Exception("Error code {} when reading GRB attr {}".format(status, name.value))
      return float(result.value)


   def _setDblAttr(self, name, value):
      """ sets a double attribute for model. DO NOT CALL IT YOURSELF
      
      Params
         name: a ctypes.c_char_p
         value: a double
      """
      value_c = ctypes.c_double(value)
      status = self.gso.GRBsetdblattr(self.model_c, name, value_c)
      if status != 0:
         raise Exception("Error {}. Could not set Gurobi param {} to {}".format(status, name.value, value))


   def _getDblAttrArray(self, name, start, length):
      """ gets an double attribute array for model. DO NOT CALL IT YOURSELF 
      
      Params:
         name: a ctypes.c_char_p
         start: starting index
         length: how many elements to get starting from start
      """
      myarray = np.zeros(length, dtype=np.float64)
      status = self.gso.GRBgetdblattrarray(self.model_c, name, ctypes.c_int(start), ctypes.c_int(length), myarray.ctypes)
      if status != 0:
         raise Exception("Error code {} when reading GRB attr {} array(start:{}, len:{})".format(status, name.value, start, length))
      return myarray


   ###################################################
   ### Helper functions for getting/setting params ###
   ###################################################

   def _getStringParam(self, name):
      """ gets a string param from gurobi. DO NOT CALL IT YOURSELF 
      
      Params:
         name: a ctypes.c_char_p
      """
      result = self.strtype()
      status = self.gso.GRBgetstrparam(self.env, name, result)
      if status != 0:
         raise Exception("Error code {} when reading GRB param {}".format(status, name.value))
      return str(result.value)

   def _setStringParam(self, name, value):
      """ sets a string param from gurobi. DO NOT CALL IT YOURSELF
      
      Params
         name: a ctypes.c_char_p
         value: a python str
      """
      assert type(value) == str
      assert len(value) <= GRB.GRB_MAX_STRLEN
      value_c = ctypes.c_char_p(value.encode("utf-8"))
      status = self.gso.GRBsetstrparam(self.env, name, value_c)
      if status != 0:
         raise Exception("Error {}. Could not set Gurobi param {} to {}".format(status, name.value, value))

   def _getIntParam(self, name):
      """ gets an int param from gurobi. DO NOT CALL IT YOURSELF 
      
      Params:
         name: a ctypes.c_char_p
      """
      result = ctypes.c_int()
      status = self.gso.GRBgetintparam(self.env, name, ctypes.byref(result))
      if status != 0:
         raise Exception("Error code {} when reading GRB param {}".format(status, name.value))
      return int(result.value)

   def _setIntParam(self, name, value):
      """ sets an int param from gurobi. DO NOT CALL IT YOURSELF
      
      Params
         name: a ctypes.c_char_p
         value: an int
      """
      value_c = ctypes.c_int(value)
      status = self.gso.GRBsetintparam(self.env, name, value_c)
      if status != 0:
         raise Exception("Error {}. Could not set Gurobi param {} to {}".format(status, name.value, value))

   def _getDblParam(self, name):
      """ gets a double param from Gurobi. DO NOT CALL IT YOURSELF 
      
      Params:
         name: a ctypes.c_char_p
      """
      result = ctypes.c_double()
      status = self.gso.GRBgetdblparam(self.env, name, ctypes.byref(result))
      if status != 0:
         raise Exception("Error code {} when reading GRB param {}".format(status, name.value))
      return float(result.value)

   def _setDblParam(self, name, value):
      """ sets an double param from Gurobi. DO NOT CALL IT YOURSELF
      
      Params
         name: a ctypes.c_char_p
         value: a double
      """
      value_c = ctypes.c_double(value)
      status = self.gso.GRBsetdblparam(self.env, name, value_c)
      if status != 0:
         raise Exception("Error {}. Could not set Gurobi param {} to {}".format(status, name.value, value))


   #############################
   #### Sets the model sense ###
   #############################

   def setSenseMaximize(self):
      self._setIntAttr(GRB.GRB_INT_ATTR_MODELSENSE, GRB.GRB_MAXIMIZE)

   def setSenseMinimize(self):
      self._setIntAttr(GRB.GRB_INT_ATTR_MODELSENSE, GRB.GRB_MINIMIZE)

   ########################################
   #### Getters and Setters for Params  ###
   ########################################

   @property
   def LogFile(self):
      return self._getStringParam(GRB.GRB_STR_PAR_LOGFILE)

   @LogFile.setter
   def LogFile(self, val):
      self._setStringParam(GRB.GRB_STR_PAR_LOGFILE, val) 
   
   @property
   def OutputFlag(self):
      return self._getIntParam(GRB.GRB_INT_PAR_OUTPUTFLAG)

   @OutputFlag.setter
   def OutputFlag(self, val):
      self._setIntParam(GRB.GRB_INT_PAR_OUTPUTFLAG, val)

   @property
   def OptimalityTol(self):
      return self._getDblParam(GRB.GRB_DBL_PAR_OPTIMALITYTOL)

   @OptimalityTol.setter
   def OptimalityTol(self, val):
      self._setDblParam(GRB.GRB_DBL_PAR_OPTIMALITYTOL, val)

   @property
   def BarConvTol(self):
      return self._getDblParam(GRB.GRB_DBL_PAR_BARCONVTOL)

   @BarConvTol.setter
   def BarConvTol(self, val):
      self._setDblParam(GRB.GRB_DBL_PAR_BARCONVTOL, val)

   @property
   def BarQCPConvTol(self):
      return self._getDblParam(GRB.GRB_DBL_PAR_BARQCPCONVTOL)

   @BarQCPConvTol.setter
   def BarQCPConvTol(self, val):
      self._setDblParam(GRB.GRB_DBL_PAR_BARQCPCONVTOL, val)

   @property
   def BarIterLimit(self):
      return self._getIntParam(GRB.GRB_INT_PAR_BARITERLIMIT)

   @BarIterLimit.setter
   def BarIterLimit(self, val):
      self._setIntParam(GRB.GRB_INT_PAR_BARITERLIMIT, val)

   @property
   def FeasibilityTol(self):
      return self._getDblParam(GRB.GRB_DBL_PAR_FEASIBILITYTOL)

   @FeasibilityTol.setter
   def FeasibilityTol(self, val):
      self._setDblParam(GRB.GRB_DBL_PAR_FEASIBILITYTOL, val)
   
   @property
   def Threads(self):
      return self._getIntParam(GRB.GRB_INT_PAR_THREADS)

   @Threads.setter
   def Threads(self, val):
      self._setIntParam(GRB.GRB_INT_PAR_THREADS, val)

   @property
   def Presolve(self):
      return self._getIntParam(GRB.GRB_INT_PAR_PRESOLVE)

   @Presolve.setter
   def Presolve(self, val):
      self._setIntParam(GRB.GRB_INT_PAR_PRESOLVE, val)

   @property
   def NumericFocus(self):
      return self._getIntParam(GRB.GRB_INT_PAR_NUMERICFOCUS)

   @NumericFocus.setter
   def NumericFocus(self, val):
      self._setIntParam(GRB.GRB_INT_PAR_NUMERICFOCUS, val)


class GRB:
   """ This class stores Gurobi Constants """

   ###########################
   ### Model attributes #####
   ##########################
   @property
   def GRB_INT_ATTR_MODELSENSE(self):
      return ctypes.c_char_p(b"ModelSense")

   @property
   def GRB_DBL_ATTR_X(self):
      return ctypes.c_char_p(b"X")

   @property 
   def GRB_DBL_ATTR_OBJVAL(self):
      return ctypes.c_char_p(b"ObjVal") 

   @property
   def GRB_INT_ATTR_STATUS(self):
      return ctypes.c_char_p(b"Status")
   ############################
   ##### Constraint senses #####
   ############################

   @property
   def GRB_LESS_EQUAL(self):
      return ctypes.c_char(b'<')
   @property
   def GRB_GREATER_EQUAL(self):
      return ctypes.c_char(b'>')
   @property
   def GRB_EQUAL(self):
      return ctypes.c_char(b'=')

  ###########################
  #####  Variable types #####
  ###########################

   @property
   def GRB_CONTINUOUS(self):
      return ctypes.c_char(b'C')
   @property
   def GRB_BINARY(self):
      return ctypes.c_char(b'B')
   @property
   def GRB_INTEGER(self):
      return ctypes.c_char(b'I')
   @property
   def GRB_SEMICONT(self):
      return ctypes.c_char(b'S')
   @property
   def GRB_SEMIINT(self):
      return ctypes.c_char(b'N')

   ###########################
   ##### Objective sense #####
   ###########################

   @property
   def GRB_MINIMIZE(self):
      return 1
   @property
   def GRB_MAXIMIZE(self):
      return -1

   #########################
   #####  SOS types ########
   #########################

   @property
   def GRB_SOS_TYPE1(self):
      return 1
   @property
   def GRB_SOS_TYPE2(self):
      return 2

   #############################
   ##### Numeric constants #####
   #############################

   @property
   def GRB_INFINITY(self):
      return 1e100
   @property
   def GRB_UNDEFINED(self):
      return 1e101
   @property
   def GRB_MAXINT(self):
      return 2000000000

   ###################
   #####  Lengths ####
   ###################

   @property
   def GRB_MAX_NAMELEN(self):
      return 255

   @property
   def GRB_MAX_STRLEN(self):
      return 512
   @property
   def GRB_MAX_CONCURRENT(self):
      return 64


   ################################### 
   ##### status codes for optimization 
   ################################### 

   @property
   def GRB_LOADED(self):
      return 1
   @property   
   def GRB_OPTIMAL(self):
      return 2
   @property
   def GRB_INFEASIBLE(self):
      return 3
   @property
   def GRB_INF_OR_UNBD(self):
      return 4
   @property
   def GRB_UNBOUNDED(self):
      return 5
   @property
   def GRB_CUTOFF(self):
      return 6
   @property
   def GRB_ITERATION_LIMIT(self):
      return 7
   @property
   def GRB_NODE_LIMIT(self):
      return 8
   @property
   def GRB_TIME_LIMIT(self):
      return 9
   @property
   def GRB_SOLUTION_LIMIT(self):
      return 10
   @property
   def GRB_INTERRUPTED(self):
      return 11
   @property
   def GRB_NUMERIC(self):
      return 12
   @property
   def GRB_SUBOPTIMAL(self):
      return 13
   @property
   def GRB_INPROGRESS(self):
      return 14
   @property
   def GRB_USER_OBJ_LIMIT(self):
      return 15

   ########################
   ##### Basis status info 
   ########################
  
   @property
   def GRB_BASIC(self):
      return 0
   @property
   def GRB_NONBASIC_LOWER(self):
      return -1
   @property
   def GRB_NONBASIC_UPPER(self):
      return -2
   @property
   def GRB_SUPERBASIC(self):
      return -3

   #################
   ##### Termination 
   #################

   @property
   def GRB_INT_PAR_BARITERLIMIT(self):
      return ctypes.c_char_p(b"BarIterLimit")
   @property
   def GRB_DBL_PAR_CUTOFF(self):
      return ctypes.c_char_p(b"Cutoff")
   @property
   def GRB_DBL_PAR_ITERATIONLIMIT(self):
      return ctypes.c_char_p(b"IterationLimit")
   @property
   def GRB_DBL_PAR_NODELIMIT(self):
      return ctypes.c_char_p(b"NodeLimit")
   @property
   def GRB_INT_PAR_SOLUTIONLIMIT(self):
      return ctypes.c_char_p(b"SolutionLimit")
   @property
   def GRB_DBL_PAR_TIMELIMIT(self):
      return ctypes.c_char_p(b"TimeLimit")
   @property
   def GRB_DBL_PAR_BESTOBJSTOP(self):
      return ctypes.c_char_p(b"BestObjStop")
   @property
   def GRB_DBL_PAR_BESTBDSTOP(self):
      return ctypes.c_char_p(b"BestBdStop")

   #################
   ##### Tolerances 
   #################

   @property
   def GRB_DBL_PAR_FEASIBILITYTOL(self):
      return ctypes.c_char_p(b"FeasibilityTol")
   @property
   def GRB_DBL_PAR_INTFEASTOL(self):
      return ctypes.c_char_p(b"IntFeasTol")
   @property
   def GRB_DBL_PAR_MARKOWITZTOL(self):
      return ctypes.c_char_p(b"MarkowitzTol")
   @property
   def GRB_DBL_PAR_MIPGAP(self):
      return ctypes.c_char_p(b"MIPGap")
   @property
   def GRB_DBL_PAR_MIPGAPABS(self):
      return ctypes.c_char_p(b"MIPGapAbs")
   @property
   def GRB_DBL_PAR_OPTIMALITYTOL(self):
      return ctypes.c_char_p(b"OptimalityTol")
   @property
   def GRB_DBL_PAR_PSDTOL(self):
      return ctypes.c_char_p(b"PSDTol")

   ##############
   ##### Simplex 
   ##############

   @property
   def GRB_INT_PAR_METHOD(self):
      return ctypes.c_char_p(b"Method")
   @property
   def GRB_DBL_PAR_PERTURBVALUE(self):
      return ctypes.c_char_p(b"PerturbValue")
   @property
   def GRB_DBL_PAR_OBJSCALE(self):
      return ctypes.c_char_p(b"ObjScale")
   @property
   def GRB_INT_PAR_SCALEFLAG(self):
      return ctypes.c_char_p(b"ScaleFlag")
   @property
   def GRB_INT_PAR_SIMPLEXPRICING(self):
      return ctypes.c_char_p(b"SimplexPricing")
   @property
   def GRB_INT_PAR_QUAD(self):
      return ctypes.c_char_p(b"Quad")
   @property
   def GRB_INT_PAR_NORMADJUST(self):
      return ctypes.c_char_p(b"NormAdjust")
   @property
   def GRB_INT_PAR_SIFTING(self):
      return ctypes.c_char_p(b"Sifting")
   @property
   def GRB_INT_PAR_SIFTMETHOD(self):
      return ctypes.c_char_p(b"SiftMethod")

   ##############
   ##### Barrier 
   ##############

   @property
   def GRB_DBL_PAR_BARCONVTOL(self):
      return ctypes.c_char_p(b"BarConvTol")
   @property
   def GRB_INT_PAR_BARCORRECTORS(self):
      return ctypes.c_char_p(b"BarCorrectors")
   @property
   def GRB_INT_PAR_BARHOMOGENEOUS(self):
      return ctypes.c_char_p(b"BarHomogeneous")
   @property
   def GRB_INT_PAR_BARORDER(self):
      return ctypes.c_char_p(b"BarOrder")
   @property
   def GRB_DBL_PAR_BARQCPCONVTOL(self):
      return ctypes.c_char_p(b"BarQCPConvTol")
   @property
   def GRB_INT_PAR_CROSSOVER(self):
      return ctypes.c_char_p(b"Crossover")
   @property
   def GRB_INT_PAR_CROSSOVERBASIS(self):
      return ctypes.c_char_p(b"CrossoverBasis")

   ##########
   ##### MIP
   ##########
   
   @property
   def GRB_INT_PAR_BRANCHDIR(self):
      return ctypes.c_char_p(b"BranchDir")
   @property
   def GRB_INT_PAR_DEGENMOVES(self):
      return ctypes.c_char_p(b"DegenMoves")
   @property
   def GRB_INT_PAR_DISCONNECTED(self):
      return ctypes.c_char_p(b"Disconnected")
   @property
   def GRB_DBL_PAR_HEURISTICS(self):
      return ctypes.c_char_p(b"Heuristics")
   @property
   def GRB_DBL_PAR_IMPROVESTARTGAP(self):
      return ctypes.c_char_p(b"ImproveStartGap")
   @property
   def GRB_DBL_PAR_IMPROVESTARTTIME(self):
      return ctypes.c_char_p(b"ImproveStartTime")
   @property
   def GRB_DBL_PAR_IMPROVESTARTNODES(self):
      return ctypes.c_char_p(b"ImproveStartNodes")
   @property
   def GRB_INT_PAR_MINRELNODES(self):
      return ctypes.c_char_p(b"MinRelNodes")
   @property
   def GRB_INT_PAR_MIPFOCUS(self):
      return ctypes.c_char_p(b"MIPFocus")
   @property
   def GRB_STR_PAR_NODEFILEDIR(self):
      return ctypes.c_char_p(b"NodefileDir")
   @property
   def GRB_DBL_PAR_NODEFILESTART(self):
      return ctypes.c_char_p(b"NodefileStart")
   @property
   def GRB_INT_PAR_NODEMETHOD(self):
      return ctypes.c_char_p(b"NodeMethod")
   @property
   def GRB_INT_PAR_NORELHEURISTIC(self):
      return ctypes.c_char_p(b"NoRelHeuristic")
   @property
   def GRB_INT_PAR_PUMPPASSES(self):
      return ctypes.c_char_p(b"PumpPasses")
   @property
   def GRB_INT_PAR_RINS(self):
      return ctypes.c_char_p(b"RINS")
   @property
   def GRB_INT_PAR_STARTNODELIMIT(self):
      return ctypes.c_char_p(b"StartNodeLimit")
   @property
   def GRB_INT_PAR_SUBMIPNODES(self):
      return ctypes.c_char_p(b"SubMIPNodes")
   @property
   def GRB_INT_PAR_SYMMETRY(self):
      return ctypes.c_char_p(b"Symmetry")
   @property
   def GRB_INT_PAR_VARBRANCH(self):
      return ctypes.c_char_p(b"VarBranch")
   @property
   def GRB_INT_PAR_SOLUTIONNUMBER(self):
      return ctypes.c_char_p(b"SolutionNumber")
   @property
   def GRB_INT_PAR_ZEROOBJNODES(self):
      return ctypes.c_char_p(b"ZeroObjNodes")

   ###############
   ##### MIP cuts 
   ###############

   @property
   def GRB_INT_PAR_CUTS(self):
      return ctypes.c_char_p(b"Cuts")

   @property
   def GRB_INT_PAR_CLIQUECUTS(self):
      return ctypes.c_char_p(b"CliqueCuts")
   @property
   def GRB_INT_PAR_COVERCUTS(self):
      return ctypes.c_char_p(b"CoverCuts")
   @property
   def GRB_INT_PAR_FLOWCOVERCUTS(self):
      return ctypes.c_char_p(b"FlowCoverCuts")
   @property
   def GRB_INT_PAR_FLOWPATHCUTS(self):
      return ctypes.c_char_p(b"FlowPathCuts")
   @property
   def GRB_INT_PAR_GUBCOVERCUTS(self):
      return ctypes.c_char_p(b"GUBCoverCuts")
   @property
   def GRB_INT_PAR_IMPLIEDCUTS(self):
      return ctypes.c_char_p(b"ImpliedCuts")
   @property
   def GRB_INT_PAR_PROJIMPLIEDCUTS(self):
      return ctypes.c_char_p(b"ProjImpliedCuts")
   @property
   def GRB_INT_PAR_MIPSEPCUTS(self):
      return ctypes.c_char_p(b"MIPSepCuts")
   @property
   def GRB_INT_PAR_MIRCUTS(self):
      return ctypes.c_char_p(b"MIRCuts")
   @property
   def GRB_INT_PAR_STRONGCGCUTS(self):
      return ctypes.c_char_p(b"StrongCGCuts")
   @property
   def GRB_INT_PAR_MODKCUTS(self):
      return ctypes.c_char_p(b"ModKCuts")
   @property
   def GRB_INT_PAR_ZEROHALFCUTS(self):
      return ctypes.c_char_p(b"ZeroHalfCuts")
   @property
   def GRB_INT_PAR_NETWORKCUTS(self):
      return ctypes.c_char_p(b"NetworkCuts")
   @property
   def GRB_INT_PAR_SUBMIPCUTS(self):
      return ctypes.c_char_p(b"SubMIPCuts")
   @property
   def GRB_INT_PAR_INFPROOFCUTS(self):
      return ctypes.c_char_p(b"InfProofCuts")

   @property
   def GRB_INT_PAR_CUTAGGPASSES(self):
      return ctypes.c_char_p(b"CutAggPasses")
   @property
   def GRB_INT_PAR_CUTPASSES(self):
      return ctypes.c_char_p(b"CutPasses")
   @property
   def GRB_INT_PAR_GOMORYPASSES(self):
      return ctypes.c_char_p(b"GomoryPasses")

   ###########################
   ### Distributed algorithms 
   ##########################

   @property
   def GRB_STR_PAR_WORKERPOOL(self):
      return ctypes.c_char_p(b"WorkerPool")
   @property
   def GRB_STR_PAR_WORKERPASSWORD(self):
      return ctypes.c_char_p(b"WorkerPassword")
   @property
   def GRB_INT_PAR_WORKERPORT(self):
      return ctypes.c_char_p(b"WorkerPort")

   ############
   ##### Other 
   ############

   @property
   def GRB_INT_PAR_AGGREGATE(self):
      return ctypes.c_char_p(b"Aggregate")
   @property
   def GRB_INT_PAR_AGGFILL(self):
      return ctypes.c_char_p(b"AggFill")
   @property
   def GRB_INT_PAR_CONCURRENTMIP(self):
      return ctypes.c_char_p(b"ConcurrentMIP")
   @property
   def GRB_INT_PAR_CONCURRENTJOBS(self):
      return ctypes.c_char_p(b"ConcurrentJobs")
   @property
   def GRB_INT_PAR_DISPLAYINTERVAL(self):
      return ctypes.c_char_p(b"DisplayInterval")
   @property
   def GRB_INT_PAR_DISTRIBUTEDMIPJOBS(self):
      return ctypes.c_char_p(b"DistributedMIPJobs")
   @property
   def GRB_INT_PAR_DUALREDUCTIONS(self):
      return ctypes.c_char_p(b"DualReductions")
   @property
   def GRB_DBL_PAR_FEASRELAXBIGM(self):
      return ctypes.c_char_p(b"FeasRelaxBigM")
   @property
   def GRB_INT_PAR_IISMETHOD(self):
      return ctypes.c_char_p(b"IISMethod")
   @property
   def GRB_INT_PAR_INFUNBDINFO(self):
      return ctypes.c_char_p(b"InfUnbdInfo")
   @property
   def GRB_INT_PAR_LAZYCONSTRAINTS(self):
      return ctypes.c_char_p(b"LazyConstraints")
   @property
   def GRB_STR_PAR_LOGFILE(self):
      return ctypes.c_char_p(b"LogFile")
   @property
   def GRB_INT_PAR_LOGTOCONSOLE(self):
      return ctypes.c_char_p(b"LogToConsole")
   @property
   def GRB_INT_PAR_MIQCPMETHOD(self):
      return ctypes.c_char_p(b"MIQCPMethod")
   @property
   def GRB_INT_PAR_NUMERICFOCUS(self):
      return ctypes.c_char_p(b"NumericFocus")
   @property
   def GRB_INT_PAR_OUTPUTFLAG(self):
      return ctypes.c_char_p(b"OutputFlag")
   @property
   def GRB_INT_PAR_PRECRUSH(self):
      return ctypes.c_char_p(b"PreCrush")
   @property
   def GRB_INT_PAR_PREDEPROW(self):
      return ctypes.c_char_p(b"PreDepRow")
   @property
   def GRB_INT_PAR_PREDUAL(self):
      return ctypes.c_char_p(b"PreDual")
   @property
   def GRB_INT_PAR_PREPASSES(self):
      return ctypes.c_char_p(b"PrePasses")
   @property
   def GRB_INT_PAR_PREQLINEARIZE(self):
      return ctypes.c_char_p(b"PreQLinearize")
   @property
   def GRB_INT_PAR_PRESOLVE(self):
      return ctypes.c_char_p(b"Presolve")
   @property
   def GRB_DBL_PAR_PRESOS1BIGM(self):
      return ctypes.c_char_p(b"PreSOS1BigM")
   @property
   def GRB_DBL_PAR_PRESOS2BIGM(self):
      return ctypes.c_char_p(b"PreSOS2BigM")
   @property
   def GRB_INT_PAR_PRESPARSIFY(self):
      return ctypes.c_char_p(b"PreSparsify")
   @property
   def GRB_INT_PAR_PREMIQCPFORM(self):
      return ctypes.c_char_p(b"PreMIQCPForm")
   @property
   def GRB_INT_PAR_QCPDUAL(self):
      return ctypes.c_char_p(b"QCPDual")
   @property
   def GRB_INT_PAR_RECORD(self):
      return ctypes.c_char_p(b"Record")
   @property
   def GRB_STR_PAR_RESULTFILE(self):
      return ctypes.c_char_p(b"ResultFile")
   @property
   def GRB_INT_PAR_SEED(self):
      return ctypes.c_char_p(b"Seed")
   @property
   def GRB_INT_PAR_THREADS(self):
      return ctypes.c_char_p(b"Threads")
   @property
   def GRB_DBL_PAR_TUNETIMELIMIT(self):
      return ctypes.c_char_p(b"TuneTimeLimit")
   @property
   def GRB_INT_PAR_TUNERESULTS(self):
      return ctypes.c_char_p(b"TuneResults")
   @property
   def GRB_INT_PAR_TUNECRITERION(self):
      return ctypes.c_char_p(b"TuneCriterion")
   @property
   def GRB_INT_PAR_TUNETRIALS(self):
      return ctypes.c_char_p(b"TuneTrials")
   @property
   def GRB_INT_PAR_TUNEOUTPUT(self):
      return ctypes.c_char_p(b"TuneOutput")
   @property
   def GRB_INT_PAR_TUNEJOBS(self):
      return ctypes.c_char_p(b"TuneJobs")
   @property
   def GRB_INT_PAR_UPDATEMODE(self):
      return ctypes.c_char_p(b"UpdateMode")
   @property
   def GRB_INT_PAR_OBJNUMBER(self):
      return ctypes.c_char_p(b"ObjNumber")
   @property
   def GRB_INT_PAR_MULTIOBJMETHOD(self):
      return ctypes.c_char_p(b"MultiObjMethod")
   @property
   def GRB_INT_PAR_MULTIOBJPRE(self):
      return ctypes.c_char_p(b"MultiObjPre")
   @property
   def GRB_INT_PAR_POOLSOLUTIONS(self):
      return ctypes.c_char_p(b"PoolSolutions")
   @property
   def GRB_DBL_PAR_POOLGAP(self):
      return ctypes.c_char_p(b"PoolGap")
   @property
   def GRB_INT_PAR_POOLSEARCHMODE(self):
      return ctypes.c_char_p(b"PoolSearchMode")
   @property
   def GRB_INT_PAR_STARTNUMBER(self):
      return ctypes.c_char_p(b"StartNumber")
   @property
   def GRB_INT_PAR_IGNORENAMES(self):
      return ctypes.c_char_p(b"IgnoreNames")
   @property
   def GRB_STR_PAR_DUMMY(self):
      return ctypes.c_char_p(b"Dummy")

   ###########################
   ##### All *CUTS parameters 
   ###########################

   @property
   def GRB_CUTS_AUTO(self):
      return -1
   @property
   def GRB_CUTS_OFF(self):
      return 0
   @property
   def GRB_CUTS_CONSERVATIVE(self):
      return 1
   @property
   def GRB_CUTS_AGGRESSIVE(self):
      return 2
   @property
   def GRB_CUTS_VERYAGGRESSIVE(self):
      return 3

   @property
   def GRB_PRESOLVE_AUTO(self):
      return -1
   @property
   def GRB_PRESOLVE_OFF(self):
      return 0
   @property
   def GRB_PRESOLVE_CONSERVATIVE(self):
      return 1
   @property
   def GRB_PRESOLVE_AGGRESSIVE(self):
      return 2

   @property
   def GRB_METHOD_AUTO(self):
      return -1
   @property
   def GRB_METHOD_PRIMAL(self):
      return 0
   @property
   def GRB_METHOD_DUAL(self):
      return 1
   @property
   def GRB_METHOD_BARRIER(self):
      return 2
   @property
   def GRB_METHOD_CONCURRENT(self):
      return 3
   @property
   def GRB_METHOD_DETERMINISTIC_CONCURRENT(self):
      return 4
   @property
   def GRB_METHOD_DETERMINISTIC_CONCURRENT_SIMPLEX(self):
      return 5
   @property
   def GRB_BARHOMOGENEOUS_AUTO(self):
      return -1
   @property
   def GRB_BARHOMOGENEOUS_OFF(self):
      return 0
   @property
   def GRB_BARHOMOGENEOUS_ON(self):
      return 1

   @property
   def GRB_MIPFOCUS_BALANCED(self):
      return 0
   @property
   def GRB_MIPFOCUS_FEASIBILITY(self):
      return 1
   @property
   def GRB_MIPFOCUS_OPTIMALITY(self):
      return 2
   @property
   def GRB_MIPFOCUS_BESTBOUND(self):
      return 3

   @property
   def GRB_BARORDER_AUTOMATIC(self):
      return -1
   @property
   def GRB_BARORDER_AMD(self):
      return 0
   @property
   def GRB_BARORDER_NESTEDDISSECTION(self):
      return 1

   @property
   def GRB_SIMPLEXPRICING_AUTO(self):
      return -1
   @property
   def GRB_SIMPLEXPRICING_PARTIAL(self):
      return 0
   @property
   def GRB_SIMPLEXPRICING_STEEPEST_EDGE(self):
      return 1
   @property
   def GRB_SIMPLEXPRICING_DEVEX(self):
      return 2
   @property
   def GRB_SIMPLEXPRICING_STEEPEST_QUICK(self):
      return 3

   @property
   def GRB_VARBRANCH_AUTO(self):
      return -1
   @property
   def GRB_VARBRANCH_PSEUDO_REDUCED(self):
      return 0
   @property
   def GRB_VARBRANCH_PSEUDO_SHADOW(self):
      return 1
   @property
   def GRB_VARBRANCH_MAX_INFEAS(self):
      return 2
   @property
   def GRB_VARBRANCH_STRONG(self):
      return 3

GRB=GRB() #create a singleton so we can call getters and setters


def test():
   so = "/apps/gurobi752/linux64/lib/libgurobi75.so"
   appname = "DAS"
   isv = "Census"
   logfile = "gb.log"
   env = Environment(logfile, appname, isv, so)
   print("Making env. Isok: {}, status: {}".format(env.isok, env.status))
   m = Model("hello", env)
   print("Making model. Isok: {}, status: {}".format(m.isok, m.status))
   m.LogFile = "mygurobi.log"
   print("LogFile:", m.LogFile) 
   m.OutputFlag = 0
   print("OutputFlag:", m.OutputFlag) 
   m.OptimalityTol = 1e-9
   print("OptimalityTol:", m.OptimalityTol) 
   m.BarConvTol =  1e-8
   print("BarConvTol:", m.BarConvTol)  
   m.BarQCPConvTol = 0
   print("BarQCPConvTol:", m.BarQCPConvTol) 
   m.BarIterLimit = 1000
   print("BarIterLimit:", m.BarIterLimit) 
   m.FeasibilityTol = 1e-9
   print("FeasibilityTol:", m.FeasibilityTol) 
   m.Threads = 1
   print("Threads:", m.Threads) 
   m.Presolve = -1
   print("Presolve:", m.Presolve) 
   m.NumericFocus = 3 
   print("NumericFocus:",m.NumericFocus) 
   print("Problem: x^2 - 3x - y + 2xy + 3y^2 s.t. u+v-x=3, u-2v=y")
   print("adding variables")
   m.addVars( np.array([-3.0, -1.0, 0.0, 0.0]))
   print("updating model")
   m.update()
   #m.addConstr(np.array([0,2,3]), np.array([-1, 1, 1]), GRB.GRB_EQUAL, 3, "const1")
   #m.addConstr(np.array([1,2,3]), np.array([-1, 1, -2]), GRB.GRB_EQUAL, 0, "const2")
   mymatrix = np.array([[-1, 0, 1, 1], [0, -1, 1, -2]])
   import scipy.sparse as ss
   csr = ss.csr_matrix(mymatrix)
   sense = GRB.GRB_EQUAL
   rhs = np.array([3, 0])
   m.addConstrs(csr, sense, rhs)
   m.update()
   rows = np.array([0, 0, 1 ])
   cols = np.array([0, 1, 1])
   values = np.array([1.0, 2.0, 3.0])
   print("setting qp terms")
   m.addQPTerms(rows, cols, values)
   m.update()
   print("seting sense maximize")
   m.setSenseMaximize()
   print("seting sense minimize")
   m.setSenseMinimize()
   print("writing model")
   m.write("mytestmodel.lp")
   print("optimizing")
   m.optimize()
   print("Optimization status code: {}".format(m.optStatus()))
   print("is optimal? {}".format(m.isOptimal()))
   a=m.X(0,2) 
   print(a)

if __name__ == "__main__":
   test()
