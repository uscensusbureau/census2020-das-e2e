# e2e_recoder.py
# William Sexton
# Last Modified: 2/16/18
# Documentation strengthened by Steve Clark on 5/22/18.

"""
    This module is a variable recoder library for the different test data sets used
    in the 2018 DAS development process. Each class is specific to a table.

    The recoder class must contain a method called "recode".
    The recode method operates on a single SparkDataFrame row
    and returns the row with the recoded variable(s) added.

    A recoder class is specified in the main config file.
    The config file reader section must contain the following options:
        table_name.recoder: recoder class to be used with table_name
        table_name.recode_variables: space-delimited list of new variable names
                               e.g.: votingage cenrace
        for each new variable the config file contains:
        var_name: space-delimited list of original table variable names needed to create var_name
            e.g.: votingage: age
            e.g.: cenrace: white black sor
    Note: recode variable names can't clash with original variable names.

    The recoder class __init__ method must have one arg for each new variable.
    An arg is a list of original table variable names needed to create one recode variable.
    The args should be ordered according to the ordering of table_name.recode_variables.

    Sample class using the above recode variable examples.
    class sample_recoder:
        def __init__(self, arg1_list, arg2_list):
            # arg1_list = ["age"]
            # arg2_list = ["white", "black", "sor"]
            self.arg1 = arg1
            self.arg2 = arg2
        def recode(self, row):
            do_some_stuff()
            return new_row

"""

from itertools import product
from pyspark.sql import Row

class person_recoder:
    """
        This is the recoder for table1 of the Decennial Census.
        It creates cenrace, voting age, and gqtype.
    """
    def __init__(self, race_list, age_varname_list, rel_varname_list):
        self.races = race_list
        self.age = age_varname_list[0] # list contains one variable "age"
        self.rel = rel_varname_list[0] # list contains one variable "relation"

    def recode(self, row):
        """
            Input:
                row: original dataframe Row

            Output:
                dataframe Row with recode variables added
        """
        row = cenrace_recode(row, self.races)
        row = votingage_recode(row, self.age)
        row = relation_recode(row, self.rel)
        return row

class table8_recoder:
    """
        This is the recoder for table8 of the Decennial Census.
        It creates cenrace.
    """
    def __init__(self, race_list):
        self.race = race_list

    def recode(self, row):
        """
            Input:
                row: original dataframe Row

            Output:
                dataframe Row with recode variables added
        """
        return cenrace_recode(row, self.race)

class votingage_recoder:
    """
        This is the recoder for the data from the 1920 Decennial Census.
        It performs the voting age recode and shifts race to start at 0.
    """
    def __init__(self, age_varnamelist, race_varnamelist, gqtype_varnamelist):
        self.age = age_varnamelist[0] # list only contains one variable "AGE"
        self.race = race_varnamelist[0] # list only contains one variable. Not a cenrace recode.
        self.gqtype= gqtype_varnamelist[0]
    def recode(self, row):
        """
            Input:
                row: original dataframe Row

            Output:
                dataframe Row with recode variables added
        """
        row = Row(**row.asDict(), RACE0=int(row[self.race])-1)
        row = gqtype1940_recode(row, self.gqtype)
        return votingage_recode(row, self.age)

class gqunit_recoder:
    """
        This is the recoder for the data from the 1920 Decennial Census.
        It performs the voting age recode and shifts race to start at 0.
    """
    def __init__(self, gqtype_varnamelist):
        self.gqtype= gqtype_varnamelist[0]
    def recode(self, row):
        """
            Input:
                row: original dataframe Row

            Output:
                dataframe Row with recode variables added
        """
        return gqtype1940_recode(row, self.gqtype)


def cenrace_recode(row, races):
    """
        This function recodes races to cenrace.

        Inputs:
            row: a SQL Row
            races: a list of names of race variables

        Output:
            a new row with cenrace added
    """
    race_map = ["".join(x) for x in product("10", repeat=len(races))]
    race_map.sort(key=lambda s: s.count("1"))
    cenrace = "".join([str(row[race]) for race in races])
    return Row(**row.asDict(), cenrace=race_map.index(cenrace)-1)

def votingage_recode(row, age_var):
    """
        This function recodes age to voting age indicator.

        Inputs:
            row: a SQL Row
            age_var: the name of age variable

        Output:
            a new row with voting age added
    """
    return Row(**row.asDict(), VA=int(int(row[age_var]) >= 18))

def gqtype1940_recode(row, gqtype_var):
    """
        This function recodes age to voting age indicator.

        Inputs:
            row: a SQL Row
            age_var: the name of age variable

        Output:
            a new row with voting age added
    """
    dict= {0:0,2:1,3:2,4:3,6:4,7:5,8:6,9:7}
    
    return Row(**row.asDict(), GQTYPE2=int(dict[int(row[gqtype_var])]))

def relation_recode(row, rel_var):
    """
        This function recodes the relation to 3 levels for development/testing.

        Inputs:
            row: a SQL Row
            rel_var: the name of the relationship variable

        Output:
            a new row with gqtype added
    """
    gq_type = 2 if int(row[rel_var]) == 16 else 1 if int(row[rel_var]) == 15 else 0
    return Row(**row.asDict(), gqtype=gq_type)

class unit_recoder:
    """
        This is the recoder for the table1 household table for the Decennial Census.
        It creates gqtype.
    """
    def __init__(self, rel_varname):
        self.rel = rel_varname[0]
    def recode(self, row):
        """
            Input:
                row: an original dataframe Row

            Output:
                a dataframe Row with recode variables added
        """
        row = relation_recode(row, self.rel)
        return row
