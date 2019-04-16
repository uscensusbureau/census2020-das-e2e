#
# demo_engine:
# A sample disclosure avoidance engine. This can be subclassed or re-implemented
# The class must have at least a single method: .run()
# engine stub 
#
# This demo uses pandas


import csv
import driver
import logging
import os
import pandas

from driver import AbstractDASEngine
class engine(AbstractDASEngine):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def run(self,df):
        """Run the disclosure avoidance engine with the pandas model.
        This is a simple DAS engine that just rounds data to the nearest integer. """
        df2 = df.copy()
        df2['Data'] = df2['Data'].round()
        return df2

    def run_csv(self,input):
        """Run the disclosure avoidance engine with the CSV model.
        This is a simple DAS engine that just rounds data to the nearest integer. """
        ret = []                # the output data
        for row in input:
            print("row=",row)
            ret.append({"ID":row['ID'], "Data":round(row['Data'])})
        return ret
#
# demo_reader:
#
# A sample reader. This can be subclassed or re-implemented.
# The class must have a single method: .read()
# 
# In this demo, the data that is read is returned as an array of dictionaries
# 


from driver import AbstractDASReader
class reader(AbstractDASReader):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
    
    def read(self):
        """Reads the data. Returns an object that allows other functions to get to the data.
        In this simple implementation, the object returned is the data itself, in an array of dictionaries.
        """
        fname = self.getconfig("input_fname")
        logging.debug("demo_reader::read({})".format(fname))
        df = pandas.read_csv(fname)
        return df


    def read_csv(self):
        """Reads the data. Returns an object that allows other functions to get to the data.
        In this simple implementation, the object returned is the data itself, in an array of dictionaries.
        """
        fname = self.getconfig("input_fname")
        logging.debug("demo_reader::read_csv({})".format(fname))
        ret = []
        with open(fname,"r") as csvfile:
            reader = csv.DictReader(csvfile)
            for row in reader:
                row['Data'] = float(row['Data']) # low cost type conversion
                ret.append(row)
        return ret


#
# demo_setup:
# A sample setup class. This can be subclassed or re-implemented.
# The class must have at least a single method: .setup()


from driver import AbstractDASSetup
class setup(AbstractDASSetup):
    def __init__(self, **kwargs):
        print("demo_setup setup object created")
        super().__init__(**kwargs)
    
#
# demo_takedown:
# A sample takedown class. This can be subclassed or re-implemented.
# The class must have at least a single method: .takedown()
#

from driver import AbstractDASTakedown
class takedown(AbstractDASTakedown):
    def __init__(self,*args,**kwargs):
        super().__init__(*args,**kwargs)

    def takedown(self):
        return True             # successful takedown
        

    def removeWrittenData(self,reference):
        """Delete what's referred to by reference."""
        logging.info("unlink({})".format(reference))
        os.unlink(reference)
        return True

#
# demo_validator:
#
# Compare the input and the output and make sure that it validates
# 
# 


from driver import AbstractDASValidator
class validator(AbstractDASValidator):
    def __init__(self, **kwargs):
        super().__init__( **kwargs)
    
    def validate(self, original_data, written_data):
        """Validate the original data against the written data.  In demo,
        original_data is the original data itself, whereas
        written_data is an object that points to the written data (in
        this case, the filaname.). Returns False if it doesn't validate"""
        
        count = len(original_data) # number of rows
        # Make sure that written_data has the correct number of lines
        linecount = open(written_data,"r").read().count("\n")
        if count != linecount-1:
            print("count=",count,"linecount=",linecount)
            return False
        
        # passed all tests
        return True
#
# dummy_writer:
#
# A sample writer. This can be subclassed or re-implemented.
# The class must have at least a single method: .write()

from driver import AbstractDASWriter

class writer(AbstractDASWriter):
    # ___init__ is not needed, but you might want to override it in another implementation.
    # if you override it, be sure to call super().__init__()
    #
    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def write(self,df):
        """Write the data with the pandas model"""
        fname = self.getconfig(driver.OUTPUT_FNAME)
        df.to_csv(fname, header='column_names', index=False)
        return fname

    def write_csv(self,data):
        """ Write the data to the location provided in the config.
        Returns an object that has some way to get back to the data that was written.
        """
        
        fname = self.getconfig(driver.OUTPUT_FNAME)
        assert os.path.exists(fname)==False
        with open(fname,"w") as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames = ['ID', 'Data'])
            writer.writeheader()
            for row in data:
                writer.writerow(row)

        return fname
