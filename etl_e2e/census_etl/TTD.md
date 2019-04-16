Things that Simson wants:
-[ ] Add join demo to the tests
-[ ] add output format to the tests
-[ ] MODE=spark
-[ ] STATS
-[ ] Read a schema from the first two lines of a file
-[ ] Read a schema from s3:// and hdfs://

Things that Bill Sexton wants:

-[ ]Generate a spark sql schema. The relevant class is pyspark.sql.types.StructType().

-[ ]Also, I assume this is part of validator but we need to verify correct number of leading zeros and/or automatically zfill() to get the correct number.

-[ ] Will the spec include file structure/storage location information?

-[ ]I assume/hope it won't  be difficult to covert code into a compiler for the CEF spec too. When are we getting that for review?

-[ ]Assuming access to CEF and MDF specs, we should check for inconsistencies between specs as well as within.

-[ ]Who runs the compiler? Is it run by the DAS driver or is it run separately before DAS? 

-[ ]Is it the compiler's job to actually create the writer module or to load/validate the information the writer needs to do its job?

-[ ]To maintain proper modularity and avoid duplicate work, I think it is important to have a clear view the compilers role within the architecture.
 
-[ ]Assuming the compiler's job is to generate the information needed by the writer and not actually create the writer module, this is what the list should look like in my mind. 

   1. Generate a parser that reads the MDF.
   2. Generate schema (added)
   3. Generate a validator
   4. Generate a MDF filled with all "9" variables. (Omit. Not compilers job, done by writer)
   5. Generate format statements for writing out the MDF
   6. Generate a translator to save a pandas dataframe as MDF (Omit. Not compilers job, done by writer)
   7. Detect inconsistencies in the spec. (e.g. the same variable name or number being used twice.)

1. Read the 2010 CEF Spec.
2. Specify which columns are to be ignored.  
   - Modify schema so that ignore is specified there.
3. Read the 2020 MDF Spec.
4. Specify which tables map to which tables
5. Specify which variables map to which variables.
6. Read the CEF and write the MDF

