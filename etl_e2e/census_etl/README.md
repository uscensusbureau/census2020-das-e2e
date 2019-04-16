# census-etl

Extract, translate and load tools for working with datasets at the US Census Bureau.

This is a toolkit for building ETL systems, and a well-developed ETL system called `etl_migrate.py`.

Most of the code in this directory is a system (`schema.py`) for representing database schemas. It includes Python classes that can:

   * Represent the schema itself, consisting of multiple tables and variables within each table. 
   * Read data schemas in a variety of formats, including CSV files, SAS7 binary files, a limited SQL dialect, and specifications written in Microsoft Word using conventions popular at the US Census Bureau.
   * Perform recodes, which are pieces of code that get automatically executed when table rows are read and/or written. Recodes are entered into a configuration file as python statements and are compiled at runtime into the python environment. 

The following commands are provided:

* <etl_migrate.py> --- A program that reads a configuration file and performs a data migration. 

* <sas_convert.py> --- Reads a SAS7 datafile and converts it into an SQLite3 database. 

This was built from the these legacy programs, some of which still work, although most of the functionality is in the above function.

* <txt_parser.py> --- Reads a text file layout specification in the form popularized by the US Census bureau 
  and generates a scanner that reads the file specified by the layout and creates an SQLite3 database. 
  This is similar to the doc_compiler.py and the two should probably be factored together. 

## etl_migrate.py language

The following commands can go into the config file. This list may not be up-to-date; be sure to investigate all of the config files, as well as look at the source-code of `etl_migrate.py`:

### [setup]
The `[setup]` section establishes the problem. It can have the following elements:

`CONFIGFILE_TYPE=etl_migrate` --- This is required for all config files

`RANDOM_RECORDS=1` --- This causes all output records to contain random data, within the parameters permitted by the schema. This option is the same as specifying the command line option `--random`

`LIMIT=nnn` --- This sets a limit on the number of records that are output. It is required if `RANDOM_RECORDS` is specified

`outdir=directory` --- This causes all output to be written to directory. Note that you can specify `$CONFIGDIR` to get the same directory as the directory containing the configuration file.

`input_nnn` --- Any configuration option that begins `input` specifies one or more tables that are used for inputs. 

`output_nnn` --- Any configuration option that beings `output` specifies one or more tables that can be used for output. Note that you will typically specify a `.docx` file to create a format for the output.

`sql_nnn` --- Any configuration option that begins `sql` specifies a table in a subset of the SQL language. 

### [recodes]

Specifies a collection of Python statements that are run in the recode function


### [run]

`stepNNN` --- Any option that begins `step` specifies a command that is run by `etl_migrate`.  The following commands are implemented:

* `BULK_LOAD` --- Reads an input table into a SQLite3 database. Note that the database only has two columns: `key`, which is the key value, and `value`, which is a JSON dictionary of the each record that was loaded.
* `PROCESS_IPUMS` --- Reads an IPUMS hierarchical file, applies recodes, and writes two tables in the specified schema.
* `OUTPUT_RECORDS` --- Reads records from a file, applies recodes, and writes each record to an output table in the specified schema.
* `OVERRIDE` --- Applies an override. 

## Libraries

The following libraries are included:

* <schema.py> --- A generic system for representing database schemas.
* <schema_test.py> --- test programs for above.
* <word_table_reader.py> --- Simson's library for reading Microsoft word tables from `.docx` files.
* <svstats.py> --- Generate single-variable statistics on a program.

# Notes
Convert specs to text with pdftotext -layout

