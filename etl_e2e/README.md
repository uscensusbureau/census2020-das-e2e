# etl-2020
Census-specific ETL code for the 2020 Census

# How-To
## To create a parser for the MDF

Use the `census_etl/census_spec_scanner.py` program to process the .docx file.

To find out the available features, try (Python 3.x):

    $ python census_etl/census_spec_scanner.py --help

To verify that you are dumping the MDF properly, try:

    $ python census_etl/census_spec_scanner.py --dump mdf/2018_mdf.docx 

To see just the schema, in SQL form, try:

    $ python census_etl/census_spec_scanner.py --schema mdf/2018_mdf.docx 

To make the python reader:

    $ python census_etl/census_spec_scanner.py --output_parser  mdf/2018_mdf_parser.py mdf/2018_mdf.docx

## To create the 2018 MDF from 210 CEF data

    $ python census_etl/etl_migrate.py [--dry-run] [--schema] [--limit 10] config-2010-to-2018.ini

# Directory Contents

2018-CEF - Synthetic 2018 CEF excerpt, provided by DRPS

cef - Code for parsing the CEF

census_etl - Sub directory with the Census ETL system

CENSUS-INTERNAL-NO-NOT-SYNC.md - A reminder

check.py - A test program we wrote to verify racial category sets

config-2010-to-2020.ini - A census_etl script that converts the 2010 CEF to the 2020 MDF

config-2020-fake-data.ini - A census_etl script that uses the 2020 MDF spec to create randomly generated data

config-stats.ini - Uses the ETL system to print information about a table.

geo - Information for parsing the GEO files.

mdf - MDF specifications
