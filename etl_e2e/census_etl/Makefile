#
# Because most of the programs provided are Python programs, this file really documents workflows
#

#
# This workflow transforms a specification for the file PSS-DEV-CEFVW_PP10_OP-2010SD.pdf
# into a text file. It then turns that into a scanner in C++

INFILE=PSS-DEV-CEFVW_PP10_OP-2010SD.pdf
OFILE=PSS-DEV-CEFVW_PP10_OP-2010SD.txt

demo1: $(INFILE)
	pdftotext -layout $(INFILE) $(OFILE)
	ls -l $(OFILE)

lint:
	pylint etl_migrate.py

clean:
	/bin/rm -f *~ */*~
	python -c 'import etl_ipums_test;etl_ipums_test.clean();'
