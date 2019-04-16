ipums_1940/ipums_1940_classes.py:
	@echo Producing ipums parsers from the ipums SAS file
	python census_etl/census_spec_scanner.py ipums_1940/EXT1940USCB.sas --output_parser ipums_1940/ipums_1940_classes.py

short: config-2010-to-2018.ini
	@echo Converting existing 2010 MDF to 2018 MDF format
	/bin/rm -f /mnt/projects/output/MDF_Person.txt
	python census_etl/etl_migrate.py --spark config-2010-to-2018.ini 


dump:
	python census_etl/census_spec_scanner.py $(CEF_SPEC) --dump


demo: cef_validator_classes.py
	python cef_validator.py --show $$DAS_S3ROOT/2010/cef/test2/cef_44_unit.txt $$DAS_S3ROOT/2010/cef/test2/cef_44_per.txt $$DAS_S3ROOT/2010/cef/test2/cef_counts_44.txt


clean:
	/bin/rm -f /mnt/projects/output/MDF*.txt

zips:
	rm -f census_etl.zip
	rm -f census_etl/ctools.zip
	rm -f census_etl/dfxml/python.zip
	(cd census_etl; zip -r ../census_etl.zip .)
	(cd ctools; zip -r ../ctools.zip .)
	(cd ../dfxml/python; zip -r ../python.zip .)
