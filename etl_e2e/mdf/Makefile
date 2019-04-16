#
# This Makefile creates the spec scanner
# It uses the MDF_SPEC and the fields to ignore
#
# usage:
# make mdf_classes.py
# python mdf.py  --MDF_UNIT <unitfile> --MDF_PER <person_file> --MDF_COUNTS <counts_file>

MDF_SPEC=2018_mdf.docx

CENSUS_SPEC_SCANNER=../census_etl/census_spec_scanner.py

IGNORE=QREL,QSEX,QSPANX,QRACE1,QRACE2,QRACE3,QRACE4,QRACE5,QRACE6,QRACE7,QRACE8,CIT,HHT,HHT2,NPF,CPLT,UPART,MULTG,HHLDRAGE,HHSPAN,HHRACE,PAOC,P18,P60,P65,P75


##
## DO NOT REMOVE validate_44:
## It is used for the 2018 End-to-End test!


mdf_spec_autogen.py: $(MDF_SPEC) $(CENSUS_SPEC_SCANNER) ../census_etl/schema.py Makefile
	python $(CENSUS_SPEC_SCANNER) --output_parser mdf_spec_autogen.py $(MDF_SPEC)

validate_44: mdf_spec_autogen.py
	python data.py --show --MDF_Person $$DAS_S3ROOT/mft/das-to-cdl/MDF_44_PER.txt --MDF_Unit $$DAS_S3ROOT/mft/das-to-cdl/MDF_44_UNIT.txt --pipe_delimited --write_json=./

mdf_classes.py: $(MDF_SPEC) $(CENSUS_SPEC_SCANNER) ../census_etl/schema.py Makefile
	python $(CENSUS_SPEC_SCANNER) $(MDF_SPEC) --output_parser mdf_classes.py --ignore $(IGNORE)

spec: mdf_spec_autogen.py

clean:
	/bin/rm -f mdf_spec_autogen.py
