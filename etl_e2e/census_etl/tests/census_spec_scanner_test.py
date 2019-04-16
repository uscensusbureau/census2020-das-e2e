#!/usr/bin/python
# -*- coding: utf-8 -*-
#
# Test for the MDF

import os
import os.path
import sys

sys.path.append(os.path.join(os.path.dirname(__file__),".."))

from schema import Range,Variable,Table
from census_spec_scanner import CensusSpec

TEST_FNAME = os.path.join(os.path.dirname(__file__), "docx_test/test_file_layout.docx")

def test_mdf_reader():
    cs = CensusSpec()
    cs.load_schema_from_file(TEST_FNAME)
    mdf = list(cs.tables())
    assert type(mdf)==list
    assert len(mdf) == 1        # demo file has but a single table
    assert type(mdf[0]) == Table
    assert mdf[0].name == "Test_Apples"


if __name__=="__main__":
    test_mdf_reader()
