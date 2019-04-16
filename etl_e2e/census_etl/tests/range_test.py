#!/usr/bin/python
# -*- coding: utf-8 -*-
#
# Test the Census ETL schema package

import os
import sys

sys.path.append(os.path.join(os.path.dirname(__file__),".."))


from schema import *

def test_range_funcs():
    r1 = Range(1,1)
    r2 = Range(1,1)
    assert r1==r2
    r3 = Range(2,2)
    assert r1 < r3

def test_combine_ranges():
    r1 = Range(1,2)
    r2 = Range(2,4)
    r3 = Range(10,12)
    l1 = [r1,r2,r3]
    assert Range.combine_ranges([r1,r2]) == [Range(1,4)]
    assert Range.combine_ranges([r1,r2,r3]) == [Range(1,4),Range(10,12)]

def test_parse():
    res = Range.extract_range_and_desc("1 hello", hardfail=True)
    assert type(res)==Range
    assert res.a == '1'
    assert res.b == '1'
    assert res.desc=="hello"

    assert Range.extract_range_and_desc("1 hello", python_type=int, hardfail=True) == Range(1,1,"hello")
    assert Range.extract_range_and_desc("1-2 hello", python_type=int, hardfail=True) == Range(1,2,"hello")
    assert Range.extract_range_and_desc("1-2 = hello", python_type=int, hardfail=True) == Range(1,2,"hello")
    assert Range.extract_range_and_desc("1-2 = (hello)", python_type=int, hardfail=True) == Range(1,2,"hello")
