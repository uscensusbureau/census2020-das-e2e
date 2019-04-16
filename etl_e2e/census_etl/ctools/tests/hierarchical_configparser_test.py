#!/usr/bin/env python3
#

import os

import sys
sys.path.append(os.path.dirname(os.path.dirname(__file__)))
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))

from ctools.hierarchical_configparser import HierarchicalConfigParser, fixpath


MYDIR=os.path.dirname(__file__)


def test_fixpath():
    assert fixpath("/a/b/c", "/a/b") == "/a/b"
    assert fixpath("/a/b/c", "b") == "/a/b/b"


def test_hierarchical_configparser1():
    hcf = HierarchicalConfigParser()
    hcf.read(MYDIR + "/hcf_file2.ini")
    assert sorted(list(hcf.sections())) == ['a', 'b', 'c']
    assert hcf['a']['color'] == 'file2-a'
    assert hcf['a']['second'] == 'file2-a'
    assert hcf['b']['color'] == 'file2-b'
    assert hcf['b']['second'] == 'file2-b'
    assert hcf['c']['color'] == 'file2-c'
    assert hcf['c']['second'] == 'file2-c'


def test_hierarchical_configparser2():
    fname = MYDIR + "/hcf_file1.ini"  # includes hcf_file2.ini as a default
    assert os.path.exists(fname)
    hcf = HierarchicalConfigParser()
    hcf.read(fname)
    # Validate what's in hcf_file1.ini
    assert hcf['a']['INCLUDE'] == 'hcf_file2.ini'
    assert hcf['a']['color'] == 'file1-a'
    assert hcf['b']['color'] == 'file1-b'

    # Validate what was included in section 'a' and was not overwritten
    assert hcf['a']['second'] == 'file2-a'

    # Validate that additional tag in section 'b' was not included
    assert 'second' not in hcf['b']

    # Validate that section 'c' was not included
    assert 'c' not in hcf.sections()


def test_hierarchical_configparser3():
    hcf = HierarchicalConfigParser()
    hcf.read(MYDIR + "/hcf_file3.ini")
    print("and we got:")
    hcf.write(open("/dev/stdout", "w"))
    assert hcf['a']['color'] == 'file2-a'
    assert hcf['a']['second'] == 'file2-a'
    assert hcf['b']['color'] == 'file2-b'
    assert hcf['b']['second'] == 'file2-b'
    assert hcf['c']['color'] == 'file2-c'
    assert hcf['c']['second'] == 'file2-c'
    assert hcf['d']['color'] == 'file1-d'
