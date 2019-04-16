#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# Some tools for manipulating PDF files


import py.test
import os
import os.path
import sys

#sys.path.append( os.path.join( os.path.dirname(__file__), "..") )
sys.path.append(os.path.dirname(os.path.dirname(__file__)))
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))

from ctools.latex_tools import *

TEST_FILES_DIR = os.path.join(os.path.dirname(__file__), "test_files")
HELLO_TEX=os.path.join(TEST_FILES_DIR,"hello.tex")
HELLO_PDF=HELLO_TEX.replace("tex","pdf")
HELLO_AUX=HELLO_TEX.replace("tex","aux")

HELLO_TEX_CONTENTS="""
\\documentclass{book}
\\begin{document}
Hello World!
\\end{document}
"""

FIVEPAGES_TEX=os.path.join(TEST_FILES_DIR,"five_pages.tex")
FIVEPAGES_PDF=os.path.join(TEST_FILES_DIR,"five_pages.pdf")
FIVEPAGES_AUX=os.path.join(TEST_FILES_DIR,"five_pages.aux")
FIVEPAGES_OUT=os.path.join(TEST_FILES_DIR,"five_pages.out")
EXTRACT_PDF=os.path.join(TEST_FILES_DIR,"extract.pdf")

def test_latex_escape():
    assert latex_escape(r"foo")=="foo"
    assert latex_escape(r"foo/bar")==r"foo/bar"
    assert latex_escape(r"foo\bar")==r"foo\textbackslash{}bar"

def test_run_latex():
    try:
        os.unlink(HELLO_PDF)
    except IOError as e:
        pass


def test_run_latex():
    # Make sure the input file exists; if not, create it
    if not os.path.exists(HELLO_TEX):
        with open(HELLO_TEX,"w") as f:
            f.write(HELLO_TEX_CONTENTS)
    assert os.path.exists(HELLO_TEX)

    # Make sure that the output file does not exist
    if os.path.exists(HELLO_PDF): 
        os.unlink(HELLO_PDF)

    # Run LaTeX. Make sure that delete_tempfiles=False leaves temp files
    run_latex(HELLO_TEX,delete_tempfiles=False)
    assert os.path.exists(HELLO_PDF)
    assert os.path.exists(HELLO_AUX)
    os.unlink(HELLO_AUX)

    # Run LaTeX. Make sure that delete_tempfiles=False deletes the temp files
    run_latex(HELLO_TEX,delete_tempfiles=True)
    assert os.path.exists(HELLO_PDF)
    assert not os.path.exists(HELLO_AUX)

    # Finally, delete HELLO_TEX and HELLO_PDF
    os.unlink(HELLO_TEX)
    os.unlink(HELLO_PDF)

def test_count_pdf_pages():
    assert os.path.exists(FIVEPAGES_PDF) # we need this file
    assert not os.path.exists(FIVEPAGES_AUX) # we do not want this
    assert not os.path.exists(FIVEPAGES_OUT) # we do not want this

    pages = count_pdf_pages(FIVEPAGES_PDF)
    assert pages==5

    assert os.path.exists(FIVEPAGES_PDF) # make sure file is still there
    assert not os.path.exists(FIVEPAGES_AUX) # we do not want this
    assert not os.path.exists(FIVEPAGES_OUT) # we do not want this



def test_extract_pdf_pages():
    if os.path.exists(EXTRACT_PDF):
        os.unlink(EXTRACT_PDF)
    assert not os.path.exists(EXTRACT_PDF)
    assert os.path.exists(FIVEPAGES_PDF)
    if os.path.exists(EXTRACT_PDF):
        os.unlink(EXTRACT_PDF)
    extract_pdf_pages(EXTRACT_PDF,FIVEPAGES_PDF,pagelist=[1])
    assert os.path.exists(EXTRACT_PDF)
    assert os.path.exists(FIVEPAGES_PDF)

    # Make sure precisely one page was extracted
    assert count_pdf_pages(EXTRACT_PDF)==1

    # Finally, delete the extracted file
    os.unlink(EXTRACT_PDF)

LINE1=r'\newlabel{"1 Cover Sheet"}{{1}{6}{2017 Food File}{chapter.1}{}}'
LINE2=r'\newlabel{EOF-"1 Cover Sheet"}{{1}{99}{2017 Food File}{chapter.1}{}}'

def parse_nested_braces_test():
    res = list(parse_nested_braces(LINE1))
    print(res)
    assert False

def test_label_parser():
    assert label_parser(LINE1)==("","1 Cover Sheet","1",6)
    assert label_parser(LINE2)==("EOF","1 Cover Sheet","1",99)
    

if __name__=="__main__":
    parse_nested_braces_test()
