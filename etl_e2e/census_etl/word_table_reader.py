#!/usr/bin/env python3

# Simson's program for reading Microsoft Word tables

#
#
# http://officeopenxml.com/WPtableGrid.php

try:
    from xml.etree.cElementTree import XML
except ImportError:
    from xml.etree.ElementTree import XML
import zipfile


"""
Module that extract text from MS XML Word document (.docx).
(Inspired by python-docx <https://github.com/mikemaccana/python-docx>)

XML tags:
<w:p> - Paragraph
<w:t> - Text - this is what we want!

"""

WORD_NAMESPACE = '{http://schemas.openxmlformats.org/wordprocessingml/2006/main}'
PARA = WORD_NAMESPACE + 'p'
TEXT = WORD_NAMESPACE + 't'
TBLPR = WORD_NAMESPACE + "tblPr"               # table properties?

TBL      = WORD_NAMESPACE + "tbl"
TBLGRID  = WORD_NAMESPACE + "tblGrid"          # table grid
GRIDCOL = WORD_NAMESPACE + "gridCol"
TR = WORD_NAMESPACE+"tr"                       # table row?
TC = WORD_NAMESPACE+"tc"                       # table cell?


def get_docx_table(path):
    """
    Find the table inside the .docx file and return it in an array
    """
    document = zipfile.ZipFile(path)
    xml_content = document.read('word/document.xml')
    document.close()
    tree = XML(xml_content)

    rows = []
    for xml_row in tree.getiterator(TR):
        row = []
        for xml_cell in xml_row.getiterator(TC):
            # Each cell consists of one or more paragraph
            text = ""
            for paragraph in xml_cell.getiterator(PARA):
                texts = [node.text for node in paragraph.getiterator(TEXT) if node.text]
                paragraph_text = "".join(texts)
                if paragraph_text:
                    text += paragraph_text + "\n"
            if text.endswith("\n"):
                text = text[0:-1]
            row.append(text)
        rows.append(row)
    return rows
            
def get_text_for_table(table):
    """
    Find the table inside the .docx file and return it in an array
    """
    rows = []
    for xml_row in table.getiterator(TR):
        row = []
        for xml_cell in xml_row.getiterator(TC):
            # Each cell consists of one or more paragraph
            text = ""
            for paragraph in xml_cell.getiterator(PARA):
                texts = [node.text for node in paragraph.getiterator(TEXT) if node.text]
                paragraph_text = "".join(texts)
                if paragraph_text:
                    text += paragraph_text + "\n"
            if text.endswith("\n"):
                text = text[0:-1]
            row.append(text)
        rows.append(row)
    return rows
            
def get_docx_tables(path):
    """
    Find the table inside the .docx file and return it in an array
    """
    document = zipfile.ZipFile(path)
    xml_content = document.read('word/document.xml')
    document.close()
    tree = XML(xml_content)
    for tbl in tree.getiterator(TBL):
        yield tbl

def get_table_rows(tbl):
    return [row for row in tbl.getiterator(TR)]

def get_row_cells(row):
    return [cell for cell in row.getiterator(TC)]

def get_docx_text(path):
    """
    Take the path of a docx file as argument, return the text in unicode.
    """
    texts = []
    for paragraph in path.getiterator(PARA):
        texts = [node.text for node in paragraph.getiterator(TEXT) if node.text]
        paragraph_text = "".join(texts)
        if paragraph_text:
            text += paragraph_text + "\n"
        texts.append(text)
    return "".join(texts)

if __name__=="__main__":
    import sys
    print("get_docx_tables:")
    tables = list(get_docx_tables(sys.argv[1]))
    for table in tables:
        for row in get_text_for_table(table):
            print(row)
        print("=====================")
    exit(0)



    for table in get_docx_tables(sys.argv[1]):
        for row in get_text_for_table(table):
            print(row)
        print("=====================")
    exit(1)
            


    for row in get_docx_table(sys.argv[1]):
        print(row)

    for table in get_docx_tables(sys.argv[1]):
        print(table)
        rows = get_row_cells(table)
        print("Rows: {}".format(len(rows)))
        if rows:
            print("Text of first row: {}".format(get_docx_text(rows[0])))
        print("=============")


