#!/usr/bin/python
# -*- coding: utf-8 -*-
#
# Test the Census ETL schema package

import os
import sys

sys.path.append(os.path.join(os.path.dirname(__file__),".."))


from schema import *

def test_valid_sql_name():
    assert valid_sql_name("MDF_TabulationGeography")==True
    assert valid_sql_name("CEFVW_PP10_OP")==True

def test_decode_vtype():
    assert decode_vtype("VARCHAR(20)") == (TYPE_VARCHAR, 20)
    assert decode_vtype("VARCHAR2") == (TYPE_VARCHAR, DEFAULT_VARIABLE_WIDTH)

def test_unquote():
    assert unquote("'this'") == "this"
    assert unquote("'this'") == "this"
    assert unquote("'this'") == "this"
    assert unquote("‘this’") == "this"
    assert unquote("“this”") == "this"
    assert unquote("that")   == "that"
    

def test_Recode():
    r = Recode("name","A[a] = B[b]")
    assert r.name            == "name"
    assert r.dest_table_name == "A"
    assert r.dest_table_var  == "a"
    assert r.statement       == "A[a] = B[b]"

    r = Recode("name","A[a]=B[b]")
    assert r.name            == "name"
    assert r.dest_table_name == "A"
    assert r.dest_table_var  == "a"
    assert r.statement       == "A[a]=B[b]"
    

schema = """CREATE TABLE students (
   name VARCHAR -- ,
   age INTEGER --
);"""

DATALINE1="jack10"
DATALINE2="mary25"

def test_Table():
    schema = Schema()
    t = Table(name="students")
    schema.add_table(t)
    name = Variable(name="name",vtype='VARCHAR(4)',column=0,width=4)
    age  = Variable(name="age",vtype='INTEGER(2)',column=4,width=2)
    t.add_variable(name)
    t.add_variable(age)
    assert name.column==0
    assert name.width==4
    assert age.column==4
    assert age.width==2


    assert t.get_variable("name") == name
    assert list(t.vars()) == [name,age]
    sql = t.sql_schema()
    assert "CREATE TABLE students" in sql
    assert "name VARCHAR" in sql
    assert "age INTEGER" in sql

    # See if the parsers work
    data = t.parse_line_to_dict(DATALINE1) 
    assert t.parse_line_to_row(DATALINE1) == ["jack","10"]
    assert data == {"name":"jack","age":"10"}

    # Add a second table
    t = Table(name="parents")
    schema.add_table(t)
    t.add_variable(Variable(name="parent",vtype=TYPE_VARCHAR))

    # See if adding a recode works
    schema.add_recode("recode1",TYPE_VARCHAR,"parents[studentname]=students[name]")
    schema.add_recode("recode2",TYPE_INTEGER,"parents[three]=3")
    schema.add_recode("recode3",TYPE_VARCHAR,"parents[student_initials]=students[name][0:1]")
    schema.compile_recodes()

    # verify that the parents table now has a student name variable of the correct type
    assert schema.get_table("parents").get_variable("studentname").name == "studentname"
    assert schema.get_table("parents").get_variable("studentname").vtype == TYPE_VARCHAR

    # Let's load a line of data for recoding
    schema.recode_load_data("students",data)

    # Now record a parent record
    parent = {"name":"xxxx"}
    schema.recode_execute("parents",parent)

    # Now make sure that the recoded data is there
    assert parent['studentname']=='jack'
    assert parent['three']==3
    assert parent['student_initials']=='j'


SQL_CREATE1="""    CREATE TABLE output (
        INTEGER StudentNumber,
	VARCHAR CourseNumber,
	VARCHAR CourseName
    );
"""

def test_sql_parse_create():
    sql = sql_parse_create(SQL_CREATE1)
    assert sql['table']=='output'
    assert sql['cols'][0]=={'vtype':'INTEGER','name':'StudentNumber'}
    assert sql['cols'][1]=={'vtype':'VARCHAR','name':'CourseNumber'}
    assert sql['cols'][2]=={'vtype':'VARCHAR','name':'CourseName'}
