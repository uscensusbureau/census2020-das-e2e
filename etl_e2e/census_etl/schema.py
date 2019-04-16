#
# Tools for representing a database schema
#
# Developed by Simson Garfinkel at the US Census Bureau
# 
# Classes:
#   Range     - Describes a range that a value can have, from a to b inclusive. Includes a description of the range
#   Variable - An individual variable. Can have multiple ranges and possible values, as well as type
#   Table    - A set of Variables, and additional metadata
#   Recode   - A recode from one table to another
#   Schema   - A set of Tables and recodes

import os
import re
import numpy
import logging
import random
import json
from collections import OrderedDict

import ctools.dconfig as dconfig

unquote_re     = re.compile("[\u0022\u0027\u2018\u201c](.*)[\u0022\u0027\u2019\u201d]")
type_width_re  = re.compile("([A-Z0-9]+)\s*[(](\d+)[)]") # matches a type and width specification
note_re        = re.compile("Note([ 0-9])*:",re.I)
assignVal_re   = re.compile("([^=]*)\s*=\s*(.*)")
assignRange_re = re.compile("(.*)\s*[\-\u2013\u2014]\s*([^=]*)(=\s*(.*))?")
range_re       = re.compile("(\S*)\s*[\-\u2013\u2014]\s*(\S*)") # handles unicode dashes
integer_re     = re.compile("INTEGER ?\((\d+)\)",re.I)

TYPE_NUMBER    = "NUMBER"
TYPE_INTEGER   = "INTEGER"
TYPE_INT       = "INT"
TYPE_VARCHAR   = "VARCHAR"
TYPE_CHAR      = "CHAR"
VALID_TYPES    = set([TYPE_NUMBER,TYPE_INTEGER,TYPE_INT,TYPE_VARCHAR,TYPE_CHAR])
PYTHON_TYPE_MAP= {TYPE_NUMBER:int,
                  TYPE_INTEGER:int,
                  TYPE_INT:int,
                  TYPE_VARCHAR:str,
                  TYPE_CHAR:str}
DEFAULT_VARIABLE_WIDTH = 8
WIDTH_MAX       = 255

SAS7BDAT_EXT   = ".sas7bdat"
CSV_EXT        = ".csv"
TXT_EXT        = ".txt"
PANDAS_EXTS    = set([SAS7BDAT_EXT,CSV_EXT,TXT_EXT])
PANDAS_CHUNKSIZE = 1000

# Special ranges
RANGE_NULL = "NULL"           # if NULL, then interpret as the empty string
RANGE_ANY  = "N/A"            # if N/A, allow any

# Included in programmatically-generated output
SCHEMA_SUPPORT_FUNCTIONS="""
def leftpad(x,width):
    return ' '*(width-len(str(x)))+str(x)

def between(a,b,c,width):
    return leftpad(a,width) <= leftpad(b,width) <= leftpad(c,width)

def safe_int(i):
    try:
        return int(i)
    except (TypeError, ValueError) as e:
        return None

def safe_float(i):
    try:
        return float(i)
    except (TypeError, ValueError) as e:
        return None

def safe_str(i):
    try:
        return str(i)
    except (TypeError, ValueError) as e:
        return None
"""


def valid_sql_name(name):
    for ch in name:
        if ch.isalnum()==False and ch not in ['_']:
            return False
    return True

def decode_vtype(t):
    """Given VARCHAR(2) or VARCHAR or VARCHAR2(2) or INTEGER(2), return the type and width"""
    if t==None:
        return (None,None)
    t = t.upper().replace("VARCHAR2","VARCHAR")
    m = type_width_re.search(t)
    if m:
        vtype = m.group(1)
        width = int(m.group(2))
    else:
        vtype = t
        width = DEFAULT_VARIABLE_WIDTH
    if vtype not in VALID_TYPES:
        raise ValueError("vtype {} is not in VALID_TYPES".format(vtype))
    return (vtype,width)
    

def vtype_for_numpy_type(t):
    try:
        return {bytes:TYPE_CHAR,
                float:TYPE_NUMBER,
                numpy.float64:TYPE_NUMBER,
                str:TYPE_CHAR,
                int:TYPE_NUMBER,
                }[t]
    except KeyError as e:
        logging.error("Unknown type: {}".format(t))
        raise e


def unquote(s):
    m = unquote_re.match(s)
    if m:
        return m.group(1)
    else:
        return s


def all_ints(ranges):
    for r in ranges:
        if r.is_int()==False:
            return False
    return True

#
# Simson's cut-rate SQL parser
#
create_re = re.compile('CREATE TABLE (\S*) \(( .* )\)',re.I);
var_re    = re.compile('(\S+) (\S+)')

SQL_TABLE = 'table'
SQL_COLUMNS = 'cols'

def sql_parse_create(stmt):
    if "--" in stmt:
        raise RuntimeError("Currently cannot handle comments in SQL")
    ret = {}
    ret[SQL_TABLE] = None
    ret[SQL_COLUMNS]  = []
    
    # make all spaces a single space
    stmt = re.sub("\s+"," ",stmt).strip()

    m = create_re.search(stmt)
    if m:
        ret[SQL_TABLE] = m.group(1)
        for vdef in m.group(2).split(","):
            vdef = vdef.strip()
            m = var_re.search(vdef)
            (vtype,name) = m.group(1,2)
            ret[SQL_COLUMNS].append({'vtype':vtype, 'name':name})
    return ret




def clean_int(s):
    """ Clean an integer """
    while len(s)>0:
        if s[0]==' ':
            s=s[1:]
            continue
        if s[-1] in " ,":
            s=s[0:-1]
            continue
        break
    return int(s)


def convertValue(val, vtype=None):
    """Make the value better"""
    try:
        if vtype not in [TYPE_VARCHAR,TYPE_CHAR]:
            return clean_int(val)
    except ValueError:
        pass
    val = val.strip()
    if val.lower()=="null" or val=="EMPTY-STRING":
        val = ""
    return val

def convertRange(a,b, desc=None, vtype=None):
    try:
        if vtype not in [TYPE_VARCHAR,TYPE_CHAR]:
            return Range(clean_int(a),clean_int(b), desc)
    except ValueError:
        pass
    return Range(a.strip(),b.strip(), desc)

DEFAULT_VARIABLE_FORMAT = '{}'

class Range:
    """Describe a range that a value can have, from a to b inclusive
    [a,b]
    This is type-free, but we should have kept the type of the variable for which the range was made. 
    """
    RANGE_RE_LIST = [
        re.compile(r"(?P<a>\d+)\s*-\s*(?P<b>\d+)\s*=?\s*(?P<desc>.*)"),
        re.compile(r"(?P<a>\d+)\s*=?\s*(?P<desc>.*)")
    ]

    @staticmethod
    def extract_range_and_desc(possible_legal_value,python_type=str,hardfail=False):
        possible_legal_value = possible_legal_value.strip()
        if possible_legal_value.count("=")>1:
            logging.error("invalid possible legal values: {} ({})".format(possible_legal_value,possible_legal_value.count("=")))
            return None
        for regex in Range.RANGE_RE_LIST:
            m = regex.search(possible_legal_value)
            if m:
                a = m.group('a')
                try:
                    b = m.group('b')
                except IndexError:
                    b = a
                desc = m.group('desc')
                return Range( python_type(a), python_type(b), desc)
        if hardfail:
            raise ValueError("Cannot recognize range in: "+possible_legal_value)
        return None

    @staticmethod
    def combine_ranges(ranges):
        """Examine a list of ranges and combine adjacent ranges"""
        # Make a new list with the ranges in sorted order

        # I should remove the all int ranges, do the combining, and the re-added the non-int tranges
        if not all_ints(ranges):
            return ranges

        ranges = list(sorted(list(ranges), key=lambda r:(r.a,r.b)))
        # Combine r[i] and r[i+1] if they are touching or adjacent with no room between
        for i in range(len(ranges)-2,-1,-1):
            if ranges[i].b in [ranges[i+1].a,ranges[i+1].a-1]:
                ranges[i] = Range(ranges[i].a,ranges[i+1].b)
                ranges[i].b=ranges[i+1].b
                del ranges[i+1]
        return ranges

    def __init__(self,a=None,b=None, desc=None):
        self.date = False       # is this a date?
        self.desc = desc
        if type(a)==str and a.startswith("YYYYMMDD: "):
            self.date = "YYYYMMDD"
            a = a.replace("YYYYMMDD: ","")
        self.a = a
        self.b = b if b else a

    def __eq__(self,other):
        assert type(other)==Range
        return (self.a==other.a) and (self.b==other.b)

    def __lt__(self,other):
        assert type(other)==Range
        return self.a < other.a

    def __repr__(self):
        if self.desc:
            return "Range({} - {}  {})".format(self.a,self.b,self.desc)
        else:
            return "Range({} - {})".format(self.a,self.b)
            
    def __hash__(self):
        return hash((self.a,self.b))

    def __contains__(self,val):
        return self.a <= val <= self.b

    def json_dict(self):
        if self.desc:
            {'a':self.a,'b':self.b,'desc':self.desc}
        return {'a':self.a,'b':self.b}

    def python_expr(self,python_type, width):
        """Return the range as a python expression in x"""
        if self.a==RANGE_ANY or self.b==RANGE_ANY:
            return "True "      # anything work

        if python_type==int or python_type==float:
            if self.a==self.b:
                return "x=={}".format(self.a)
            return "{} <= x <= {}".format(self.a,self.b)

        if python_type==str:
            """This needs to handle padding"""
            if self.a==self.b:
                if self.a==RANGE_NULL:
                    return "x.strip()=='' "
                return "leftpad(x,{})==leftpad('{}',{})".format(width, self.a, width)
            return "between('{}',x,'{}',{})".format(self.a, self.b, width)

        logging.error("Cannot make python range expression for type %s",str(python_type))
        raise ValueError("Don't know how to create python range expression for type "+str(python_type))

    def is_int(self):
        return type(self.a)==int and type(self.b)==int

    def random_value(self):
        """Return a random value within the range"""
        if self.is_int():
            return random.randint(self.a,self.b+1)
        if self.a==self.b:
            return self.a
        if len(self.a)==1 and len(self.b)==1:
            return chr(random.randint(ord(self.a),ord(self.b)+1))
        if self.a[0:8]=='YYYYMMDD':
            return "19800101"   # not very random
        raise RuntimeError("Don't know how to make a random value for a={} ({}) b={} ({})".
                           format(self.a,type(self.a),self.b,type(self.b)))



class Variable:
    """The MDF Variable"""

    __slots__ = ('position','name','python_type','vtype','desc','width','column','ranges','default','format','prefix')

    def __init__(self,position=None,name=None,desc="",vtype=None,width=None,column=None):         # variable number
        self.set_name(name)
        self.set_vtype(vtype)
        self.position    = position      # position number
        self.desc        = desc          # description
        self.column      = column        # Starting column in the line if this is a column-specified file 0

        # If width is specified and vtype was specified, make sure they match
        if width and vtype:
            assert self.width == width 
        # If width was specified and no vtype was specified, set it
        if width and not vtype:
            self.width   = width         # number of characters wide

        self.ranges      = set()
        self.default     = None    
        self.format      = DEFAULT_VARIABLE_FORMAT
        self.prefix      = ""

    def __str__(self):
        return "{}({} column:{} width:{})".format(self.name,self.python_type.__name__,self.column,self.width)

    def __repr__(self):
        return "Variable(position:{} name:{} desc:{} vtype:{})".format(self.position,self.name,self.desc,self.vtype)

    def json_dict(self):
        return {"name":self.name,
                "vtype":self.vtype,
                "position":self.position,
                "desc":self.desc,
                "column":self.column,
                "width":self.width,
                "ranges":[range.json_dict() for range in self.ranges] }

    def set_name(self,name):
        self.name = name
        if self.name and not valid_sql_name(name):
            raise RuntimeError("invalid SQL Variable name: {}".format(name)) 

    def set_vtype(self,v):
        """sets both vtype and python_type.
        v may be in VALID_TYPES and it may have an optional width specification.
        VARCHAR2 (used by Oracle) is automatically converted to VARCHAR."""
        if v:
            (self.vtype,self.width) = decode_vtype(v)
            assert 1<=self.width<=WIDTH_MAX
            self.python_type = PYTHON_TYPE_MAP[self.vtype]
        else:
            self.vtype = None
            self.width = None
            self.python_type = None

    def set_column(self, start:int, end:int):
        """Note the first column is column 0, not column 1.  Data is in start..end inclusive"""
        assert start>=0
        assert end>=0
        assert end>=start
        assert type(start)==int
        assert type(end)==int
        self.column = start
        self.width    = (end-start)+1
        
    def sql_type(self):
        """Return the type as an SQL expression"""
        if self.width:
            return "{}({})".format(self.vtype,self.width)
        return self.vtype
        
    def find_default_from_allowable_range_descriptions(self,text):
        """Search through all of the allowable ranges. If one of them has a
        TEXT in its description, use the first value of the range as
        the default value

        """
        # If there is only one allowable range and there is no range, it's the default
        if len(self.ranges)==1:
            r = next(iter(self.ranges))
            if r.a==r.b:
                self.default = r.a
                return

        # The 'in' operator is the fastest way to do this; see:
        # https://stackoverflow.com/questions/4901523/whats-a-faster-operation-re-match-search-or-str-find
        text = text.lower()
        for r in self.ranges:
            if text in r.desc.lower():
                self.default = r.a
                return
                
    def add_range(self,newrange):
        """Add a range of legal values for this variable."""
        assert isinstance(newrange,Range)
        if newrange in self.ranges:
            raise RuntimeError("{}: duplicate range: {}".format(self.name,r))
        self.ranges.add(newrange)

    def add_valid_data_description(self,desc):
        """Parse the variable descriptions typical of US Census Bureau data
        files that are used to describe valid values."""

        # If there are multiple lines, process each separately
        if "\n" in desc:
            for line in desc.split("\n"):
                self.add_valid_data_description(line)
            return

        # If this is a note, just add it to the description and return
        if note_re.search(desc):
            if len(self.desc) > 0:
                self.desc += " "
            self.desc += desc
            return

        # Look to see if this is a longitude or a latitude:
        if "(Latitude)" in desc:
            self.format = "{:+.6f}"
            self.ranges.add( Range(-90,90,"Latitude") )
            return

        if "(Longitude)" in desc:
            self.format = "{:+.8f}"
            self.ranges.add( Range(-180, 180, "Longitude") )
            return

        if desc.startswith("EMPTY-STRING"):
            self.format = ""
            self.ranges.add( Range("","",desc) )
            return

        # Look for specific patterns that are used. 
        # Each pattern consists of a LEFT HAND SIDE and a RIGHT HAND SIDE of the equal sign.
        # The LHS can be a set of values that are separated by commas. Each value can be a number or a range.
        # The RHS is the description.
        # For example:
        #      3 = Three People
        #  3,4,5 = Number of people
        #  3-5,8 = Several people
        #
        m = assignVal_re.search(desc)
        if m:
            (lhs,rhs) = m.group(1,2)
            assert "=" not in lhs
            valDesc = rhs          # the description
            if valDesc=="":
                valDesc = "Allowable Values"
            # Now loop for each comma item
            for val in lhs.split(","):

                val = unquote(val.strip())
                if "EMPTY-STRING" in val:
                    self.ranges.add(Range("","", desc=valDesc))
                    continue

                # Do we have a range?
                m = range_re.search(val)
                if m:
                    (a,b) = m.group(1,2)
                    assert a!="EMPTY"
                     
                    # Check for T numbers... (used for American Indian reservations)
                    if a[0:1]=='T' and b[0:1]=='T':
                        self.prefix = 'T'
                        a = a[1:]
                        b = b[1:]
            
                    # Smart convert the range
                    self.ranges.add( convertRange( a, b, desc=valDesc, vtype=self.vtype))
                    continue
                val = convertValue(val, vtype=self.vtype)
                self.ranges.add( Range(val,val, desc=valDesc))
                continue
            return
        #
        # No equal sign was found. Check for a range.
        #
        m = range_re.search(desc) # see if we have just a range
        if m:
            (a,b) = m.group(1,2)
            if a=="EMPTY-STRING": a=''
            if b=="EMPTY-STRING": b=''
            self.add_range( convertRange( a, b, desc="(inferred allowable range)", vtype=self.vtype))
            return
        # 
        # No range was found. See if it is a magic value that we know how to recognize
        #
        
        if "EMPTY-STRING" in desc:
            self.add_range( Range(""))
            return
        if "not reported" in desc.lower():
            self.add_range( Range(""))
            return
        
        # 
        # Can't figure it out; if it is a single word, add it.
        #
        if len(desc)>0 and (" " not in desc):
            self.add_range( Range(desc) )
            return

        #
        # Give up
        #
        raise RuntimeError("{} Unknown range description: '{}'".format(self.name,desc))
        

    def random_value(self):
        """Generate a random value"""
        if "f" in self.format:   # special case for floating points that specify a range
            r = random.choice(self.ranges)
            val = self.format.format(random.uniform(r[0],r[1]))

        elif self.ranges:
            random_range = random.choice(self.ranges)
            val = random_range.random_value()

        elif self.vtype==TYPE_VARCHAR or self.vtype==TYPE_CHAR: # make a random VCHAR
            width = self.width if self.width else 6
            val = "".join(random.choice("ABCDEFGHIJKLMNOPQRSTUVWXYZ") for i in range(width))

        elif self.vtype==TYPE_NUMBER or self.vtype==TYPE_INTEGER: # make a random number
            width = self.width
            val   = random.randint(0,(10**self.width)-1)
        else:
            raise RuntimeError("variable:{} vtype:{} width:{} no allowable values or ranges".format(self.name,self.vtype,self.width))

        if self.prefix:
            val = self.prefix + str(val)

        # Look for "up to nn characters" or "nn characters"
        if type(val)==str:
            if "characters" in val.lower():
                m = re.search("(up to)?\s*([0-9]+)\s*characters",val,re.I)
                if not m:
                    raise RuntimeError("Cannot decode: '{}'".format(val))
                val = "".join(random.choice("ABCDEFGHIJKLMNOPQRSTUVWXYZ") for i in range(int(m.group(2))))

            # Look for "nn blanks"
            if "blanks" in val.lower():
                m = re.search("([0-9]+)\s+blanks",val,re.I)
                if not m:
                    raise RuntimeError("Cannot decode: '{}'".format(val))
                val = " " * int(m.group(1))
        return val

    def python_validator_name(self):
        return "is_valid_" + self.name

    def python_validation_text(self):
        ranges = Range.combine_ranges(self.ranges)
        return ", ".join(['{}-{}'.format(r.a,r.b) for r in ranges])

    def python_validator(self):
        ret = []
        ret.append("    @classmethod")
        ret.append("    def {}(self,x):".format(self.python_validator_name()))
        ret.append('        """{}"""'.format(self.desc))
        if self.python_type == int or self.python_type==float:
            ret.append('        x = str(x).strip()')
            ret.append('        try:')
            if self.python_type==int:
                ret.append('            x = int(x)')
            if self.python_type==float:
                ret.append('            x = float(x)')
            ret.append('        except ValueError:')
            ret.append('            return False')
        ranges = Range.combine_ranges(self.ranges)
        try:
            expr = " or ".join(["({})".format(r.python_expr(self.python_type, self.width)) for r in ranges])
        except ValueError as e:
            logging.error("Cannot create python range expression for variable "+str(self))
            raise RuntimeError("Cannot create python range expression for variable "+str(self))
            
        if expr=="":
            expr = "True"
        ret.append("        return "+expr)
        return "\n".join(ret)+"\n"

        
    def vformat(self,val):
        """Format a value according to val's type and the variable's type. By default, just pass it through."""
        if type(val)==int and self.vtype==TYPE_CHAR:
            """An integer is being formatted as a fixed with character. Be sure it is 0-filled"""
            return str(val).zfill(self.width)
        if type(val)==str and self.vtype==TYPE_CHAR:
            """An integer is being formatted as a fixed with character. Be sure it is 0-filled"""
            return str(val).rjust(self.width,' ')
        return str(val)

    def dump(self,func=print):
        func("  #{} [{}] {} {} {}".format(self.position,self.column,self.name,self.desc,self.vtype))
        for r in sorted(self.ranges):
            func("      {}".format(r))
        if self.default:
            func("      DEFAULT: {}".format(self.default))


class Table:
    """A class to represent a table in a database. Tables consist of variables, comments, overrides, and other data. They can be learned from SQL statements, from a Microsoft Word table, from a pandas data frame, or other mechanisms."""
    def __init__(self,*,name):
        if not valid_sql_name(name):
            raise RuntimeError("invalid SQL Table name: {}".format(name)) 
        self.filename = None    # filename from which table was read
        self.name     = name.replace(" ","_").replace(".","_")       # name of this table, change spaces to dots
        self.version  = None
        self.vardict  = OrderedDict() # an ordered list of the variables, by name
        self.comments = []
        self.overrides= {}      # variable:value
        
    def json_dict(self):
        return {"name":self.name,
                "variables":[var.json_dict() for var in self.vars()]}

    def dump(self,func=print):
        func("Table: {}  version:{}  variables:{}".format(self.name,self.version,len(self.vars())))
        for var in self.vars():
            var.dump(func)

    def add_comment(self,comment):
        self.comments.append(comment)

    def vars(self):
        """We need a list of the variables so much, this returns it"""
        return self.vardict.values()

    def varnames(self):
        """Provide a list of variable names"""
        return self.vardict.keys()

    def python_name(self):
        return self.name.replace(".","_").replace(" ","_")

    def get_variable(self,name):
        """Get a variable by name"""
        return self.vardict[name]

    def add_variable(self,var):
        """Add a Variable"""
        assert isinstance(var,Variable)
        if var.name in self.vardict:
            raise RuntimeError("Variable {} already in table {}".format(var.name,self.name))
        self.vardict[var.name] = var
        logging.info(f"Table {self.name}: Added variable {var}")

    def random_file_record(self,delim='|'):
        """Returns a random file record"""
        return delim.join([str(v.random_value()) for v in self.vars()])

    def random_dict_record(self):
        """Returns a random record as a dictionary"""
        return dict( [ (v.name,v.random_value()) for v in self.vars() ] )

    def default_record(self):
        """Returns a record with the defaults"""
        ret = {}
        for v in self.vars():
            if not (v.default is None):
                ret[v.name] = v.default
        return ret
        

    def sas_definition(self):
        """Generate a SAS table definition for this table"""
        vars = " ".join(self.varnames())
        tablename = self.name.upper().replace(".","").replace(" ","")
        tablefn = tablename+".txt"
        return SAS_TEMPLATE.format(tablefn,tablename,vars,tablename)

    def python_class(self,ignore_vars=[]):
        """Generate a Python validator and class for a table element. Ignore any variables specified in ignore_vars"""
        ret  = []
        ret.append(SCHEMA_SUPPORT_FUNCTIONS)
        ret.append("")
        ret.append("class {}_validator:".format(self.python_name()))
        ret.append("".join([var.python_validator() for var in self.vars()]))
        ret.append("    @classmethod")
        ret.append("    def validate_pipe_delimited(self,x):".format(self.python_name()))
        ret.append("        fields = x.split('|')")
        ret.append("        if len(fields)!={}: return False".format(len(self.vars())))
        for (i,v) in enumerate(self.vars(), 1):
            if v.name in ignore_vars:
                continue
            ret.append("        if {}(fields[{}]) == False: return False".
                           format(v.python_validator_name(),i))
        ret.append("        return True")
        ret.append("")
        ret.append("class {}:".format(self.python_name()))
        ret.append("    __slots__ = [" + ", ".join(["'{}'".format(var.name) for var in self.vars()]) + "]")
        ret.append("    def __repr__(self):")
        ret.append("        return '{}<".format(self.python_name()) +
                   ",".join(["%s:{}" % var.name for var in self.vars()]) + ">'.format(" +
                   ",".join(["self.%s" % var.name for var in self.vars()]) + ')')
        
        ret.append("    def __init__(self,line=None):")
        ret.append("        if line: ")
        ret.append("            if '|' in line: ")
        ret.append("                self.parse_pipe_delimited(line)")
        ret.append("            else:")
        ret.append("                self.parse_position_specified(line)")
        ret.append("    @classmethod")
        ret.append("    def name(self):")
        ret.append("        return '{}'".format(self.python_name()))
        ret.append("")
        ret.append("    def parse_pipe_delimited(self,line):")
        ret.append("        fields = line.split('|')")
        ret.append("        if len(fields)!={}:".format(len(self.vars())))
        ret.append("            raise ValueError(f'expected {} fields, found {}')".format(len(self.vars()),"{len(fields)}"))
        for (i,v) in enumerate(self.vars(), 0):
            if v.name in ignore_vars:
                continue
            ret.append("        self.{} = fields[{}]".format(v.name,i))
        ret.append("")
        ret.append("    def parse_position_specified(self,line):")
        for var in self.vars():
            if (var.column==None) or (var.width==None) or (var.name in ignore_vars):
                ret.append("        self.{} = None   # no column information for {}".format(var.name,var.name))
            else:
                ret.append("        self.{} = line[{}:{}]".format(
                    var.name, var.column, var.column+var.width))

        ret.append("")
        ret.append("    def validate(self):")
        ret.append('        """Return True if the object data validates"""')
        for var in self.vars():
            if var.name in ignore_vars:
                continue
            ret.append("        if not %s_validator.is_valid_%s(self.%s): return False" %
                       (self.python_name(), var.name, var.name))
        ret.append("        return True")
        ret.append("")
        ret.append("    def validate_reason(self):")
        ret.append("        reason=[]")
        for var in self.vars():
            if var.name in ignore_vars:
                continue
            ret.append("        if not %s_validator.is_valid_%s(self.%s): reason.append('%s ('+str(self.%s)+') out of range (%s)')" %
                       (self.python_name(), var.name, var.name, var.name, var.name, var.python_validation_text()))
        ret.append("        return ', '.join(reason)")

        ret.append("")
        ret.append("    def SparkSQLRow(self):")
        ret.append('        """Return a SparkSQL Row object for this object."""')
        ret.append('        from pyspark.sql import Row')
        ret.append('        return Row(')
        for var in self.vars():
            if var.name in ignore_vars:
                continue
            ret.append("            {name_lower}=safe_{python_type}(self.{name}),"
                       .format(name_lower=var.name.lower(),
                               python_type=var.python_type.__name__,
                               name=var.name))
        ret.append('        )')
        ret.append('')

        return "\n".join(ret) + "\n"
                                               

    def sql_schema(self):
        """Generate CREATE TABLE statement for this schema"""
        ret = []
        for line in self.comments:
            ret.append("-- {}".format(line))
        ret.append("CREATE TABLE {} (".format(self.name))
        for v in self.vars():
            ret.append("   {} {}, -- {}".format(v.name, v.sql_type(), v.desc))
        ret.append(");")
        return "\n".join(ret)+"\n"

    def sql_insert(self):
        """Generate a prepared INSERT INTO statement for this schema"""
        return "INSERT INTO {} ".format(self.name) +\
            "(" + ",".join(self.varnames()) + ") " +\
            "VALUES (" + ",".join("?" for v in self.vars()) + ");"
        
    def sql_prepare(self):
        lines = []
        i = 0
        for (i,v) in enumerate(self.vars(), 1):
            if not v.column:
                raise RuntimeError("NO COLUMN FOR {}".format(v.name))
            (start,end) = v.column
            assert start<=end
            s = None
            if v.vtype in [TYPE_INTEGER,TYPE_NUMBER]:
                s = "if (sqlite3_bind_int( s, {}, get_int( line, {}, {})) != SQLITE_OK) error({});"\
                    .format(i+1, start, end, i+1)
            if v.vtype in [TYPE_CHAR,TYPE_VARCHAR]:
                s = "if (sqlite3_bind_text( s, {}, get_text( line, buf, sizeof(buf), {}, {}), {}, SQLITE_TRANSIENT) != SQLITE_OK) error({});"\
                    .format(i+1, start, end, start, i+1)
            if s==None:
                raise RuntimeError("Cannot compile {} type {}".format(v.name,v.vtype))
            lines.append(s)
        return "\\\n".join(lines)
                
    def data_line_len(self):
        return max([v.column[1] if v.column else 0 for v in self.vars()])

    def write_sql_scanner(self,f):
        f.write('#define SQL_CREATE "{}"\n'.format(self.sql_schema().replace("\n"," ")))
        f.write('#define SQL_INSERT "{}"\n'.format(self.sql_insert()))
        f.write("#define SQL_PREPARE(s,line)\\\n{}\n".format(self.sql_prepare()))
        f.write("#define DATA_LINE_LEN {}\n".format(self.data_line_len()))
    
    def open_csv(self,fname,delimiter=',',extra=[],mode='w',write_header=True):
        import csv
        """Open a CSV file for this table"""
        logging.info("open_csv('{}','{}')".format(fname,mode))
        if os.path.exists(fname) and mode!='a':
            logging.error("{}: exists".format(fname))
            raise RuntimeError("{}: exists".format(fname))
        self.csv_file  = open(fname,mode, newline="", encoding='utf-8')
        self.csv_writer= csv.writer(self.csv_file, delimiter=delimiter, quoting=csv.QUOTE_MINIMAL)
        if write_header:
            self.csv_writer.writerow(list(self.varnames()) + extra)

    def parse_line_to_dict(self,line, delimiter=None):
        """Parse a line (that was probably read from a text file) using the current table schema and return a dictionary of values.
        If no delimiter is specified, assumes that the line is column-specified."""
        assert len(self.vars()) > 0
        if delimiter:
            fields = line.split(delimiter)
            try:
                return {v.name:fields[i] for i, v in enumerate(self.vars())}
            except IndexError as ie:
                logging.error("Number of fields in line ({}) does not match number of variables ({})".
                              format(len(fields),len(self.vars())))
                logging.error("fields: {}".format(fields))
                logging.error("vars:   {}".format(self.vars()))
                raise ie
        return { v.name:line[ v.column: v.column+v.width+1] for v in self.vars() if v.column}

    def unparse_dict_to_line(self,d,delimiter=','):
        return delimiter.join([v.vformat(self.overrides.get(v.name,d[v.name])) for v in self.vars()])

    def write_dict(self,d):
        """Write a dict using the current .csv_writer. Honors overrides."""
        try:
            self.csv_writer.writerow( [v.vformat(self.overrides.get(v.name,d[v.name])) for v in self.vars()] )
        except KeyError as e:
            logging.error("*****************   KeyError Error in write_dict for table {} **************".format(self.name))
            logging.error("varnames: {}".format(self.varnames()))
            logging.error("overrides: {}".format(self.overrides))
            logging.error("d: {}".format(d))
            logging.error("d is missing the following keys:")
            for key in self.varnames():
                if key not in d:
                    logging.error("   {}".format(key))
            raise e

    #def extract_variable_from_line(self,varname,line):
    #    v = self.vardict[varname]
    #    return line[ v.column[0]: v.column[1]+1]

    #def write_row(self,row,extra=[]):
    #    """Write a row using the CSV writer"""
    #    self.csv_writer.write_row( row + extra)

    def parse_line_to_row(self,line):
        """Parse a line using the current table schema and return an array of values. 
        Much more efficient than extracting variables one at a time.""" 
        assert len(self.vars()) > 0
        return [ line[ v.column: v.column+v.width+1] for v in self.vars() if v.column]

    #def parse_and_write_line(self,line,extra=[]):
    #    self.write_row(self.parse_line_to_row(line), extra=extra)

    def create_overrides(self,keyword):
        for var in self.vars():
            for r in var.ranges:
                if keyword in r.desc:
                    self.overrides[var.name] = r.a # we only override with the first element of the range

    def find_default_from_allowable_range_descriptions(self,text):
        """Call this for every variable. We do that a lot; perhaps we need an @every decorator or something"""
        print("TABLE: {}".format(self.name))
        for var in self.vars():
            var.find_default_from_allowable_range_descriptions(text)



# Recodes are implemented as compiled code. The function is put into the __main__ namespace.
# https://stackoverflow.com/questions/19850143/

class Recode:
    """Represent a recode from one table to another.  A recode consists of two parts:
    DESTINATION=SOURCE
    Where DESTINATION is a Python variable, ideally a TABLE[VARIABLE] form, but could be anything.
    Where SOURCE=A Python statement that will be evaluated. (It is actually compiled). Any Python may be used.
    Any use of TABLE[VARIABLE] may be provided; a row from TABLE must have bee read.
    """
    recode_re = re.compile(r"(\w+)\[(\w+)\]\s*=\s*(.*)")

    def __init__(self,name,desc):
        """@param name - The name of the recode, e.g. "recode1"
        @param desc - The description of the record, e.g. "A[T] = B[T]"
        """
        self.name = name
        m = self.recode_re.search(desc)
        if not m:
            raise RuntimeError("invalid Recode description: '{}'".format(desc))
        (self.dest_table_name,self.dest_table_var)  = m.group(1,2)
        self.statement = desc
        
class Schema:
    """A Schema is a collection of tables and recodes"""
    def __init__(self,name=''):
        self.tabledict = OrderedDict()        # by table name
        self.recodes = OrderedDict()
        self.tables_with_recodes = set()
        self.name    = name        

    def json_dict(self):
        return {"tables":{table.name:table.json_dict() for table in self.tables()}}

    def dump(self,func=print):
        func("Schema DUMP {}:".format(self.name))
        func("Tables: {}  Recodes: {}".format(len(self.tables()),len(self.recodes)))
        for table in self.tables():
            table.dump(func)
        for recode in self.recodes.values():
            recode.dump(func)

    def add_table(self,t):
        self.tabledict[t.name] = t
        logging.info("Added table {}".format(t.name))

    def get_table(self,name):
        try:
            return self.tabledict[name]
        except KeyError as e:
            logging.error("Table {} requested; current tables: {}".format(name,self.table_names()))
            raise e

    def tables(self):
        return self.tabledict.values()

    def table_names(self):
        return self.tabledict.keys()

    def sql_schema(self):
        return "\n".join([table.sql_schema() for table in self.tables()])
        
    ################################################################
    ### SQL Support
    ################################################################
    def add_sql_table(self,stmt):
        """Use the SQL parser to parse the create statement. Each parsed row is returned as a dictionary.
        The keys of the dictionary just happen to match the parameters for the Variable class."""
        sql = sql_parse_create(stmt)
        table = Table(name=sql[SQL_TABLE])
        for vdef in sql[SQL_COLUMNS]:
            v = Variable(vtype=vdef['vtype'],name=vdef['name'])
            table.add_variable(v)
        self.add_table(table)


    ################################################################
    ### Pandas support
    ################################################################
    def get_pandas_file_reader(self,filename,chunksize=PANDAS_CHUNKSIZE):
        (base,ext) = os.path.splitext(filename)
        import pandas
        if ext==SAS7BDAT_EXT:
            return pandas.read_sas( dconfig.dopen(filename), chunksize=chunksize, encoding='latin1')
        if ext==CSV_EXT:
            return pandas.read_csv( dconfig.dopen(filename), chunksize=chunksize, encoding='latin1')
        if ext==TXT_EXT:
            # Get the first line and figure out the seperator
            with dconfig.dopen(filename) as f:
                line = f.readline()
            if line.count("|") > 2:
                sep='|'
            elif line.count("\t") > 2:
                sep='\t'
            else:
                sep=','
            logging.info('sep={}'.format(sep))
            return pandas.read_csv( dconfig.dopen(filename), chunksize=chunksize, sep=sep, encoding='latin1')
        logging.error("get_pandas_file_reader: unknown extension: {}".format(ext))
        raise RuntimeError("get_pandas_file_reader: unknown extension: {}".format(ext))


    ################################################################
    ### Load tables from a specification file
    ################################################################
    def load_schema_from_file(self,filename):

        if filename.endswith(".docx"):
            raise RuntimeError("Schema cannot read .docx files; you probably want to use CensusSpec")

        if filename.endswith(".xlsx"):
            raise RuntimeError("Schema cannot read .docx files; you probably want to use CensusSpec")

        # Make a table
        table_name = os.path.splitext(os.path.split(filename)[1])[0]
        table_name = table_name.replace("-","_")
        table = Table(name=table_name)
        table.filename = filename
        table.add_comment("Parsed from {}".format(filename))

        # Load the schema from the data file.
        # This will use pandas to read a single record.
        for chunk in self.get_pandas_file_reader(filename,chunksize=1):
            for row in chunk.to_dict(orient='records'):
                for colName in chunk.columns:
                    v = Variable()
                    v.set_name(colName)
                    v.set_vtype(vtype_for_numpy_type(type(row[colName])))
                    table.add_variable(v)
                self.add_table(table)
                return

    ################################################################
    ### Read records support
    ################################################################
    def read_records_as_dicts(self,*,filename=None,tablename,limit=None,random=False):
        table = self.get_table(tablename)
        if filename==None and table.filename:
            filename = table.filename
        count = 0
        
        if random:
            # Just make up random records
            assert limit>0
            for count in range(limit):
                yield table.random_dict_record()
            return

        # Get pandas file reader if we have one
        reader = self.get_pandas_file_reader(filename)
        if reader:
            for chunk in reader:
                for row in chunk.to_dict(orient='records'):
                    yield(row)
                    count +=1
                    if limit and count>=limit:
                        return
        else:
            with open(filename,"r") as f:
                for line in f:
                    data = table.parse_line_to_dict(line)
                    yield data
                    count +=1 
                    if limit and count>=limit:
                        break

    ################################################################
    ### Recode support
    ################################################################

    def recode_names(self):
        """Return an array of all the recode names"""
        return [r.name for r in self.recodes.values()]

    def add_recode(self,name,vtype,desc):
        """Add a recode, and create the variable for the recode in the destination table"""
        assert type(name)==str
        assert type(vtype)==str
        assert type(desc)==str
        assert desc.count("=")==1
        r = Recode(name=name, desc=desc)
        v = Variable(name=r.dest_table_var, vtype=vtype)
        self.get_table(r.dest_table_name).add_variable(v)
        self.recodes[name]=r
        self.tables_with_recodes.add(r.dest_table_name)

    def compile_recodes(self):
        """The recode function is a function called recode() in the self.recode_module.
        Global variables are put in the module for each variable in every table. When the recode module is called,
        additional global variables are created for the data of every line that has been read, and the dictionary of data being recoded."""
        from types import ModuleType
        self.recode_module = ModuleType('recodemodule')

        # Add the variables for every table
        for table in self.tables():
            for v in table.vardict.values():
                self.recode_module.__dict__[v.name] = v.name

        self.recode_func = "def recode():\n"
        for recode in self.recodes.values():
            self.recode_func += "    {}\n".format(recode.statement)
        self.recode_func += "    return\n" # terminate the function (useful if there are no recodes)
        compiled = compile(self.recode_func, '', 'exec')
        exec(compiled, self.recode_module.__dict__)
        # Now create empty directories to receive the variables for the first recode
        for tablename in self.tables_with_recodes:
            self.recode_module.__dict__[tablename] = {}

    def recode_load_data(self,tablename,data):
        self.recode_module.__dict__[tablename] = data

    def recode_execute(self,tablename,data):
        if tablename not in self.tables_with_recodes:
            return
        self.recode_module.__dict__[tablename] = data
        try:
            self.recode_module.recode()
        except Exception as e:
            print("Error in executing recode.")
            print("tablename: {}  data: {}".format(tablename,data))
            print("Recode function:")
            print(self.recode_func)
            print(e)
            print("tables with recodes: {}".format(self.tables_with_recodes))
            print("recode environment:")
            print("defined variables:")
            for v in sorted(self.recode_module.__dict__.keys()):
                print("   ",v)
            raise e


    ################################################################
    ### Overrides
    ################################################################
    def create_overrides(self,keyword):
        """An override is a set of per-table values for variables. They are found by searching for a keyword
        in the description of allowable variables. Once found, they are automatically applied when records are written.
        """
        for table in self.tables():
            table.create_overrides(keyword)


    ### Defaults
    def find_default_from_allowable_range_descriptions(self,text):
        """Call this for every table. We do that a lot; perhaps we need an @every decorator or something"""
        for table in self.tables():
            table.find_default_from_allowable_range_descriptions(text)


if __name__=="__main__":
    from argparse import ArgumentParser,ArgumentDefaultsHelpFormatter
    parser = ArgumentParser( formatter_class = ArgumentDefaultsHelpFormatter,
                             description="Test functions for the schema module" )
    parser.add_argument("--dumpfile", help="examine a FILE and dump its schema")
    args = parser.parse_args()
    if args.dumpfile:
        db = Schema()
        db.load_schema_from_file(args.dumpfile)
        db.dump(print)
        print(db.sql_schema())

    
