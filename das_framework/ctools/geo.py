#!/usr/bin/env python3
#
"""
geo.py: some information about geographies.  This should be recast to use an official Census publication.
"""          
#
# 

__author__ = "Simson L. Garfinkel"
__version__ = "0.0.1"


STATE_DATA=[
    "Alabama/,Alabama,AL,01",
    "Alaska/,Alaska,AK,02",
    "Arizona/,Arizona,AZ,04",
    "Arkansas/,Arkansas,AR,05",
    "California/,California,CA,06",
    "Colorado/,Colorado,CO,08",
    "Connecticut/,Connecticut,CT,09",
    "Delaware/,Delaware,DE,10",
    "District_of_Columbia/,District_of_Columbia,DC,11",
    "Florida/,Florida,FL,12",
    "Georgia/,Georgia,GA,13",
    "Hawaii/,Hawaii,HI,15",
    "Idaho/,Idaho,ID,16",
    "Illinois/,Illinois,IL,17",
    "Indiana/,Indiana,IN,18",
    "Iowa/,Iowa,IA,19",
    "Kansas/,Kansas,KS,20",
    "Kentucky/,Kentucky,KY,21",
    "Louisiana/,Louisiana,LA,22",
    "Maine/,Maine,ME,23",
    "Maryland/,Maryland,MD,24",
    "Massachusetts/,Massachusetts,MA,25",
    "Michigan/,Michigan,MI,26",
    "Minnesota/,Minnesota,MN,27",
    "Mississippi/,Mississippi,MS,28",
    "Missouri/,Missouri,MO,29",
    "Montana/,Montana,MT,30",
    "Nebraska/,Nebraska,NE,31",
    "Nevada/,Nevada,NV,32",
    "New_Hampshire/,New_Hampshire,NH,33",
    "New_Jersey/,New_Jersey,NJ,34",
    "New_Mexico/,New_Mexico,NM,35",
    "New_York/,New_York,NY,36",
    "North_Carolina/,North_Carolina,NC,37",
    "North_Dakota/,North_Dakota,ND,38",
    "Ohio/,Ohio,OH,39",
    "Oklahoma/,Oklahoma,OK,40",
    "Oregon/,Oregon,OR,41",
    "Pennsylvania/,Pennsylvania,PA,42",
    "Rhode_Island/,Rhode_Island,RI,44",
    "South_Carolina/,South_Carolina,SC,45",
    "South_Dakota/,South_Dakota,SD,46",
    "Tennessee/,Tennessee,TN,47",
    "Texas/,Texas,TX,48",
    "Utah/,Utah,UT,49",
    "Vermont/,Vermont,VT,50",
    "Virginia/,Virginia,VA,51",
    "Washington/,Washington,WA,53",
    "West_Virginia/,West_Virginia,WV,54",
    "Wisconsin/,Wisconsin,WI,55",
    "Wyoming/,Wyoming,WY,56" ]
STATES=[dict(zip("dir_name,state_name,state_abbr,fips_state".split(","),line.split(","))) for line in STATE_DATA]

def state_rec(name=None,fips=None):
    for rec in STATES:
        if name:
            if name.lower()==rec['state_name'].lower() or name.lower()==rec['state_abbr'].lower():
                return rec
        if fips:
            if fips==rec['fips_state']:
                return rec
    raise ValueError(f"{name}: not a valid state name or abbreviation")


def state_fips(name):
    """Convert state name or abbreviation to FIPS code"""
    return state_rec(name=name)['fips_state']

def state_abbr(fips):
    """Convert state FIPS code to the abbreviation"""
    return state_rec(fips=fips)['state_abbr']

def all_state_abbrs():
    # Return a list of all the states 
    return [rec['state_abbr'].lower() for rec in STATES]
