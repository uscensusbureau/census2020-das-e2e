#!/usr/bin/env python3
#
"""
hierarchical_configparser.py:

Like a regular configparser, but supports the INCLUDE= statement.
If INCLUDE=filename.ini is present in any section, the contents of that section are
read from filename.ini. If filename.ini includes its own INCLUDE=, that is included as well.

Name/value pairs in the included file are read FIRST, so that they can be shadowed by name/value
pairs in the including file. 

If the INCLUDE= is when the [DEFAULT] section, then the values of *every* section are included.

Don't have INCLUDE loops. I tried to protect against them, but it was too hard.
"""

import os
import os.path
from configparser import ConfigParser
from copy import copy

DEFAULT='default'
INCLUDE='include'

def fixpath(base,name):
    """If name is not an absolute path name, make it relative to the directory of base"""
    assert base[0]=='/'
    if name[0]=='/': 
        return name
    return os.path.join(os.path.dirname(base), name)

class HierarchicalConfigParser(ConfigParser):
    def read(self,filename):
        """First read the requested filename into a temporary config parser.
        Scan for any INCLUDE statements.If any are found in any section, read the included file 
        recursively, unless it has already been read.
        """
        if filename[0]!='/':
            filename = os.path.abspath(filename)

        cf = ConfigParser()
        if not os.path.exists(filename):
            raise FileNotFoundError(filename)
        cf.read(filename)

        sections = set(cf.sections())
        #print(filename,"READ SECTIONS:",sections)

        # If there is an INCLUDE in the default section, see if the included file
        # specifies any sections that we did not have. If there is, get those sections too
        default_include_file = None
        if DEFAULT in cf:
            if INCLUDE in cf[DEFAULT]:
                #print(filename,"INCLUDE IS IN DEFAULT SECTION")
                default_include_file = cf[DEFAULT][INCLUDE]
                if default_include_file:
                    pfn = fixpath(filename, default_include_file)
                    #print(filename,"READ",pfn,"TO GET SECTIONS")
                    if not os.path.exists(pfn):
                        raise FileNotFoundError(pfn)
                    cp = HierarchicalConfigParser()
                    cp.read(pfn)
                    #print(filename,"READ SECTIONS:",list(cp.sections()))
                    for section in cp.sections():
                        #print(filename,"FOUND SECTION",section,"IN",pfn)
                        if section not in sections:
                            #print(filename,"ADDING SECTION",section)
                            sections.add(section)

        # Now, for each section from the file we were supposed to read combined with the sections
        # specified in the default include file, see if there are any include files.
        # Note that there may potentially be two include files: one from the section, and one from
        # the default. We therefore read the default include file first, if it exists, and copy those
        # options over. Then we read the ones in the include if, if there is any, and copy those options over.
        #print(filename,"READING INCLUDE FILES FOR EACH SECTION")
        for section in sections:
            # make a local copy of the files we read for this section

            # If this section is not in self or cf, add it. (We must have gotten it from the default)
            if section not in self:
                self.add_section(section)
            if section not in cf:
                cf.add_section(section)

            section_include_file = cf[section].get(INCLUDE,None)
            for include_file in [default_include_file, section_include_file]:
                if include_file:
                    #print(filename,"READING SECTION",section,"FROM",include_file)
                    
                    pfn = fixpath(filename, include_file)
                    if not os.path.exists(pfn):
                        raise FileNotFoundError(pfn)
                    cp = HierarchicalConfigParser()
                    cp.read(pfn)
                    if section in cp:
                        #print(filename,"ADDING SECTION",section)
                        for option in cp[section]:
                            self.set(section,option, cp[section][option])

            # Now, copy over all of the options for this section in the file that we were 
            # actually asked to read, rather than the include file
            for option in cf[section]:
                self.set(section,option, cf[section][option])

        # All done
        #print(filename,"RETURNING:")
        #self.write(open("/dev/stdout","w"))
        #print(filename,"============")

    def read_string(self,string,source=None):
        raise RuntimeError("read_string not implemented")

    
