#!/usr/bin/env python3
#
# Code to construct and print the certificate
#

import hashlib
from modulefinder import ModuleFinder
import sys
from subprocess import run,Popen,PIPE
from tytable import ttable,LONGTABLE
import datetime
import socket
from latex_tools import latex_escape,run_latex
import copy
import shutil
import tempfile
import os

__version__='0.0.1'

LOCAL_PATH = ["das_framework","certificate_printer"]
FULL_PATH  = os.path.dirname(__file__)

TEMPLATE   = "cert_template.tex"
SEAL       = "Seal_of_the_United_States_Census_Bureau.pdf"
BACKGROUND = "background1.jpg"

def system_module(mod):
    """A simple way to determine if a module is a system module"""
    if mod.__file__:
        return "lib/python" in mod.__file__
    return True                 # if no file, must be a system module

def file_stats(path):
    """Look at a path and return the lines, bytes, and sha-1"""
    try:
        with open(path,"rb") as f:
            data = f.read()
            hasher = hashlib.sha1()
            hasher.update(data)
            return (data.count(b"\n"), len(data), hasher.hexdigest())
    except TypeError as e:
        return None, None, None

def shell(cmd):
    """Run cmd and return stdout"""
    import sys
    if sys.version >= '3.6':
        return Popen(cmd,stdout=PIPE,encoding='utf-8').communicate()[0].strip()
    else:
        return Popen(cmd,stdout=PIPE).communicate()[0].strip().decode('utf-8')

def make_bom():
    """Generate a bill of materials and return the tt object."""

    finder = ModuleFinder()
    finder.run_script(sys.argv[0])
    tt = ttable()
    tt.add_option(LONGTABLE)
    tt.add_head(['name','ver','path','lines','bytes','sha-1'])
    tt.set_col_alignments("lllrrl")
    for inc in [False, True]:
        for name,mod in sorted(finder.modules.items()):
            if system_module(mod)!=inc:
                continue        # don't use this one
            stats = file_stats(mod.__file__)
            ver   = mod.globalnames.get('__version__', '---')
            if ver==1 and name in sys.modules:
                # It has a version number; get it.
                try:
                    ver = sys.modules[name].__version__
                except AttributeError as e:
                    ver = '---'
            fname = mod.__file__
            if type(name)!=str:
                name = ""
            if type(fname)!=str:
                fname = ""
            tt.add_data([ latex_escape(name), ver, latex_escape(fname) , stats[0], stats[1], stats[2]])
        tt.add_data(ttable.HR)
    return tt

def make_runtime():
    """Generate information about the runtime and return the tt object"""
    tt = ttable()
    tt.set_col_alignment(0, ttable.LEFT)
    tt.set_col_alignment(1, ttable.LEFT)
    tt.add_data(ttable.HR)
    for (k,v) in [[ "hostname", socket.gethostname()], 
                  [ "uptime"  , shell("uptime")],
                  [ "time"    , datetime.datetime.now().isoformat()[0:19]]]:
        tt.add_data([ k, v ])
        tt.add_data(ttable.HR)
    return tt

class CertificatePrinter:
    def __init__(self,*,title=None,template=None,params={}):
        self.title = title
        self.parts = []
        self.params = copy.deepcopy(params)        # parameters to substitute

        if os.path.exists(os.path.join(FULL_PATH, TEMPLATE)):
            self.template = os.path.join(FULL_PATH, TEMPLATE)
            self.seal = os.path.join(FULL_PATH, SEAL)
            self.background = os.path.join(FULL_PATH, BACKGROUND)
        else:
            self.template = os.path.join(*(LOCAL_PATH + [TEMPLATE]))
            self.seal = os.path.join(*(LOCAL_PATH + [SEAL]))
            self.background = os.path.join(*(LOCAL_PATH + [BACKGROUND]))


        if template is not None:
            self.template = template

    def add_params(self,params):
        """Merge params into the existing params"""
        self.params = {**self.params, **params}

    def typeset(self,pdf_name):
        """Typeset the template"""
        tt_bom = make_bom()
        tt_bom.latex_colspec="lllrrl"
        tt_run = make_runtime()
        self.params['%%ADDITION%%'] = tt_bom.typeset(mode='latex') + tt_run.typeset(mode='latex')
        with open(self.template) as f:
            data = f.read()
            for (k,v) in self.params.items():
                data = data.replace(k,v)
            outdir = tempfile.mkdtemp()
            out = tempfile.NamedTemporaryFile(encoding="utf-8",suffix=".tex",mode="w+t",dir=outdir,delete=False)
            out.write(data)
            out.flush()
            # Save a copy for testing
            # open("test.tex","w").write(data)
            shutil.copy(self.seal, os.path.dirname(out.name))
            shutil.copy(self.background, os.path.dirname(out.name))
            run_latex(out.name, delete_tempfiles=False, repeat=2)
            shutil.move(out.name.replace(".tex",".pdf"), pdf_name)
            shutil.rmtree(outdir)
