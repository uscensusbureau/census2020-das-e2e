import zipfile
from urllib.parse import urlparse
from subprocess import run,Popen,PIPE
import copy
import json
import os
import os.path
import tempfile
import sys

#sys.path.append( os.path.join( os.path.dirname(__file__), "..") )
sys.path.append(os.path.dirname(os.path.dirname(__file__)))
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))

from ctools.s3 import S3File

if __name__=="__main__":
    from argparse import ArgumentParser, ArgumentDefaultsHelpFormatter
    parser = ArgumentParser(formatter_class=ArgumentDefaultsHelpFormatter)
    parser.add_argument("zipfile",help="zipfile to list")
    args = parser.parse_args()
    
    if not args.zipfile.startswith("s3://"):
        print("Please specify a zipfile that is on Amazon S3")
        exit(1)
    s3 = S3File( args.zipfile )
    zf = zipfile.ZipFile(s3, mode='r', allowZip64=True)
    print("Files in {}:".format(args.zipfile))
    first = None
    for name in zf.namelist():
        print(name)
        if not first:
            first = name
    print("\n")
    print("Contents of {}:".format(first))
    print( zf.open(name).read().decode('utf-8'))


    
