import pytest
import sys
import os
import os.path
from subprocess import call,check_call
import tempfile

sys.path.append( os.path.join(os.path.dirname(__file__), ".."))

from s3cat import *

def test_get_bucket_key():
    assert get_bucket_key("s3://bucket/path/1") == ("bucket", "path/1")
    assert get_bucket_key("bucket/path/1") == ("bucket", "path/1")
    assert get_bucket_key("/bucket/path/1") == ("bucket", "path/1")

LINE_TEMPLATE="{line}: This is line {line}. The remainder of the line is ignored. It's more efficient to make the line longer\n"


def validate_file(fname):
    with open(fname) as f:
        linenumber = 1
        for line in f:
            if int(line.split(":",1)[0])!=linenumber:
                raise RuntimeError("expected line {}: got line:".format(linenumber,line))
            linenumber+=1
    print(f"{linenumber} lines validated")

S3LOC =  f"{os.getenv('DAS_S3ROOT')}/test_s3cat/run{os.getpid()}"

def run_s3_testcase(case):
    # Make the files that we need, in order, and then upload them
    s3locs = []
    print("==== test case '{}' ====".format(case))
    linecounter = 1
    partcounter = 1
    for c in case:
        with tempfile.NamedTemporaryFile('w', delete=True) as tf:
            while tf.tell() < {"s":3000,
                               "m":3000000,
                               "B":1024*1024*5+1}[c]:
                tf.write(LINE_TEMPLATE.format(line=linecounter))
                linecounter += 1
            s3loc = S3LOC+"/part_{:04}".format(partcounter)
            cmd = ['aws','s3','cp',tf.name,s3loc]
            print(" ".join(cmd))
            check_call(cmd)
            s3locs.append(s3loc)
        partcounter += 1

    # Now test the join
    s3cat = os.path.join(os.path.dirname(__file__), "../s3cat.py")
    cmd = [sys.executable, s3cat, S3LOC] 
    print("CMD: {}".format(" ".join(cmd)))
    check_call(cmd)

    # Now download the result and validate
    with tempfile.NamedTemporaryFile(delete=False) as tf:
        cmd = ['aws','s3','cp',S3LOC,tf.name]
        print(" ".join(cmd))
        check_call(cmd)
        validate_file(tf.name)
        os.unlink(tf.name) # delete here, so it lives if we fail

    # And clean up S3
    for s3loc in s3locs + [S3LOC]:
        check_call(['aws','s3','rm',s3loc])

def test_s3cat_all_cases():
    # For testing s3cat there are many conditions that need to be considered.
    # Amazon's protocol cannot combine parts smaller than 5mb
    # We might be combining any number of parts that are bigger than 5MB (B) or smaller than 5mb (s).
    # We also want to test the case where combining small files makes them a big file, so we introduce medium files (M)
    # The program combine small parts into big ones by downloading them into /mnt/tmp.
    # We want to test a lot of test cases. So this specifies the test cases with combinations of B and s characters
    # It then creates the files, runs s3cat, and verifies that the results are correct.
    # The files consist of self-validating lines so that we can determine if they are correct or not automatically.

    test_cases=["s", "B", "ss", "BB", "sB", "Bs", 
                "ssB", "sBs", "Bss",
                "BsBs", "sBsB", "ssBss", "BBsBB",
                "m","mm","mmm","Bmm", "mmB", "smms",
                "BmmB"]

    for case in test_cases:
        run_s3_testcase(case)


if __name__=="__main__":
    from argparse import ArgumentParser,ArgumentDefaultsHelpFormatter
    parser = ArgumentParser( formatter_class = ArgumentDefaultsHelpFormatter,
                             description="Combine multiple files on Amazon S3 to the same file." )
    parser.add_argument("--validate", help="file to validate")
    parser.add_argument("--all", action='store_true', help="test all cases")
    parser.add_argument("cases",nargs="*")

    args = parser.parse_args()
    if args.validate:
        validate_file(args.validate)
        exit(0)
    if args.all:
        test_s3cat_all_cases()
        exit(0)
    for case in args.cases:
        run_s3_testcase(case)
                             
