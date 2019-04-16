#!/usr/bin/env python3
"""procfname: Open a process and return a filename. Done with pipes and stuff."""

from subprocess import Popen,PIPE

def procfname(cmd,mode='r'):
    if 'r' in mode:
        p = Popen(cmd,stdout=PIPE,shell='/bin/sh')
        fd = p.stdout.fileno()
        print("fd=",fd)
        return f"/dev/fd/{fd}"
    
    if 'w' in mode:
        p = Popen(cmd,stdin=PIPE,shell='/bin/sh')
        fd = p.stdin.fileno()
        print("fd=",fd)
        return f"/dev/fd/{fd}"

    raise ValueError("mode ('{}') must contain a 'r' or a 'w'".format(mode))
    

if __name__=="__main__":
    # test by opening the calendar program
    name = procfname("cal 2018","r")
    print("name=",name)
    with open(name,"r") as f:
        for line in f:
            print("line:",line.strip())
