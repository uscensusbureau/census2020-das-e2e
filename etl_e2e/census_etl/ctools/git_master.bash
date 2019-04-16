#!/bin/bash
git checkout -b newbranch; git checkout master; git merge newbranch; git branch -d newbranch
