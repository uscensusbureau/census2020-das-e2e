#!/usr/bin/env python3
#
# single variable stats:
# 
# A class that provides a simple statistics package
# 
import math


class SVStats:
    def __init__(self):
        self.values  = set()
        self.count  = 0
        self.countx = 0
        self.sumx   = 0
        self.sumxx  = 0

    def add(self,val):
        self.values.add(val)
        self.count += 1
        try:
            self.sumx   += val
            self.sumxx  += val*val
            self.countx += 1
        except TypeError as e:
            pass                # guess we can't compute stats for this one
    def uniques(self):
        return len(self.values)
    def mean(self):
        return  self.sumx/self.countx
    def numbers(self):
        return (x for x in self.values if isinstance(x, (int, float, complex)))
    def min(self):
        return min((self.numbers()))
    def max(self):
        return max(self.numbers())
