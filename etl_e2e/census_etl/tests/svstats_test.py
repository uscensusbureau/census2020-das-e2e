import sys
import os

sys.path.append(os.path.join(os.path.dirname(__file__),".."))


from svstats import *
import statistics

def test_svstats():
    a = SVStats()
    a.add(1)
    a.add(3)
    a.add(5)
    assert a.count==3
    assert a.countx==3
    assert a.sumx==9
    assert a.sumxx==35
    assert a.mean()==statistics.mean([1,3,5])
    
    b = SVStats()
    b.add(1)
    b.add(3)
    b.add("Test")
    assert b.uniques()==3
    assert b.min() == 1
    assert b.max() == 3
    
