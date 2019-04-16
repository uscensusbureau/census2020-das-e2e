# Some tests for tytable


from ctools.tytable import *

def test_ttable():
    a = ttable()
    a.add_head(['Head One','Head Two'])
    a.add_data([1,2])
    a.add_data(['foo','bar'])
    print( a.typeset(mode=LATEX ))
    print( a.typeset(mode=HTML ))

    a.add_data(['foo','bar'],annotations=['\\cellcolor{blue}',''])
    assert 'blue' in a.typeset(mode=LATEX)
