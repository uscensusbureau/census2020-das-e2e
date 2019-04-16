import pytest
import os
import sys
import pickle

# If there is __init__.py in the directory where this file is, then Python adds das_decennial directory to sys.path
# automatically. Not sure why and how it works, therefore, keeping the following line as a double
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(__file__)))))

try:
    import nodes
except ImportError as e:
    import programs.engine.nodes as nodes

def aNode():
    return nodes.geounitNode(geocode='44', geocodeDict={2: 'State', 1: 'National'})

def test_nodes_slotsFrozen():
    n = aNode()
    with pytest.raises(AttributeError):
        n.newAttribute = "This shouldn't work."

#def test_node_init(caplog):
def test_node_init():

    n = nodes.geounitNode(geocode='4400700010111000', geocodeDict={16: 'Block', 12: 'Block_Group', 11: 'Tract', 5: 'County'})
    assert n.parentGeocode == '440070001011'
    assert n.geolevel == 'Block'

    n = nodes.geounitNode(geocode='44', geocodeDict={16: 'Block', 12: 'Block_Group', 11: 'Tract', 5: 'County', 2: 'State'})
    assert n.parentGeocode == '0'
    assert n.geolevel == 'State'

    with pytest.raises(AttributeError) as err:
        nodes.geounitNode(geocode='44')
        assert 'geocodeDict not provided for creation of geoUnitNode' in err.value
       #assert 'geocodeDict not provided for creation of geoUnitNode' in caplog.text


def test_equal():
    fname = os.path.join(os.path.dirname(__file__), 'geounitnode.pickle')
    n1 = pickle.load(open(fname,'rb'))
    n2 = pickle.load(open(fname,'rb'))
    assert n1 == n2

if __name__ == "__main__":
    test_nodes_slotsFrozen()
