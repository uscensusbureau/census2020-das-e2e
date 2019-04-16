import os
import sys
import logging
import numpy as np
from configparser import ConfigParser
import pytest
import zipfile
import tempfile
import shutil
import glob
import pickle


# If there is __init__.py in the directory where this file is, then Python adds das_decennial to sys.path
# automatically. Not sure why and how it works, therefore, keeping the following line as a double
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(__file__)))))

import programs.writer.e2e_writer as e2ew

# def test_to_per_line(caplog):
def test_to_per_line():

    with pytest.raises(AssertionError) as e:
        e2ew.to_per_line((1,2,3,4,5))
    assert "Record" in str(e.value)

    with pytest.raises(AssertionError) as e:
        e2ew.to_per_line(range(10))
    assert "Record" in str(e.value)

    with pytest.raises(AssertionError) as e:
        e2ew.to_per_line((1, 2, 3, 4, 5, 6, 7))
    assert "is not in str format" in str(e.value)

    with pytest.raises(AssertionError) as e:
        e2ew.to_per_line(("0123456789abcde",2,3,4,5,6,7))
    assert "Geocode of record" in str(e.value)

    with pytest.raises(AssertionError) as e:
        e2ew.to_per_line(("0123456789abcdef",2,3,"4",5,6,7))
    assert "HHGQ" in str(e.value)

    with pytest.raises(AssertionError) as e:
        e2ew.to_per_line(("0123456789abcdef",2,3,4,"5",6,7))
    assert "VAGE" in str(e.value)

    with pytest.raises(ValueError) as e:
        e2ew.to_per_line(("0123456789abcdef", "2", "3", 4, 5, 6, 7))
    assert "CENHISP is neither 1, nor 2 (" in str(e.value)
    #assert "CENHISP is neither 1, nor 2 (" in caplog.text

    with pytest.raises(ValueError) as e:
        e2ew.to_per_line(("0123456789abcdef", "2", "3", 4, 5, 1, 77))
    assert "CENRACE is outside [1,63] range (" in str(e.value)
    #assert "CENRACE is outside [1,63] range (" in caplog.text

    assert e2ew.to_per_line(("0123456789abcdef", "2", "3", 4, 5, 0, 7)) == "MPD|3.1.4|01|234|56789a|b|cdef|2|4|5|99|9|18|1|08|9999|9999|9999|9999|9999|9999|9999|9999|9999|9"
    assert e2ew.to_per_line(("0123456789abcdef", "2", "3", np.array([4])[0], 5, 0, 7)) == "MPD|3.1.4|01|234|56789a|b|cdef|2|4|5|99|9|18|1|08|9999|9999|9999|9999|9999|9999|9999|9999|9999|9"
    assert e2ew.to_per_line(("0123456789abcdef", "2", "3", 4, 5, 1, 22)) == "MPD|3.1.4|01|234|56789a|b|cdef|2|4|5|99|9|18|2|23|9999|9999|9999|9999|9999|9999|9999|9999|9999|9"
    assert e2ew.to_per_line(("0123456789abcdef", "2", "3", 4, 0, 0, 7)) == "MPD|3.1.4|01|234|56789a|b|cdef|2|4|5|99|9|17|1|08|9999|9999|9999|9999|9999|9999|9999|9999|9999|9"
    assert e2ew.to_per_line(("0123456789abcdef", "2", "3", 0, 5, 0, 7)) == "MPD|3.1.4|01|234|56789a|b|cdef|2|4|3|99|9|18|1|08|9999|9999|9999|9999|9999|9999|9999|9999|9999|9"