import os
import sys
import numpy as np
import pytest

# If there is __init__.py in the directory where this file is, then Python adds das_decennial to sys.path
# automatically. Not sure why and how it works, therefore, keeping the following line as a double
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(__file__)))))

@pytest.fixture()
def e2er():
    import programs.reader.e2e_reader as e2er
    return e2er

#def test_map_to_hhgq(e2er, caplog):
def test_map_to_hhgq(e2er):
    # caplog.set_level(logging.INFO)
    assert e2er.map_to_hhgq("112") == 1
    assert e2er.map_to_hhgq("234") == 2
    assert e2er.map_to_hhgq("200") == 2
    assert e2er.map_to_hhgq("  \t") == 0
    assert e2er.map_to_hhgq("   ") == 0
    assert e2er.map_to_hhgq("\t\n\t") == 0
    assert e2er.map_to_hhgq("\n\n\n") == 0

    with pytest.raises(ValueError) as err:
        e2er.map_to_hhgq("ads")
    assert "invalid literal" in str(err.value)
    # assert "GQTYPE value" in caplog.text

    with pytest.raises(AssertionError) as err:
        e2er.map_to_hhgq(1.23)
    assert "GQTYPE is not a str" in str(err.value)

    with pytest.raises(AssertionError) as err:
        e2er.map_to_hhgq("sadfasdf")
    assert "GQTYPE is not a str of length 3" in str(err.value)

    assert e2er.map_to_hhgq("299") == 2
    assert e2er.map_to_hhgq("845") == 7
    assert e2er.map_to_hhgq("999") == 7


#def test_per_keyed_by_mafid(e2er, caplog):
def test_per_keyed_by_mafid(e2er):
    # caplog.set_level(logging.INFO)

    line_template = "0{MAFID} 12345{AGE}9 123456789 123456789 123456789 123456789 1234567{CENHISP}9 123456789 123456789 123456789 1{CENRACE}456789 1234"

    line = line_template.format(**{"MAFID": "123456789", "AGE": " 67", "CENHISP": "2", "CENRACE": "23"})

    assert e2er.per_keyed_by_mafid(e2er.CEF_PER(line)) == (123456789, (1, 1, 22))

    line = line_template.format(**{"MAFID": "123456789", "AGE": " 17", "CENHISP": "1", "CENRACE": "01"})
    assert e2er.per_keyed_by_mafid(e2er.CEF_PER(line)) == (123456789, (0, 0, 0))

    line = line_template.format(**{"MAFID": "123456789", "AGE": " 17", "CENHISP": "1", "CENRACE": "YY"})
    with pytest.raises(ValueError) as err:
        e2er.per_keyed_by_mafid(e2er.CEF_PER(line))
    assert "invalid literal" in str(err.value)
    # assert "CENRACE value" in caplog.text

    line = line_template.format(**{"MAFID": "123456789", "AGE": " XX", "CENHISP": "1", "CENRACE": "23"})
    with pytest.raises(ValueError) as err:
        e2er.per_keyed_by_mafid(e2er.CEF_PER(line))
    assert "invalid literal" in str(err.value)
    # assert "QAGE value" in caplog.text

    line = line_template.format(**{"MAFID": "123456789", "AGE": " 12", "CENHISP": "X", "CENRACE": "23"})
    with pytest.raises(ValueError) as err:
        e2er.per_keyed_by_mafid(e2er.CEF_PER(line))
    assert "invalid literal" in str(err.value)
    # assert "CENHISP value" in caplog.text

    line = line_template.format(**{"MAFID": "123456789", "AGE": " 12", "CENHISP": "1", "CENRACE": "99"})
    with pytest.raises(ValueError) as err:
        e2er.per_keyed_by_mafid(e2er.CEF_PER(line))
    assert "CENRACE is outside" in str(err.value)
    # assert "CENRACE is outside" in caplog.text

    line = line_template.format(**{"MAFID": "123456789", "AGE": "012", "CENHISP": "8", "CENRACE": "23"})
    with pytest.raises(ValueError) as err:
        e2er.per_keyed_by_mafid(e2er.CEF_PER(line))
    assert "CENHISP is neither" in str(err.value)
    # assert "CENHISP is neither" in caplog.text


#def test_unit_keyed_by_mafid(e2er, caplog):
def test_unit_keyed_by_mafid(e2er):
    #caplog.set_level(logging.INFO)

    lt = "0{MAFID} 1234567{TEN}9 1{QGQTYP}{OIDTB}"

    assert e2er.unit_keyed_by_mafid(e2er.CEF_UNIT(lt.format(**{"MAFID":"123456789","TEN":"1","QGQTYP":"305","OIDTB":"0123456789012345678901"}))) == (123456789,(1,3,"0123456789012345678901"))
    assert e2er.unit_keyed_by_mafid(e2er.CEF_UNIT(lt.format(**{"MAFID":"123456789","TEN":"4","QGQTYP":"999","OIDTB":"0123456789012345678901"}))) == (123456789,(1,7,"0123456789012345678901"))

    with pytest.raises(ValueError) as err:
        e2er.unit_keyed_by_mafid(e2er.CEF_UNIT(lt.format(**{"MAFID":123456789,"TEN":"7","QGQTYP":"999","OIDTB":"0123456789012345678901"})))
    assert "TEN is outside" in str(err.value)
    #assert "TEN is outside" in caplog.text

    with pytest.raises(ValueError) as err:
        e2er.unit_keyed_by_mafid(e2er.CEF_UNIT(lt.format(**{"MAFID":123456789,"TEN":"X","QGQTYP":"999","OIDTB":"0123456789012345678901"})))
    assert "invalid literal" in str(err.value)
    #assert "TEN value" in caplog.text


#def test_unit_keyed_by_oidtb(e2er, caplog):
def test_unit_keyed_by_oidtb(e2er):
    # caplog.set_level(logging.INFO)

    lt = "0{MAFID} 1234567{TEN}9 1{QGQTYP}{OIDTB}"

    assert e2er.unit_keyed_by_oidtb(e2er.CEF_UNIT(lt.format(**{"MAFID": "123456789",
                                                               "TEN": "1",
                                                               "QGQTYP": "305",
                                                               "OIDTB": "0123456789012345678901"}))
                                    ) == (123456789012345678901, (1, 3, 123456789))

    assert e2er.unit_keyed_by_oidtb(e2er.CEF_UNIT(lt.format(**{"MAFID": "123456789",
                                                               "TEN": "4",
                                                               "QGQTYP": "999",
                                                               "OIDTB": "0123456789012345678901"}))
                                    ) == (123456789012345678901, (1, 7, 123456789))

    with pytest.raises(ValueError) as err:
        e2er.unit_keyed_by_oidtb(e2er.CEF_UNIT(lt.format(**{"MAFID": 123456789, "TEN": "7", "QGQTYP": "999", "OIDTB": "0123456789012345678901"})))
    assert "TEN is outside" in str(err.value)
    # assert "TEN is outside" in caplog.text

    with pytest.raises(ValueError) as err:
        e2er.unit_keyed_by_oidtb(e2er.CEF_UNIT(lt.format(**{"MAFID": 123456789, "TEN": "X", "QGQTYP": "999", "OIDTB": "0123456789012345678901"})))
    assert "invalid literal" in str(err.value)
    # assert "TEN value" in caplog.text

    with pytest.raises(ValueError) as err:
        e2er.unit_keyed_by_oidtb(e2er.CEF_UNIT(lt.format(**{"MAFID": 123456789, "TEN": "X", "QGQTYP": "999", "OIDTB": "01234567890123456abcde"})))
    assert "invalid literal" in str(err.value)
    # assert "is not numeric, conversion" in caplog.text


def test_per_to_agg(e2er):
    assert e2er.per_to_agg((18, 1, 0, "blank", 3, "GEOCODE")) == (("GEOCODE", 3, 18, 1, 0), 1)
    assert e2er.per_to_agg((1, 0, 23, "blank", 7, "GEOCODE")) == (("GEOCODE", 7, 1, 0, 23), 1)

    with pytest.raises(AssertionError) as err:
        e2er.per_to_agg((1, 2, 3))
    assert "Person row tuple (1, 2, 3) is not of length" in str(err.value)


def test_per_keyed_by_geo(e2er):
    assert e2er.per_keyed_by_geo((("GEOCODE", 3, 18, 1, 22), 100)) == (("GEOCODE",), ((3, 18, 1, 22), 100))

    with pytest.raises(AssertionError) as err:
        e2er.per_keyed_by_geo((1, 2, 3))
    assert "Person row tuple (1, 2, 3) is not of length" in str(err.value)


def test_unit_to_agg(e2er):
    assert e2er.unit_to_agg(("_", (0, 3, "GEOCODE"))) == (("GEOCODE", 3), 1)
    assert e2er.unit_to_agg(("_", (0, 0, "GEOCODE"))) == (("GEOCODE", 8), 1)
    assert e2er.unit_to_agg(("_", (1, 0, "GEOCODE"))) == (("GEOCODE", 0), 1)

    with pytest.raises(AssertionError) as err:
        e2er.unit_to_agg((1, 2, 3))
    assert "Unit row tuple (1, 2, 3) is not of length" in str(err.value)


def test_unit_keyed_by_geo(e2er):
    assert e2er.unit_keyed_by_geo((("GEOCODE", 3), 100)) == (("GEOCODE",), ((3,), 100))

    with pytest.raises(AssertionError) as err:
        e2er.unit_keyed_by_geo((1, 2, 3))
    assert "Unit row tuple (1, 2, 3) is not of length" in str(err.value)


# def test_to_ndarray(e2er, caplog):
def test_to_ndarray(e2er):
    # caplog.set_level(logging.INFO)

    row = [((0, 0), 100), ((0, 1), 50), ((1, 0), 10), ((1, 1), 1)]
    assert np.array_equal(e2er.to_ndarray(row, (2, 2)), np.array([[100, 50], [10, 1]]))

    row = [((0, 0), None ), ((0, 1), 50), ((1, 0), 10), ((1, 1), 1)]
    assert np.all(np.isclose(e2er.to_ndarray(row, (2, 2)), np.array([[np.nan, 50], [10, 1]]), equal_nan = True))
    # assert "Count in histogram element" in caplog.text

    row = [((0, 0), 100), ((0, 1), 50), ((1, 3), 10), ((1, 1), 1)]
    with pytest.raises(IndexError) as err:
        e2er.to_ndarray(row, (2, 2))
    assert "One or more indices in element" in str(err.value)
    # assert "One or more indices in element" in caplog.text

    row = [((0, 0), 100, 200), ((0, 1), 50), ((1, 0), 10), ((1, 1), 1)]
    with pytest.raises(ValueError) as err:
        e2er.to_ndarray(row, (2, 2))
    assert "is not a pair" in str(err.value)
    # assert "is not a pair" in caplog.text

    row = [((0, 0), 100), ((0, 1), 50), ((1, 0), 10), ((1, 1), 1)]
    with pytest.raises(IndexError) as err:
        e2er.to_ndarray(row, (2, 2, 3))
    assert "has different index dimensions" in str(err.value)
    # assert "has different index dimensions" in caplog.text
