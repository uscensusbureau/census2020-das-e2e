import das_utils
import logging
import pytest

#def test_int_wlog(caplog):
def test_int_wlog():
    # caplog.set_level(logging.INFO)
    assert das_utils.int_wlog("2", "var") == 2
    assert das_utils.int_wlog("2003 ", "var") == 2003
    with pytest.raises(ValueError) as err:
        das_utils.int_wlog("2c03 ", "var")
    assert "invalid literal" in str(err.value)
    # assert "var value" in caplog.text