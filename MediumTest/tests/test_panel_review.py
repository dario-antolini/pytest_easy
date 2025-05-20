import pytest

# import the module
from source_panel_review import double_number, divide_10

def test_double_number_0():
  assert double_number(0) == 0  

def test_double_number_2():
  assert double_number(2) == 4

@pytest.mark.xfail
def test_double_number_1_fail():
  assert double_number(1) == 1

class Test_divideBy10:
  def test_divide_by_2(self):
    assert divide_10(2) == 5

  @pytest.mark.xfail
  def test_divide_by_5_fail(self):
    assert divide_10(5) == 3  

  def test_divide_by_zero(self):
    with pytest.raises(ZeroDivisionError):
      divide_10(0)






