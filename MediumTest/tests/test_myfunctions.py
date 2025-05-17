##
## The file imports pytest and a variety of other pyspark packages.
##
import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

## Import the functions from src 
from helpers.setupCatalogSchema import *

def test_true():
  assert True

def test_false():
  assert False