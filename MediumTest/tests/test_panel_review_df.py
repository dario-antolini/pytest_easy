import pytest
import datetime
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.testing import assertDataFrameEqual, assertSchemaEqual

# import the module
from source_panel_review_df import *

spark = SparkSession.builder.getOrCreate()

@pytest.fixture(scope="function")
def df():
  schema = "id int, name string, email string, price int"
  df = spark.createDataFrame([(1, "Dario" , "dario.test@databricks.com"   , 3)
                            , (2, "Andrea", "andrea.mock@snowflake.fk.edu", 7)
                            , (3, "Sofia" , "sofia.err@avanade.com"       , 10)
                            , (4, None    , None                          , None)
                            ]
                           , schema=schema)
  return df

@pytest.fixture(scope="function")
def empty_df():
  schema = "id int, email string, price int"
  df = spark.createDataFrame([], schema=schema)
  return df

################################################################
# testing function add_current_date_column
def test_AddCurrentDateColumn(df):
  new_df = df.transform(add_current_date_column)

  today = datetime.datetime.today().date()
  expected_schema = "id int, name string, email string, price int, current_dt date"
  expected_df = spark.createDataFrame([(1 , "Dario" , "dario.test@databricks.com"   , 3     , today)
                                      , (2, "Andrea", "andrea.mock@snowflake.fk.edu", 7     , today)
                                      , (3, "Sofia" , "sofia.err@avanade.com"       , 10    , today)
                                      , (4, None    , None                          , None  , today)
                                      ]
                                    , schema=expected_schema)
  assertSchemaEqual(new_df.schema, expected_df.schema)
  assertDataFrameEqual(new_df, expected_df)

def test_AddCurrentDateColumn_toEmptyDf(empty_df):
  new_df = empty_df.transform(add_current_date_column)

  expected_schema = "id int, email string, price int, current_dt date"
  expected_df = spark.createDataFrame([], schema=expected_schema)
  assertSchemaEqual(new_df.schema, expected_df.schema)
  assertDataFrameEqual(new_df, expected_df)

################################################################
# testing function get_domainName_fromCol
def test_GetDomainNameFromCol(df):
  new_df = get_domainName_fromCol(df, "email")

  expected_schema = "id int, name string, email string, price int, domainName string"
  expected_df = spark.createDataFrame([(1 , "Dario" , "dario.test@databricks.com"   , 3   , "databricks")
                                      , (2, "Andrea", "andrea.mock@snowflake.fk.edu", 7   , "snowflake.fk")
                                      , (3, "Sofia" , "sofia.err@avanade.com"       , 10  , "avanade")
                                      , (4, None    , None                          , None, None)
                                      ]
                                    , schema=expected_schema)
  assertSchemaEqual(new_df.schema, expected_df.schema)
  assertDataFrameEqual(new_df, expected_df)

def test_GetDomainNameFromCol_ifColumnNotExist(df):
  with pytest.raises(Exception):
    get_domainName_fromCol(df, "fake_column")

def test_GetDomainNameFromCol_toEmptyDf(empty_df):
  new_df = get_domainName_fromCol(empty_df, "email")

  expected_schema = "id int, email string, price int, domainName string"
  expected_df = spark.createDataFrame([], schema=expected_schema)
  assertSchemaEqual(new_df.schema, expected_df.schema)
  assertDataFrameEqual(new_df, expected_df)

################################################################
# testing function compute_sum_fromCol
def test_ComputeSumFromCol(df):
  new_df = compute_sum_fromCol(df, "price")

  expected_schema = "total_sum long"
  expected_df = spark.createDataFrame([(20,)], schema=expected_schema)
  assertSchemaEqual(new_df.schema, expected_df.schema)
  assertDataFrameEqual(new_df, expected_df)

def test_ComputeSumFromCol_ifColumnNotExist(df):
  with pytest.raises(Exception):
    compute_sum_fromCol(df, "fake_column")

def test_ComputeSumFromCol_toEmptyDf(empty_df):
  new_df = compute_sum_fromCol(empty_df, "price")

  expected_schema = "total_sum long"
  expected_df = spark.createDataFrame([(None,)], schema=expected_schema)
  assertSchemaEqual(new_df.schema, expected_df.schema)
  assertDataFrameEqual(new_df, expected_df)
  
  




