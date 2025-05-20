import datetime
import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

def add_current_date_column(df):
  return df.withColumn("current_dt", F.current_date())

def get_domainName_fromCol(df, col_name : str):
  if col_name not in df.columns:
    raise Exception(f"Column {col_name} not found in dataframe")
  return df.withColumn("domainName", F.regexp_extract(col_name, r".+@(.+)\.\w{2,4}", 1))

def compute_sum_fromCol(df, col_name : str):
  if col_name not in df.columns:
    raise Exception(f"Column {col_name} not found in dataframe")
  return df.select(F.sum(col_name).alias("total_sum"))