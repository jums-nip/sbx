# Databricks notebook source
# Databricks notebook source
from pyspark.sql import SparkSession

# Create a Spark session
spark_session = SparkSession.builder.appName("nip_refined").getOrCreate()
# SQL code without %sql magic command
sql_code = """CREATE OR REPLACE TABLE nip_refined.ref_organization (
  organization STRING,
  book STRING,
  account_name STRING,
  specialty_program STRING,
  carrier STRING)
;
"""
spark_session.sql(sql_code)
