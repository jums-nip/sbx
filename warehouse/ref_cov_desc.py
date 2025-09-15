# Databricks notebook source
# Databricks notebook source
from pyspark.sql import SparkSession

# Create a Spark session
spark_session = SparkSession.builder.appName("nip_refined").getOrCreate()
# SQL code without %sql magic command
sql_code = """CREATE OR REPLACE TABLE nip_refined.ref_cov_desc (
  book STRING,
  LOB STRING,
  TPA STRING,
  descriptors_source_coverage STRING,
  cov_desc STRING,
  cov_desc_detail STRING)
;
"""
spark_session.sql(sql_code)
