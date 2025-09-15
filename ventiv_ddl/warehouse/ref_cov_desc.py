# Databricks notebook source
spark.sql("USE CATALOG hive_metastore")
spark.sql("USE SCHEMA delta_nip")

# COMMAND ----------

from pyspark.sql import SparkSession

# Create a Spark session
spark_session = SparkSession.builder.appName("delta_nip").getOrCreate()
# SQL code without %sql magic command
sql_code = """CREATE OR REPLACE TABLE delta_nip.ref_cov_desc (
  book STRING,
  LOB STRING,
  TPA STRING,
  descriptors_source_coverage STRING,
  cov_desc STRING,
  cov_desc_detail STRING)
;
"""
spark_session.sql(sql_code)
