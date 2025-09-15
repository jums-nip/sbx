# Databricks notebook source
spark.sql("USE CATALOG hive_metastore")
spark.sql("USE SCHEMA delta_nip")

# COMMAND ----------

from pyspark.sql import SparkSession

# Create a Spark session
spark_session = SparkSession.builder.appName("delta_nip").getOrCreate()
# SQL code without %sql magic command
sql_code = """CREATE OR REPLACE TABLE delta_nip.ref_class_code (
  LOB STRING,
  class_code STRING,
  class_code_desc STRING,
  auto_size_class STRING,
  auto_size_class_type STRING,
  auto_size_class_weight STRING,
  auto_business_use_class STRING,
  auto_radius_class STRING,
  auto_fleet STRING,
  auto_classification STRING,
  auto_subclassification STRING,
  auto_type STRING,
  auto_subtype STRING)
;
"""
spark_session.sql(sql_code)
