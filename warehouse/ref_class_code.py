# Databricks notebook source
# Databricks notebook source
from pyspark.sql import SparkSession

# Create a Spark session
spark_session = SparkSession.builder.appName("nip_refined").getOrCreate()
# SQL code without %sql magic command
sql_code = """CREATE OR REPLACE TABLE nip_refined.ref_class_code (
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
