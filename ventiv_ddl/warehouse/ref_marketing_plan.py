# Databricks notebook source
spark.sql("USE CATALOG hive_metastore")
spark.sql("USE SCHEMA delta_nip")

# COMMAND ----------

from pyspark.sql import SparkSession

# Create a Spark session
spark_session = SparkSession.builder.appName("delta_nip").getOrCreate()
# SQL code without %sql magic command
sql_code = """CREATE OR REPLACE TABLE delta_nip.ref_marketing_plan (
  marketing_plan_code STRING,
  marketing_plan STRING)
;
"""
spark_session.sql(sql_code)
