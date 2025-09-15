# Databricks notebook source
# Databricks notebook source
from pyspark.sql import SparkSession

# Create a Spark session
spark_session = SparkSession.builder.appName("nip_refined").getOrCreate()
# SQL code without %sql magic command
sql_code = """CREATE OR REPLACE TABLE nip_refined.ref_marketing_plan (
  marketing_plan_code STRING,
  marketing_plan STRING)
;
"""
spark_session.sql(sql_code)
