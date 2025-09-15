# Databricks notebook source
spark.sql("USE CATALOG hive_metastore")
spark.sql("USE SCHEMA delta_nip")

# COMMAND ----------

from pyspark.sql import SparkSession

# Create a Spark session
spark_session = SparkSession.builder.appName("delta_nip").getOrCreate()
# SQL code without %sql magic command
sql_code = """CREATE OR REPLACE TABLE delta_nip.organization (
  as_of DATE,
  address_1 STRING,
  address_2 STRING,
  address_3 STRING,
  admin_unit_level_3_cd STRING,
  city STRING,
  country_iso_code STRING,
  description STRING,
  end_date DATE,
  fax_number STRING,
  last_updated_by STRING,
  last_updated_date DATE,
  latitude STRING,
  legal_name STRING,
  organization STRING,
  organization_status STRING,
  phone_number STRING,
  postal_code STRING,
  reason_for_leaving STRING,
  role STRING,
  start_date DATE,
  state STRING,
  update_geocoding STRING,
  organization_currency_iso_code STRING,
  org_last_updated_date_char STRING,
  organization_created_date_char STRING,
  organization_last_updated_date DATE,
  organization_created_date DATE)
;
"""
spark_session.sql(sql_code)
