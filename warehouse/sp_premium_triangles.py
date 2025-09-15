# Databricks notebook source
# Databricks notebook source
from pyspark.sql import SparkSession

# Create a Spark session
spark_session = SparkSession.builder.appName("nip_refined").getOrCreate()
# SQL code without %sql magic command
sql_code = """CREATE OR REPLACE TABLE nip_refined.sp_premium_triangles (
  as_of DATE,
  book STRING,
  specialty_program STRING,
  carrier STRING,
  pol_eff_yr BIGINT,
  CAY SMALLINT,
  CAQ STRING,
  CAY_age DOUBLE,
  LOB STRING,
  organization STRING,
  account_id STRING,
  mac_id STRING,
  insured_name STRING,
  policy_number STRING,
  uw_company STRING,
  broker STRING,
  axa_broker STRING,
  billing_code STRING,
  marketing_plan STRING,
  program STRING,
  pol_eff_dt DATE,
  pol_exp_dt DATE,
  multiline_policy STRING,
  new_renew_flag DOUBLE,
  primary_coverage STRING,
  governing_class_code STRING,
  policy_state STRING,
  policy_state_region STRING,
  policy_state_ca_ind STRING,
  policy_zip STRING,
  insured_city STRING,
  insured_state STRING,
  insured_zip STRING,
  valen_score STRING,
  policy_count DOUBLE,
  written_premium DOUBLE,
  earned_premium DOUBLE)
;
"""
spark_session.sql(sql_code)
