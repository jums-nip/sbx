# Databricks notebook source
from pyspark.sql import SparkSession

# COMMAND ----------

db_url = dbutils.secrets.get(scope = "nip-scope-dev", key = "AzureSql-ConnectionString-Ventiv-JDBC")

# COMMAND ----------

# Define Databricks widgets for dynamic input
dbutils.widgets.text("src_schema", "dbo")              # Source SQL schema
dbutils.widgets.text("src_table", "source_table_name") # Source SQL table name
dbutils.widgets.text("trg_schema", "nip_refined")      # Target Delta schema
dbutils.widgets.text("trg_table", "delta_table_name")  # Target Delta table name

# Read widget values
src_schema = dbutils.widgets.get("src_schema")
src_table = dbutils.widgets.get("src_table")
trg_schema = dbutils.widgets.get("trg_schema")
trg_table = dbutils.widgets.get("trg_table")


# COMMAND ----------

# Combine schema and table for SQL source
full_src_table = f"{src_schema}.{src_table}"

# Read the source table into a DataFrame
source_df = spark.read.format("jdbc").options(
    url=db_url,
    dbtable=full_src_table
).load()

# Combine schema and table for Delta target
full_trg_table = f"{trg_schema}.{trg_table}"

# Write data to the target Delta table
source_df.write.format("delta").mode("overwrite").saveAsTable(full_trg_table)

dbutils.notebook.exit(f"Data from {full_src_table} successfully synced to {full_trg_table}")
