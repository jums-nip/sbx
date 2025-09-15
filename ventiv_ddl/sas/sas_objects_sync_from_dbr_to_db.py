# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.functions import col


# COMMAND ----------

db_url = dbutils.secrets.get(scope = "nip-scope-dev", key = "AzureSql-ConnectionString-Ventiv-JDBC")

# COMMAND ----------

# Define Databricks widgets for dynamic input
dbutils.widgets.text("src_schema", "nip_refined")      # Source Delta schema
dbutils.widgets.text("src_table", "delta_table_name")  # Source Delta table name
dbutils.widgets.text("trg_schema", "dbo")             # Target SQL schema
dbutils.widgets.text("trg_table", "target_table_name") # Target SQL table name

# Read widget values
src_schema = dbutils.widgets.get("src_schema")
src_table = dbutils.widgets.get("src_table")
trg_schema = dbutils.widgets.get("trg_schema")
trg_table = dbutils.widgets.get("trg_table")



# COMMAND ----------

# Combine schema and table for Delta source
full_src_table = f"{src_schema}.{src_table}"

# Read the Delta table into a DataFrame
source_df = spark.table(full_src_table)

print(f"Data read from Delta table: {full_src_table}")

# COMMAND ----------

# Combine schema and table for SQL target
full_trg_table = f"{trg_schema}.{trg_table}"

# Write data to the SQL table using JDBC
source_df.write.format("jdbc").options(
    url=db_url,
    dbtable=full_trg_table,
    driver="com.microsoft.sqlserver.jdbc.SQLServerDriver"  # Change driver if needed
).mode("overwrite").save()

print(f"Data from {full_src_table} successfully synced to {full_trg_table}")
