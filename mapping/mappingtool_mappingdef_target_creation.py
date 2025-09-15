# Databricks notebook source
from pyspark.sql.functions import col, lit, expr, when
from pyspark.sql.types import IntegerType, DecimalType, BinaryType 

dbutils.widgets.text("target_path","")
dbutils.widgets.text("selection_set_filename","")
dbutils.widgets.text("container_name","")
dbutils.widgets.text("columns","")
dbutils.widgets.text("mapping_def_file_name","")
dbutils.widgets.text("conditions","")

target_path = dbutils.widgets.get("target_path") #unused
selection_set_filename = dbutils.widgets.get("selection_set_filename")
container_name = dbutils.widgets.get("container_name") #unused
columns = dbutils.widgets.get("columns")
mappingdef_filename = dbutils.widgets.get("mapping_def_file_name")
conditions = dbutils.widgets.get("conditions")
column_list = columns.split(",")

targeturl = f"/mnt/light-refined/mapping-definition/target_{mappingdef_filename}_{selection_set_filename}.parquet"
input_filename = f"/mnt/light-refined/selection-set/{selection_set_filename}.parquet"
df = spark.read.parquet(input_filename)

# COMMAND ----------

df = df.select(column_list)

if conditions is not None and conditions.strip() and conditions != "null":
    df = df.filter(conditions)

# bug 805278: added fillna to have nullity consistent with import/export and UI
df = df.distinct().fillna("")

# COMMAND ----------

df.write.option("compression", "snappy").mode("overwrite").parquet(targeturl)

print("parquet file write successfully")
print("Output location: " + targeturl ) 

dbutils.notebook.exit(f'{{"status": "VALID", "status_desc": "", "row_cnt": "{df.count()}"}}')
