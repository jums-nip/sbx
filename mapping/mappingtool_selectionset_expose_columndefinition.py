# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC This should be replaced with a JDBC query like:
# MAGIC
# MAGIC select * from parquet.\`/mnt/{container_name}/{file_name}\` WHERE 1=0
# MAGIC
# MAGIC and getting the column definition from JDBC metadata.

# COMMAND ----------

import json

dbutils.widgets.text("file_name","")
dbutils.widgets.text("container_name","")
dbutils.widgets.text("target_path","")
dbutils.widgets.text("selection_set_condition_expression","")
dbutils.widgets.text("selection_set_query_type","")
dbutils.widgets.text("conditions","")


file_name = dbutils.widgets.get("file_name")
container_name = dbutils.widgets.get("container_name")
target_path = dbutils.widgets.get("target_path") #unsed
selection_set_condition_expression = dbutils.widgets.get("selection_set_condition_expression").strip(';\n') #they are changing it again to conditions - final
conditions = dbutils.widgets.get("conditions")
selection_set_query_type = dbutils.widgets.get("selection_set_query_type")

if target_path == container_name:
    target_path = ''

input_file = f"/mnt/{container_name}/{target_path}/{file_name}"

# COMMAND ----------

#debug
# file_name = 'cust_dim'
# container_name = 'input-mdm'
# conditions = ''
# selection_set_query_type = 'FILTER'
# input_file = f"/mnt/{container_name}/{target_path}/{file_name}"

# COMMAND ----------

df_data = spark.read.parquet(input_file)

# COMMAND ----------

# throw an error if comma in column name

for i in df_data.columns:
    if ',' in i:
        dbutils.notebook.exit(f'{{"fields":[],"status_message":"Comma in column name not supported: {i}"}}')

# COMMAND ----------

#bug 738414 - remove partitioning columns
#bug 801135 - do not remove partitioning columns, users like them; refactor reading algorithm instead

# partition_columns = [ i.split("=")[0] for i in df_data.inputFiles()[0].replace(input_file, "").split("/") if "=" in i]
# print("Dropping partition columns:", partition_columns)
# df_data = df_data.drop(*partition_columns)

# COMMAND ----------

try:
    df_data.createOrReplaceTempView('dataset')

    if selection_set_query_type == "FULL_QUERY":
        print("returned as full query")
        df_data = spark.sql(conditions)
    
    # convert column names to uppercase, bug 789641
    df_data = df_data.toDF ( *[i.upper() for i in df_data.columns])

    # If no exception, return status_message: OK
    response = json.loads(df_data.schema.json())
    response["status_message"] = "OK"

except Exception as e:
    dbutils.notebook.exit(f'{{"fields":[],"status_message":"{str(e)}"}}') 

# Convert the response to JSON and exit
dbutils.notebook.exit(json.dumps(response))
