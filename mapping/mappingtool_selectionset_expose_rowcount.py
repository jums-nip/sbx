# Databricks notebook source
dbutils.widgets.text("file_name","")
dbutils.widgets.text("container_name","")
dbutils.widgets.text("conditions","")

file_name = dbutils.widgets.get("file_name")
container_name = dbutils.widgets.get("container_name") #dynamic
conditions = dbutils.widgets.get("conditions")

# COMMAND ----------

# # debug values
# file_name = 'MM_HHP_BUYR_GRP_SRCE'
# container_name = 'cdl'
# columns = 'BUYR_GRP_SKID,BUYR_GRP_EXTRN_ID,BUYR_GRP_SKID,BUYR_GRP_EXTRN_ID,BUYR_GRP_SKID,BUYR_GRP_EXTRN_ID'.split(",")

# COMMAND ----------

try:
    df = spark.read.parquet(f"/mnt/{container_name}/{file_name}")

    if conditions.strip() and conditions != "null":
        df = df.filter(conditions)

    rowcount = df.count()

    status = "OK" if rowcount > 0 else "ERROR"
    status_desc = "INVALID" if rowcount == 0 else ""
    
except Exception as e:
    status = "ERROR"
    status_desc = str(e)
    rowcount = 0

finally:
    dbutils.notebook.exit(f'{{"status": "{status}", "status_desc": "{status_desc}", "rowcount": "{rowcount}"}}')
