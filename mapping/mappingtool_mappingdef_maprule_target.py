# Databricks notebook source
import json
import unicodedata

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ##This notebook and job shouldn't exist at all, it should be replaced with the following JDBC query:
# MAGIC ```
# MAGIC select distinct {columns} from 
# MAGIC parquet.`/mnt/light-refined/mapping-definition/target_{mapping_def_file_name}_{selection_set_filename}.parquet`
# MAGIC where 1=1 and {conditions}
# MAGIC ```

# COMMAND ----------

dbutils.widgets.text("target_path","")
dbutils.widgets.text("selection_set_filename","")
dbutils.widgets.text("container_name","")
dbutils.widgets.text("columns","")
dbutils.widgets.text("mapping_def_file_name","")
dbutils.widgets.text("conditions","")

target_path = dbutils.widgets.get("target_path")
selection_set_filename = dbutils.widgets.get("selection_set_filename")
columns = dbutils.widgets.get("columns").split(",")
mapping_def_file_name = dbutils.widgets.get("mapping_def_file_name")
conditions = dbutils.widgets.get("conditions")

# COMMAND ----------

# #debug
# target_path = "output"
# selection_set_filename = "PN_TRG_I"
# columns = "*"
# mapping_def_file_name = "MULTI"
# conditions = ""


# COMMAND ----------

full_file_path = f"/mnt/light-refined/mapping-definition/target_{mapping_def_file_name}_{selection_set_filename}.parquet"

print('full file path:',full_file_path)
print('columns:',columns)

df = spark.read.parquet(full_file_path)

# COMMAND ----------

df = df.select(columns)

# check if conditions is None or if it is a whitespace-only string - Karl
if conditions is not None and conditions.strip() and conditions != "null":
    df = df.filter(conditions)


# ADDED TEMP LIMIT TO 100 ROWS
df = df.distinct().limit(100)

# COMMAND ----------

#debug
#outputdf.display()

# COMMAND ----------

resultlist_json = [json.loads(x) for x in df.toJSON().collect()] 
exposedata = json.dumps(resultlist_json, sort_keys=True)
exposedata = unicodedata.normalize("NFC",exposedata)
dbutils.notebook.exit(exposedata)
