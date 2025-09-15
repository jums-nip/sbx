# Databricks notebook source
dbutils.widgets.text("target_path","")
dbutils.widgets.text("selection_set_filename","")
dbutils.widgets.text("container_name","")
dbutils.widgets.text("columns","")
dbutils.widgets.text("mapping_def_file_name","")
dbutils.widgets.text("conditions","")
# dbutils.widgets.text("start_position","")
# dbutils.widgets.text("offset","")

# target_path = "output"
# container_name  = "refined"
target_path = dbutils.widgets.get("target_path") #unusable
selection_set_filename = dbutils.widgets.get("selection_set_filename")
container_name = dbutils.widgets.get("container_name") #unusable
columns = dbutils.widgets.get("columns").split(",") 
mapping_def_file_name = dbutils.widgets.get("mapping_def_file_name")
conditions = dbutils.widgets.get("conditions")
# startposition_object = dbutils.widgets.get("start_position") #uncomment for the meantime as Olga mentioned
# offsetposition_object= dbutils.widgets.get("offset") #uncomment for the meantime as Olga mentioned

# COMMAND ----------

# #debug 

# target_path = 'output'
# selection_set_filename = 'dim_val_type'
# container_name = 'selection-set'
# columns = 'dim_val_type_key'.split(",")
# mapping_def_file_name = 'testwp'
# conditions = '' # not used so far

# COMMAND ----------

# bug 713531
# mapping_def_file_name="IMPRT_ATK_AV"
# mapping_rule_set_key="185"
# mapping_type="Attribute to Key"
# selection_set_filename="A2K_TST_AV_SRC"
# target_path = 'output'
# container_name = 'selection-set'
# columns = "*"


# COMMAND ----------

concatStrings = ("selection-set"+"/"+selection_set_filename)
print('concatStrings:',concatStrings)
print('columns:',columns)
print(conditions)

targeturl = (f"/mnt/light-refined/mapping-definition/source_{mapping_def_file_name}_{selection_set_filename}.parquet")
df = spark.read.parquet(f"/mnt/light-refined/{concatStrings}.parquet")

# bug 805278: added fillna to have nullity consistent with import/export and UI
df = df.select(columns).distinct().fillna("")

# COMMAND ----------

df.write.option("compression", "snappy").mode("overwrite").parquet(targeturl)

print("parquet file write successfully")
print("Output location: " + targeturl )
dbutils.notebook.exit(f'{{"status": "VALID", "status_desc": "", "row_cnt": "{df.count()}"}}')
