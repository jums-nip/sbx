# Databricks notebook source
dbutils.widgets.text("mapping_def_file_name","")
dbutils.widgets.text("selection_set_filename","")
dbutils.widgets.text("columns","")
dbutils.widgets.text("conditions","")

selection_set_filename = dbutils.widgets.get("selection_set_filename")
mapping_def_file_name = dbutils.widgets.get("mapping_def_file_name")

columns = dbutils.widgets.get("columns").split(",") 
conditions = dbutils.widgets.get("conditions")


# COMMAND ----------

#debug
# selection_set_filename = 'JAN2024'
# mapping_def_file_name = 'TESTWP'

# COMMAND ----------

ssurl = f"/mnt/light-refined/selection-set/{selection_set_filename}.parquet"
targeturl = f"/mnt/light-refined/mapping-definition/source_{mapping_def_file_name}_{selection_set_filename}.parquet"

print(ssurl)
print(targeturl)

#some safeguard against empty columns parameter
if not columns or columns[0].strip() == '' or columns[0].strip() == 'null': columns = ['*'] 

#read selection set parquet for the mapdef source selection set parquet file for matching if columns match
# bug 805278: added fillna to have nullity consistent with import/export and UI
df_selectionset = spark.read.parquet(ssurl).select(columns).fillna("")

if conditions.strip() and conditions != "null":
    df_selectionset = df_selectionset.filter(conditions)

#read parquet for the selection set parquet file for matching if columns match
df_mapdef_selectionset =spark.read.parquet(targeturl)


# COMMAND ----------

#logic to match if the columns are refreshed before updating the data
if set(df_selectionset.columns) != set(df_mapdef_selectionset.columns):
    dbutils.notebook.exit('{"status": "INVALID", "status_desc": "columns in both source data do not match.", "row_cnt": ""}')


# COMMAND ----------

df_selectionset.write.option("compression", "snappy").mode("overwrite").parquet(targeturl)
dbutils.notebook.exit(f'{{"status": "VALID", "status_desc": "", "row_cnt": "{df_selectionset.count()}"}}')
