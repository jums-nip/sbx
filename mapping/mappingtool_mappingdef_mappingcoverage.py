# Databricks notebook source
dbutils.widgets.text("selection_set_filename","")
dbutils.widgets.text("container_name","")
dbutils.widgets.text("target_path","")

selection_set_filename = dbutils.widgets.get("selection_set_filename")
container_name = dbutils.widgets.get("container_name") #unusable
target_path = dbutils.widgets.get("target_path") #unusable

# COMMAND ----------

# #debug
# selection_set_filename = 'AV_NEW_AV_SS_4'
# container_name = 'light-refined' 
# target_path = 'output'

# COMMAND ----------

#BUG - 634098 - prefix source string , replacing of spaces with underscores , should be handled at backend
concatStrings = ("mapping-definition/"+"source_"+selection_set_filename).replace(" ", "_")
print(concatStrings)

df = spark.read.parquet(f"/mnt/light-refined/{concatStrings}.parquet")

# COMMAND ----------

#df.distinct()
#df.display(n=10)

# COMMAND ----------

#it did a grouping by everything, running counts (all would be 1) after distinct, and then count of that
#flterdat = df.groupBy(*df).count()
#exposedata = flterdat.count()
dbutils.notebook.exit(df.distinct().count())
