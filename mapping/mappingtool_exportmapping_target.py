# Databricks notebook source
import datetime
import pandas as pd
import re

# COMMAND ----------

dbutils.widgets.text("container_name","")
dbutils.widgets.text("sort_order","")
dbutils.widgets.text("mapping_def_file_name","")
dbutils.widgets.text("target_path","")
dbutils.widgets.text("conditions","")
dbutils.widgets.text("mapping_rule_set_key","")
dbutils.widgets.text("output_file_name","")
dbutils.widgets.text("selection_set_filename","")
dbutils.widgets.text("mapping_type","")

conditions = dbutils.widgets.get("conditions")
sort_order = dbutils.widgets.get("sort_order")
container_name = dbutils.widgets.get("container_name")
selection_set_filename = dbutils.widgets.get("selection_set_filename")
mapping_rule_set_key = dbutils.widgets.get("mapping_rule_set_key")
target_path = dbutils.widgets.get("target_path")
mapping_def_file_name = dbutils.widgets.get("mapping_def_file_name")
mapping_type = dbutils.widgets.get("mapping_type")

# COMMAND ----------

# debug
# columns="*"
# conditions=""
# container_name="light-refined"
# file_name="null"
# mapping_def_file_name="MAPDEF_MULTITARGET_KS"
# mapping_rule_set_key="55"
# mapping_type="Attribute to Attribute"
# sort_order=""
# target_path="output"

# COMMAND ----------

# MAGIC %run ./common/query_list

# COMMAND ----------

db_url = dbutils.secrets.get(scope = "mappings", key = "mappings-backend-spring-datasource-url")
    
col_src_brc, col_src, col_src_head, col_src_name, col_trg_brc, col_trg_head = get_col_sets_cor_mapping_def(mapping_rule_set_key, db_url)

# COMMAND ----------

trg_col_assoc_keys = re.sub('\\[|\\]', '', col_trg_brc)

header_query = f"""select ss.selection_set_name from mapping_rule_set mrs
join mapping_rule_set_selection_set_assoc mrsssa on mrs.mapping_rule_set_key = mrsssa.mapping_rule_set_key 
  and mrsssa.is_srce_selection_set_flag = 0
join selection_set ss on mrsssa.selection_set_key = ss.selection_set_key
where mrs.mapping_rule_set_key = {mapping_rule_set_key}"""

df_header_query = spark.read.format("com.microsoft.sqlserver.jdbc.spark")\
    .option("url", db_url) \
    .option("query", header_query) \
    .load()

# COMMAND ----------

# this makes no sense Karl
values_list = df_header_query.select("*").rdd.flatMap(lambda x: x).collect()
flatten_list = ','.join(values_list)
formatted_strings = [s.strip() for s in flatten_list.split(',')]

filenames = [filename.strip('"') for filename in formatted_strings]

# xlsxwriter engine
excel_file_name = f"Target_{mapping_type}_{mapping_def_file_name}_{datetime.date.today().strftime('%Y%m%d')}.xlsx"
excel_writer = pd.ExcelWriter(f"/tmp/{excel_file_name}", engine='xlsxwriter')

# Read each Parquet file into a DataFrame and write it to a separate sheet
for filename in filenames:
    parquet_path = f"/mnt/light-refined/mapping-definition/target_{mapping_def_file_name}_{filename.replace(' ', '_')}.parquet"

    try:
        # Convert the PySpark DataFrame to a Pandas DataFrame
        pandas_df = spark.read.parquet(parquet_path).distinct().toPandas()
        # Write the Pandas DataFrame to a new sheet in the Excel file with the desired name
        pandas_df.head(10000).to_excel(excel_writer, sheet_name=filename, index=False)
    except Exception as e:
        dbutils.notebook.exit(f"")    
    
excel_writer.close()

# COMMAND ----------

dbutils.fs.mv(f"file:///tmp/{excel_file_name}", f"/mnt/light-refined/export/{excel_file_name}")
print("write successful")

# COMMAND ----------

# find storage account which is mounted as unrefined
for i in dbutils.fs.mounts():
    if i[0] == '/mnt/light-refined':
        storage_url = i[1].replace("wasbs://light-refined@", "https://")

urltarget = f"{storage_url}/light-refined/export/{excel_file_name}"
dbutils.notebook.exit(urltarget)
