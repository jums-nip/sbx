# Databricks notebook source
import datetime
import pandas as pd
import re, ast

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
dbutils.widgets.text("target_selection_set_filename_map","")

conditions = dbutils.widgets.get("conditions")
sort_order = dbutils.widgets.get("sort_order")
container_name = dbutils.widgets.get("container_name")
selection_set_filename = dbutils.widgets.get("selection_set_filename")
mapping_rule_set_key = dbutils.widgets.get("mapping_rule_set_key")
target_path = dbutils.widgets.get("target_path")
mapping_def_file_name = dbutils.widgets.get("mapping_def_file_name")
mapping_type = dbutils.widgets.get("mapping_type")
target_selection_set_filename_map = ast.literal_eval(dbutils.widgets.get("target_selection_set_filename_map"))



# COMMAND ----------

# # bug 769357
# columns= "*"
# container_name= "light-refined"
# mapping_def_file_name= "MMA_TEST_DEC_15_A"
# mapping_rule_set_key= "494"
# mapping_type= "Key to Key"
# selection_set_filename= "MMA_TEST_DEC_15_1"
# selection_set_key= "579"
# target_path= "output"
# target_selection_set_filename_map= ast.literal_eval('{"928":"MMA_TEST_DEC_15_3","927":"MMA_TEST_DEC_15_2"}')

# COMMAND ----------

source_path = f"/mnt/light-refined/mapping-definition/source_{mapping_def_file_name}_{selection_set_filename}.parquet"
df_src=spark.read.parquet(source_path)

source_type = mapping_type.split(' ')[0]
target_type = mapping_type.split(' ')[2]

# COMMAND ----------

# MAGIC %run ./common/query_list

# COMMAND ----------

db_url = dbutils.secrets.get(scope = "mappings", key = "mappings-backend-spring-datasource-url")
    
col_src_brc, col_src, col_src_head, col_src_name, col_trg_brc, col_trg_head = get_col_sets_cor_mapping_def(mapping_rule_set_key, db_url)

# COMMAND ----------

trg_col_assoc_keys = re.sub('\\[|\\]', '', col_trg_brc)

header_query = f"""select case mrsssa.is_srce_selection_set_flag when 1 then 'SOURCES' else 'TARGETS' end  as src_trg,
ss.selection_set_name, ssc.selection_set_mapping_tool_column_name, mrsssa.mapping_rule_set_selection_set_assoc_key
from mapping_rule_set mrs
join mapping_rule_set_selection_set_assoc mrsssa on mrs.mapping_rule_set_key = mrsssa.mapping_rule_set_key
join mapping_rule_set_selection_set_column_assoc ca on ca.mapping_rule_set_selection_set_assoc_key = mrsssa.mapping_rule_set_selection_set_assoc_key
join selection_set_column ssc on ssc.selection_set_column_key = ca.selection_set_column_key
join selection_set ss on mrsssa.selection_set_key = ss.selection_set_key
where mrs.mapping_rule_set_key = {mapping_rule_set_key}		
order by mrsssa.mapping_rule_set_selection_set_assoc_key, ca.mapping_rule_set_selection_set_column_assoc_key offset 0 rows"""

df_header_query = spark.read.format("com.microsoft.sqlserver.jdbc.spark")\
    .option("url", db_url) \
    .option("query", header_query) \
    .load()

# COMMAND ----------

# build hierarchical header
tuple_list = []
for row_number, row in df_header_query.toPandas().iterrows():
    tuple_list.append((row.src_trg, row.selection_set_name, row.selection_set_mapping_tool_column_name))

print(tuple_list)

# COMMAND ----------

pivot_query = get_pivot_query_mapping_def(col_src, col_src_brc, col_src_head, col_trg_brc, col_trg_head)

df_maprules = spark.read.format("com.microsoft.sqlserver.jdbc.spark") \
        .option("url", db_url) \
        .option("query", pivot_query) \
        .load()

# COMMAND ----------

df_final = df_src.join(df_maprules,col_src_name.split(","),'outer')

# check if conditions is None or if it is a whitespace-only string - Karl
if conditions is not None and conditions.strip() and conditions != "null":
    df_final = df_final.filter(conditions)

# Remove mapping_rule_column_group_id column <- the note that export includes unmapped rows
df_final = df_final.drop("mapping_rule_column_group_id","is_missing_srce_flag","is_missing_trgt_flag")
#I think we don't have any values for each df for multitarget ss voytek :D
# check if sort_order is None or if it is a whitespace-only string - Karl
if sort_order is not None and sort_order.strip() and sort_order != "null":
    df_final = df_final.orderBy(sort_order.split(","))


# COMMAND ----------

# if mapping of type a2k, k2k, target from DB includes only key column, the rest needs to be joined from targets' parquet files
if target_type == 'Key':
    # spark join breaks the column order so let's maintain the correct list = src + trg_1 + trg_2 + ...
    final_columns = df_src.columns
    for assoc_id in target_selection_set_filename_map:
        target_input_file_name = f"/mnt/light-refined/mapping-definition/target_{mapping_def_file_name}_{target_selection_set_filename_map[assoc_id]}.parquet"
        df_trg_pqt=spark.read.parquet(target_input_file_name)

        join_columns = [c for c in df_final.columns if c.startswith(f"trg#{assoc_id}")]
        # add prefix trg#NNNN_ to columns of this target, so they match the pivot output
        df_trg_addprefix = df_trg_pqt.toDF( *[ f'trg#{assoc_id}_{c}' for c in df_trg_pqt.columns])
        df_final = df_trg_addprefix.join(df_final, (join_columns), how='right')
        final_columns = final_columns + df_trg_addprefix.columns
    # enclose columns in `` - bug 716608
    df_final = df_final.select(*[ f"`{i}`" for i in final_columns ])

# COMMAND ----------

#convert spark to pandas dataframe
pdf = df_final.toPandas()

# COMMAND ----------

# Create a new MultiIndex for proper column headers
pdf.columns = pd.MultiIndex.from_tuples(tuple_list)


excel_file_name = f"Source_{mapping_type}_{mapping_def_file_name}_{datetime.date.today().strftime('%Y%m%d')}.xlsx".replace(" ", "_")
writer = pd.ExcelWriter(f"/tmp/{excel_file_name}", engine='xlsxwriter')
#write just header, removing first column
pdf.head(0).to_excel(writer, sheet_name='Sheet1',startcol=-1)
#flatten the multitindex
pdf.columns = ['#'.join(col).strip() for col in pdf.columns.values]

# exception returns empty url - to_excel might fail for example if the dataframe is too big (>1M rows)
try:
    # write just rows, no header, from row 4
    pdf.to_excel(writer, sheet_name='Sheet1', startrow=3, index=False, header = False)
    writer.close()
except Exception as e:
    dbutils.notebook.exit("")    

dbutils.fs.mv(f"file:///tmp/{excel_file_name}", f"/mnt/light-refined/export/{excel_file_name}")
print("write successful")

# COMMAND ----------

# find storage account which is mounted as unrefined
for i in dbutils.fs.mounts():
    if i[0] == '/mnt/light-refined':
        storage_url = i[1].replace("wasbs://light-refined@", "https://")
        break

urltarget = f"{storage_url}/light-refined/export/{excel_file_name}"
dbutils.notebook.exit(urltarget)
