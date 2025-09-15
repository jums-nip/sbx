# Databricks notebook source
import pyodbc, re, ast
from pyspark.sql.functions import col, coalesce
from functools import reduce

dbutils.widgets.text("mapping_def_file_name","")
dbutils.widgets.text("source_selection_set_filename","")
dbutils.widgets.text("mapping_rule_set_key","")
dbutils.widgets.text("target_selection_set_filename_map","")
dbutils.widgets.text("mapping_type","")


# COMMAND ----------

def run_pivot_query(mapping_rule_set_key, db_url):
    query = f"""(SELECT col_src_brc, col_src, col_src_head, col_src_name, col_trg_brc, col_trg_head FROM [mapping_rule_set_columns_fnc]({mapping_rule_set_key}))"""
    df_queryresult = spark.read.format("com.microsoft.sqlserver.jdbc.spark") \
        .option("url", db_url) \
        .option("query", query) \
        .load()

    col_src_brc, col_src, col_src_head, col_src_name, col_trg_brc, col_trg_head = df_queryresult.first()

    pivot_query = f"""(
    select mapping_rule_column_group_id, {col_src_head}, {col_trg_head}
    from (
    select mapping_rule_column_group_id, mapping_rule_srce_column_val,mapping_rule_set_srce_selection_set_column_assoc_key, mapping_rule_trgt_column_val, mapping_rule_set_trgt_selection_set_column_assoc_key
    from mapping_rule mr
    where mapping_rule_set_srce_selection_set_column_assoc_key in ({col_src})
    ) a
    pivot(
    max(mapping_rule_srce_column_val) for mapping_rule_set_srce_selection_set_column_assoc_key in ({col_src_brc})
    ) piv1
    pivot(
    max(mapping_rule_trgt_column_val) for mapping_rule_set_trgt_selection_set_column_assoc_key in ({col_trg_brc})
    ) piv2 group by mapping_rule_column_group_id
    ) """

    df = spark.read.format("com.microsoft.sqlserver.jdbc.spark") \
        .option("url", db_url) \
        .option("query", pivot_query) \
        .load()

    return df

# COMMAND ----------

def compare_columns(data_columns, mapdef_columns, option):
    # for A, mapdef columns and data columns need to be identical
    # for K, mapdef columns (key) need to exist in data
    data_columns = list(data_columns)
    mapdef_columns = list(mapdef_columns)

    if option == 'Attribute':
        if set(data_columns) != set(mapdef_columns):
            return (f"columns don't match, data has: {data_columns}, mapping definition has: {mapdef_columns}")
        else: return None
    if option == 'Key':
        if not set(mapdef_columns).issubset(set(data_columns)):
            return (f"columns don't match, data has: {data_columns}, mapping definition has: {mapdef_columns}")
        else: return None
    
    return (f"Unknown option: {option}")


# COMMAND ----------

def run_and_commit (cursor, update_command):
    cursor.execute (update_command)
    cursor.commit()

# COMMAND ----------

#set params to variable
src_selection_set_filename = dbutils.widgets.get("source_selection_set_filename")
mappingdef_filename = dbutils.widgets.get("mapping_def_file_name")
mapping_rule_set_key = dbutils.widgets.get("mapping_rule_set_key")
mapping_type = dbutils.widgets.get("mapping_type")
target_selection_set_filename_map = ast.literal_eval(dbutils.widgets.get("target_selection_set_filename_map"))

# COMMAND ----------

#debug k2k

# mappingdef_filename = 'SMOKE_KA'
# mapping_rule_set_key=51
# mapping_type = 'Key to Attribute'
# src_selection_set_filename='cust_dim'
# target_selection_set_filename_map = ast.literal_eval ('{"6":"prod_dim"}')


# bug  700634
# mappingdef_filename = 'MULTI_EDIT_AA'
# mapping_rule_set_key=103
# mapping_type = 'Attribute to Attribute'
# src_selection_set_filename='AV_SS_4'
# target_selection_set_filename_map = ast.literal_eval ('{"122":"Edit_Rule_2","123":"dim_type_1","124":"AV_SS_2A"}')

# # empty column test case
# mappingdef_filename = 'TEST_A_TO_K_II'
# mapping_rule_set_key = 164 
# mapping_type = 'Attribute to Attribute'
# src_selection_set_filename = 'PN_SRC' 
# target_selection_set_filename_map = ast.literal_eval('{"234":"PN_TRG_I","235":"PN_TRG_II","236":"PN_TRG_III"}' )

#bug 713531

# mappingdef_filename="IMPRT_ATK_AV"
# mapping_rule_set_key="185"
# mapping_type="Attribute to Key"
# src_selection_set_filename="A2K_TST_AV_SRC"
# target_selection_set_filename_map=ast.literal_eval('{"292":"A2K_TST_AV_TRGT_2", "293":"A2K_TST_AV_TRGT_3"}')


# COMMAND ----------

db_url = dbutils.secrets.get(scope = "mappings", key = "mappings-backend-spring-datasource-url")
conn_string = dbutils.secrets.get(scope = "mappings", key = "sqldb-pyodbc-conn-string")
cnxn = pyodbc.connect(conn_string)
cursor = cnxn.cursor()
source_input_file_name = f"/mnt/light-refined/mapping-definition/source_{mappingdef_filename}_{src_selection_set_filename}.parquet"

df_src_pqt=spark.read.parquet(source_input_file_name)
df_pivot = run_pivot_query(mapping_rule_set_key, db_url)
all_cg_ids = ",".join([str(i) for i in df_pivot.select('mapping_rule_column_group_id').distinct().rdd.flatMap(lambda x: x).collect()])


if mapping_type.lower() == 'expression mapping':
    source_type = target_type = 'Attribute'
else:    
    source_type = mapping_type.split(' ')[0]
    target_type = mapping_type.split(' ')[2]


# COMMAND ----------

# build columns dictionary: index by source or target ID, entry is a list of columns
# example how it looks like: {'src': ['BUYR_GRP_NAME', 'BUYR_GRP_SKID', 'RUN_ID'], '80': ['GEO_CODE', 'RUN_ID']}
# targets are marked by mapping_rule_set_selection_set_assoc_keys, 80 in this case

coldict = {}

for i in df_pivot.columns:
    if i == 'mapping_rule_column_group_id':
        continue
    
    if i.startswith('trg'):
        # search for #NNNNN_, remove # and _, convert to int
        assoc_id = re.search('#[0-9]*_', i).group()[1:-1]
        # column name: remove the trg#NNNN_
        column = re.sub("trg#[0-9]*_", "", i) 
    else:
        assoc_id = 'src'
        column = i
    #dict entry needs to be coerced into list type first, otherwise it won't have append method
    if assoc_id not in coldict:
        coldict[assoc_id] = []
    coldict[assoc_id].append(column)

print(coldict)


# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ###Comparison schema
# MAGIC
# MAGIC |object|A2A|A2K|K2A|K2K|
# MAGIC |-|-|-|-|-|
# MAGIC |Source columns|DB cols == pqt.cols|DB cols == pqt.cols|DB cols in pqt.cols|DB cols in pqt.cols|
# MAGIC |Target columns|DB cols == pqt.cols|DB cols in pqt.cols|DB cols == pqt.cols|DB cols in pqt.cols|
# MAGIC |Source data|Compare all cols|Compare all cols|Compare key cols|Compare key cols|
# MAGIC |Target data|Compare all cols|Compare key cols|Compare all cols|Compare key cols|
# MAGIC
# MAGIC
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------

#compare source columns
msg_source = compare_columns(df_src_pqt.columns, coldict['src'], source_type)

if msg_source:
    run_and_commit(cursor, f"""UPDATE mapping_rule SET is_missing_srce_flag = 1 WHERE mapping_rule_column_group_id IN ({all_cg_ids}) """)
    dbutils.notebook.exit({"status": "INVALID", "status_desc": f"Source {msg_source}"})

# COMMAND ----------

#compare target columns
for trg_count, assoc_id in enumerate(target_selection_set_filename_map):
    trg_file = target_selection_set_filename_map[assoc_id]
    target_input_file_name = f"/mnt/light-refined/mapping-definition/target_{mappingdef_filename}_{trg_file}.parquet"
    df_trg_pqt=spark.read.parquet(target_input_file_name)

    msg_target = compare_columns(df_trg_pqt.columns, coldict[assoc_id], target_type)
    if msg_target:
        run_and_commit(cursor, f"""UPDATE mapping_rule SET is_missing_trgt_flag = 1 WHERE mapping_rule_column_group_id IN ({all_cg_ids}) """)
        dbutils.notebook.exit({"status": "INVALID", "status_desc": f"Target {trg_count+1} {msg_target}"})

# COMMAND ----------

# now that columns match, compare data
# source
missing_srce = df_pivot.fillna("").join(df_src_pqt.fillna(""), on=coldict['src'], how='leftanti')

if not missing_srce.isEmpty():
    cg_ids = ",".join([str(i) for i in missing_srce.select('mapping_rule_column_group_id').rdd.flatMap(lambda x: x).collect()])
    run_and_commit(cursor, f"""UPDATE mapping_rule SET is_missing_srce_flag = 1 WHERE mapping_rule_column_group_id IN ({cg_ids});""")
    dbutils.notebook.exit('{"status": "INVALID", "status_desc": "Missing rows in source"}')


# COMMAND ----------

# targets
for trg_count, assoc_id in enumerate(target_selection_set_filename_map):
    trg_file = target_selection_set_filename_map[assoc_id]
    target_input_file_name = f"/mnt/light-refined/mapping-definition/target_{mappingdef_filename}_{trg_file}.parquet"


    df_trg_pqt=spark.read.parquet(target_input_file_name)

    join_columns = [c for c in df_pivot.columns if c.startswith(f"trg#{assoc_id}")]

    # add prefix trg#NNNN_ to columns of this target, so they match the pivot output
    df_trg_addprefix = df_trg_pqt.toDF( *[ f'trg#{assoc_id}_{c}' for c in df_trg_pqt.columns])

    # select only rows where columns for current target are not null
    # borderline unreadable, I know :(
    #df_pivot_current_rule = df_pivot.where(reduce(lambda col1, col2: col1 | col2, [col(col_name).isNotNull() for col_name in join_columns]))

    #enclose column names in ``
    df_pivot_current_rule = df_pivot.where( coalesce(*[ f"`{i}`" for i in join_columns ]).isNotNull())

    #fillna replaces nulls with "" so the verifying join works even if one of the join columns is null
    missing_trg = df_pivot_current_rule.fillna("").join(df_trg_addprefix.fillna(""), on=join_columns, how='leftanti')

    if not missing_trg.isEmpty():
        cg_ids = ",".join([str(i) for i in missing_trg.select('mapping_rule_column_group_id').rdd.flatMap(lambda x: x).collect()])
        run_and_commit(cursor, f"""UPDATE mapping_rule SET is_missing_trgt_flag = 1 WHERE mapping_rule_column_group_id IN ({cg_ids});""")
        dbutils.notebook.exit({"status": "INVALID", "status_desc": f"Missing rows in target {trg_count+1}"})

# COMMAND ----------

#if not exited earlier, everything looks good and matching - return valid! :)

# does not make sense to update anything if there are no mapping rules
if all_cg_ids: 
    run_and_commit(cursor, f"""UPDATE mapping_rule SET is_missing_srce_flag = 0, is_missing_trgt_flag = 0 WHERE mapping_rule_column_group_id IN ({all_cg_ids}) """)
dbutils.notebook.exit(f'{{"status": "VALID", "status_desc": ""}}')
