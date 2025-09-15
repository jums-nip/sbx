# Databricks notebook source
import json
from pyspark.sql.types import StringType
import re, ast

dbutils.widgets.text("container_name","")
dbutils.widgets.text("sort_order","")
dbutils.widgets.text("mapping_def_file_name","")
dbutils.widgets.text("target_path","")
dbutils.widgets.text("conditions","")
dbutils.widgets.text("mapping_rule_set_key","")
dbutils.widgets.text("output_file_name","")
dbutils.widgets.text("start_position","")
dbutils.widgets.text("offset","")
dbutils.widgets.text("selection_set_filename","")
dbutils.widgets.text("mapping_type","")
dbutils.widgets.text("target_selection_set_filename_map","")

# COMMAND ----------

conditions = dbutils.widgets.get("conditions")
sort_order = dbutils.widgets.get("sort_order")
container_name = dbutils.widgets.get("container_name")
selection_set_filename = dbutils.widgets.get("selection_set_filename")
mapping_rule_set_key = dbutils.widgets.get("mapping_rule_set_key")
target_path = dbutils.widgets.get("target_path")
mapping_def_file_name = dbutils.widgets.get("mapping_def_file_name")
start_position = dbutils.widgets.get("start_position")
offset= dbutils.widgets.get("offset")
mapping_type = dbutils.widgets.get("mapping_type")
target_selection_set_filename_map = ast.literal_eval(dbutils.widgets.get("target_selection_set_filename_map"))


# COMMAND ----------

# debug - bug 719582
# container_name = "light-refined"
# conditions=""
# columns="*"
# mapping_def_file_name="TESTMMASEPTA"
# mapping_rule_set_key="197"
# mapping_type="Attribute to Attribute"
# offset="1500"
# selection_set_filename="TEST_MMA_9282023_3"
# selection_set_key="304"
# start_position="750"
# target_path="output"
# target_selection_set_filename_map='{"324":"TEST_MMA_9252023_B"}'
# sort_order = "PROD_SKID asc"

# COMMAND ----------

# #To test the bug 739214, discussed with maciej that this will be on dbr to adjust the condition value since it returns string prod_key < "x" instead of x
# columns="*"
# prod_key = "123"
# conditions = f"""prod_key > "{prod_key}" """
# container_name="light-refined"
# mapping_def_file_name="REVPRODKEY"
# mapping_rule_set_key="256"
# mapping_type="Attribute to Attribute"
# offset="5050"
# selection_set_filename="TEST_PROD_KEY"
# selection_set_key="370"
# start_position="1"
# target_path="output"
# target_selection_set_filename_map=ast.literal_eval('{"460":"MM_HHP_PROD_SKID"}')

# COMMAND ----------

# bug 714015
# columns= "*"
# container_name= "light-refined"
# mapping_def_file_name= "BUGSEVENONEFOUROHONEFIVE"
# mapping_rule_set_key= "269"
# mapping_type= "Attribute to Key"
# offset= "5050"
# selection_set_filename= "A2K_TST_AV_SRC"
# selection_set_key= "247"
# start_position= "1"
# target_path= "output"
# target_selection_set_filename_map= ast.literal_eval('{"487":"A2K_TST_AV_TRGT_2"}')

# COMMAND ----------

# columns= "*" 
# container_name= "light-refined" 
# mapping_def_file_name= "WP_QUOTING1" 
# mapping_rule_set_key= "454" 
# mapping_type= "Attribute to Attribute" 
# offset= "5050" 
# selection_set_filename= "WP_CHARS_AA" 
# selection_set_key= "552" 
# start_position= "1" 
# target_selection_set_filename_map= ast.literal_eval('{"846":"WP_CHARS_BB"}')


# COMMAND ----------

# bug 765728
# columns= "*"
# container_name= "light-refined"
# mapping_def_file_name= "TESTEXPVOID"
# mapping_rule_set_key= "472"
# mapping_type= "Expression Mapping"
# offset= "5050"
# selection_set_filename= "TESTMZL20231211A"
# selection_set_key= "567"
# start_position= "1"
# target_path= "output"
# target_selection_set_filename_map= ast.literal_eval('{"882" :"TESTMZL20231211C"}')


# COMMAND ----------

input_file_name = f"/mnt/light-refined/mapping-definition/source_{mapping_def_file_name}_{selection_set_filename}.parquet"
print('Input file name:',input_file_name)

if mapping_type.lower() == 'expression mapping':
    target_type = 'Attribute'
else:
    target_type = mapping_type.split(' ')[2]

# COMMAND ----------

#reading of source file
# bug714015 - fill nulls, so joins on null columns work
df_pqt=spark.read.parquet(input_file_name).fillna("")

# COMMAND ----------

db_url = dbutils.secrets.get(scope = "mappings", key = "mappings-backend-spring-datasource-url")
    
query = f"""(SELECT col_src_brc, col_src, col_src_head, col_src_name, col_trg_brc, col_trg_head FROM [mapping_rule_set_columns_fnc]({mapping_rule_set_key}))"""
df_queryresult = spark.read.format("com.microsoft.sqlserver.jdbc.spark") \
        .option("url", db_url) \
        .option("query", query) \
        .load()

col_src_brc, col_src, col_src_head, col_src_name, col_trg_brc, col_trg_head = df_queryresult.first()

# COMMAND ----------

pivot_query = f"""(select mapping_rule_column_group_id, is_missing_srce_flag, is_missing_trgt_flag, _mdmp_last_update_user_email as updated_by, {col_src_head}, {col_trg_head}
from (
select mapping_rule_column_group_id, is_missing_srce_flag, is_missing_trgt_flag, mapping_rule_srce_column_val, _mdmp_last_update_user_email, mapping_rule_set_srce_selection_set_column_assoc_key, mapping_rule_trgt_column_val, mapping_rule_set_trgt_selection_set_column_assoc_key
from mapping_rule mr
where mapping_rule_set_srce_selection_set_column_assoc_key in ({col_src})
) a
pivot(
max(mapping_rule_srce_column_val) for mapping_rule_set_srce_selection_set_column_assoc_key in ({col_src_brc})
) piv1
pivot(
max(mapping_rule_trgt_column_val) for mapping_rule_set_trgt_selection_set_column_assoc_key in ({col_trg_brc})
) piv2 group by mapping_rule_column_group_id, is_missing_srce_flag, is_missing_trgt_flag, _mdmp_last_update_user_email)"""
print(pivot_query)
df_queryresult2 = spark.read.format("com.microsoft.sqlserver.jdbc.spark") \
        .option("url", db_url) \
        .option("query", pivot_query) \
        .load()

# COMMAND ----------

df_joined = df_pqt.join(df_queryresult2,col_src_name.split(","),'outer')

if conditions is not None and conditions.strip() and conditions != "null":
    df_joined = df_joined.select("*").filter(conditions)

df_joined = df_joined.distinct().orderBy(df_joined["mapping_rule_column_group_id"].desc())

# COMMAND ----------

# if mapping of type a2k, k2k, target from DB includes only key column, the rest needs to be joined from parquet files
if target_type == 'Key':
    for trg_count, assoc_id in enumerate(target_selection_set_filename_map):
        trg_file = target_selection_set_filename_map[assoc_id]
        target_input_file_name = f"/mnt/light-refined/mapping-definition/target_{mapping_def_file_name}_{trg_file}.parquet"
        df_trg_pqt=spark.read.parquet(target_input_file_name)

        join_columns = [c for c in df_joined.columns if c.startswith(f"trg#{assoc_id}")]
        # add prefix trg#NNNN_ to columns of this target, so they match the pivot output
        df_trg_addprefix = df_trg_pqt.toDF( *[ f'trg#{assoc_id}_{c}' for c in df_trg_pqt.columns])
        df_joined = df_trg_addprefix.join(df_joined, (join_columns), how='right')


# COMMAND ----------

if sort_order is not None and sort_order.strip() and sort_order != "null":
    order_clause = f" ORDER BY {sort_order}"
else:
    # ORDER BY 1 does not work in OVER() clause - must have a column name
    # evidently bug in Spark SQL
    order_clause = f" ORDER BY `{df_joined.columns[0]}`"    

df_joined.createOrReplaceTempView("v_pagination_resultset")

# COMMAND ----------

#pagination logic + ORDER BY (if supplied)
# bug 719582 - row number is supposed to reflect ORDER BY
pagination_query = spark.sql(f"""with result as (select a.*,ROW_NUMBER() OVER ({order_clause}) AS RowNum FROM v_pagination_resultset a) select * from result where RowNum >= {start_position} and RowNum < ({start_position} + {offset}) {order_clause}""")

# COMMAND ----------



# COMMAND ----------

# this conversion loop causes problems with spaces/periods in column names
# json creation logic
# for col in pagination_query.columns:
#     print(col)
#     pagination_query = pagination_query.withColumn(col, pagination_query[col].cast(StringType()))

# most probably conversion is not needed at all, but if it is, should look like this:
# from pyspark.sql.functions import col
# for i in pagination_query.dtypes:
#     if i[1] != 'string':
#         pagination_query = pagination_query.withColumn(i[0], col(i[0]).cast(StringType()))
#         print("Converted column ", i )
    
resultlist_json = [json.loads(x) for x in pagination_query.na.fill("").toJSON().collect()]
exposedata = json.dumps(resultlist_json, sort_keys=True)
dbutils.notebook.exit(exposedata)
