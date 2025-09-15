# Databricks notebook source
import pandas as pd
from numpy import nan as NAN
import pyodbc

dbutils.widgets.text("mapping_rule_set_key","")
dbutils.widgets.text("temp_table_name","")
dbutils.widgets.text("user_email","")

mapping_rule_set_key = dbutils.widgets.get("mapping_rule_set_key")
temp_table_name = dbutils.widgets.get("temp_table_name")
user_email = dbutils.widgets.get("user_email")

# COMMAND ----------

#debug

# temp_table_name = 'dbo.[tmp_import_6bb24c2f-a555-42ef-8f8e-35bc0cac3dc5]'
# mapping_rule_set_key = 522
# user_email = 'whateverski@pg.com'


# COMMAND ----------

db_url = dbutils.secrets.get(scope = "mappings", key = "mappings-backend-spring-datasource-url")
cursor = pyodbc.connect(dbutils.secrets.get(scope = "mappings", key = "sqldb-pyodbc-conn-string")).cursor()

final_temp_table = temp_table_name[:-1] + '_final]'
spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "false") # this to suppress arrow opt warning

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC The job implements the mapping transformation described here:
# MAGIC https://jira-pg-ds.atlassian.net/wiki/spaces/EDWMIC/pages/4257579364/Export+import+mapping+rules#Data-flow.1
# MAGIC

# COMMAND ----------

# load data
df = spark.read.format("com.microsoft.sqlserver.jdbc.spark").option("url", db_url).option("dbtable", temp_table_name).load().toPandas()
# row number in Excel -> group_id
df.reset_index(inplace=True, names='mapping_rule_column_group_id')
#identify source columns - source prefix is what the second column has before "/"
src_prefix = df.columns[1].split('/')[0]
src_cols = [col for col in df.columns if col.startswith(src_prefix + '/')]
#now some pandas magic (2x unpivot) to transform into target mapping_rule def format
mdf = df.melt(id_vars = src_cols + ['mapping_rule_column_group_id'], value_name= 'mapping_rule_trgt_column_val', var_name='selection_set_trgt_column_name') \
    .melt(id_vars = ['selection_set_trgt_column_name', 'mapping_rule_trgt_column_val', 'mapping_rule_column_group_id'], value_name = 'mapping_rule_srce_column_val', var_name = 'selection_set_srce_column_name')
#clean up columns
mdf[['selection_set_name', 'selection_set_trgt_column_name']] = mdf['selection_set_trgt_column_name'].str.split('/', expand=True, n=1)
mdf[['srce_selection_set_name', 'selection_set_srce_column_name']] = mdf['selection_set_srce_column_name'].str.split('/', expand=True, n=1)
mdf['selection_set_trgt_column_name'] = mdf['selection_set_trgt_column_name'].str.replace('trg#[0-9]*\_','',regex=True)
# bug 699450 - rows with empty target (unmapped) should be removed
# plus bug 810073: include empty strings as well
mdf.mapping_rule_trgt_column_val.replace("", NAN, inplace=True)
mdf.dropna(subset=['mapping_rule_trgt_column_val'], inplace=True)
# check if multitarget
multitarget = mdf.selection_set_name.drop_duplicates().count() > 1

# COMMAND ----------

qry_src_col_ids = f"""
SELECT
	upper(SSC.selection_set_mapping_tool_column_name) as selection_set_srce_column_name,
	MSCAS.mapping_rule_set_selection_set_column_assoc_key
	FROM mapping_rule_set MRS
	JOIN mapping_rule_set_selection_set_assoc MSAS
		ON MRS.mapping_rule_set_key = MSAS.mapping_rule_set_key
	JOIN selection_set SS
		ON MSAS.selection_set_key = SS.selection_set_key
	JOIN selection_set_column SSC
		ON SS.selection_set_key = SSC.selection_set_key
	JOIN mapping_rule_set_selection_set_column_assoc MSCAS
		ON MSAS.mapping_rule_set_selection_set_assoc_key = MSCAS.mapping_rule_set_selection_set_assoc_key
		AND SSC.selection_set_column_key = MSCAS.selection_set_column_key
	where MRS.mapping_rule_set_key = {mapping_rule_set_key}
	AND MSAS.is_srce_selection_set_flag = 1
	AND (
			/* Attribute to Attribute, Attribute to Key and Expression*/
			MRS.mapping_rule_set_type_id IN (1003, 1002, 1004) OR
			/* Key to Attribute and Key to Key */
			MRS.mapping_rule_set_type_id in (1005, 1001) AND SSC.is_selection_set_column_primary_key_flag = 1
	)
"""

qry_trg_col_ids = f"""	
	SELECT
    SS.selection_set_name,
	upper(SSC.selection_set_mapping_tool_column_name) as selection_set_srce_column_name ,
	MSCAS.mapping_rule_set_selection_set_column_assoc_key
	FROM mapping_rule_set MRS
	JOIN mapping_rule_set_selection_set_assoc MSAS
		ON MRS.mapping_rule_set_key = MSAS.mapping_rule_set_key
	JOIN selection_set SS
		ON MSAS.selection_set_key = SS.selection_set_key
	JOIN selection_set_column SSC
		ON SS.selection_set_key = SSC.selection_set_key
	JOIN mapping_rule_set_selection_set_column_assoc MSCAS
		ON MSAS.mapping_rule_set_selection_set_assoc_key = MSCAS.mapping_rule_set_selection_set_assoc_key
		AND SSC.selection_set_column_key = MSCAS.selection_set_column_key
	where MRS.mapping_rule_set_key ={mapping_rule_set_key}
	AND MSAS.is_srce_selection_set_flag = 0
	AND (
			/* Attribute to Attribute, Key to Attribute and Expression*/
			MRS.mapping_rule_set_type_id IN (1003, 1005, 1004) OR
			/* Attribute to Key and Key to Key */
			MRS.mapping_rule_set_type_id  in (1002, 1001) AND SSC.is_selection_set_column_primary_key_flag = 1
	)
"""
	

# COMMAND ----------

src_col_ids = spark.read.format("com.microsoft.sqlserver.jdbc.spark").option("url", db_url).option("query", qry_src_col_ids).load().toPandas()
trg_col_ids = spark.read.format("com.microsoft.sqlserver.jdbc.spark").option("url", db_url).option("query", qry_trg_col_ids).load().toPandas()

# COMMAND ----------

#join for source column assoc key
mdf = mdf.merge(src_col_ids,on='selection_set_srce_column_name', how = 'inner'  )
# clean up columns
mdf.rename(columns = {'mapping_rule_set_selection_set_column_assoc_key': 'mapping_rule_set_srce_selection_set_column_assoc_key'}, inplace=True)
mdf.drop(columns=['selection_set_srce_column_name','srce_selection_set_name'], inplace=True)

# if there are multiple targets, join by target selection set name + column name instead
if multitarget:
    mdf = mdf.merge(trg_col_ids, left_on=['selection_set_name','selection_set_trgt_column_name'], right_on=['selection_set_name','selection_set_srce_column_name'], how='inner')
    mdf.drop(columns=['selection_set_name'], inplace=True)
else:    #join for target column assoc key
    mdf = mdf.merge(trg_col_ids, left_on='selection_set_trgt_column_name', right_on='selection_set_srce_column_name', how='inner')
    # this column gets duplicated when not used in join - Bug 699721
    mdf.drop(columns=['selection_set_name_x', 'selection_set_name_y'], inplace=True)

#clean up more columns
mdf.rename(columns = {'mapping_rule_set_selection_set_column_assoc_key': 'mapping_rule_set_trgt_selection_set_column_assoc_key'}, inplace=True)
mdf.drop(columns=['selection_set_trgt_column_name', 'selection_set_srce_column_name'], inplace=True)


# COMMAND ----------

#bug 714015: empty values in Excel are nulls/nones; regular UI rules have empty strings ''
# we need to convert so they join with each other in the UI screens
mdf.mapping_rule_srce_column_val = mdf.mapping_rule_srce_column_val.fillna("")
mdf.mapping_rule_trgt_column_val = mdf.mapping_rule_trgt_column_val.fillna("")


# COMMAND ----------

qry_insert = f"""
INSERT INTO dbo.mapping_rule
(mapping_rule_column_group_id, mapping_rule_set_srce_selection_set_column_assoc_key, mapping_rule_srce_column_val, 
mapping_rule_set_trgt_selection_set_column_assoc_key, mapping_rule_trgt_column_val, 
is_missing_srce_flag, is_missing_trgt_flag, 
begin_date, end_date, curr_ind, active_ind, created_by_party_key, updated_by_party_key, data_owner_key, create_date, update_date, 
secure_group_key, process_run_key, [_mdmp_created_by_user_email], [_mdmp_last_update_user_email])
select 
mapping_rule_column_group_id, mapping_rule_set_srce_selection_set_column_assoc_key, mapping_rule_srce_column_val, 
mapping_rule_set_trgt_selection_set_column_assoc_key, mapping_rule_trgt_column_val,
0,0,
getdate(), '9999-12-31','Y','Y', null, null, null, getdate(), getdate(),
0, 0, '{user_email}', '{user_email}'
from {final_temp_table}
"""

qry_delete = f"""
delete from mapping_rule where mapping_rule_key in
(select mr.mapping_rule_key from mapping_rule_set mrs
join mapping_rule_set_selection_set_assoc mrsssa on mrs.mapping_rule_set_key = mrsssa.mapping_rule_set_key 
  and mrsssa.is_srce_selection_set_flag = 1
join mapping_rule_set_selection_set_column_assoc mrsssca on mrsssa.mapping_rule_set_selection_set_assoc_key = mrsssca.mapping_rule_set_selection_set_assoc_key
join mapping_rule mr on mrsssca.mapping_rule_set_selection_set_column_assoc_key = mr.mapping_rule_set_srce_selection_set_column_assoc_key
where mrs.mapping_rule_set_key = {mapping_rule_set_key} )
"""


# COMMAND ----------

try:
    cursor.execute(qry_delete)
    print("rows deleted: ", cursor.rowcount)

    # bug 773330 - if nothing mapped, just delete existing rules, do no insert
    if mdf.shape[0] > 0:
        spark.createDataFrame(mdf).write.format("jdbc").option("url", db_url).option("dbtable", final_temp_table).mode("overwrite").save()
        cursor.execute(qry_insert)
        print("rows inserted: ", cursor.rowcount)
        cursor.execute(f'drop table {final_temp_table}')
    cursor.execute(f'drop table {temp_table_name}')
    cursor.commit()    
except Exception as e:
    cursor.rollback()
    cursor.execute(f'drop table if exists {temp_table_name}')
    cursor.execute(f'drop table if exists {final_temp_table}')
    cursor.commit()     
    dbutils.notebook.exit(f'{{"status": "ERROR", "status_desc": "Error saving mapping rules: {str(e)}" }}')

# COMMAND ----------

dbutils.notebook.exit('{"status": "OK"}')
