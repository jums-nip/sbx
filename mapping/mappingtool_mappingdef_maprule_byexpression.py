# Databricks notebook source
import pandas as pd
import pyodbc
import uuid
from pyspark.sql.functions import col

# COMMAND ----------

dbutils.widgets.text("mapping_rule_set_key","")
dbutils.widgets.text("mapping_def_file_name","")
dbutils.widgets.text("selection_set_filename","")
dbutils.widgets.text("mapping_expression","")
dbutils.widgets.text("user_email","")
dbutils.widgets.text("mode","EDIT")


mapping_rule_set_key = dbutils.widgets.get("mapping_rule_set_key")
selection_set_filename = dbutils.widgets.get("selection_set_filename")
mapping_def_file_name = dbutils.widgets.get("mapping_def_file_name")
mapping_expression = dbutils.widgets.get("mapping_expression")
user_email = dbutils.widgets.get("user_email")
mode = dbutils.widgets.get("mode").upper()

db_url = dbutils.secrets.get(scope = "mappings", key = "mappings-backend-spring-datasource-url")
cursor = pyodbc.connect(dbutils.secrets.get(scope = "mappings", key = "sqldb-pyodbc-conn-string")).cursor()

temp_table_name = f"dbo.[tmp_expression_{uuid.uuid4()}]"

# COMMAND ----------

# MAGIC %md
# MAGIC ##Functionality
# MAGIC
# MAGIC Runs SQL expression on source selection set and creates mapping rules based on result.
# MAGIC 3 modes:
# MAGIC * Preview: read source selection set, run expression on it, return json, don't save anything
# MAGIC * Save = do not return json, transform rules to DB format and save to DB
# MAGIC * Edit (current operation Edit by expression) - do not return json, transform rules to DB format, compare with existing rules in DB, merge with existing rules in DB
# MAGIC
# MAGIC
# MAGIC ##Algo
# MAGIC
# MAGIC * find source dataset
# MAGIC * run query with expression on it
# MAGIC * if MODE == PREVIEW, return these rows as json, exit
# MAGIC * if MODE == EDIT
# MAGIC   * pull existing rules from DB
# MAGIC * if MODE == SAVE
# MAGIC   * create empty placeholder instead  
# MAGIC * pivot them to match query on source
# MAGIC * compare 
# MAGIC * transform comparison result into mapping_rule table format (multiple melts, like in import)
# MAGIC * put into temp table on SQL DB
# MAGIC * if MODE == SAVE
# MAGIC   * delete existing rules
# MAGIC * run merge to mapping_rule table
# MAGIC * drop temp table

# COMMAND ----------

#debug

#bug 794078
# mapping_def_file_name= "TST_XPRSN_2"
# mapping_expression= "CASE\nWHEN DATA_PROVIDER_NAME = '7-11 TH' THEN 'Rod'\nEND "
# mapping_rule_set_key= "23"
# mode= "EDIT"
# selection_set_filename= "TST_AV_SRC"
# user_email= "testovich.t@pg.com"

# bug 796708
# mapping_def_file_name= "XPRSN_TST"
# mapping_expression= " CASE\n WHEN DIM_TYPE_KEY = '1' THEN 'VALUE'\n END"
# mapping_rule_set_key= 827
# mode= "SAVE"
# selection_set_filename= "EXPRSN_SRC"



# COMMAND ----------

qry_src_col_ids = f"""
SELECT
	SSC.selection_set_srce_column_name,
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
	SSC.selection_set_srce_column_name,
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

# used in EDIT and SAVE mode
qry_merge = f"""
MERGE INTO dbo.mapping_rule AS tgt
USING {temp_table_name} AS src
ON (
tgt.mapping_rule_key in (
	select mr.mapping_rule_key from mapping_rule_set mrs
	join mapping_rule_set_selection_set_assoc mrsssa on mrs.mapping_rule_set_key = mrsssa.mapping_rule_set_key 
	  and mrsssa.is_srce_selection_set_flag = 1
	join mapping_rule_set_selection_set_column_assoc mrsssca on mrsssa.mapping_rule_set_selection_set_assoc_key = mrsssca.mapping_rule_set_selection_set_assoc_key
	join mapping_rule mr on mrsssca.mapping_rule_set_selection_set_column_assoc_key = mr.mapping_rule_set_srce_selection_set_column_assoc_key
	where mrs.mapping_rule_set_key = {mapping_rule_set_key}
	)
and tgt.mapping_rule_set_srce_selection_set_column_assoc_key = src.mapping_rule_set_srce_selection_set_column_assoc_key
and tgt.mapping_rule_set_trgt_selection_set_column_assoc_key = src.mapping_rule_set_trgt_selection_set_column_assoc_key
and tgt.mapping_rule_srce_column_val = src.mapping_rule_srce_column_val
and tgt.mapping_rule_column_group_id = src.mapping_rule_column_group_id
)
WHEN MATCHED
THEN UPDATE SET
	tgt.mapping_rule_trgt_column_val=src.mapping_rule_trgt_column_val, 
	tgt.update_date=getdate(),  
	tgt.[_mdmp_last_update_user_email]='{user_email}'
WHEN NOT MATCHED
THEN INSERT (
	mapping_rule_column_group_id, mapping_rule_set_srce_selection_set_column_assoc_key, mapping_rule_srce_column_val, 
	mapping_rule_set_trgt_selection_set_column_assoc_key, mapping_rule_trgt_column_val, 
	is_missing_srce_flag, is_missing_trgt_flag, 
	begin_date, end_date, curr_ind, active_ind, created_by_party_key, updated_by_party_key, data_owner_key, create_date, update_date, 
	secure_group_key, process_run_key, [_mdmp_created_by_user_email], [_mdmp_last_update_user_email]
	)
	VALUES (
	mapping_rule_column_group_id, mapping_rule_set_srce_selection_set_column_assoc_key, mapping_rule_srce_column_val, 
	mapping_rule_set_trgt_selection_set_column_assoc_key, mapping_rule_trgt_column_val,
	0,0,
	getdate(), '9999-12-31','Y','Y', null, null, null, getdate(), getdate(),
	0, 0, '{user_email}','{user_email}'
	);
"""

# used in SAVE mode (overwrite) to delete existing mapping rules
qry_delete = f"""
DELETE FROM dbo.mapping_rule
WHERE mapping_rule_key in (
	select mr.mapping_rule_key from mapping_rule_set mrs
	join mapping_rule_set_selection_set_assoc mrsssa on mrs.mapping_rule_set_key = mrsssa.mapping_rule_set_key 
	  and mrsssa.is_srce_selection_set_flag = 1
	join mapping_rule_set_selection_set_column_assoc mrsssca on mrsssa.mapping_rule_set_selection_set_assoc_key = mrsssca.mapping_rule_set_selection_set_assoc_key
	join mapping_rule mr on mrsssca.mapping_rule_set_selection_set_column_assoc_key = mr.mapping_rule_set_srce_selection_set_column_assoc_key
	where mrs.mapping_rule_set_key = {mapping_rule_set_key}
	);
"""

#pull rules for comparison (EDIT mode)
qry_fetch_rules = f"""
	select 
	mapping_rule_column_group_id, mapping_rule_set_srce_selection_set_column_assoc_key,
	mapping_rule_srce_column_val, mapping_rule_trgt_column_val
	from dbo.mapping_rule tgt WHERE
	tgt.mapping_rule_key in (
		select mr.mapping_rule_key from mapping_rule_set mrs
		join mapping_rule_set_selection_set_assoc mrsssa on mrs.mapping_rule_set_key = mrsssa.mapping_rule_set_key 
		and mrsssa.is_srce_selection_set_flag = 1
		join mapping_rule_set_selection_set_column_assoc mrsssca on mrsssa.mapping_rule_set_selection_set_assoc_key = mrsssca.mapping_rule_set_selection_set_assoc_key
		join mapping_rule mr on mrsssca.mapping_rule_set_selection_set_column_assoc_key = mr.mapping_rule_set_srce_selection_set_column_assoc_key
		where mrs.mapping_rule_set_key = {mapping_rule_set_key}
		)
	"""


# COMMAND ----------

 #get column IDs for source and target, plus name of the target column (needed for Preview mode)
src_col_ids = spark.read.format("com.microsoft.sqlserver.jdbc.spark").option("url", db_url).option("query", qry_src_col_ids).load().toPandas() 
src_col_ids = src_col_ids.mapping_rule_set_selection_set_column_assoc_key.to_list()
trg_col_ids = spark.read.format("com.microsoft.sqlserver.jdbc.spark").option("url", db_url).option("query", qry_trg_col_ids).load().toPandas() 
trg_col_names = trg_col_ids.selection_set_srce_column_name.to_list()
trg_col_ids = trg_col_ids.mapping_rule_set_selection_set_column_assoc_key.to_list()

if len(trg_col_ids) != 1:
    dbutils.notebook.exit(f'{{"status": "ERROR", "status_desc": "Target selection set has more than 1 column."}}')

# COMMAND ----------

#execute expression
try:
    new_rules = spark.sql(f"""
        select *, {mapping_expression} as `{trg_col_names[0]}` from parquet.`/mnt/light-refined/mapping-definition/source_{mapping_def_file_name}_{selection_set_filename}.parquet` LIMIT 5000
    """)
    new_rules = new_rules.select([col(c).cast("string") for c in new_rules.columns]).toPandas()
except Exception as e:
    dbutils.notebook.exit(f'{{"status": "ERROR", "status_desc": "Error applying expression: {str(e)}" }}')




# COMMAND ----------

# if preview, just return the horizontal rules as json, do not do transformation/writes
# return all rows, mapped and unmapped

if mode == 'PREVIEW':
    preview_data = new_rules.fillna("").to_json(orient='records')
    dbutils.notebook.exit(f'{{"status": "OK", "status_desc":"", "preview_data": {preview_data} }}')


# COMMAND ----------

#name columns as column IDs
new_rules.columns = src_col_ids + trg_col_ids  

# now filter out unmapped rows
new_rules = new_rules [ new_rules[trg_col_ids[0]].notnull() ]
# create column group_id to identify rules after unpivoting
new_rules.reset_index(inplace=True, names='mapping_rule_column_group_id')

# COMMAND ----------

if mode == 'EDIT':
	# read current rules for comparison
	current_rules = spark.read.format("com.microsoft.sqlserver.jdbc.spark").option("url", db_url).option("query", qry_fetch_rules).load().toPandas()

if mode == 'SAVE':
    # delete existing rules
	cursor.execute(qry_delete)
	cursor.commit()
    # do not read anything, we'll overwrite anyway. Create empty df
	current_rules = pd.DataFrame()

# if no mapping rules exist, create a dummy dataframe, so the rest of the code stays the same
if current_rules.shape[0] == 0:
	current_rules = pd.DataFrame ( { 'mapping_rule_set_srce_selection_set_column_assoc_key': src_col_ids  })
	current_rules['mapping_rule_column_group_id'] = -1
	current_rules['mapping_rule_srce_column_val'] = ''
	current_rules['mapping_rule_trgt_column_val'] = ''
    

# pivot for comparison
current_rules = current_rules.pivot(columns=['mapping_rule_set_srce_selection_set_column_assoc_key'], index=['mapping_rule_column_group_id' ] )
# bug 796708: drop redundant target columns, by leaving only first of them - iloc: all rows (:), columns from 0 to N
current_rules = current_rules.iloc[:, :len (src_col_ids) + len(trg_col_ids)]

# rename columns to standardized
prefixed_trg = ["trg_" + str(t) for t in trg_col_ids ]
current_rules.columns = src_col_ids + prefixed_trg
current_rules.reset_index(inplace=True)
# create indexes for join
current_rules.set_index(src_col_ids, inplace=True)
new_rules.set_index(src_col_ids, inplace=True)
#join
new_rules = new_rules.join(current_rules, how = 'left', lsuffix='_drop' )


# COMMAND ----------

#filter only values that changed mapping
changed_rules = new_rules.loc[ new_rules[trg_col_ids[0]] != new_rules[prefixed_trg[0]], new_rules.columns]
# if no changed values, exit but with OK status, no need to do anything
if changed_rules.shape[0] == 0:
    dbutils.notebook.exit('{"status": "OK", "status_desc": "No changed/mapped values" }')
changed_rules.drop(columns = ['mapping_rule_column_group_id_drop'] + prefixed_trg, inplace=True)    
changed_rules.reset_index(inplace=True)

# missing values in mapping_rule_column_group_id column = new entries, fill the group id with sequential numbers
max_id = current_rules.mapping_rule_column_group_id.max() 
if pd.isna(max_id): max_id = 0
changed_rules['mapping_rule_column_group_id'] = changed_rules['mapping_rule_column_group_id'].fillna(changed_rules['mapping_rule_column_group_id'].isnull().cumsum() + max_id)

# COMMAND ----------

# unpivot go brrrrr (twice) 
# 1st to transpose target values to rows, 2nd for source values
mdf = changed_rules.melt(id_vars = src_col_ids + ['mapping_rule_column_group_id'],  value_name= 'mapping_rule_trgt_column_val', var_name='mapping_rule_set_trgt_selection_set_column_assoc_key') \
    .melt(id_vars = ['mapping_rule_set_trgt_selection_set_column_assoc_key', 'mapping_rule_trgt_column_val', 'mapping_rule_column_group_id'], value_name = 'mapping_rule_srce_column_val', var_name = 'mapping_rule_set_srce_selection_set_column_assoc_key')

mdf = spark.createDataFrame(mdf)
# overwrite - the table will be created or dropped/created
mdf.write.format("jdbc").option("url", db_url).option("dbtable", temp_table_name).mode("overwrite").save()

# COMMAND ----------

# execute merge, drop temp table
try:
    cursor.execute(qry_merge)
    rows_updated = cursor.rowcount
    cursor.execute(f'drop table {temp_table_name}')
    cursor.commit()
except Exception as e:
    cursor.execute(f'drop table if exists {temp_table_name}')
    cursor.commit()   
    dbutils.notebook.exit(f'{{"status": "ERROR", "status_desc": "Error merging mapping rules: {str(e)}" }}')

# COMMAND ----------

# oof all went well
dbutils.notebook.exit(f'{{"status": "OK", "status_desc": "Rows updated: {rows_updated}" }}')
