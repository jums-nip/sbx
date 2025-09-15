# Databricks notebook source
# DBTITLE 1,TODO - technical debt
# MAGIC %md
# MAGIC
# MAGIC This file is now identical to mappingtool_datarefresh_selectionset (except for checking if columns match)
# MAGIC Due to time pressure, it was decided to leave it this way.
# MAGIC
# MAGIC To avoid duplicate code, these jobs should be merged and backend should call the same job in both cases - datarefresh_selectionset.
# MAGIC It already has conditional checking of columns implemented.
# MAGIC

# COMMAND ----------

import pandas as pd
from pyspark.sql.functions import col, date_format, StringType   

dbutils.widgets.text("file_name","")
dbutils.widgets.text("output_file_name","")
dbutils.widgets.text("container_name","")
dbutils.widgets.text("target_path","")
dbutils.widgets.text("columns","")
dbutils.widgets.text("selection_set_condition_expression","")
dbutils.widgets.text("conditions","")
dbutils.widgets.text("selection_set_query_type","")
dbutils.widgets.text("pk_columns","")

# COMMAND ----------



# COMMAND ----------

file_name = dbutils.widgets.get("file_name")
output_file_name = dbutils.widgets.get("output_file_name")
container_name = dbutils.widgets.get("container_name") 
target_path = dbutils.widgets.get("target_path").strip() 
columns = dbutils.widgets.get("columns").split(",")
selection_set_condition_expression = dbutils.widgets.get("selection_set_condition_expression").strip(';\n')
conditions = dbutils.widgets.get("conditions")
selection_set_query_type = dbutils.widgets.get("selection_set_query_type") 
pk_columns = dbutils.widgets.get("pk_columns")

# COMMAND ----------

# this function is not needed now, but I'll leave it
# might come in handy if list of files to read is too long and needs to be split into chunks

def split_list(lst, n):
    """Split list into n-sized segments."""
    for i in range(0, len(lst), n):
        yield lst[i:i + n]

# COMMAND ----------

# debug

#columns="prod_key,sid_part,smv_part,data_provider_code_part,secure_group_key_part,process_run_key_part,prod_type_key,prod_id,prod_name"
# columns="sid_part,smv_part,data_provider_code_part,secure_group_key_part,process_run_key_part,prod_key,active_ind,curr_ind,prod_name".split(",")
# container_name="refined"
# file_name="prod_dim"
# output_file_name="WP_LARGE_TEST"
# target_path=""
# conditions=""

# bug 713531
# columns="BUYR_GRP_SKID,BUYR_GRP_NAME,BUYR_GRP_DESC,RUN_ID".split(",")
# conditions=""
# #'RUN_ID = 74978'
# container_name="cdl"
# file_name="MM_HHP_BUYR_GRP_SRCE"
# output_file_name="A2K_TST_AV_SRC"

# if pk_columns:
#     pk_columns="BUYR_GRP_SKID,RUN_ID".split(",")


# COMMAND ----------

if target_path:
    source_file_name = f"/mnt/{container_name}/{target_path}/{file_name}"
else:
    source_file_name = f"/mnt/{container_name}/{file_name}"

output_file_name = f"/mnt/light-refined/selection-set/{output_file_name}.parquet"
    
print(f"Source file: {source_file_name}")  
print(f"Target file: {output_file_name}")  


# COMMAND ----------

#this is the old code for reading max prks - it loses the partition key columns - bug 801135

def read_most_recent_prks_old (source_file_name):
    df=spark.read.parquet(source_file_name)
    #if df is partitioned by process_run_key (file path contains 'process_run_key='), find max process_run_key and read only these partitions
    if ('process_run_key=' in df.inputFiles()[0]):
        # 1) make list of folders (remove file name from the end), 2) deduplicate list, 3) split by last /
        folders = pd.DataFrame([x.rsplit('/',1)[0].rsplit('/',1) for x in df.inputFiles()]).drop_duplicates()
        # 4) select max process_run_key in each_partition
        folders_max = folders.groupby(0).max().reset_index()
        # 5) convert to list
        filesList = (folders_max[0] + '/' + folders_max[1]).tolist()
        print(f"Input file is partitioned by process_run_key, reading only partitions with max prk: ({len(filesList)} partitions out of {folders.shape[0]})")
        df = spark.read.parquet(*filesList)
        if len(filesList) > 100:
            df = df.repartition(16)


# COMMAND ----------

def read_most_recent_prks(source_file_name):
    df=spark.read.parquet(source_file_name)
    #if df is partitioned by process_run_key (file path contains 'process_run_key='), 
    #find max process_run_key in every partition and read only these partitions    
    if ('process_run_key=' in df.inputFiles()[0]):
        # make dataframe of folders (remove file name from the end), split by last "/" to separate process_run_key, deduplicate
        folders = pd.DataFrame([x.rsplit('/',1)[0].rsplit('/',1) for x in df.inputFiles()]).drop_duplicates()
        # select max process_run_key in each_partition
        folders_max = folders.groupby(0).max().reset_index()
        print(f"Input file is partitioned by process_run_key, reading only partitions with max prk: ({folders_max.shape[0]} partitions out of {folders.shape[0]})")
        # split folders (column 0) by /
        folders_max = folders_max[0].str.split('/', expand=True).add_prefix('folder_').join(folders_max)
        # drop column 0, no longer needed
        folders_max.drop(0, inplace=True, axis = 'columns')        
        # drop columns that don't contain "=", leaving only partition keys
        for i in folders_max.columns:
            if not folders_max[i].str.contains('=').any():
                folders_max.drop(i, inplace=True, axis='columns')
        # rename columns to partition key names
        folders_max.columns = [folders_max[i][0].split('=')[0]  for i in folders_max.columns ]
        # remove partition key name and "=" from dataframe fields, leaving just the key values
        for i in folders_max.columns:
            folders_max[i] = folders_max[i].str.replace(i + '=', '')
        #convert to spark, do a join with the original dataframe to filter only the relevant partitions
        part_table = spark.createDataFrame(folders_max)
        # join is super-efficient because it only uses the partition keys
        df = df.join(part_table, part_table.columns)
        if folders_max.shape[0] > 100:
            df = df.repartition(16)
    return df

# COMMAND ----------

try:
    df = read_most_recent_prks(source_file_name)

    # convert column names to uppercase, bug 789641
    df = df.toDF ( *[i.upper() for i in df.columns])

    if selection_set_query_type =="FULL_QUERY":
        df.createOrReplaceTempView('dataset') #full query mode uses the 'dataset' placeholder in the SQL
        df = spark.sql(conditions) 
    else:
        df = df.select(columns)
        # Check if conditions is null or empty string
        if conditions.strip() and conditions != 'null':
            df = df.filter(conditions)

    
except Exception as e:
    dbutils.notebook.exit(f'{{"status": "ERROR", "status_desc": "Error reading source dataset: {str(e)}" }}')        


# COMMAND ----------

if pk_columns:
    pk_columns = pk_columns.split(',')
    if df.count() != df.select(pk_columns).distinct().count():
        dbutils.notebook.exit(f'{{"status": "ERROR", "status_desc": "Provided Primary Key: {pk_columns} is not unique in this dataset." }}')  


# COMMAND ----------

#coerce everything to strings to avoid specific conversion issues later, as in mapping_rule everything is stored as VARCHAR anyway

for i in df.dtypes:
    #BUG 767282 colnames with spaces/periods need to be quoted
    qcol = f"`{i[0]}`"
    if i[1] in ['timestamp', 'date', 'datetime']:
        df = df.withColumn(i[0],  date_format(col(qcol), "yyyy-MM-dd HH:mm:ss.SSSSSS"))
        print ("Converted timestamp column ", i)
    elif i[1] != 'string':
        df = df.withColumn(i[0], col(qcol).cast(StringType()))
        print("Converted column ", i )

# COMMAND ----------

try:
    df.write.option("compression", "snappy").mode("overwrite").parquet(output_file_name)
except Exception as e:
    dbutils.notebook.exit(f'{{"status": "ERROR", "status_desc": "Error writing selection set: {str(e)}" }}')

dbutils.notebook.exit(f'{{"status": "OK", "status_desc": "{output_file_name}"}}')
