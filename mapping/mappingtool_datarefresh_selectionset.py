# Databricks notebook source
import pandas as pd
import re
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

file_name = dbutils.widgets.get("file_name")
output_file_name = dbutils.widgets.get("output_file_name")
container_name = dbutils.widgets.get("container_name")#it will be dynamic container (input-cdl,input-mdm,etc.) so there will be changes to databas
target_path = dbutils.widgets.get("target_path").strip()#it will be dynamic, maybe just adjust what is the target_path so there will be changes to database and backend to slice targeth path
columns = dbutils.widgets.get("columns").split(",")
selection_set_condition_expression = dbutils.widgets.get("selection_set_condition_expression") #they are changing it again to conditions - final
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

# container_name="input-direct"
# columns="`1.CORPORATE SUB SECTOR NAME`,`2.CORPORATE CATEGORY NAME`,`3.CORPORATE FORM DETAIL NAME`".split(",")
# file_name="SEE_Local Sub Category Name_BC_SRC"
# output_file_name="SEE_LOCAL_SUB_CATEGORY_NAME_BABY"
# selection_set_query_type="FILTER"


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
# left on purpose - used for rowcount/column comparison in the "unit test"

def read_most_recent_prks_old(source_file_name):
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
    return df

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

# Check if output_filename exists in the directory and only then read and do column matching, if file doesnt exist create the selection set:
try:
    dbutils.fs.ls(output_file_name)
    print ("Output file exists - comparing columns")

    df_selectionset =spark.read.parquet(output_file_name)
    if set(df_selectionset.columns) != set(df.columns):
        dbutils.notebook.exit('{"status": "ERROR", "status_desc": "Mismatched columns" }')
    print ("Columns match")

except Exception as e:
    if 'java.io.FileNotFoundException' in str(e):
        print("Output file does not exist")
    else:
        dbutils.notebook.exit(f'{{"status": "ERROR", "status_desc": "Error reading output file: {str(e)}" }}')

# COMMAND ----------

if pk_columns:
    pk_columns = pk_columns.split(',')
    if df.count() != df.select(pk_columns).distinct().count():
        dbutils.notebook.exit(f'{{"status": "ERROR", "status_desc": "Provided Primary Key: {pk_columns} is not unique in this dataset." }}')  

# COMMAND ----------

#coerce everything to strings to avoid specific conversion issues later, as in mapping_rule everything is stored as VARCHAR anyway
# known issues were with: null integers, timestamps, integers with format 122345.0

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
    dbutils.notebook.exit(f'{{"status": "ERROR", "status_desc": "Error writing selectionset: {str(e)}" }}')

dbutils.notebook.exit('{"status": "OK"}')

# COMMAND ----------

# unit test "engine" - comparing straight parquet read vs old function vs new function
def unit_test():
  parquets = [
      "/mnt/input-mdm/dim_val_type",
      "/mnt/input-mdm/geo_dim",
      "/mnt/input-mdm/prod_dim",
      "/mnt/input-mdm/prod_dim_ext",
      "/mnt/input-mdm/site_dim",
      "/mnt/input-mdm/data_provider",
      "/mnt/input-mdm/dim_type",
      "/mnt/input-mdm/cust_dim",
      "/mnt/input-mdm/attr_type_generic",
      "/mnt/input-tradepanel/HHP_BUYER_MAP_SOURCE_DDAPI",
      "/mnt/input-tradepanel/HHP_DATA_CNTXT_MAPNG_TRGT",
      "/mnt/input-tradepanel/HHP_DATACONTEXT_MAP_SOURCE_DDAPI",
      "/mnt/input-tradepanel/HHP_GEO_CODE_MAPNG_TRGT",
      "/mnt/input-tradepanel/HHP_ONE_TRGT",
      "/mnt/input-tradepanel/HHP_PRODUCT_MAP_SOURCE_DDAPI",
      "/mnt/input-tradepanel/HHP_SLVR_BUYR_GRP_TRGT",
      "/mnt/input-tradepanel/HHP_THREE_TRGT",
      "/mnt/input-tradepanel/HHP_TIME_MAP_DDAPI",
      "/mnt/input-tradepanel/HHP_TIME_MAPNG_TRGT",
      "/mnt/input-tradepanel/HHP_TWO_TRGT",
      "/mnt/input-direct/NAM_MOSP_DS_GEO_MARKET_LIST_V",
      "/mnt/input-direct/NAM_MOSP_DS_MEDIA_LIST_V",
      "/mnt/input-direct/NAM_MOSP_DS_PROD_LIST_V",
      "/mnt/input-direct/NAM_NA_TAXONOMY_V",
      "/mnt/input-direct/NAM_NIELSEN_CC_DAR_GEO_LIST_V",
      "/mnt/input-direct/NAM_NIELSEN_CC_DAR_PROD_LIST_V",
      "/mnt/input-direct/NAM_TARGET_DEMO_DIM_V",
  ]
  for i in parquets:
      print(f"-----------File: {i} ---------------")
      d1 = spark.read.parquet(i)
      d2 = read_most_recent_prks_old(i)
      d3 = read_most_recent_prks(i)
      print(f"Rows: dummy: {d1.count()}, old: {d2.count()}, new: {d3.count()}, ")
      print(
          f"Column difference: old - dummy: {set(d1.columns) - set(d2.columns)}, new - dummy: {set(d1.columns) - set(d3.columns)}"
      )
