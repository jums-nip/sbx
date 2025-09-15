# Databricks notebook source
import pandas as pd
import uuid
from pyspark.sql.functions import col

dbutils.widgets.text("import_file_name", "")
import_file_name = dbutils.widgets.get("import_file_name")

db_url = dbutils.secrets.get(scope = "mappings", key = "mappings-backend-spring-datasource-url")
temp_table_name = f"dbo.[tmp_import_{uuid.uuid4()}]"


# COMMAND ----------

#debug
# import_file_name = 'import_test.xlsx'
# import_file_name = 'AttrToAttrMapping_Olga_test2_20230719.xlsx'
# import_file_name = '/tmp/Attribute to Attribute_MULTI_EDIT_AA_20230830 all nulls.xlsx'
# import_file_name = 'Source_Key_to_Key_MMA_TEST_DEC_15_A_20231218.xlsx'
# import_file_name = 'MMA_TEST_DEC_25_A_source.xlsx'
# import_file_name = 'AttrToAttrMapping_SEE_Local_Sub_Category_Name_BABY_20240216.xlsx'

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Assumptions: 
# MAGIC
# MAGIC ####Design
# MAGIC
# MAGIC Design article: https://jira-pg-ds.atlassian.net/wiki/spaces/CDLBOK/pages/4339925855/Export+import+mapping+rules
# MAGIC
# MAGIC #### Format 1 (new mapping tool)
# MAGIC * Row 1 contains Sources and Targets markers
# MAGIC * Row 2 contains Source marker and targets names
# MAGIC * Rows 3 contains column names
# MAGIC
# MAGIC (Columns in Excel might be merged, markdown doesn't do that)
# MAGIC
# MAGIC | Sources | | Targets  | | | | 
# MAGIC |--|--|--|--|--|--|
# MAGIC | **Source name** | |**Target 1 name** | | **Target 2 name** |
# MAGIC | **Country** | **Region** | **Product** | **Subproduct** | **Customer** | **POS**|
# MAGIC | Poland | Silesia | Product 1 | Subprod1 | Cust 1 | Pos 1 |
# MAGIC
# MAGIC
# MAGIC #### Format 2 (legacy EDWM)
# MAGIC * Row 1 contains Source and Target markers
# MAGIC * Row 2 contains column names
# MAGIC
# MAGIC | Source | | Target |
# MAGIC |--------|-|--------|
# MAGIC | **Corp Sub Sector** | **Corp Category** | **Local Product Sector Name** |
# MAGIC | Fabric Care | Prof Fabric Care | |
# MAGIC | Fabric Care | Laundry | PGP |
# MAGIC | Fabric Care | Fabric Enhancers | Fabric Care |
# MAGIC
# MAGIC

# COMMAND ----------

# detect format 1 or 2
import_file_path = f'/dbfs/mnt/light-refined/import/{import_file_name}'
# read just the 1st row
try:
    row1 = pd.read_excel(import_file_path, engine='openpyxl', nrows = 0)
except Exception as e:
    dbutils.notebook.exit('{"status": "ERROR", "status_desc": "Error reading Excel file:' + str(e) + '" }')

if row1.columns[0].lower() == 'sources':
    xlformat = 1
    xlheader = [0,1,2]
elif row1.columns[0].lower() == 'source':
    xlformat = 2
    xlheader = [0,1]
else: 
    dbutils.notebook.exit('{"status": "ERROR", "status_desc": "Excel file does not contain Source(s) in cell A1" }')

# now read everything, using 2 or 3 rows as header; the warning can be safely ignored
# keep_default NA - bug 805278: added to prevent parsing string "NA" as null value
df = pd.read_excel(import_file_path, engine='openpyxl', header = xlheader, dtype=str, keep_default_na=False)    


# COMMAND ----------

df

# COMMAND ----------

# convert column multiindex to flat column names
if xlformat == 1:
    df.columns = [f"{col[1]}/{col[2]}".upper() for col in df.columns]
else:
    df.columns = [f"{col[0]}/{col[1]}".upper() for col in df.columns]
df = spark.createDataFrame(df)
# bug 699734: force columns to string in case any of them is totally empty
# in this case Spark sees it as VOID and JDBC write fails
# bug 769404: enclose column name in `` for the select to work
df = df.select([col(f"`{c}`").cast("string") for c in df.columns])


# COMMAND ----------

#debug
#df.display()

# COMMAND ----------

try:
    # overwrite - the table will be created or dropped/created
    df.write.format("jdbc").option("url", db_url).option("dbtable", temp_table_name).mode("overwrite").save()
except Exception as e:
    dbutils.notebook.exit(f'{{"status": "ERROR", "status_desc": "Error saving preview data: {str(e)}" }}')



# COMMAND ----------

dbutils.notebook.exit(f'{{"status": "OK", "table_name": "{temp_table_name}", "xlformat": {xlformat}}}')
