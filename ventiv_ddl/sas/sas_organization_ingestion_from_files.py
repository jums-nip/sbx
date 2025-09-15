# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, substring, to_date, lit
from datetime import datetime

# Define file paths and database parameters
excel_file_path = "/mnt/sas/I2I_ORGANIZATION_NIP_20240930.CSV"  # Path to the Excel file
date_string = excel_file_path.split('/')[-1].split('_')[-1].split('.')[0]
delta_table = "nip_refined.organization" # Delta table for the main dataset
db_url = dbutils.secrets.get(scope = "nip-scope-dev", key = "AzureSql-ConnectionString-Ventiv-JDBC")


# COMMAND ----------

df_import_01 = spark.read.csv(excel_file_path, header=True, inferSchema=True)

# COMMAND ----------

# Set `as_of` date
as_of_date = datetime.strptime(date_string, '%Y%m%d').strftime('%Y-%m-%d')

# Transform the data to match `organization_import_02` structure
df_import_02 = df_import_01 \
    .withColumn("as_of", to_date(lit(as_of_date), "yyyy-MM-dd")) \
    .withColumn("ORGANIZATION_LAST_UPDATED_DATE", to_date(col("ORGANIZATION_LAST_UPDATED_DATE"), "MM-dd-yyyy")) \
    .withColumn("ORGANIZATION_CREATED_DATE", to_date(col("ORGANIZATION_CREATED_DATE"), "MM-dd-yyyy")) \
    .withColumn("End_Date", to_date(col("End_Date"), "MM-dd-yyyy")) \
    .withColumn("Start_Date", to_date(col("Start_Date"), "MM-dd-yyyy")) \
    .withColumn("Last_Updated_Date", to_date(col("Last_Updated_Date"), "MM-dd-yyyy")) \
    .withColumn("postal_code", col("postal_code").cast("string"))
    

# Append data to Delta table (`nip_refined.organization`)
df_import_02.write.format("delta").option("mergeSchema", "true").mode("append").saveAsTable(delta_table)

print(f"Data successfully written to Delta table: {delta_table}")

