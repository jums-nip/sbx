# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, substring, to_date, lit
from datetime import datetime

# Define file paths and database parameters
excel_file_path = "/mnt/sas/I2I_PREMIUM_NIP_20240930.CSV"  # Path to the Excel file
date_string = excel_file_path.split('/')[-1].split('_')[-1].split('.')[0]
delta_table = "nip_refined.premium" # Delta table for the main dataset
db_url = dbutils.secrets.get(scope = "nip-scope-dev", key = "AzureSql-ConnectionString-Ventiv-JDBC")


# COMMAND ----------

df = spark.read.csv(excel_file_path, header=False, inferSchema=True)

# Step 2: Extract the first row (original header) as a DataFrame
header_row = df.limit(1)

# Step 3: Get the remaining rows
data_rows = df.subtract(header_row)

# Step 4: Combine the header row as the second row in the DataFrame
combined_df = data_rows.union(header_row)

# Step 5: Optionally assign default column names
df_import_01 = combined_df.toDF(*[f"_c{i}" for i in range(len(combined_df.columns))])



# COMMAND ----------

# Set `as_of` date
as_of_date = datetime.strptime(date_string, '%Y%m%d').strftime('%Y-%m-%d')

# Transform the data to match `organization_import_02` structure
df_import_02 = df_import_01 \
    .withColumn("as_of", to_date(lit(as_of_date), "yyyy-MM-dd"))
    

# Append data to Delta table (`nip_refined.organization`)
df_import_02.write.format("delta").option("mergeSchema", "true").mode("append").saveAsTable(delta_table)

print(f"Data successfully written to Delta table: {delta_table}")

