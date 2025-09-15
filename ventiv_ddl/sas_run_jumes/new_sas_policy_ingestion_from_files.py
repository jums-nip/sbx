# Databricks notebook source
# DBTITLE 1,Import libraries and initialize the helper functions first
# MAGIC %run ./init

# COMMAND ----------

# DBTITLE 1,Create the temporary views for the delta ref tables
load_reference_tables()

# COMMAND ----------

# DBTITLE 1,Initialize the variables
# Input from workflow
dbutils.widgets.text("table_name","")
dbutils.widgets.text("book","default")
dbutils.widgets.text("catalog_name","hive_metastore")
dbutils.widgets.text("schema_name","delta_nip")

# Define file paths and database parameters
table_name = dbutils.widgets.get("table_name")
book = dbutils.widgets.get("book")
catalog_name = dbutils.widgets.get("catalog_name") # Default catalog
schema_name = dbutils.widgets.get("schema_name") # Default schema
root_path = "/user/hive/warehouse/delta_nip.db/" # Default root path
delta_table_path = f"{root_path}{table_name}"
delta_table = f"{schema_name}.{table_name}"

# Call the function to calculate the AS OF and date suffix
date_suffix, as_of_date = calculate_as_of_date()

# Call the function to get the file path
excel_file_path = get_file_path(table_name, date_suffix, book)

# Initialize Catalog and Schema
spark.sql(f"""USE CATALOG {catalog_name};""")
spark.sql(f"""USE SCHEMA {schema_name};""")

# COMMAND ----------

# DBTITLE 1,Import 01 and 02
# IMPORT 01
# Read the schema metadata CSV into a pandas DataFrame
schema_path = f"/dbfs/mnt/schema/{table_name}_import_01_schema.csv"  # Adjust this path
schema_pd = pd.read_csv(schema_path)

# Build Spark schema based on SQL Server metadata
schema_fields = build_spark_schema(schema_pd)

# Create the Spark schema
spark_schema = StructType(schema_fields)

# Read the Excel file into a Spark DataFrame
df_import_01 = spark.read.csv(excel_file_path, header=True, inferSchema=False, schema=spark_schema)
df_import_01.createOrReplaceTempView("df_import_01")

# IMPORT 02
# Load the target delta table (import 02) for new data to be loaded
target_delta_table = spark.read.format("delta").load(delta_table_path)

# Join data from the import_01 DataFrame and the delta ref tables (temp views) to create the import_02 DataFrame
df_import_02 = spark.sql(f"""
    SELECT '{as_of_date}' AS as_of,
        b.book,
        b.account_name,
        CASE WHEN a.billing_code in ('161856')
                THEN 'Tree Pro'
            ELSE b.specialty_program
        END AS specialty_program,
        b.carrier,	
        CASE WHEN a.primary_coverage IN ('EPL/POL')
                THEN a.primary_coverage
            ELSE c.lob 
        END AS LOB, /* Not adding 'EPL/POL' as an entry to the reference LOB table because claims should NOT come in this way; these premium records are ok. */
        a.*,
        (CAST(a.sub_producer_commission_char AS FLOAT) / 100) AS sub_producer_commission
    FROM df_import_01 AS a
        LEFT JOIN delta_ref_org AS b ON
            a.organization = b.organization
        LEFT JOIN delta_ref_lob AS c ON
            a.primary_coverage = c.coverage;
""")

# Match the schema from the target delta table to the newly created DataFrame (import_02)
df_import_02_with_schema = align_schema(df_import_02, target_delta_table)

# Verify schemas match
verify_schema_match = df_import_02_with_schema.schema == target_delta_table.schema
if verify_schema_match:
    df_import_02_with_schema.write.format("delta") \
        .mode("overwrite") \
        .option("truncate", "true") \
        .saveAsTable(delta_table)
