# Databricks notebook source
# DBTITLE 1,Import libraries and initialize the helper functions first
# MAGIC %run ./init

# COMMAND ----------

# DBTITLE 1,Create the temporary views for the delta ref tables
load_reference_tables()

dbutils.widgets.removeAll()

# COMMAND ----------

# DBTITLE 1,Initialize the variables
# Define file paths and database parameters
table_name = "exposures" # Update table name here as neccessary, accepted values: claims, exposures, organization, policy, and premium
time_machine = 7 # Defaults to 1, this is used to determine how many months back in terms of coverage to load
catalog_name = "hive_metastore" # Default catalog
schema_name = "delta_nip" # Default schema
root_path = "/user/hive/warehouse/delta_nip.db/" # Default root path
delta_table_path = f"{root_path}{table_name}"
delta_table = f"{schema_name}.{table_name}"

# Call the function to calculate the AS OF and date suffix
date_suffix, as_of_date = calculate_as_of_date(time_machine)

# Call the function to get the file path
excel_file_path = get_file_path(table_name, date_suffix)
print(f"File path: {excel_file_path}")

# Initialize Catalog and Schema
spark.sql(f"""USE CATALOG {catalog_name};""")
spark.sql(f"""USE SCHEMA {schema_name};""")

# Show in widgets, will remove later
dbutils.widgets.text("table_name", table_name) 
dbutils.widgets.text("catalog_name", catalog_name)
dbutils.widgets.text("schema_name", schema_name) 
dbutils.widgets.text("root_path", root_path)
dbutils.widgets.text("delta_table_path", delta_table_path)
dbutils.widgets.text("delta_table", delta_table)
dbutils.widgets.text("excel_file_path", excel_file_path)

# COMMAND ----------

# DBTITLE 1,Import 01
# Read the schema metadata CSV into a pandas DataFrame
schema_path = f"/dbfs/mnt/schema/{table_name}_import_01_schema.csv"  # Adjust this path
schema_pd = pd.read_csv(schema_path)

# Build Spark schema based on SQL Server metadata
schema_fields = build_spark_schema(schema_pd)

# Create the Spark schema
spark_schema = StructType(schema_fields)

# Read the Excel file into a Spark DataFrame
df_import_01 = spark.read.csv(excel_file_path, header=True, inferSchema=False, schema=spark_schema)
df_import_01.createOrReplaceTempView("df_import_01") #  Create a temp view for import_01

# COMMAND ----------

# DBTITLE 1,Import 02
# Load the target delta table (import 02) for new data to be loaded
target_delta_table = spark.read.format("delta").load(delta_table_path)
target_delta_table.schema

# Join data from the import_01 DataFrame and the delta ref tables (temp views) to create the import_02 DataFrame
df_import_02 = spark.sql(f"""
    SELECT '{as_of_date}' AS as_of,
        b.book,
        b.account_name,
        b.specialty_program,
        b.carrier,	
        c.lob, /* Not adding 'Not Selected' as an entry to the reference LOB table because claims should NOT come in this way; these exposure records are ok. */
        a.*,
        CASE WHEN c.lob IN ('AL', 'APD')
                THEN 1.0 * DATEDIFF(DAY, a.expos_eff_dt, a.expos_exp_dt) / 365 /* have to recode exposures which oddly stores the vehicle # for AL and APD & adjust for time on the policy to get vehicle_years as the AL and APD exposure base */
            WHEN c.lob IN ('Property')
                THEN COALESCE(a.building_limit,0) + COALESCE(a.contents_limit,0) + COALESCE(a.time_element_limit,0) + COALESCE(a.stock,0) + COALESCE(a.personal_stock,0) + COALESCE(a.eq_limit,0) + COALESCE(a.flood_limit,0) /* the a.value field doesn't capture stock & personal_stock */
            ELSE COALESCE(a.value,0)
        END AS exposures
    FROM df_import_01 AS a
        LEFT JOIN delta_ref_org AS b ON
            a.organization = b.organization
        LEFT JOIN delta_ref_lob AS c ON
            a.coverage = c.coverage;
""")

# Match the schema from the target delta table to the newly created DataFrame (import_02)
df_import_02_with_schema = align_schema(df_import_02, target_delta_table)

# Verify schemas match
verify_schema_match = df_import_02_with_schema.schema == target_delta_table.schema
print("Schema matches target:", verify_schema_match)
if verify_schema_match:
    df_import_02_with_schema.write.format("delta").mode("append").saveAsTable(delta_table)
    display(df_import_02_with_schema)
