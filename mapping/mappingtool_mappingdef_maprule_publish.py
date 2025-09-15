# Databricks notebook source
dbutils.widgets.text("mapping_rule_set_key","")

# COMMAND ----------

from mdm_integration_lib import *
import os
import pyspark.sql.functions as F

# COMMAND ----------

mapping_rule_set_key = dbutils.widgets.get("mapping_rule_set_key")

# COMMAND ----------

#debug
#mapping_rule_set_key = 200

# COMMAND ----------

# the subquery with many joins is inefficiently repeated twice in this query, as src and trgt
# it should be declared as CTE at the beginning, but currently Spark does not support CTE (WITH clause) in MS SQL, this will be fixed in later versions (but not in 3.4.0, probably later)
# https://github.com/apache/spark/pull/36440
# alternatively, the query might be moved to a view on SQL DB

maprule_publish_query = f"""
(
select 
mr.mapping_rule_key,
mr.mapping_rule_column_group_id,
mr.mapping_rule_srce_column_val,
mr.mapping_rule_trgt_column_val,
mr.is_missing_srce_flag,
mr.is_missing_trgt_flag,
mr.begin_date,
mr.end_date,
mr.curr_ind,
mr.active_ind,
mr.created_by_party_key,
mr.updated_by_party_key,
mr.data_owner_key,
mr.create_date,
mr.update_date,
mr.secure_group_key,
--mr.process_run_key,
mr.[_mdmp_created_by_user_email],
mr.[_mdmp_last_update_user_email],
src.selection_set_column_key AS srce_selection_set_column_key,
trgt.selection_set_column_key AS trgt_selection_set_column_key,
src.mapping_rule_set_selection_set_assoc_key AS mapping_rule_set_srce_selection_set_key,
trgt.mapping_rule_set_selection_set_assoc_key AS mapping_rule_set_trgt_selection_set_key,
src.selection_set_name AS srce_selection_set_name,
trgt.selection_set_name AS trgt_selection_set_name,
src.mapping_rule_set_name AS mapping_rule_set_name,
src.selection_set_mapping_tool_column_name AS srce_selection_set_mapping_tool_column_name,
trgt.selection_set_mapping_tool_column_name AS trgt_selection_set_mapping_tool_column_name,
src.selection_set_srce_column_name AS srce_selection_set_srce_column_name,
trgt.selection_set_srce_column_name AS trgt_selection_set_srce_column_name,
src.mapping_rule_set_key AS mapping_rule_set_key,
src.mapping_rule_set_type_name AS mapping_rule_set_type_name,
src.mapping_rule_data_comparison_type_name AS mapping_rule_data_comparison_type_name,
src.data_provider_key AS data_provider_key,
src.data_provider_code AS data_provider_code
FROM 
dbo.mapping_rule mr
join 
(
SELECT 
	mrsssc.mapping_rule_set_selection_set_column_assoc_key,
	ssc.selection_set_column_key, 
	mrsss.mapping_rule_set_selection_set_assoc_key,
	ss.selection_set_name, 
	mrs.mapping_rule_set_name, 
	ssc.selection_set_mapping_tool_column_name,
	ssc.selection_set_srce_column_name,
	mrs.mapping_rule_set_key,
	mrst.mapping_rule_set_type_name,
	mrdct.mapping_rule_data_comparison_type_name,
	dp.data_provider_key,
	dp.data_provider_code 
FROM
	dbo.mapping_rule_set_selection_set_column_assoc as mrsssc
	INNER JOIN dbo.selection_set_column AS ssc ON mrsssc.selection_set_column_key = ssc.selection_set_column_key 
	INNER JOIN dbo.mapping_rule_set_selection_set_assoc AS mrsss ON mrsssc.mapping_rule_set_selection_set_assoc_key = mrsss.mapping_rule_set_selection_set_assoc_key
	INNER JOIN dbo.mapping_rule_set AS mrs ON mrsss.mapping_rule_set_key = mrs.mapping_rule_set_key
	INNER JOIN dbo.selection_set AS ss ON mrsss.selection_set_key = ss.selection_set_key
	INNER JOIN dbo.data_provider AS dp ON mrs.data_provider_key = dp.data_provider_key
	INNER JOIN dbo.mapping_rule_set_type_lkp AS mrst ON mrs.mapping_rule_set_type_id = mrst.mapping_rule_set_type_id
	INNER JOIN dbo.mapping_rule_data_comparison_type_lkp AS mrdct ON mrs.mapping_rule_data_comparison_type_id = mrdct.mapping_rule_data_comparison_type_id
) src on mr.mapping_rule_set_srce_selection_set_column_assoc_key = src.mapping_rule_set_selection_set_column_assoc_key 
join 
(
SELECT 
	mrsssc.mapping_rule_set_selection_set_column_assoc_key,
	ssc.selection_set_column_key, 
	mrsss.mapping_rule_set_selection_set_assoc_key,
	ss.selection_set_name, 
	mrs.mapping_rule_set_name, 
	ssc.selection_set_mapping_tool_column_name,
	ssc.selection_set_srce_column_name,
	mrs.mapping_rule_set_key,
	mrst.mapping_rule_set_type_name,
	mrdct.mapping_rule_data_comparison_type_name,
	dp.data_provider_key,
	dp.data_provider_code 
FROM
	dbo.mapping_rule_set_selection_set_column_assoc as mrsssc
	INNER JOIN dbo.selection_set_column AS ssc ON mrsssc.selection_set_column_key = ssc.selection_set_column_key 
	INNER JOIN dbo.mapping_rule_set_selection_set_assoc AS mrsss ON mrsssc.mapping_rule_set_selection_set_assoc_key = mrsss.mapping_rule_set_selection_set_assoc_key
	INNER JOIN dbo.mapping_rule_set AS mrs ON mrsss.mapping_rule_set_key = mrs.mapping_rule_set_key
	INNER JOIN dbo.selection_set AS ss ON mrsss.selection_set_key = ss.selection_set_key
	INNER JOIN dbo.data_provider AS dp ON mrs.data_provider_key = dp.data_provider_key
	INNER JOIN dbo.mapping_rule_set_type_lkp AS mrst ON mrs.mapping_rule_set_type_id = mrst.mapping_rule_set_type_id
	INNER JOIN dbo.mapping_rule_data_comparison_type_lkp AS mrdct ON mrs.mapping_rule_data_comparison_type_id = mrdct.mapping_rule_data_comparison_type_id
) trgt on mr.mapping_rule_set_trgt_selection_set_column_assoc_key  = trgt.mapping_rule_set_selection_set_column_assoc_key 
where src.mapping_rule_set_key = {mapping_rule_set_key}
)
"""


# COMMAND ----------

# run query

db_url = dbutils.secrets.get(scope = 'mappings', key = "mappings-backend-spring-datasource-url")
df = spark.read.format("com.microsoft.sqlserver.jdbc.spark") \
        .option("url", db_url) \
        .option("query", maprule_publish_query) \
        .load()

# COMMAND ----------

#debug
#df.count()
#df.display()

# COMMAND ----------

# publish to mappings light-refined
spark.conf.set("spark.sql.sources.partitionOverwriteMode","dynamic")
df.withColumn('part_mapping_rule_set_name', F.col('mapping_rule_set_name')).write.option("compression", "snappy").mode("overwrite").partitionBy('part_mapping_rule_set_name').parquet('/mnt/light-refined/publish/mapping_rule')

# COMMAND ----------

# publish to CDL Refined

os.environ["MDM_OVERWRITE_UPDATE_DATE"] = "true"
os.environ["MDM_INTEGRATION_LIB_APPLICATION_KEY"] = dbutils.secrets.get(scope = "mappings", key = "mdm-integration-lib-application-key")
configuration = MdmIntegrationLibConfiguration(configuration_id=dbutils.secrets.get(scope = "mappings", key = "mdm-integration-lib-conf-id-publish"))
external_storage_configuration = configuration.storage_configuration.replace(container_name="light-refined")
lib = MdmIntegrationLib(spark_session=spark, configuration=configuration)

lib.push(
  object_name="mapping_rule",
  df=df,
  refresh_mode=MdmRefinedRefreshMode.full,
  generic_schema_version="1",
  external_storage_configuration=external_storage_configuration,
  external_storage_directory="mapping_rule",
  external_storage_filter={"refresh_mode": [MdmRefinedRefreshMode.full], "layout_type": [MdmRefinedLayoutType.generic]},
        )
