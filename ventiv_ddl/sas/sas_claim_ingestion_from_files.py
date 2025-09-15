# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, substring, to_date, lit
from datetime import datetime

# Define file paths and database parameters
excel_file_path = "/mnt/sas/I2I_CLAIM_NIP_20240930.CSV"  # Path to the Excel file
date_string = excel_file_path.split('/')[-1].split('_')[-1].split('.')[0]
delta_table = "nip_refined.claims" # Delta table for the main dataset
db_url = dbutils.secrets.get(scope = "nip-scope-dev", key = "AzureSql-ConnectionString-Ventiv-JDBC")

spark.sql("USE CATALOG hive_metastore")
spark.sql("USE SCHEMA nip_refined")



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

df.display()

# COMMAND ----------

# Provided list of new column names
new_column_names = [
    "accident_am_or_pm","accident_address_1","accident_address_2","accident_address_3","accident_city","accident_county","accident_department","accident_description","accident_map","accident_postal_code","accident_shift","accident_state","accident_time","account_id","activity_before_loss","address_claimant_address_1","address_claimant_address_2","address_claimant_address_3","address_claimant_city","address_claimant_postal_code","address_claimant_state","alternate_claim_number","area_of_accident","attorney_representation_date","basel_business_line","basel_loss_event_type","benefit_state","bodily_injury","body_ncci","captive_os_expense","captive_os_indemnity_bi_pd_coll","captive_os_legal","captive_os_medical_comp","captive_os_other","captive_os_total","captive_paid_expense","captive_paid_indem_bi_pd_coll","captive_paid_legal","captive_paid_medical_comp","captive_paid_other","captive_paid_total","case_type","cause","cause_ncci","claim_deductible","claim_number","claim_or_incident","claim_stage","claim_status","claimant_death_date","claimant_first_name","claimant_last_name","claimant_m_i","claimant_name","claimant_title","claims_made_date","claim_created_date","claim_currency_iso_code","claim_last_updated_date_char","clash_claim","class_code","closed_date","construction_defect","controverted","coverage","coverage_code","created_by","created_by_data_process","created_date","created_date_rc_claim","TPA","date_suit_filed","date_placed_in_litigation","dates_source_claims_made_date","dates_source_close_date","dates_source_entry_date","dates_source_loss_date","dates_source_reopen_date","dates_source_report_date","days_paid","dde_copy_data_from_ee_record","ded_sir_os_expense","ded_sir_os_indemnity_bi_pd_coll","ded_sir_os_legal","ded_sir_os_medical_comp","ded_sir_os_other","ded_sir_os_total","ded_sir_paid_expense","ded_sir_paid_indem_bi_pd_coll","ded_sir_paid_legal","ded_sir_paid_medical_comp","ded_sir_paid_other","ded_sir_paid_total","deductible_description","deductible_from_policy","descr_amtrust_spoiled_claim_num","descriptors_source_body_part","descriptors_source_cause","descriptors_source_coverage","descriptors_source_location","descriptors_source_nature","descriptors_source_pol_eff_date","descriptors_source_policy_number","descriptors_source_policy_period","descriptors_source_state","descriptors_source_status","descriptors_travelers_sai_number","descr_travelers_st_paul_pol_num","downtime_units","driver_age","driver_birth_date","driver_first_name","driver_last_name","driver_m_i","driver_relationship","driver_sex","driver_title","drivers_license_number","driver_vehicle_additional_info","drivers_first_name","employer_notification_date","first_aid_care","garage_state","gross_incurred_expense","gross_incurred_indem_bi_pd_coll","gross_incurred_legal","gross_incurred_medical_comp","gross_incurred_other","gross_incurred_total","healthcare_address_1","healthcare_address_2","healthcare_city","healthcare_phone","healthcare_postal_code","healthcare_provider_type","healthcare_state","hire_car_required","hospitalized_overnight","id_for_data_loads","in_litigation","information_claimant_age","info_claimant_date_of_birth","information_claimant_gender","information_claimant_job_title","info_claimant_marital_status","info_claimant_phone_number","information_email","information_employee_id","information_employee_status","information_employment_end_date","info_employment_start_date","information_hire_date","information_job_termination_date","information_ncci_class_code","info_occupation_when_injured","information_supervisor_email","information_supervisor_name","information_tax_id","information_tax_id_admin","information_wage_amount","information_wage_type","initial_reserve","initial_treatment_date","injury_or_illness_description","insured_name","insured_driver_citation","insured_driver_information","insured_driver_injured","insured_driver_injury_desc","insured_vehicle_damage_desc","insured_vehicle_damaged","insurer_os_expense","insurer_os_indemnity_bi_pd_coll","insurer_os_legal","insurer_os_medical_comp","insurer_os_other","insurer_os_total","insurer_paid_expense","insurer_paid_indem_bi_pd_coll","insurer_paid_legal","insurer_paid_medical_comp","insurer_paid_other","insurer_paid_total","jif_incurred","jif_paid","last_financial_update","last_load_date","last_updated_by","last_updated_date","last_worked_date","loss_date","loss_description_code","lost_days","lot_number","managed_care_fees","manufactured_date","medical_treatment_sought","model_number","nature","nature_ncci","net_incurred_expense","net_incurred_indem_bi_pd_coll","net_incurred_legal","net_incurred_medical_comp","net_incurred_other","net_incurred_total","object_substance_causing_harm","occ_severity_code","occurrence_lead_claim","event_number","office_code","organization","osha_completed_by_rep","osha_injury_type","osha_privacy","osha_recordable","osha_rep_phone","osha_rep_title","part","physician_healthcare_name","police_professional_liability","pol_eff_dt","pol_exp_dt","policy_number","policy_record_id","policy_year","primary_insurer_limit","product_name","program_name","property_damage","purpose_of_journey","ql_case_type","recovery_expected_expense","recov_expected_indem_bi_pd_coll","recovery_expected_legal","recovery_expected_medical_comp","recovery_expected_other","recovery_expected_total","recovery_paid","recovery_received_expense","recov_received_indem_bi_pd_coll","recovery_received_legal","recovery_received_medical_comp","recovery_received_other","recovery_received_total","reimbursement","reimbursement_amount","reinsurance_recovery","release_to_work_date","reopen_date","reopened","report_date","report_lag","restricted_days","return_to_work_date","road_conditions","road_type","salvage_indicator","salvage_recovery","serial_number","subro_indicator","subro_recovery","suit_date","time_employee_began_work","total_incurred_net","total_loss","total_outstanding","total_paid","total_paid_net","total_recovery","town_incurred","town_paid","tpa_adjuster","treated_in_er","user_claim_type_override","vehicle_description","vehicle_downtime","vehicle_make","vehicle_model","vehicle_number","vehicle_tag","vehicle_year","vin","visibility","weather_conditions","work_began_am_or_pm","year"
]

# Generate default column names (_c0 to _c286)
default_columns = [f"_c{i}" for i in range(len(new_column_names))]

# Create a mapping of old column names to new ones
column_mapping = dict(zip(default_columns, new_column_names))

# Rename columns in df_import_01
for old_col, new_col in column_mapping.items():
    df_import_01 = df_import_01.withColumnRenamed(old_col, new_col)

# Show the updated schema
df_import_01.printSchema()


# COMMAND ----------

df_import_01.display()

# COMMAND ----------

# Set `as_of` date
as_of_date = datetime.strptime(date_string, '%Y%m%d').strftime('%Y-%m-%d')

# Transform the data to match `organization_import_02` structure
df_import_02 = df_import_01 \
    .withColumn("as_of", to_date(lit(as_of_date), "yyyy-MM-dd"))
    



# COMMAND ----------


# Append data to Delta table (`nip_refined.claims`)
df_import_02.write.format("delta").option("mergeSchema", "true").mode("append").saveAsTable(delta_table)

print(f"Data successfully written to Delta table: {delta_table}")

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW CATALOGS;
# MAGIC
