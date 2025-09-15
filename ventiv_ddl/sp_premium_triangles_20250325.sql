-- Ventiv.dbo.sp_premium_triangles definition

-- Drop table

-- DROP TABLE Ventiv.dbo.sp_premium_triangles;

CREATE TABLE Ventiv.dbo.sp_premium_triangles (
	as_of date NULL,
	book varchar(100) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	specialty_program varchar(100) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	carrier varchar(100) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	pol_eff_yr numeric(11,0) NULL,
	CAY smallint NULL,
	CAQ varchar(6) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	CAY_age float NULL,
	LOB varchar(100) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	organization varchar(100) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	account_id varchar(100) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	mac_id varchar(100) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	insured_name varchar(255) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	policy_number varchar(200) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	uw_company varchar(100) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	broker varchar(100) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	axa_broker varchar(100) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	billing_code varchar(100) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	marketing_plan varchar(100) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	program varchar(100) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	pol_eff_dt date NULL,
	pol_exp_dt date NULL,
	multiline_policy varchar(100) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	new_renew_flag float NULL,
	primary_coverage varchar(100) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	governing_class_code varchar(100) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	policy_state varchar(100) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	policy_state_region varchar(100) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	policy_state_ca_ind varchar(100) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	policy_zip varchar(100) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	insured_city varchar(100) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	insured_state varchar(100) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	insured_zip varchar(100) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	valen_score varchar(100) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	policy_count float NULL,
	written_premium float NULL,
	earned_premium float NULL
);