-- Ventiv.dbo.organization definition

-- Drop table

-- DROP TABLE Ventiv.dbo.organization;

CREATE TABLE Ventiv.dbo.organization (
	as_of date NULL,
	address_1 nvarchar(50) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	address_2 nvarchar(50) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	address_3 nvarchar(50) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	admin_unit_level_3_cd nvarchar(50) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	city nvarchar(50) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	country_iso_code nvarchar(50) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	description nvarchar(50) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	end_date date NULL,
	fax_number nvarchar(50) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	last_updated_by nvarchar(50) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	last_updated_date date NULL,
	latitude nvarchar(50) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	legal_name nvarchar(50) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	organization nvarchar(50) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	organization_status nvarchar(50) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	phone_number nvarchar(50) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	postal_code nvarchar(50) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	reason_for_leaving nvarchar(50) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	[role] nvarchar(50) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	start_date date NULL,
	state nvarchar(50) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	update_geocoding nvarchar(50) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	organization_currency_iso_code nvarchar(50) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	org_last_updated_date_char nvarchar(50) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	organization_created_date_char nvarchar(50) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	organization_last_updated_date date NULL,
	organization_created_date date NULL
);