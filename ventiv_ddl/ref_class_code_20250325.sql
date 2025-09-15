-- Ventiv.dbo.ref_class_code definition

-- Drop table

-- DROP TABLE Ventiv.dbo.ref_class_code;

CREATE TABLE Ventiv.dbo.ref_class_code (
	LOB varchar(50) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	class_code varchar(20) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	class_code_desc varchar(300) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	auto_size_class varchar(60) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	auto_size_class_type varchar(30) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	auto_size_class_weight varchar(30) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	auto_business_use_class varchar(15) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	auto_radius_class varchar(30) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	auto_fleet varchar(2) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	auto_classification varchar(40) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	auto_subclassification varchar(70) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	auto_type varchar(30) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	auto_subtype varchar(30) COLLATE SQL_Latin1_General_CP1_CI_AS NULL
);