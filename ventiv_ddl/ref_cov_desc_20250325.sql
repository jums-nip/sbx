-- Ventiv.dbo.ref_cov_desc definition

-- Drop table

-- DROP TABLE Ventiv.dbo.ref_cov_desc;

CREATE TABLE Ventiv.dbo.ref_cov_desc (
	book varchar(25) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	LOB varchar(20) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	TPA varchar(50) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	descriptors_source_coverage varchar(50) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	cov_desc varchar(50) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	cov_desc_detail varchar(50) COLLATE SQL_Latin1_General_CP1_CI_AS NULL
);