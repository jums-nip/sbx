-- Ventiv.dbo.ref_organization definition

-- Drop table

-- DROP TABLE Ventiv.dbo.ref_organization;

CREATE TABLE Ventiv.dbo.ref_organization (
	organization varchar(50) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	book varchar(25) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	account_name varchar(50) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	specialty_program varchar(30) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	carrier varchar(30) COLLATE SQL_Latin1_General_CP1_CI_AS NULL
);