

ALTER WAREHOUSE IF EXISTS MY_WH RESUME IF SUSPENDED;

-- Below -- Exercises for Chapter 4 'DATA MOVEMENT'

-- 3.0 Domain: Data Movement
-- 3.1 Outline different commands used to load data and when they should be used.
-- COPY
-- INSERT
-- PUT
-- GET
-- VALIDATE
-- 3.2 Define bulk as compared to continuous data loading methods.
-- COPY
-- Snowpipe
-- 3.3 Define best practices that should be considered when loading data.
-- File size
-- Folders
-- 3.4 Outline how data can be unloaded from Snowflake to either local storage or cloud storage locations. §‼
-- Define formats supported for unloading data from Snowflake
-- Define best practices that should be considered when unloading data 
-- 3.5 Explain how to work and load semi-structured data.
-- Supported file formats
-- VARIANT column
-- Flattening the nested structure
-- 5.0 Domain: Snowflake Overview & Architecture
-- 5.1 Outline key components of Snowflake’s Cloud data platform
-- Data types
-- 5.7 Outline Snowflake’s catalog and objects.
-- Database
-- Schema
-- Data Types
-- 6.0 Domain: Storage and Protection
-- 6.1 Outline Snowflake Storage concepts.
-- Micro-partitions
-- Stage Types
-- File Formats

-- #### COMMaNDS {copy, insert, put, get, validate} # Copy vs Snowpipe # FILE_FORMATS for unstructured data # VARIANT column for unstructured data # STORAGE {Micro-partations, Stage Types, File Formats}

-- SSCC EX(ch4)

--------------------------------------------------
-- Loading On-premises Data via the Table Stage
-- list @%<TABLE_NAME>
--------------------------------------------------
CREATE OR REPLACE DATABASE demo_data_loading;


USE DATABASE DEMO_DATA_LOADING;

CREATE table customer
(
name STRING,
phone STRING,
email STRING,
address STRING,
postalCode STRING,
region STRING,
country STRING
);

SHOW TABLES;

LIST @%customer;
-- #TABLE STAGE 
-- Every table in Snowflake is automatically assigned a table stage. The table stage exists for the lifetime of a table and is dropped when the table is dropped. A table stage is a suitable option if you are aiming to load data into a single table. Files loaded into a table stage can only be loaded into the table associated with that stage. Multiple users can access a table stage, but a table stage can load data into only one table.

PUT 'file:///mnt/c/users/blsin/prompt_Git_Snowflake saves/customers.csv' @%customer;
-- #PUT command: upload local files

LIST @%customer;

USE WAREHOUSE MY_WH;

COPY INTO customer FROM @%customer FILE_FORMAT=(TYPE = 'CSV' FIELD_DELIMITER = '|' SKIP_HEADER = 1 COMPRESSION = 'GZIP');
-- COPY INTO <target tbl nm>
-- FROM @%TABLE_NAME 
-- file_format

SELECT * FROM customer;

remove @%customer;

--------------------------------------------------------------
-- Loading On-premises Data via the User Stage
-- LIST @~;
-- @~/<TABLE_NAME>
--------------------------------------------------------------


CREATE DATABASE IF NOT EXISTS demo_data_loading;

USE DATABASE demo_data_loading;
CREATE table vehicle(
Make STRING,
Model STRING,
Year STRING,
Category STRING
);

PUT 'file:///mnt/c/users/blsin/prompt_Git_Snowflake saves/vehicles.csv' @~;

LIST @~;
-- USER STAGE @~
-- User stages are unique to a user, which means users cannot access each other's stages. However, user stages can be used to load multiple tables if required, unlike a table stage, which is tied to a table.

CREATE OR REPLACE FILE FORMAT CSV_No_Header_Blank_Lines
	type = 'CSV'
	field_delimiter = ','
	field_optionally_enclosed_by = '"'
	skip_header = 0
	skip_blank_lines = true;
	
COPY INTO vehicle
FROM @~/vehicles.csv.gz
file_format = CSV_No_Header_Blank_Lines;
	
SELECT * FROM vehicle;

REMOVE @~/vehicles.csv.gz;

---------------------------------------
-- Loading On-premises Data via the Named Internal Stage

-- @<stage_ame>/<tbl_name>
---------------------------------------

CREATE DATABASE IF NOT EXISTS demo_data_loading;

USE DATABASE demo_data_loading;

CREATE TABLE Locations(
	latitude NUMBER,
	longitude NUMBER,
	place STRING,
	CountryCode STRING,
	TimeZone STRING
);

CREATE OR REPLACE FILE FORMAT TSV_No_Headers
	type = 'CSV'
	field_delimiter = '\t'
	skip_header = 0;

CREATE OR REPLACE STAGE ETL_Stage
	file_format = TSV_No_Headers;

SHOW STAGES;

PUT 'file:///mnt/c/users/blsin/prompt_Git_Snowflake saves/locations.csv' @ETL_Stage;

LIST @ETL_Stage;

COPY INTO locations
FROM @ETL_Stage/locations;

SELECT * FROM Locations LIMIT 10;

REMOVE @ETL_Stage;

---------------------------------------
-- Loading Cloud Data via a named External Stage

-- "pointing to the "S3" location (URL) containing the upload file"
---------------------------------------

USE DATABASE demo_data_loading;
CREATE OR REPLACE TABLE prospects(
	first_name STRING,
	last_name STRING,
	email STRING,
	phone STRING,
	acquired_date_time DATE,
	city STRING,
	ssn STRING,
	job STRING
);


CREATE OR REPLACE STAGE prospect_stage url='s3://snowpro-core-study-guide/dataloading/prospects/' file_format = (type = 'CSV' field_delimiter = ','   field_optionally_enclosed_by = '"' skip_header = 0); 


-- | Stage area PROSPECTS_STAGE successfully created. |
--►►►►---------------------------------------------------------------
/*-- -- SKIPPED -- SKIPPED -- SKIPPED -- SKIPPED ## if this code is run we create external stages(i.e. pointers that do not take up snfk resourses.)
CREATE OR REPLACE STAGE prospect_stage2 URL = 'S3://snowflake-external-stg-tr0' CREDENTIALS = (AWS_KEY_ID = '*******************' AWS_SECRET_KEY = '*********************************');

CREATE OR REPLACE STAGE prospect_stage1 URL='S3://snowflake-external-stg-tr0' file_format = (type = 'CSV' field_delimiter = ','   field_optionally_enclosed_by = '"' skip_header = 0) CREDENTIALS = (AWS_KEY_ID = '*******************' AWS_SECRET_KEY = '*********************************');

		trouble shoot

	CREATE STORAGE INTEGRATION link
		type = external_stage
		storage_provider = 'S3'
		enabled = TRUE
		storage_allowed_locations = ('s3://snowflake-external-stg-tr0/');
	-- Missing required property 'STORAGE_AWS_ROLE_ARN' on storage integration with storage provider S3.


	CREATE OR REPLACE STAGE prospect_stage URL = 's3://snowflake-external-stage-tr0' storage_integration = link CREDENTIALS = (AWS_KEY_ID = '*******************' AWS_SECRET_KEY = '*********************************') file_format = (type = 'CSV' field_delimiter = ',' field_optionally_enclosed_by = '"' skip_header = 0);
	-- ►► in this one like added storage_integration,
	-- ▲ the n-1 command missing AWS IAM
*/



COPY INTO prospects FROM @prospect_stage;

SELECT * FROM prospects LIMIT 10;

--------------------------------------------------------------------------------
-- Basic Data Transformation While Ingesting

-- COPY command supports (transformations):: chg column ordinality, casting data types
--------------------------------------------------------------------------------

USE DATABASE demo_data_loading;

CREATE TABLE prospects_simple (
	first_name STRING,
	last_name STRING,
	email STRING,
	phone STRING,
	acquired_date_time DATE,
	job STRING
);

COPY INTO prospects_simple
FROM (
	SELECT $1, $2, $3, $4, SUBSTR($5,1,10), $8
	FROM
	@prospect_stage
);

SELECT * FROM PROSPECTs_SIMPLE LIMIT 3;
SELECT * FROM PROSPECTS LIMIT 3;



-------------------------------------------------
-- CREATE AN External TABLE on Cloud Storage
-------------------------------------------------

CREATE DATABASE IF NOT EXISTS demo_data_loading;

CREATE OR REPLACE STAGE customer_stage url = 's3://snowpro-core-study-guide/dataloading/external/';

CREATE OR REPLACE EXTERNAL TABLE customer_external
	WITH location = @customer_stage
		file_format = (type = CSV field_delimiter = '|' skip_header =1);



SELECT  * FROM customer_external;

SELECT $1:c1 AS Name, $1:c2 as SSN, $1:c3 AS emailAddress, 
$1:c4 AS Address, $1:c5 as Zip,$1:c6 AS Location, $1:c7 as Country
FROM customer_external;


CREATE OR REPLACE EXTERNAL TABLE customer_external (
	Name STRING as (value:c1::STRING),
	Phone STRING as (value:c2::STRING),
	Email STRING as (value:c3::STRING),
	Address STRING as (value:c4::STRING),
	PostalCode STRING as (value:c5::STRING),
	City STRING as (VALUE:C6::STRING),
	Country STRING as (value:c7::STRING))
WITH location = @prospect_stage1/prospects.csv
	file_format = (type = CSV field_delimiter = '|' skip_header = 1);

SELECT * FROM customer_external LIMIT 10;

SELECT $2,$3,$4,$5,$6,$7,$8 FROM customer_external LIMIT 10;

---------------------------
--Loading JSON Data via an Eternal Stage
---------------------------

USE DATABASE demo_data_loading;

CREATE OR REPLACE TABLE employees_temp (
	rj VARIANT
);

CREATE OR REPLACE STAGE flights_json_stage
 url = 's3://snowpro-core-study-guide/dataloading/json'
 file_format = (type = json);


COPY INTO employees_temp
FROM @flights_json_stage;

SELECT rj FROM employees_temp;

---- ------ END ---- -------



 


