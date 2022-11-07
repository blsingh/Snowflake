
// Type of the SQL for each exercise
// 1) Execute Priori with e.g.
// 2) Ex Post state of data

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

-- Ad HOC use @% (table_stage) STORAGE to load on premise data.
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

// e.g. Priori
SHOW TABLES;

DESCRIBE TABLE CUSTOMER;

LIST @%customer;
-- #TABLE STAGE 
-- Every table in Snowflake is automatically assigned a table stage. The table stage exists for the lifetime of a table and is dropped when the table is dropped. A table stage is a suitable option if you are aiming to load data into a single table. Files loaded into a table stage can only be loaded into the table associated with that stage. Multiple users can access a table stage, but a table stage can load data into only one table. 

PUT 'file:///mnt/c/users/blsin/prompt_Git_Snowflake saves/customers.csv' @%customer;
// Priori table_stage storage using
LIST @%customer;

COPY INTO customer FROM @%customer FILE_FORMAT=(TYPE = 'CSV' FIELD_DELIMITER = '|' SKIP_HEADER = 1 COMPRESSION = 'GZIP');
// ExPost state
SELECT * FROM customer;

remove @%customer;

-- USER_STAGE @~;


CREATE DATABSE IF NOT EXIST demo_data_loading;

USE DATABASE demo_data_loading;
CREATE table vehicle(
Make STRING,
Model STRING,
Year STRING,
Category STRING
);

PUT 'file:///mnt/c/users/blsin/prompt_Git_Snowflake saves/customers.csv' @~;

LIST @~;

CREATE OR REPLACE FILE FORMAT CSV_No_Header_Blank_Lines
	type = 'CSV'
	field_delimiter = ','
	fielf_optionally_enclosed_by = '"'
	skip_header = 0
	skip_blank_lines = true;
	
COPY INTO vehicle
FROM @~/vehicles.csv.gz
file_format = CSV_No_Header_Blank_Lines;
	
SELECT * FROM vehicle;
	
REMOVE @~/vehicles.csv.gz;