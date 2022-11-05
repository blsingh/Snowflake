ls
# comment if works
####
'''
pwd
'''
snowsql -a ysb81193 -u cortomaltese
-- enter password at prompt

SELECT CURRENT_TIMESTAMP;

SELECT CURRENT_DATE;

SELECT CURRENT_DATABASE();

show databases;

CREATE DATABASE demo_data_loading;

USE DATABASE DEMO_DATA_LOADING;

SHOW TABLES;

CREATE table customer(
    name STRING,
    phone STRING,
    email STRING,
    address STRING,
    postalCode STRING,
    region STRING,
    country STRING
);

DESCRIBE CUSTOMER;

SELECT * FROM customer;
-- pass comment;

SHOW TABLES;


-- SSCC EX(ch4)

CREATE OR REPLACE DATABASE demo_data_loading;

USE DATABASE DEMO_DATE_LOADING;

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

PUT 'file:///mnt/c/users/blsin/prompt_Git_Snowflake saves/customers.csv' @%customer;

LIST @%customer;

COPY INTO customer FROM @%customer FILE_FORMAT=(TYPE = 'CSV' FIELD_DELIMITER = '|' SKIP_HEADER = 1 COMPRESSION = 'GZIP');

ALTER TABLE customer add led smallint default 1;

SELECT * FROM customer;