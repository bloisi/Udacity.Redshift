# Project Data Warehouse

This project reads data from JSON files to load into a Redshift database.The Redshift keeps a star schema within dimension and fact tables.

# sql_queries.py

This file has all project queries. There are DROP, CREATE, STAGING TABLES, FINAL TABLES AND ELIMINATE DUPLICITY query groups. 

# create_tables.py

This file creates all tables used in the project importing queries from sql_queries.py.

# etl.py

This file loads data from JSON files, inserts data into star schema tables and eliminates duplicities.

function load_staging_tables: Loads the meta data from JSON files to staging tables;  

function insert_tables: Inserts the data from staging tables to dimension and fact tables;

function dup_tables: Eliminates duplicating records based on the primary key;

function alter_tables: Creates primary keys in users and artists tables;

> **_Your question_**

I Don´t know if I used the best way to eliminate duplicate records in users and artists tables. I created a auxiliar table keeping duplicate records using redshift rowID to set a difference between then. After, I used the table for keep just the max(rowID) each group of records duplicated.