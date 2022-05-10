# Data Engineering
# The Projects in this repository contains various projects done within the Udacity Data Engineering nanodegree.

## The ETL_Postgres project.
[ETL_Postgres](https://github.com/CharlesIro1125/DataWarehouse/tree/master/ETL_postgres). 
This project contains an extract, transform and load process to a Postgre Database.
Data from Sparkify streaming app of "song logs" and "song play" [Source data](https://github.com/CharlesIro1125/DataWarehouse/blob/master/ETL_postgres/describe.ipynb) was used to model
a star schema for analytical processing. All scripts required for the ETL process can be 
found in the ETL_Postgres project repository.

## The ETL_cassandra project.
[ETL_cassandra](https://github.com/CharlesIro1125/DataWarehouse/tree/master/ETL_cassandra).
This project contains an extract, transform and load process to a Cassandra Database.
the schema used was designed on the queries to be performed.
A cassandra database is not efficient for join operations, so the database schema is design 
to suit the query to be performed on the database. its a query dependent database 
for handling big data.

## The ETL_Redshift_Warehouse project.
[redshift warehouse](https://github.com/CharlesIro1125/DataWarehouse/tree/master/ETL_Redshift_Warehouse). 
This project contains a redshift cluster launched using "Boto3 library" (infrastructure as code) with an 
IAM permission to read data from an S3 bucket,then load the data on a staging area in 
redshift and perform an analytical model development. All scripts required to launch the 
cluster using infrastructure as code and to perform the analytical processses can be found in the 
ETL_Redshift_Warehouse repository.

## The ETL_Spark_Datalake project.
[spark datalake](https://github.com/CharlesIro1125/DataWarehouse/tree/master/ETL_Spark_DataLake).
This project contains a spark cluster that reads-in data from an S3 bucket (source), processses and transforms the
data and finally saves it as a partitioned parquet file to an S3 location (sink). The redshift cluster
needs a structured data for its schema development, Thus using spark to process any instructured
or semi structured data to a structured data.


## The Airflow_DataPipeline project.
[airflow pipeline](https://github.com/CharlesIro1125/DataWarehouse/tree/master/Airflow_DataPipeline/home/airflow).
This project contains an automated process, that runs a spark job,
copies data from S3 to redshift, performs the schema development in redshift for the analytical tables 
and finally does some quality checks on the tables to verify completeness of record, record validity and 
integrity constraint. The code also does some back filling and copies the data in partitions from S3 to 
redshift. All scripts can be found in the Airflow_DataPipeline repository.


## The Capstone project.
[capstone project](https://github.com/CharlesIro1125/DataWarehouse/tree/master/CapstoneProject).
This project contains four different dataset including the USA I-94 immigration 
dataset of arrivals into USA. The project is the final project that test the ability to build a full analytical
process with any tools of choice and develope an analytical schema based on the chosen aim of building the analytical
warehouse.
 
