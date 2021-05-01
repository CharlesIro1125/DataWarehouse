# Data 

## The Projects in this repostory contains various projects done within the Udacity Data Engineering nanodegree.

The ETL_Postgres project [ETL_Postgres project](). contains an extract, transform and load process 
to a Postgre Database.
Data from Sparkify streaming app of song logs and song play [Source data]() was used to model
a star schema for analytical processing. All scripts required for the ETL process can be 
found in the ETL_Postgres project repository.


## The ETL_Redshift_Warehouse project [redshift warehouse](https://github.com/CharlesIro1125/DataWarehouse/tree/master/ETL_Redshift_Warehouse). 
This project contains a redshift cluster launched using infrastructure as code with an 
IAM permission to read data from an S3 bucket,then load the data on a staging area in 
redshift and perform an analytical model development. All scripts required to launch the 
cluster using infrastructure as code and perform the analytical processses can be found in the 
ETL_Redshift_Warehouse repository.

## The ETL_Spark_Datalake project  [spark datalake](https://github.com/CharlesIro1125/DataWarehouse/tree/master/ETL_Spark_DataLake).
This project contains a spark cluster that reads-in data from an S3 bucket processses and transforms the
data and finally saves it as a partitioned parquet file to an S3 location. The redshift cluster
needs a structured data for its schema development, Thus using spark to process any instructured
or semi structured data to a structured data.


## The Airflow_DataPipeline project  [airflow pipeline](https://github.com/CharlesIro1125/DataWarehouse/tree/master/Airflow_DataPipeline/home/airflow).
This project contains an automated process, that runs a spark job,
copies data from S3 to redshift, performs the schema development in redshift for the analytical tables 
and finally does some quality checks on the tables to verify completeness of record, record validity and 
integrity constraint. The code also does some back filling and copies the data in partitions from S3 to 
redshift. All scripts can be found in the Airflow_DataPipeline repository.


## The Capstone project  [capstone project](https://github.com/CharlesIro1125/DataWarehouse/tree/master/CapstoneProject).
This project contains four different dataset including the USA I-94 immigration 
dataset of arrivals into USA. This project is the final project that test your ability to build a full analytical
process with any tools of your choice and develope an analytical schema based on your chosen aim for this analytical
warehouse.
 


ETL pipelines for various Data Warehouse project.

Detailed description for each project can be seen in the project folder
