## The purpose of this database in the context of the startup, sparkify, and their analytical goals.

<p> The analytical star schema </p>
<img src="https://github.com/CharlesIro1125/DataWarehouse/blob/master/ETL_Spark_DataLake/analyticSchema.png" alt="schema" width="600" height="420" />


This is a datalake that eliminates the need for creating a database table and inserting data into it. The datalake reads dataset from AWS S3 bucket using the pyspark library and transforms the data before loading it back to an S3 bucket as a partitioned parquet file. The datalake technique makes handling unstructured and structured data easier and this resulting tables can be saved in an S3 bucket to be used for analytical queries when required. The parquet file uses columnar storage and each partition is dedicated to a folder, making it easy to query a specific partition without loading the whole data<br>


## The schema design and ETL pipeline.
     
The schema design uses the star schema, which creates dimension tables with unique primary keys and some constraint, these are the User table, Artist table, Song table, and Time table. The fact table (Songplay) has a foreign key referencing this dimension tables.
The ETL pipeline does an extraction of data (song_data and log_data) from an S3 bucket, and loads this data into a spark dataframe, this data is further transformed and extracted using spark sql method and the results are saved as parquet file to an S3 bucket. Analytical process are done by extracting this tables directly from the S3 bucket.
 

## Files in the repository

- The etl.py file contains function that instantiates a spark session, and other functions that extracts data from the S3 bucket, reads this data into a spark dataframe, transforms and extracts using spark sql method and the result is saved as parquet file to an S3 bucket.

- The dwh.cfg file contains configuration variables used in the file above

- The etlprocess.ipynb file is a step-by-step example of the datalake processs

## How to run this python scripts

To run this script, you will need to first provide your aws access key and secret key to the dwh.cfg file, then run the etl.py file. This will extract, transform, extract and load back the generated analytical tables to an S3 bucket<br>
Also you will need to create an S3 bucket and provide its bucket path.



