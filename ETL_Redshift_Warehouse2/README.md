## The purpose of this database in the context of the startup, sparkify, and their analytical goals.

<p> The analytical star schema </p>
<img src="https://github.com/CharlesIro1125/DataWarehouse/blob/master/ETL_Redshift_Warehouse2/analyticSchema.png" alt="schema" width="600" height="420" />

The database is a full dataset from the sparkify streaming site. To handle big data a redshift cluster deployed on amazon web service is used. The database is developed on a dataset in an aws S3 bucket. This dataset is transferred to a staging area in an aws redshift cluster with database capabilities. For an analytical process to be done on this dataset, dimension tables were created from the staging table. Since the redshift cluster has a massive parallel proccessing capability, it spins 4 nodes (cpu) to process this data. The analytical tables are developed with a star schema to provide information about the relations, like information about the users (dimUsers table) visiting the site, information about the artist (dimArtists table) for songs currently available, information about the songs (dimSongs table) currently available in the sparkify streaming site, information about the time (dimTimes table) showing event time of the users on the site and finally a fact table (songplay) to measure useful metrics on user preferences and business goals for the services provided by the streaming site.<br>

This schema provides an easy understanding of relationship between the various tables and an easy analytical process compared to the traditional 3NF schema. <br>

Various purposes can be obtained from this database, which are information on the level (free or paid) subscribed for by the users, information on the most preferred songs, information on users on a paid or free plain, information on daily hours of high or low traffic, information on users upgrading their plain (changing from free to page level), information on the most rated artist and information on the location for a higher demand.


## The database schema design and ETL pipeline.
     
The star schema is a setup of dimension tables and fact table. The dimension table provides information on *WHO* (who are the users), *WHAT* (what songs are clicked), *WHEN* (what time the song was played), *WHICH* (which artist owns the song) and the fact table utilises these information from the dimension tables to measure useful metrics on *HOW* (how the services are utilised and how it can be improved or optimised).The tables also utilises a distribution and sorting key to speed up query request.<br>

The ETL pipeline does an extraction of data (song_data and log_data) from an S3 bucket, and loads this data into a staging_events table and staging_songs table created in the redshift cluster, this data is further extracted, transformed and loaded to some dimension tables created for analytical purposes. Analytical process are then performed on the dimension tables.
 

## Files in the repository

- The redshift.py file first reads configuration variables from a configuration file (dwh.cfg).This redshift.py file contains a function that creates an identity and access management (iam) role using the aws account owner access key and secret key, and a function that creates a redshift cluster with thesame user authentication credentials and applies the created iam role to the cluster, and another function that uses an EC2 VPC (Virtual Private Cloud) to allow external communication to the redshift cluster. Finally, it writes the redshift endpoint and roleArn to the dwh.cfg configuration file.
 
- The drop_redshift.py file first reads configuration variables from a configuration file (dwh.cfg) and it has a main function that deletes the created redshift cluster and also detaches the role policy before deleting the role.
 
- The sql_queries.py file contains all the sql query for dropping pre-existing tables, creating required tables,coping data to redshift cluster, inserting data from staging tables into analytical tables. All the queries are assigned to variables and the variables are imported into the create_tables.py file and the etl.py file.

- The create_table.py file contains function use to establish connection to the redshift cluster, drop all existing tables in the database and also create all required tables.Finally, it closes connection to the database.

- The etl.py file contains function that extracts data from the S3 bucket and copies this data into a staging table in the redshift cluster, i.e the log_data and the song_data . And another function that extracts data from the staging table, transforms it and loads it into the created analytical tables in the database. Finally, it closes connection to the database.

- The dwh.cfg file contains configuration variables used in the files above

            
## How to run this python scripts

To run this script, first provide your aws access key and secret key to the dwh.cfg file, then run the redshift.py file to create the redshift cluster, then run the create_table.py file to initialise the database. This file should be run only once before the etl.py file, as it contains script to delete existing tables in the database.<br>
After this, run the etl.py file to extract data from S3 bucket to redshift staging tables, and from staging tables to analytical tables. With this done, the analytical queries can be performed on the database to get operations insight.<br>
**After the analytical queries, run the drop_redshift.py file to drop all aws resources.cost attach**

###  Example queries.The queries are executed on the test.ipynb folder. 

> Who are the artists our users listen to and which of their songs?
```
  %sql SELECT a.artist_name, g.title, count(DISTINCT(s.userId)) AS number_of_users FROM songplay AS s JOIN dimArtists AS a \
          ON (a.artist_id = s.artist_id) JOIN dimSongs AS g ON (g.song_id= s.song_id)\
                    group by artist_name,title order by number_of_users DESC LIMIT 10;
```
<img src="https://github.com/CharlesIro1125/DataWarehouse/blob/master/ETL_Redshift_Warehouse2/query11.png" alt="result1" width="560" height="360" />

> Are the users on paid plan or free plan?

```
  %sql SELECT level AS user_level, a.artist_name, g.title, count(DISTINCT(s.userId)) AS number_of_users FROM songplay AS s \              
               JOIN dimArtists AS a ON (a.artist_id = s.artist_id) JOIN dimSongs AS g ON (g.song_id= s.song_id) \
                    group by level,artist_name,title order by number_of_users DESC LIMIT 10;
```            
<img src="https://github.com/CharlesIro1125/DataWarehouse/blob/master/ETL_Redshift_Warehouse2/query22.png" alt="result2" width="560" height="360" />     

> What location has the highest users?

```
    %sql SELECT level AS user_level, location, count(DISTINCT(userId)) AS number_of_users FROM songplay \
        group by level,location order by number_of_users DESC LIMIT 10;
```            
<img src="https://github.com/CharlesIro1125/DataWarehouse/blob/main/query33.png" alt="result3" width="560" height="360" />        


            
            
            


