## The purpose of this database in the context of the startup, sparkify, and their analytical goals.

<p> The analytical star schema </p>
<img src="https://github.com/CharlesIro1125/DataWarehouse/blob/master/ETL_postgres/analyticSchema.png" alt="schema" width="600" height="420" />

The database utilises the star schema to provide information about the relations, like information about the users (dimUsers table) visiting the site, information about the artist (dimArtists table) for songs currently available, information about the songs (dimSongs table) currently available in the sparkify streaming site, information about the time (dimTimes table) showing event time of the users on the site and finally a fact table (songplay) to measure useful metrics on user preferences and business goals for the services provided by the streaming site.

This schema provides an easy understanding of relationship between the various tables and an easy analytical process compared to the traditional 3NF schema.  
Various purposes can be obtained from this database, which are information on the level (free or paid) subscribed for by the users, information on the most preferred songs, information on users on a paid or free plain, information on daily hours of high or low traffic, information on users upgrading their plain (changing from free to page level), information on the most rated artist and information on the location for a higher demand.


## The database schema design and ETL pipeline.

      
The star schema is a setup of dimension tables and fact table. The dimension table provides information on 'WHO' (who are the users), 'WHAT' (what songs are clicked), 'WHEN' (what time the song was played), 'WHICH' (which artist owns the song) and the fact table utilises these information from the dimension tables to measure useful metrics on 'HOW' (how the services are utilised and how it can be improved or optimised).The tables also utilises an upsert conflict constraint to update records. This maintains the data integrity and provides an easy means to update any data.



The ETL pipeline does an extraction of data from the log files and song files generated, transforms the data to a preferable format and then loads it into the database model. The ETL pipeline script can be synchronise to trigger/run whenever a log file is generated, i.e as users lands on the site. These keeps the database updated at all time.

## Files in the repository

The sql_queries.py file contains all the sql query for dropping a table, creating a table, inserting into a table and a single query to select from some tables. All the queries are assigned to variables. This variables are imported into the create_tables.py file and the etl.py file.

The create_table.py file contains function use to establish connection to the database. It also contains function use to drop all existing tables in the database and also create all required tables.Finally, it closes connection to the database.

The etl.py file contains function use to extract csv files from the source directory into a list, i.e the log_data and the song_data directories. And another function that processes and transforms content of each csv file before loading it into the created tables in the database. Finally, it closes connection to the database.

The etl.ipynb file is a jupyter notebook showing the step-by-step preparation of the extract, transform and load process.

The describe.ipynb file shows the content of each file from the source directories.


            
### How to run this python scripts

To run this script, first run the create_table.py file to initialise the database. This should be run only once before the etl.py file, as it contains script to delete existing tables in the database.
After this, the etl.py file can be run to extract file from the source directory, transform the data and loads it into the database. With this done, the analytical queries can be performed on the database to get operational insight.

 


            


