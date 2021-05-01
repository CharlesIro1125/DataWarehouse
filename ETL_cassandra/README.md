
The data modeling done uses a denormalised table. The modeling was done based on the queries provided to enable an efficient and fast performance from the cassandra database. Each table was modeled for each query and the neccessary columns required were included. Partition key and clustering columns were utilised in each model.

The folders present are 

1] The sql_queries.py which holds all the sql queries needed to develope an ETL pipeline.

2] The create_table.py which is used to drop existing tables in the database and also create a keyspace instance.

3] The project_1B_project_template.ipynb which holds the analytical developement of the ETL pipeline.

4] The etl.py which is used to run the modularized script to extract data from the event_data file and load the data into  the database.

5] The test.ipynb file which is used to validate the queries provided.


