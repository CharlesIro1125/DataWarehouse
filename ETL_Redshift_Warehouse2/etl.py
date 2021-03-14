import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    
    """
        Description: 
        
            This function copies files from an aws S3 bucket using the
            sql copy command to a staging area in a redshift cluster.
            The files from S3 bucket are the song_data file and the log_data file.
              
        Arguments:
        
            cur: the cursor object.
            conn: connection to the database.
        
            
               
        Returns:
            None
    """
   
    
    for query in copy_table_queries:
        
        cur.execute(query)
        conn.commit()
       

    
def insert_tables(cur, conn):
    
    """
        Description: 
        
            This function insert rows from tables in staging area in  
            redshift to dimension tables for analytical
            processing.
            The tables in staging area are the staging_events_table
            and staging_songs_table.
              
        Arguments:
        
            cur: the cursor object.
            conn: connection to the database.
            
               
        Returns:
            None
    """
    
    for query in insert_table_queries:
        
        cur.execute(query)
        conn.commit()
        

def main():
    
    """
        Description: 
        
        - Reads variables from the configuration file
        
        - Establishes connection with the aws redshift cluster and gets
            cursor and conn to it
             
        - calls load_staging_tables funtion and insert_tables function
            including all of its arguments
            
        - Finally, closes the connection
            
    """
    
    
    config = configparser.ConfigParser()
    config.read('dwh.cfg')
    
    try:
        conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
        cur = conn.cursor()
        
        try:
            load_staging_tables(cur, conn)
        except Exception as e:
            print(e)
            
        try:
            insert_tables(cur, conn)
        except Exception as e:
            print(e)
            
    except:
        print("connection to cluster failed")
        
    cur.close()
    conn.close()


if __name__ == "__main__":
    main()
