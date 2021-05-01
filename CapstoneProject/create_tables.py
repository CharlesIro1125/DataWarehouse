import configparser
import psycopg2
from sql_queries import create_table_queries,drop_table_queries


def drop_tables(cur, conn):
    
    """
        Description:
            Drops each table using the queries in `drop_table_queries` list.
    
        Arguments:
            cur: cursor object
            conn: connection to the database
        Return:
            None
        
    """
    
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    
    """
        Description:
            Creates each table using the queries in `create_table_queries` list.
    
        Arguments:
            cur: cursor object
            conn: connection to the database
        Return:
            None
        
    """
    
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    
    """
        - Reads variables from the configuration file. 
    
        - Establishes connection with the redshift cluster and gets
            cursor to it.  
    
        - Drops all pre-existing tables.  
    
        - Creates all tables needed. 
    
        - Finally, closes the connection. 
        
    """
    
    config = configparser.ConfigParser()
    config.read('dl.cfg')
    
    try:

        conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
        cur = conn.cursor()
        
        try:
            drop_tables(cur, conn)
        except Exception as e:
            print(e)
            
        try:
            create_tables(cur, conn)
        except Exception as e:
            print(e)
            
            
    except:
        print('connection to cluster failed')
        
        

    cur.close()
    conn.close()


if __name__ == "__main__":
    main()