import cassandra

from cassandra.cluster import Cluster

from sql_queries import create_table_queries, drop_table_queries


def create_database():
    """
    - Creates and connects to the udacity keyspace
    - Returns the session to the keyspace
    """

    
    cluster= Cluster(['127.0.0.1'])

    # To establish connection and begin executing queries, need a session
    session = cluster.connect()
    
    #create keyspace
    session.execute("DROP KEYSPACE IF EXISTS udacity")
    
    session.execute("""CREATE KEYSPACE IF NOT EXISTS udacity
    WITH REPLICATION = {'class':'SimpleStrategy','replication_factor':1}""")
    
    # connect to the keyspace
    
    session.set_keyspace('udacity')
    
    return session,cluster


def drop_tables(session):
    """
    Drops each table using the queries in `drop_table_queries` list.
    """
    for query in drop_table_queries:
        session.execute(query)


def create_tables(session):
    """
    Creates each table using the queries in `create_table_queries` list. 
    """
    for query in create_table_queries:
        session.execute(query)
        


def main():
    """
    - Drops (if exists) and Creates the udacity keyspace. 
    
    - Establishes connection with the udacity keyspace and gets
     the session.  
    
    - Drops all the tables.  
    
    - Creates all tables needed. 
    
    - Finally, closes the connection. 
    """
    session, cluster = create_database()
    
    drop_tables(session)
    create_tables(session)

    session.shutdown()
    cluster.shutdown()


if __name__ == "__main__":
    main()