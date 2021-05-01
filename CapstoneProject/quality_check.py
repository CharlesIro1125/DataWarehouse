import pandas as pd
import os
import json
import configparser
from sql_queries import *
import psycopg2
from sql_queries import test_duplicate, test_null, test_invalid_record, test_record_complete

config = configparser.ConfigParser()
config.read_file(open(os.path.join(os.getcwd(),'dl.cfg')))

os.environ['AWS_ACCESS_KEY_ID'] = config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY'] = config['AWS']['AWS_SECRET_ACCESS_KEY']


conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
cur = conn.cursor()
    
    
def check_duplicate(primary_key,table_name):
    """
        Description:       
            This function checks for duplacate record.
            
        Arguments:  
            primary_key : the primary key of the table or any column with
                        unique constraint.
            table_name : the table to perform quality check on.
               
        Returns:
            the duplicate rows.
        
    """
    
    cur.execute(test_duplicate.format(column = primary_key,table = table_name))
    val=cur.fetchall()
    conn.commit()
    
    return val

def check_null(primary_key,table_name):
    """
        Description:       
            This function checks for null values.
            
        Arguments:  
            primary_key : the primary key of the table or any column with
                        not null constraint.
            table_name : the table to perform quality check on
               
        Returns:
            all null rows.
        
    """
    
    cur.execute(test_null.format(column = primary_key,table = table_name))
    val=cur.fetchall()
    conn.commit()
    
    return val


def check_invalid_record():
    """
        Description:       
            This function checks for valid record by checking
            if the depart date is further ahead of the arrive date.

        Returns:
            number of invalid records.
        
    """
    
    cur.execute(test_invalid_record)
    val=cur.fetchall()
    conn.commit()
    
    return val.pop()[0]

def check_record_complete(table_name):
    """
        Description:       
            This function checks for complete record in database.
            
        Arguments:  
            table name: name of the table to check for complete 
            record.
               
        Returns:
            number of rows.
        
    """
    
    cur.execute(test_record_complete.format(table = table_name))
    val=cur.fetchall()
    conn.commit()
    
    return val.pop()[0]
    

    
def close_conn():
    """
        Description:       
            Closes connection to database.
             
        Returns:
            None. 
    """
    
    cur.close() 
    conn.close()
    
    return True