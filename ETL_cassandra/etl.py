import pandas as pd
import cassandra
import re
import os
import glob
import numpy as np
import json
import csv
from sql_queries import *
from cassandra.cluster import Cluster




def process_event_file(session,full_data_rows_list):

    #writting from the full_data_rows_list to the tables in the database
    i=0
    for line in full_data_rows_list:
        # ommitting rows without artist name
        if (line[0] == ''):
                
            continue

        #INSERT data into artist_and_song_by_sessionid  table, the insert_query_1
        session.execute(insert_query_1,(int(line[12]),int(line[4]),line[0],line[13],float(line[6])))
            
        #INSERT data into artist_info_by_userid  table, the insert_query_2
        session.execute(insert_query_2,(int(line[16]),int(line[12]),int(line[4]),line[0],line[13],line[2],line[5]))
            
        #INSERT data into user_by_song_heard table, the insert_query_3
        session.execute(insert_query_3, (line[13],int(line[16]),line[2],line[5]))
        i +=1
        print('{}/{} valid rows with artist name inserted.'.format(i,len(full_data_rows_list)))
        
            
            
        
def process_data(session,filepath,func):
    all_files=[]
    # Get your current folder and subfolder event data'/event_data'
    filepath = os.getcwd() + filepath

    # Create a for loop to create a list of files and collect each filepath
    for root, dirs, files in os.walk(filepath):
        
        # join the file path and roots with the subdirectories using glob
        file_path_list = glob.glob(os.path.join(root,'*s.csv'))
        
        for file in file_path_list:
            
            all_files.append(os.path.abspath(file))
            
    #sort the files
    all_files.sort()
    
    
    # initiating an empty list of rows that will be generated from each file
    full_data_rows_list = []

    # for every filepath in the file path list 
    i=0
    for f in all_files:

        # reading csv file 
        with open(f, 'r', encoding = 'utf8', newline='') as csvfile: 
            #creating a csv reader object 
            csvreader = csv.reader(csvfile) 
            next(csvreader)
            
            i +=1
            print('{}/{} files read.'.format(i,len(all_files)))
            # extracting each data row one by one and append it        
            for line in csvreader:
                # list of rows
                full_data_rows_list.append(line) 
                
                
    func(session,full_data_rows_list)
    
def main():
    
    cluster = Cluster(['127.0.0.1'])

    # To establish connection and begin executing queries, need a session
    session = cluster.connect()
    
    # connect to the keyspace
    session.set_keyspace('udacity')
    
    process_data(session,filepath = '/event_data',func=process_event_file)
    
    session.shutdown()
    cluster.shutdown()
    
if __name__ == '__main__':
    
    main()
    

 
        
        