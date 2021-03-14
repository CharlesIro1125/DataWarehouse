import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *
from psycopg2 import Error as e


def process_song_file(cur, datapath):
    
    """
        Description: 
        
            this function is responsible for reading data from each 
            file of the song_data directory and performing the required 
            transformation on the data before loading the data into the 
            created dimSongs table and dimArtists table.
              
        Arguments:
        
            cur: the cursor object.            
            datapath: the path to the csv file
               
        Returns:
            None
    """
                  
    # open song file
    df = pd.read_json(datapath,lines=True)

    # insert song record
    song_data = df.loc[0,["song_id","title","artist_id","year","duration"]]
    song_data=song_data.to_dict()
    song_data['year'] = int(song_data['year'])
    song_data =list(song_data.values())
    
    
    cur.execute(song_table_insert, song_data)
    
    
    
    # insert artist record
    artist_data = df.loc[0,["artist_id","artist_name","artist_location","artist_latitude","artist_longitude"]].values
    artist_data= artist_data.tolist()
    
    cur.execute(artist_table_insert, artist_data)


def process_log_file(cur, datapath):
    
    """
        Description: 
        
            this function is responsible for reading data from each 
            file of the log_data directory and performing the required 
            transformation on the data before loading the data into the 
            created dimUsers table, dimTimes table, and songplay table.
        
        Arguments:
        
            cur: the cursor object.
            datapath: the path to the csv file
            
        Returns:
            None
    """
    
    # open log csv file
    df = pd.read_json(datapath,lines=True)

    # filter by NextSong action
    df = df[df.page=="NextSong"]
    df_2 =df.copy()

    # convert timestamp column to datetime
    df_2["ts"] = pd.to_datetime(df_2["ts"],format='%Y-%m-%dT%H:%M:%SZ',infer_datetime_format=True)
    t=df_2["ts"]

    
    # insert time data records
    time_data = ([t,t.dt.hour,t.dt.day,t.dt.weekofyear,t.dt.month,t.dt.year,t.dt.weekday])
    column_labels = ("start_time","hour","day","week_of_year","month","year","weekday")
    time_df = pd.DataFrame({column_labels[0]:time_data[0],column_labels[1]:time_data[1],column_labels[2]:time_data[2],\
                            column_labels[3]:time_data[3],column_labels[4]:time_data[4],\
                            column_labels[5]:time_data[5],column_labels[6]:time_data[6]})

    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))

    # load user table
    user_df = df[["userId","firstName","lastName","gender","level"]]

    # insert user records
    for i, row in user_df.iterrows():
        cur.execute(user_table_insert, row)

    # insert songplay records
    for index, row in df_2.iterrows():
        
        # get songid and artistid from song and artist tables,,row.length 
        cur.execute(song_select, (row.song, row.artist))
        results = cur.fetchone()
        
        if results:
            songid, artistid = results
        else:
            songid, artistid = None, None

        # insert songplay record
        songplay_data = (row.ts,row.userId,row.level,songid,artistid,
                         row.sessionId,row.location,row.userAgent)
        cur.execute(songplay_table_insert, songplay_data)


def process_data(cur, conn, filepath, func):
    
    """
        Description:
        
            this function is responsible for extracting files from 
            the song_data and log_data directory and passing this
            files to the process_song_file and process_log_file functions.
            and then commits the changes to the database
        
        Arguments:
        
            cur: the cursor object.
            conn: connection to the database.
            filepath: the path to the source directory.
            func: defined function.
            
            
        Returns:
        
            None
    """
    
    # get all files matching extension from directory
    all_files = []
    for root, dirs, files in os.walk(filepath):
        
        files = glob.glob(os.path.join(root,'*.json'))
        
        for f in files :
            
            all_files.append(os.path.abspath(f))
            
    #sort files        
    all_files.sort()
    # get total number of files found
    num_files = len(all_files)
    print('{} files found in {}'.format(num_files, filepath))

    # iterate over files and process
    for i, datafile in enumerate(all_files, 1):
        func(cur, datafile)
        conn.commit()
        print('{}/{} files processed.'.format(i, num_files))


def main():
    """
    Description:
        
        - Establishes connection with the sparkify database and gets
            cursor and conn to it
             
        - calls process_data funtion including all of its arguments
            
        - Finally, closes the connection
    """
    
try:
    
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()

except Exception as e:
    print(e)
    print("connection to database failed")
    
try:
    
    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)
    conn.close()
    
except Exception as e:
    conn.close()
    print(e)
    print("processing data from source directory unsuccessful")


if __name__ == "__main__":
    main()
    
    