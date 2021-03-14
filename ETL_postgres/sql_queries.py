# DROP TABLES


songplay_table_drop = "DROP TABLE IF EXISTS songplay "
user_table_drop = "DROP TABLE IF EXISTS dimUsers"
song_table_drop = "DROP TABLE IF EXISTS dimSongs"
artist_table_drop = "DROP TABLE IF EXISTS dimArtists"
time_table_drop = "DROP TABLE IF EXISTS dimTimes"
    

# CREATE TABLES
songplay_table_create = ("""CREATE TABLE IF NOT EXISTS songplay (
                            songplay_id SERIAL,
                            start_time timestamp,
                            user_id  int NOT NULL,
                            level VARCHAR,
                            song_id VARCHAR,
                            artist_id VARCHAR,
                            session_id INT,
                            location TEXT,
                            user_agent TEXT,
                            PRIMARY KEY (songplay_id))
                            """)



user_table_create = ("""CREATE TABLE IF NOT EXISTS dimUsers (
                            user_id int,
                            first_name VARCHAR,
                            last_name VARCHAR,
                            gender CHAR(1),
                            level VARCHAR,
                            PRIMARY KEY (user_id ))
                            """)

    

song_table_create = ("""CREATE TABLE IF NOT EXISTS dimSongs (
                            song_id VARCHAR,
                            title TEXT,
                            artist_id VARCHAR NOT NULL,
                            year BIGINT,
                            duration NUMERIC,
                            PRIMARY KEY (song_id))
                            """)

    

artist_table_create = ("""CREATE TABLE IF NOT EXISTS dimArtists (
                            artist_id VARCHAR,
                            artist_name TEXT,
                            location VARCHAR,
                            latitude NUMERIC,
                            longitude NUMERIC,
                            PRIMARY KEY (artist_id))
                            """)


    
    
time_table_create = ("""CREATE TABLE IF NOT EXISTS dimTimes (
                            start_time timestamp,
                            hour int,
                            day int,
                            week int,
                            month int,
                            year int,
                            weekday VARCHAR,
                            PRIMARY KEY (start_time))
                            """)


# INSERT RECORDS

songplay_table_insert = ("""INSERT INTO songplay (start_time,user_id,level,song_id,
                            artist_id,session_id,location,user_agent) VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
                            """)

user_table_insert = ("""INSERT INTO dimUsers (user_id,first_name,last_name,gender,level)
                        VALUES (%s,%s,%s,%s,%s)
                        ON CONFLICT (user_id) 
                        DO UPDATE SET level = EXCLUDED.level
                        """)

song_table_insert = ("""INSERT INTO dimSongs (song_id,title,artist_id,year,duration) VALUES (%s,%s,%s,%s,%s)
                        ON CONFLICT (song_id) 
                        DO NOTHING
                        """)

artist_table_insert=("""INSERT INTO dimArtists (artist_id,artist_name,location,latitude,longitude) 
                        VALUES (%s,%s,%s,%s,%s)
                        ON CONFLICT (artist_id) 
                        DO UPDATE SET location=EXCLUDED.location,latitude=EXCLUDED.latitude,longitude=EXCLUDED.longitude
                        """)


time_table_insert = ("""INSERT INTO dimTimes (start_time,hour,day,week,month,year,weekday) 
                        VALUES (%s,%s,%s,%s,%s,%s,%s)
                        ON CONFLICT (start_time) 
                        DO NOTHING
                    """)

# FIND SONGS

song_select = ("""SELECT dimSongs.song_id ,dimArtists.artist_id FROM (dimSongs JOIN 
                    dimArtists ON dimSongs.artist_id= dimArtists.artist_id)
                   WHERE (dimSongs.title,dimArtists.artist_name) IN ( VALUES (%s,%s))""")

# QUERY LISTS

create_table_queries = [user_table_create, artist_table_create,song_table_create,time_table_create,songplay_table_create]
drop_table_queries = [songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
