import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events_table "
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs_table"
songplay_table_drop = "DROP TABLE IF EXISTS songplay "
user_table_drop = "DROP TABLE IF EXISTS dimUsers"
song_table_drop = "DROP TABLE IF EXISTS dimSongs"
artist_table_drop = "DROP TABLE IF EXISTS dimArtists"
time_table_drop = "DROP TABLE IF EXISTS dimTimes"

# CREATE TABLES

staging_events_table_create= ("""CREATE TABLE IF NOT EXISTS staging_events_table(
                                staging_event_id bigint identity(0,1),
                                artist TEXT distkey,
                                auth varchar(20),
                                firstName varchar,
                                gender char(2) ,
                                itemInSession integer,
                                lastName varchar,
                                length real, 
                                level varchar(10),
                                location varchar(50),
                                method varchar(10),
                                log_id integer,
                                page varchar(20) sortkey,
                                registration bigint,
                                sessionId int,
                                song TEXT , 
                                status integer,
                                ts bigint,
                                userAgent TEXT,
                                userId int,
                                PRIMARY KEY (staging_event_id))
                                diststyle key;
                                """)

staging_songs_table_create = ("""CREATE TABLE IF NOT EXISTS staging_songs_table(
                                stagingsong_id bigint identity(0,1),
                                num_songs integer,
                                artist_id varchar(30),
                                artist_latitude real,
                                artist_longitude real,
                                artist_location TEXT,
                                artist_name TEXT distkey,
                                song_id varchar(30),
                                title TEXT,
                                duration real,
                                year integer,
                                PRIMARY KEY (stagingsong_id))
                                diststyle key;
                                """)

songplay_table_create = ("""CREATE TABLE IF NOT EXISTS songplay (
                            songplay_id bigint identity(0,1) distkey,
                            start_time timestamp NOT NULL sortkey,
                            userId int NOT NULL,
                            level VARCHAR(10),
                            song_id VARCHAR(30) NOT NULL,
                            artist_id VARCHAR(30) NOT NULL,
                            sessionId int,
                            location TEXT,
                            userAgent TEXT,
                            PRIMARY KEY (songplay_id),
                            foreign key(start_time) references dimTimes(start_time),
                            foreign key(userId) references dimUsers(userId),
                            foreign key(song_id) references dimSongs(song_id),
                            foreign key(artist_id) references dimArtists(artist_id))
                            diststyle key;
                            """)

user_table_create = ("""CREATE TABLE IF NOT EXISTS dimUsers(
                            userId int sortkey,
                            firstName VARCHAR,
                            lastName VARCHAR,
                            gender CHAR(1),
                            level VARCHAR(10),
                            PRIMARY KEY (userId))
                            diststyle all;
                            """)

song_table_create = ("""CREATE TABLE IF NOT EXISTS dimSongs(
                            song_id VARCHAR(30) sortkey,
                            title TEXT,
                            artist_id VARCHAR(30),
                            year INT,
                            duration real,
                            PRIMARY KEY (song_id))
                            diststyle all;
                            """)

artist_table_create = ("""CREATE TABLE IF NOT EXISTS dimArtists(
                            artist_id VARCHAR(30) sortkey,
                            artist_name TEXT,
                            location TEXT,
                            latitude real,
                            longitude real,
                            PRIMARY KEY (artist_id))
                            diststyle all;
                            """)

time_table_create = ("""CREATE TABLE IF NOT EXISTS dimTimes (
                            start_time timestamp sortkey,
                            hour int,
                            day int,
                            week int,
                            month int,
                            year int,
                            weekday VARCHAR(20),
                            PRIMARY KEY (start_time))
                            diststyle all;
                            """)

# STAGING TABLES SET 

staging_events_copy = """copy staging_events_table from 's3://udacity-dend/log_data'
                        credentials 'aws_iam_role={}'
                        json 'auto ignorecase' region 'us-west-2';""".format(*config['IAM_ROLE'].values())

staging_songs_copy = """copy staging_songs_table from 's3://udacity-dend/song_data'
                        credentials 'aws_iam_role={}'
                        json 'auto ignorecase' region 'us-west-2';""".format(*config['IAM_ROLE'].values())

# FINAL TABLES

songplay_table_insert = ("""INSERT INTO songplay (start_time,userId,level,song_id,
                                artist_id,sessionId,location,userAgent)
                                  SELECT DISTINCT '1970-01-01 00:00:00 GMT'::timestamp + ((e.ts)::text)::interval AS
                                  start_time,
                                  e.userId,e.level,s.song_id,s.artist_id,e.sessionId,
                                  e.location,e.userAgent
                                  FROM staging_events_table AS e
                                  JOIN staging_songs_table AS s ON (e.artist=s.artist_name AND e.song=s.title)
                                  WHERE e.page = 'NextSong';
                                """)

user_table_insert = ("""INSERT INTO dimUsers (userId,firstName,lastName,gender,level)
                            SELECT DISTINCT userId, firstName, lastName, gender,level
                            FROM staging_events_table
                            WHERE page = 'NextSong';
                          """)

song_table_insert = ("""INSERT INTO dimSongs (song_id,title,artist_id,year,duration)
                            SELECT DISTINCT song_id,title,artist_id,year,duration FROM staging_songs_table;
                         """)

artist_table_insert = ("""INSERT INTO dimArtists (artist_id,artist_name,location,latitude,longitude)
                            SELECT DISTINCT artist_id,artist_name,artist_location,artist_latitude,artist_longitude
                            FROM staging_songs_table;
                         """)

time_table_insert = ("""INSERT INTO dimTimes (start_time,hour,day,week,month,year,weekday)
                            SELECT DISTINCT ('1970-01-01 00:00:00 GMT'::timestamp + (ts::text)::interval) AS start_time,
                            EXTRACT(hour from ('1970-01-01 00:00:00 GMT'::timestamp + (ts::text)::interval)) AS hour,
                            EXTRACT(day from ('1970-01-01 00:00:00 GMT'::timestamp + (ts::text)::interval)) AS day,
                            EXTRACT(week from ('1970-01-01 00:00:00 GMT'::timestamp + (ts::text)::interval)) AS week,
                            EXTRACT(month from ('1970-01-01 00:00:00 GMT'::timestamp + (ts::text)::interval)) AS month,
                            EXTRACT(year from ('1970-01-01 00:00:00 GMT'::timestamp + (ts::text)::interval)) AS year,
                            EXTRACT(weekday from ('1970-01-01 00:00:00 GMT'::timestamp + (ts::text)::interval)) AS weekday
                            FROM staging_events_table
                            WHERE page = 'NextSong';
                        """)

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, user_table_create, song_table_create, artist_table_create, time_table_create, songplay_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [ staging_songs_copy,staging_events_copy]
insert_table_queries = [user_table_insert,artist_table_insert,time_table_insert,song_table_insert,songplay_table_insert]
