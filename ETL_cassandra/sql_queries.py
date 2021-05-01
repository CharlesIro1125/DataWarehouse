# DROP QUERIES

drop_query_1 ="DROP TABLE IF EXISTS artist_and_song_by_sessionid"

drop_query_2 ="DROP TABLE IF EXISTS artist_info_by_userid"


drop_query_3 ="DROP TABLE IF EXISTS user_by_song_heard"




# CREATE QUERIES

create_query_1 =  "CREATE TABLE IF NOT EXISTS artist_and_song_by_sessionid (session_id int,item_insession int,\
                artist text,song_title text,song_length double, PRIMARY KEY (session_id,item_insession)) "
    
    
create_query_2 =   "CREATE TABLE IF NOT EXISTS artist_info_by_userid (user_id int,session_id int,item_insession int,\
                        artist text,song_title text,user_first_name text,user_last_name text,\
                        PRIMARY KEY (user_id,session_id,item_insession)) "     
    
    
create_query_3 =   "CREATE TABLE IF NOT EXISTS user_by_song_heard (song_title text,user_id int,\
                        user_first_name text,user_last_name text,PRIMARY KEY (song_title,user_id)) "
    
    
    
    
    
    
    
#INSERT QUERIES 

insert_query_1 = "INSERT INTO artist_and_song_by_sessionid (session_id,item_insession,\
                        artist,song_title,song_length) VALUES (%s,%s,%s,%s,%s)"


insert_query_2 = "INSERT INTO artist_info_by_userid (user_id,session_id,item_insession,\
                        artist,song_title,user_first_name,user_last_name) VALUES (%s,%s,%s,%s,%s,%s,%s)"


insert_query_3 = "INSERT INTO user_by_song_heard (song_title,user_id,user_first_name,\
                        user_last_name) VALUES (%s,%s,%s,%s)"



create_table_queries = [create_query_1,create_query_2,create_query_3]

drop_table_queries =   [drop_query_1,drop_query_2,drop_query_3]