import configparser
from datetime import datetime
from pyspark.sql import functions as F
import os
import time
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql import types
from pyspark.sql.functions import expr
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format



config = configparser.ConfigParser()
config.read_file(open(os.path.join(os.getcwd(),'dl.cfg')))

os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    """
        Description: 
        
            This function crestes a spark session with a hadoop
            package/utility and returns the instance of the spark session.
        
        Returns:
            spark : spark session
    
    """
    
    
    print('initialising spark session')
        
    spark = SparkSession.builder.config('spark.jars.packages','org.apache.hadoop:hadoop-aws:2.7.0').getOrCreate() 
    time.sleep(5)
    
    return spark


def data_reader(input_data, spark):
    """
        Description:       
            This function reads datafiles from an aws S3 bucket using the
            spark read method.The files from S3 bucket are 
            the song_data file and the log_data file.
            
        Arguments:      
            input_data: the input root directory of the s3 bucket
               
        Returns:
            logdf : the spark dataframe from the log_data file
            songdf: the spark dataframe from the song_data file
        
    """
    
    print('reading log data from s3 bucket')
    log_data = os.path.join(input_bucket,'log_data/*/*/*.json')
    logdf = spark.read.json(log_data)
    print('reading log data completed')
    

              
   
    print('reading song data from s3 bucket')      
    song_data = os.path.join(input_bucket,'song_data/A/A/*/*.json')
    songdf = spark.read.json(song_data)
    print('reading song data completed')
    
    
    return logdf, songdf
    
    

def process_song_data(spark, songdf, output_bucket):
    """
        Description: 
        
            This function processes the song data read from s3 bucket,
            performs the neccessery transformation and writes the
            output table in parquet format to an s3 bucket.
              
        Arguments:
        
            spark: spark session
            songdf: the spark dataframe read from the song_data file
            output_data: the output root directory of the s3 bucket
           
        Returns:
            None

    """
    
    songdf.createOrReplaceTempView('SongView')
    
    # extract columns to create songs table
    dimSongs = spark.sql("""SELECT DISTINCT song_id, title, artist_id, year, duration
                            FROM SongView
                            """)
    
    # write songs table to parquet files partitioned by year and artist
    try:
        print("writing Song table to s3 bucket")
        dimSongs.write.mode("overwrite").partitionBy("year","artist_id").parquet(os.path.join(output_bucket,"dimSongs"))
        print("writing Song table completed")
    except:
        print('unable to write Song table')
        
    # extract columns to create artists table
    dimArtists = spark.sql("""SELECT DISTINCT artist_id, artist_name,
                                artist_location, artist_latitude, artist_longitude
                                FROM SongView
                                """)
    
    # write artists table to parquet files
    try:
        print("writing artist table to s3 bucket") 
        dimArtists.write.mode("overwrite").parquet(os.path.join(output_bucket,"dimArtist"))
        print("writing artist table completed")
    except:
        print('unable to write artist table')

def process_log_data(spark, output_bucket, logdf, songdf):
    
    """
       Description: 
            This function processes the log data read from s3 bucket,
            performs the neccessery transformation and writes the
            output table in parquet format to an s3 bucket.
            
        Arguments:
        
            spark: spark session
            songdf: the spark dataframe read from the song_data file
            logdf: the spark dataframe read from the log_data file
            output_data: the output root directory of the s3 bucket
           
        Returns:
            None     
    """
    
    # filter by actions for song plays
    df = logdf.where(logdf['page'] == 'NextSong')
    
    # creates a timestamp from the original epoch timestamp
    df = df.withColumn('timeStamp',expr("cast(cast(ts/1000 as int) as timestamp)"))
    
    # creates a temporary view for running sql queries
    df.createOrReplaceTempView('LogView')

    # extract columns for users table    
    dimUsers = spark.sql("""SELECT DISTINCT cast(userId as int), firstName,
                            lastName, gender, level
                            FROM LogView
                            """) 
    
    # write users table to parquet files
    try:
        print("writing Users table to s3 bucket") 
        dimUsers.write.mode("overwrite").parquet(os.path.join(output_bucket,"dimUsers"))
        print("writing Users table completed")
    except:
        print('unable to write Users table')
   
    
    # extract columns to create time table
    dimTime = spark.sql("""SELECT DISTINCT timeStamp AS start_time, hour(timeStamp) AS hour, 
                            day(timeStamp) AS day, month(timeStamp) AS month, year(timeStamp) AS year,
                            weekday(timeStamp) AS weekday, weekofyear(timeStamp) AS week FROM LogView
                            """)
    
    # write time table to parquet files partitioned by year and month
    try:
        print("writing Time table to s3 bucket")
        dimTime.write.mode("overwrite").partitionBy("year","month").parquet(os.path.join(output_bucket,"dimTIme"))
        print("writing Time table completed")
    except:
        print('unable to write Time table')

    # creates a temporary view for songplays table
    songdf.createOrReplaceTempView('SongView')

    # extract columns from joined song and log datasets to create songplays table 
    Songplay = spark.sql("""SELECT DISTINCT (timeStamp) AS start_time, cast(e.userId AS int), e.level,
                                s.song_id, s.artist_id, e.sessionId, e.location, e.userAgent
                                FROM LogView AS e
                                JOIN SongView AS s 
                                ON (e.artist = s.artist_name AND e.song = s.title AND e.length = s.duration)
                                """)

    # write songplays table to parquet files partitioned by year and month
    try:
        print("writing Songplay table to s3 bucket")
        Songplay.write.mode("overwrite").partitionBy("year","month").parquet(os.path.join(output_bucket,"Songplay"))
        print("writing Songplay table completed")
    except:
        print('unable to write Songplay table')

def main():
    
    """
    Description:
        The main method initialises some variables
        and exexutes the various defined methods
    
    """
    
    
    input_bucket = "s3a://udacity-dend/"
    output_bucket = "s3a://charles-sink/"
    spark = create_spark_session()
    
    logdf, songdf = data_reader(input_bucket, spark)
    
    process_song_data(spark, songdf, output_bucket)
    process_log_data(spark, output_bucket, logdf, songdf)
        
    


if __name__ == "__main__":
    main()
