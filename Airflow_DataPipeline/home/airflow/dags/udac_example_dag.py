from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (LoadDimensionOperator,DataQualityOperator,StageToRedshiftOperator,LoadFactOperator)

from airflow.operators.subdag_operator import SubDagOperator
from subdag import dataQualityCheck
import configparser
from helpers import SqlQueries


config =configparser.ConfigParser()
#os.environ['AWS_KEY']=config['ACCESS']['key']
#os.environ['AWS_SECRET']=config['ACCESS']['secret']
#AWS_KEY = os.environ.get('AWS_KEY')
#AWS_SECRET = os.environ.get('AWS_SECRET')



start_date =  datetime(2018, 11, 1)
default_args = {
    'owner': 'udacity',
    'depends_on_past': False,
    'retries':3,
    'retry_delay':timedelta(minutes = 0.1),
    'start_date': start_date,
    
}

#

dag = DAG('udac_example_dag',
          default_args = default_args,
          description = 'Load and transform data in Redshift with Airflow',
          schedule_interval = '@hourly',
          catchup = False
         )

start_operator = DummyOperator(task_id = 'Begin_execution',  dag = dag)
    
    
    
create_tables = PostgresOperator( 
    task_id = 'create_tables',
    postgres_conn_id = 'redshift',
    dag = dag ,
    sql = 'template/create_tables.sql'
)

stage_events_to_redshift = StageToRedshiftOperator(
    task_id = "Stage_events",
    dag = dag,
    redshift_conn_id = "redshift",
    aws_credentials = "aws_credentials",
    target_table = "staging_events",
    schema = "public",
    file_format = "json",
    s3_bucket = "udacity-dend",
    s3_key = "log_data/{{execution_date.year}}/{{execution_date.month}}",
    region = "us-west-2"

)
                 
stage_songs_to_redshift = StageToRedshiftOperator(
    task_id ='stage_songs',
    dag = dag,
    redshift_conn_id = "redshift",
    aws_credentials = "aws_credentials",
    target_table = "staging_songs",
    schema = "public",
    file_format = "json",
    s3_bucket = "udacity-dend",
    s3_key = "song_data",
    region = "us-west-2"
)


load_songplays_table = LoadFactOperator(
    task_id = 'Load_songplays_fact_table',
    dag = dag,
    redshift_conn_id = "redshift",
    sql_statement = SqlQueries.songplay_table_insert,
    schema = "public",
    table = "songplays",
    params = {'temp_table':'temp_songplays'}
)

load_user_dimension_table = LoadDimensionOperator(
    task_id = 'Load_user_dim_table',
    dag = dag,
    redshift_conn_id = "redshift",
    sql_statement = SqlQueries.user_table_insert,
    schema = "public",
    table = "users",
    params = {'temp_table':'temp_users'}
)

load_song_dimension_table = LoadDimensionOperator(
    task_id = 'Load_song_dim_table',
    dag = dag,
    redshift_conn_id = "redshift",
    sql_statement = SqlQueries.song_table_insert,
    schema = "public",
    table = "songs",
    params = {'temp_table':'temp_songs'}
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id = 'Load_artist_dim_table',
    dag = dag,
    redshift_conn_id = "redshift",
    sql_statement = SqlQueries.artist_table_insert,
    schema = "public",
    table = "artists",
    params = {'temp_table':'temp_artists'}
)



load_time_dimension_table = LoadDimensionOperator(
    task_id = 'Load_time_dim_table',
    dag = dag,
    redshift_conn_id = "redshift",
    sql_statement = SqlQueries.time_table_insert,
    schema = "public",
    table = "time",
    params = {'temp_table':'temp_time'}
)



Run_data_quality_check = SubDagOperator(
    subdag = dataQualityCheck(parent_dag_name = 'udac_example_dag', task_id = "data_quality_check",
                              redshift_conn_id = "redshift", start_date = start_date),
    task_id = "data_quality_check",
    dag = dag
)


end_operator = DummyOperator(task_id = 'Stop_execution',  dag = dag)

start_operator >> create_tables >>  [stage_events_to_redshift,stage_songs_to_redshift] >> load_songplays_table
load_songplays_table >>[load_song_dimension_table,load_time_dimension_table, \
                        load_user_dimension_table,load_artist_dimension_table] >> Run_data_quality_check >> end_operator 