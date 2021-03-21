from airflow.operators.postgres_operator import PostgresOperator
from airflow import DAG
from airflow.operators import DataQualityOperator
from airflow.operators.dummy_operator import DummyOperator



def dataQualityCheck(parent_dag_name, task_id, redshift_conn_id,*args,**kwargs):
    
    dag = DAG(f"{parent_dag_name}.{task_id}",**kwargs)
    
    run_quality_checks_user = DataQualityOperator(
        task_id = "Run_data_quality_checks_users",
        dag = dag,
        redshift_conn_id = redshift_conn_id,
        schema = "public",
        table = "users"
        )
    run_quality_checks_song = DataQualityOperator(
        task_id = "Run_data_quality_checks_songs",
        dag = dag,
        redshift_conn_id = redshift_conn_id,
        schema = "public",
        table = "songs"
        )
    run_quality_checks_artist = DataQualityOperator(
        task_id = "Run_data_quality_checks_artists",
        dag = dag,
        redshift_conn_id = redshift_conn_id,
        schema = "public",
        table = "artists"
        )
    run_quality_checks_time = DataQualityOperator(
        task_id = "Run_data_quality_checks_time",
        dag = dag,
        redshift_conn_id = redshift_conn_id,
        schema = "public",
        table = "time"
        )
    
    start_check = DummyOperator(task_id = 'Begin_check',  dag = dag)
    
    check_passed = DummyOperator(task_id = 'Check_successful',  dag = dag)
    
    
    start_check >> [run_quality_checks_user,run_quality_checks_song,run_quality_checks_artist,run_quality_checks_time] >> \
    check_passed
    
    return dag
    
