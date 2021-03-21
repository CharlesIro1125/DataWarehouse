from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):
    
    """
        Description: 
        
            This custom function performs data quality check on the 
            dimension tables to verify if the table is empty or 
            wasn't returned.
              
        Arguments:
        
            redshift_conn_id : the connection id to the cluster .
            schema : the table schema.
            table : the table name
                       
        Returns:
            logs info of the check result. 
    """
    
    

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id = "",
                 schema = "",
                 table = "",
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.schema = schema
        self.table = table
       
    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id = self.redshift_conn_id)
        records = redshift.get_records(f"SELECT COUNT(*) FROM {self.schema}.{self.table}")
        
        
        if len(records) < 1 or len(records[0]) < 1:
            raise ValueError(f"Data quality checked failed.{self.schema}.{self.table} returned no results")
            
        if records[0][0] < 1:
            raise ValueError(f"Data quality checked failed.{self.schema}.{self.table} contained 0 records")
        
        self.log.info(f" Data quality check on table {self.schema}.{self.table} passed with {records[0][0]} records")
            