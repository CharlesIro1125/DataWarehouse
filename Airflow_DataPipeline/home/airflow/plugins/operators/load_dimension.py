from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):
    
    """
        Description: 
        
            This custom function loads data into the 
            dimension tables using a truncate insert method.
              
        Arguments:
        
            redshift_conn_id : the connection id to the cluster .
            schema : the table schema.
            table : the table name.
            sql_statement : an sql selct statement
            operation : either an append or truncate_insert,
                        default is set to truncate_insert.
            params : a temporary table created in the database, 
                     use for append operation.
        Returns:
            None
    """

    
    
    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id = "",
                 sql_statement = "",
                 schema = "",
                 operation = "truncate_insert",
                 table = "",
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.sql_statement = sql_statement
        self.schema = schema
        self.table = table
        self.operation = operation
        self.temp_table = kwargs['params']['temp_table']
  

    def execute(self, context):
        self.log.info('Implementing LoadDimensionOperator')
        redshift = PostgresHook(postgres_conn_id = self.redshift_conn_id)
        
        if self.operation == "truncate_insert":
       
            sql = f"""
                BEGIN;
                TRUNCATE {self.schema}.{self.table};
                INSERT INTO {self.schema}.{self.table} {self.sql_statement};
                COMMIT;
               """
            redshift.run(sql)
        
        if self.operation == "append":
            
            sql_temp_table = f"""
                BEGIN;
                TRUNCATE {self.schema}.{self.temp_table};
                INSERT INTO {self.schema}.{self.temp_table} {self.sql_statement};
                COMMIT;
               """
            redshift.run(sql_temp_table)
        
            sql_append = f"""
                ALTER TABLE {self.schema}.{self.table} APPEND
                FROM {self.schema}.{self.temp_table}
                IGNOREEXTRA
                """
            redshift.run(sql_append, autocommit = True)
                
        
