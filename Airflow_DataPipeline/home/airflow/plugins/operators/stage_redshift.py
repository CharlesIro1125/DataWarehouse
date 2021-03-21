from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.utils.decorators import apply_defaults
import os


class StageToRedshiftOperator(BaseOperator):
    
    
    """
        Description: 
        
            This custom function extracts data from the 
            s3 storage to the database for analytical operation.
              
        Arguments:
        
            redshift_conn_id : the connection id to the cluster.
            schema : the table schema.
            sql_statement : an sql selct statement.
            aws_credentials : the aws credentials.
            target_table : the staging table in the database.
            file_format : the file formate, either json or csv.
            s3_bucket : the s3 bucket name.
            s3_key : the s3 key prefix.
            delimeter : the csv delimiter, for csv file.
            ignore_header : either to ignore the header or not.
            region : the region of the database.
                  
        Returns:
            None
    """
        
        
    ui_color = '#89DA59'
    
    template_fields = ("s3_key",)
    

    @apply_defaults
    def __init__(self,
                 redshift_conn_id = "",
                 aws_credentials = "",
                 target_table = "",
                 schema = "",
                 file_format = "",
                 s3_bucket = "",
                 s3_key = "",
                 delimeter = ",",
                 ignore_header = 1,
                 region = "",*args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials = aws_credentials
        self.target_table = target_table
        self.schema = schema
        self.file_format = file_format
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.delimeter = delimeter
        self.ignore_header = ignore_header
        self.region = region
        
        

    def execute(self, context):
        self.log.info('implementing StageToRedshiftOperator')
        aws_hook = AwsHook(self.aws_credentials)
        credentials = aws_hook.get_credentials()
        redshift = PostgresHook(postgres_conn_id = self.redshift_conn_id)
        self.log.info(f"Clearing data from {self.target_table}")
        redshift.run(f"TRUNCATE {self.schema}.{self.target_table}")
        rendered_key = self.s3_key
        s3_path = f"s3://{self.s3_bucket}/{rendered_key}/"
        
        if self.file_format == "json":
            copy_statement = f""" 
                COPY {self.schema}.{self.target_table} FROM '{s3_path}'
                ACCESS_KEY_ID '{credentials.access_key}' SECRET_ACCESS_KEY '{credentials.secret_key}'
                json 'auto ignorecase'  region '{self.region}'     
                """
        elif self.file_format == "csv":
            copy_statement = f""" 
                COPY {self.schema}.{self.target_table} FROM '{s3_path}'
                ACCESS_KEY_ID '{credentials.access_key}' SECRET_ACCESS_KEY '{credentials.secret_key}'
                IGNOREHEADER {self.ignore_header} DELIMITER '{self.delimeter}' region '{self.region}'
                """
        else:
            return self.log.info("StageToRedshiftOperator allows only csv or Json file format")
        
        
        redshift.run(copy_statement)
        


        





