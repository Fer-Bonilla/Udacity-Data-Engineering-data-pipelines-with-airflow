# -*- coding: utf-8 -*-
"""
    This operator implements the data quality verification task, based on the BaseOperator
               
"""

from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class StageToRedshiftOperator(BaseOperator):
    

    """
        Define class StageToRedshiftOperator instanced from BaseOperator
        
        atributes:
            - ui_color
        
        methods:
            - __init__
            - execute
    """       
    
    ui_color = '#358140'
    
    copy_sql = """
        COPY {}
        FROM '{}'
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}'
        REGION AS '{}'
        FORMAT AS json '{}';
    """
    
    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 aws_credentials_id="",
                 table="",
                 s3_bucket="",
                 s3_key="",
                 json="",
                 region="",                 
                 *args, **kwargs):

        """
            The function implements the inicialization function
            Parameters:
                redshift_conn_id
                aws_credentials_id
                table
                s3_bucket
                s3_key=
                json=
            Returns:
                NA
            Note:
                None
        """         
        
        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        
        self.redshift_conn_id=redshift_conn_id
        self.aws_credentials_id=aws_credentials_id
        self.table=table
        self.s3_bucket=s3_bucket
        self.s3_key=s3_key
        self.json=json
        self.region = region

    def execute(self, context):
        
        """
            The function implements the execution for the StageToRedshiftOperator
            Parameters:
                Contex (object)
            Returns:
                NA
            Note:
                Call the sql statements
        """   
        
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        redshift.run("DELETE FROM {}".format(self.table))
        s3_path = "s3://{}/{}".format(self.s3_bucket, self.s3_key)
        
        formatted_sql = StageToRedshiftOperator.copy_sql.format(
            self.table,
            s3_path,
            credentials.access_key,
            credentials.secret_key,
            self.region,
            self.json
        )

        redshift.run(formatted_sql)