# -*- coding: utf-8 -*-
"""
    This operator implements the data quality verification task, based on the BaseOperator
               
"""

from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.contrib.hooks.aws_hook import AwsHook

class DataQualityOperator(BaseOperator):

    """
        Define class DataQualityOperator instanced from BaseOperator
        
        atributes:
            - ui_color
        
        methods:
            - __init__
            - execute
    """    
    
    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 aws_credentials_id="",
                 sql_quality_checks=[],
                 *args, **kwargs):
        
        """
            The function implements the inicialization function
            Parameters:
                redshift_conn_id
                aws_credentials_id
            Returns:
                NA
            Note:
                None
        """          

        # 
        super(DataQualityOperator, self).__init__(*args, **kwargs)
        
        
        self.redshift_conn_id=redshift_conn_id
        self.aws_credentials_id=aws_credentials_id
        self.sql_quality_checks=sql_quality_checks

    def execute(self, context):
        
        """
            The function implements the execution for the DataQualityOperator
            Parameters:
                Contex (object)
            Returns:
                NA
            Note:
                None
        """           
        
        # Get the Redshift connection
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        for quality_check in self.sql_quality_checks:
            sql_quality_check = quality_check.get('sql_quality_check')
            expected_result = quality_check.get('expected_result')
            rows = redshift.get_records(sql_quality_check)[0]
 
            if rows[0] != expected_result:
               self.log.info("ERROR EXECUTION: ",sql_quality_check)
               raise ValueError('QUALITY CHECK FAILED! GAME OVER') 