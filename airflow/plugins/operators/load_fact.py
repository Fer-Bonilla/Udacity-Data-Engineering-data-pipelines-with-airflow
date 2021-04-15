"""
    This operator implements the LoadFactOperator class that execute the data load process from staging tables to fact table
               
"""

from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class LoadFactOperator(BaseOperator):
    
    """
        Define class LoadFactOperator instanced from BaseOperator
        
        atributes:
            - ui_color: Color value to plot the task operator
        
        methods:
            - __init__: Constructor method
            - execute: Task execution function
    """     
    
    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 aws_credentials_id="",
                 sql="",
                 table="",
                 *args, **kwargs):

        """
            The function implements the inicialization function
            Parameters:
                redshift_conn_id
                aws_credentials_id
                sql
                table
            Returns:
                NA
            Note:
                None
        """          
        
        
        super(LoadFactOperator, self).__init__(*args, **kwargs)
        
        self.redshift_conn_id=redshift_conn_id
        self.aws_credentials_id=aws_credentials_id
        self.sql=sql
        self.table=table

    def execute(self, context):
        
        """
            The function implements the execution for the LoadFactOperator
            Parameters:
                Contex (object)
            Returns:
                NA
            Note:
                Call the sql statements
        """                
        
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)      
        sql_statement = 'INSERT INTO %s %s' % (self.table, self.sql)
        redshift.run(sql_statement)