"""
    This operator implements the LoadDimensionOperator class that execute the data load process from staging tables to dimension tables
               
"""

from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.contrib.hooks.aws_hook import AwsHook

class LoadDimensionOperator(BaseOperator):

    """
        Define class LoadDimensionOperator instanced from BaseOperator
        
        atributes:
            - ui_color
        
        methods:
            - __init__: Constructor method
            - execute: Task execution function
    """    
    
    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 aws_credentials_id="",
                 sql="",
                 table="",
                 insert_mode=None,
                 *args, **kwargs):

        """
            The function implements the inicialization function
            Parameters:
                redshift_conn_id
                aws_credentials_id
                sql
                table
                append_data
            Returns:
                NA
            Note:
                None
        """             
        
        super(LoadDimensionOperator, self).__init__(*args, **kwargs)

        self.redshift_conn_id=redshift_conn_id
        self.aws_credentials_id=aws_credentials_id
        self.sql=sql
        self.table=table
        self.insert_mode=insert_mode

    def execute(self, context):

        """
            The function implements the execution for the LoadDimensionOperator
            Parameters:
                Contex (object)
            Returns:
                NA
            Note:
                Call the sql statements
        """    
        
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        
        ## Check the insert_mode parameter valur to define the insert mode (Append or not append to table)
        if self.insert_mode == True:
            sql_statement = 'INSERT INTO %s %s' % (self.table, self.sql)
            redshift.run(sql_statement)
        else:
            sql_statement = 'DELETE FROM %s' % self.table
            redshift.run(sql_statement)
            sql_statement = 'INSERT INTO %s %s' % (self.table, self.sql)
            redshift.run(sql_statement)