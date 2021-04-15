# -*- coding: utf-8 -*-
"""
    This is the DAG configuration for the airflow pipeline where the functions are defined.
    All the SQL statements are defined in the create_tables.sql file.
    
    FUNCTIONS:
    
        Execute tables creation:
            - create_tables: Defines the SQL statement for create the staging, dimension and fact tables.

        Load the staging tables:
            - stage_events_to_redshift: Load the data into the staging events table from S3 bucket
            - stage_songs_to_redshift: : Load the data into the songs table from S3 bucket

        Load fact table:
            - load_songplays_table: Load data into songplays table from staging tables

        Load dimension tables:
            - load_user_dimension_table: Load data into user table from staging tables
            - load_song_dimension_table: Load data into song table from staging tables
            - load_artist_dimension_table: Load data into artist table from staging tables
            - load_time_dimension_table: Load data into user time from staging tables

        Data load verification:
            - run_quality_checks: Load data into user table from staging tables

    
    TABLES STRUCTURE:
    
        Staging tables:
            - staging_events - Load the raw data from log events json files artist
            auth, firstName, gender, itemInSession,    lastName, length, level, location, method, page, registration, sessionId, song, status, ts, userAgent, userId
            - staging_songs - num_songs artist_id artist_latitude artist_longitude artist_location artist_name song_id title duration year    


        Dimension tables:
            - users - users in the app: user_id, first_name, last_name, gender, level
            - songs - songs in music database: song_id, title, artist_id, year, duration
            - artists - artists in music database: artist_id, name, location, latitude, longitude
            - time - timestamps of records in songplays: start_time, hour, day, week, month, year, weekday

        Fact Table:
            - songplays - records in log data associated with song plays.
        
        
"""

import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (StageToRedshiftOperator, 
                               LoadFactOperator,
                               LoadDimensionOperator, 
                               DataQualityOperator,
                               PostgresOperator)
from datetime import datetime, timedelta
from helpers import SqlQueries


# Load default arguments to control the airflow task execution
default_args = {
    'owner': 'udacity',
    'start_date': datetime(2021, 1, 12),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'email_on_retry': False,
    'catchup': False,
}


# Create the dag object 
dag = DAG('Sparkify airflow pipeline',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='0 * * * *'
        )


# Define the initial execution point
start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)


# Execute the tables creation
create_tables = PostgresOperator(
    task_id="create_tables",
    dag=dag,
    postgres_conn_id="redshift",
    sql="create_tables.sql"
)

# Execute the data load from the s3 bucket to the events staging table
stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    dag=dag,
    table="staging_events",
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    s3_bucket="udacity-dend",
    s3_key="log_data",
    json="s3://udacity-dend/log_json_path.json"
)

# Execute the data load from the s3 bucket to the events staging table
stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    dag=dag,
    table="staging_songs",
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    s3_bucket="udacity-dend",
    s3_key="song_data",
    json="auto"
)


# Execute the data load from the s3 bucket to the songs staging table
load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=dag,
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    sql=SqlQueries.songplay_table_insert,
    table='songplays'
)

# Execute the data load from the events staging table to the user table
load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    dag=dag,
        
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    sql=SqlQueries.user_table_insert,
    table='users',
    insert_mode=False
)

# Execute the data load from the songs staging table to the song table
load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    dag=dag,        
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    sql=SqlQueries.song_table_insert,
    table='songs',
    insert_mode=False
)

# Execute the data load from the events staging table to the artist table
load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    sql=SqlQueries.song_table_insert,
    table='artists',
    insert_mode=False
)

# Execute the data load from the events staging table to the time table
load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    sql=SqlQueries.time_table_insert,
    table='time',
    insert_mode=False
)

# Execute querys to verify the data loaded in the dimensions tables
run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    sql_quality_checks=[
               {'sql_quality_check': "SELECT COUNT(*) FROM users WHERE userid IS NULL", 'expected_result': 0},
               {'sql_quality_check': "SELECT COUNT(*) FROM songs WHERE songid IS NULL", 'expected_result': 0}       
               {'sql_quality_check': "SELECT COUNT(*) FROM artists WHERE userid IS NUL", 'expected_result': 0},
               {'sql_quality_check': "SELECT COUNT(*) FROM time WHERE songid IS NULL", 'expected_result': 0}
    ]
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

start_operator >> create_tables
create_tables >> stage_events_to_redshift
create_tables >> stage_songs_to_redshift
stage_events_to_redshift >> load_songplays_table
stage_songs_to_redshift >> load_songplays_table 
load_songplays_table >> load_song_dimension_table
load_songplays_table >> load_user_dimension_table
load_songplays_table >> load_artist_dimension_table
load_songplays_table >> load_time_dimension_table
load_song_dimension_table >> run_quality_checks
load_user_dimension_table >> run_quality_checks
load_artist_dimension_table >> run_quality_checks
load_time_dimension_table >> run_quality_checks
run_quality_checks >> end_operator