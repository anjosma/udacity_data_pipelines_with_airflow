from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from operators import (StageToRedshiftOperator, LoadFactOperator, LoadDimensionOperator, DataQualityOperator)
from helpers import SqlQueries, load_yml_config
import logging

config_path = os.path.join("config", 'sparkify_etl.yml')
config_tables_path = os.path.join("config", 'sparkify_tables.yml')
config = load_yml_config(config_path)
config_tables = load_yml_config(config_tables_path)

logging.info("Loaded YML config file for S3, Redshift and Airflow.")

s3 = config.get("aws").get('s3')
redshift = config.get("aws").get('redshift')
airflow = config.get('airflow')

REGION = config.get("aws").get("region")
REDSHIFT_SCHEMA = redshift.get('schema')
REDSHIFT_DATABASE = redshift.get('database')
S3_BUCKET = s3.get('bucket')
S3_LOG_KEY = s3.get('log_data_key')
S3_SONG_KEY = s3.get('song_data_key')
AIRFLOW_REDSHIFT_CONNECTION = airflow.get('redshift_conn_id')
AIRFLOW_AWS_CREDENTIALS = airflow.get('aws_credentials_id')

# AWS_KEY = os.environ.get('AWS_KEY')
# AWS_SECRET = os.environ.get('AWS_SECRET')

default_args = {
    'owner': airflow.get("owner"),
    'start_date': datetime(2019, 1, 12),
    'catchup': airflow.get("catchup"),
    'retry_on_failure': airflow.get('retry_on_failure'),
    'retry_delay': timedelta(minutes=airflow.get('retry_delay_in_minutes'))
}

dag = DAG('etl_pipeline_sparkify',
        default_args=default_args,
        description=airflow.get('description'),
        schedule_interval=airflow.get('schedule_interval'),
        tags=airflow.get('tags')
        )

start_operator = DummyOperator(task_id='begin_execution',  dag=dag)

table = 'staging_events'
stage_events_to_redshift = StageToRedshiftOperator(
    task_id='stage_events',
    s3_bucket=S3_BUCKET,
    s3_key=S3_LOG_KEY,
    database=REDSHIFT_DATABASE,
    schema=REDSHIFT_SCHEMA,
    table=table,
    redshift_conn_id=AIRFLOW_REDSHIFT_CONNECTION,
    aws_credentials_id=AIRFLOW_AWS_CREDENTIALS,
    table_queries=config_tables.get(table),
    dag=dag
)


table = 'staging_songs'
stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='stage_songs',
    s3_bucket=S3_BUCKET,
    s3_key=S3_SONG_KEY,
    database=REDSHIFT_DATABASE,
    schema=REDSHIFT_SCHEMA,
    table=table,
    redshift_conn_id=AIRFLOW_REDSHIFT_CONNECTION,
    aws_credentials_id=AIRFLOW_AWS_CREDENTIALS,
    table_queries=config_tables.get(table),
    dag=dag
)

"""
load_songplays_table = LoadFactOperator(
    task_id='load_songplays_fact_table',
    dag=dag
)

load_user_dimension_table = LoadDimensionOperator(
    task_id='load_user_dim_table',
    dag=dag
)

load_song_dimension_table = LoadDimensionOperator(
    task_id='load_song_dim_table',
    dag=dag
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id='load_artist_dim_table',
    dag=dag
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='load_time_dim_table',
    dag=dag
)

run_quality_checks = DataQualityOperator(
    task_id='run_data_quality_checks',
    dag=dag
)

end_operator = DummyOperator(task_id='stop_execution',  dag=dag)
"""

start_operator >> [stage_events_to_redshift, stage_songs_to_redshift] #>> load_songplays_table
#load_songplays_table >> [load_song_dimension_table, load_artist_dimension_table, load_user_dimension_table, load_time_dimension_table] >> run_quality_checks
#run_quality_checks >> end_operator