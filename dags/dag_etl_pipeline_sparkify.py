from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.subdag_operator import SubDagOperator

from operators import (StageToRedshiftOperator, LoadFactOperator, DataQualityOperator)
from subdag_dimension import load_dimension_subdag
from subdag_data_quality import check_tables_subdag
from helpers import load_yml_config
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
AIFLOW_DAG_ID = 'etl_pipeline_sparkify'


default_args = {
    'owner': 'Matheus',
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2018, 11, 1),
    'email_on_retry': False,
    'catchup': False,
}

dag = DAG(AIFLOW_DAG_ID,
        default_args=default_args,
        description="Load and transform data in Redshift with Airflow",
        schedule_interval=None,
        tags=['udacity', 'sparkify']
        )

start_operator = DummyOperator(task_id='begin_execution',  dag=dag)

table = 'staging_events'
stage_events_to_redshift = StageToRedshiftOperator(
    task_id='stage_events' ,
    s3_bucket=S3_BUCKET,
    s3_key=S3_LOG_KEY,
    schema=REDSHIFT_SCHEMA,
    table=table,
    redshift_conn_id=AIRFLOW_REDSHIFT_CONNECTION,
    aws_credentials_id=AIRFLOW_AWS_CREDENTIALS,
    region=REGION,
    table_queries=config_tables.get(table),
    dag=dag
)


table = 'staging_songs'
stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='stage_songs',
    s3_bucket=S3_BUCKET,
    s3_key=S3_SONG_KEY,
    schema=REDSHIFT_SCHEMA,
    table=table,
    redshift_conn_id=AIRFLOW_REDSHIFT_CONNECTION,
    aws_credentials_id=AIRFLOW_AWS_CREDENTIALS,
    region=REGION,
    table_queries=config_tables.get(table),
    dag=dag
)

table = 'songplays'
load_songplays_table = LoadFactOperator(
    task_id='load_songplays_fact_table',
    schema=REDSHIFT_SCHEMA,
    table=table,
    redshift_conn_id=AIRFLOW_REDSHIFT_CONNECTION,
    insert_query=config_tables.get(table).get('insert'),
    create_query=config_tables.get(table).get('create'),
    dag=dag
)

dim_tables = ['users', 'songs', 'time', 'artists']
load_dimension_tables = SubDagOperator(
    task_id='load_dimension_tables',
    subdag=load_dimension_subdag(
        parent_dag=AIFLOW_DAG_ID, 
        child_dag='load_dimension_tables',
        redshift_conn_id=AIRFLOW_REDSHIFT_CONNECTION,
        schema=REDSHIFT_SCHEMA,
        dimension_tables=dim_tables,
        config_tables=config_tables,
        default_args=default_args
        ),
    default_args=default_args,
    dag=dag
)


check_tables = ['users', 'songs', 'time', 'artists', 'songplays']
run_quality_checks = SubDagOperator(
    task_id='run_data_quality_checks',
    subdag=check_tables_subdag(
        parent_dag=AIFLOW_DAG_ID, 
        child_dag='run_data_quality_checks',
        redshift_conn_id=AIRFLOW_REDSHIFT_CONNECTION,
        schema=REDSHIFT_SCHEMA,
        check_tables=check_tables,
        default_args=default_args
    ),
    default_args=default_args,
    dag=dag
)

end_operator = DummyOperator(task_id='stop_execution', dag=dag)

start_operator >> [stage_events_to_redshift, stage_songs_to_redshift] >> load_songplays_table
load_songplays_table >> load_dimension_tables >> run_quality_checks >> end_operator