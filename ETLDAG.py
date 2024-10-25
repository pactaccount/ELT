from airflow import DAG
from airflow.models import Variable
from airflow.decorators import task
from airflow.operators.python import get_current_context
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from datetime import datetime, timedelta
import snowflake.connector
import requests

# Default args
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 10, 23),
    'retries': 1
}

# DAG definition
with DAG(
    dag_id ='ETLDAG',
    default_args=default_args,
    schedule_interval=None,  # You can set this based on your needs
    catchup=False
) as dag:

    # Task 1: Create user_session_channel table
    create_user_session_channel = SnowflakeOperator(
        task_id='create_user_session_channel',
        sql="""
        CREATE TABLE IF NOT EXISTS dev.raw_data.user_session_channel (
            userId int not NULL,
            sessionId varchar(32) primary key,
            channel varchar(32) default 'direct'  
        );
        """,
        snowflake_conn_id='snowflake_conn'
    )

    # Task 2: Create session_timestamp table
    create_session_timestamp = SnowflakeOperator(
        task_id='create_session_timestamp',
        sql="""
        CREATE TABLE IF NOT EXISTS dev.raw_data.session_timestamp (
            sessionId varchar(32) primary key,
            ts timestamp  
        );
        """,
        snowflake_conn_id='snowflake_conn'
    )

    # Task 3: Create blob_stage
    create_blob_stage = SnowflakeOperator(
        task_id='create_blob_stage',
        sql="""
        CREATE OR REPLACE STAGE dev.raw_data.blob_stage
        url = 's3://s3-geospatial/readonly/'
        file_format = (type = csv, skip_header = 1, field_optionally_enclosed_by = '"');
        """,
        snowflake_conn_id='snowflake_conn'
    )

    # Task 4: Copy data into user_session_channel
    copy_user_session_channel = SnowflakeOperator(
        task_id='copy_user_session_channel',
        sql="""
        COPY INTO dev.raw_data.user_session_channel
        FROM @dev.raw_data.blob_stage/user_session_channel.csv;
        """,
        snowflake_conn_id='snowflake_conn'
    )

    # Task 5: Copy data into session_timestamp
    copy_session_timestamp = SnowflakeOperator(
        task_id='copy_session_timestamp',
        sql="""
        COPY INTO dev.raw_data.session_timestamp
        FROM @dev.raw_data.blob_stage/session_timestamp.csv;
        """,
        snowflake_conn_id='snowflake_conn'
    )

    # DAG dependencies
    create_user_session_channel >> create_blob_stage >> copy_user_session_channel
    create_session_timestamp >> create_blob_stage >> copy_session_timestamp
