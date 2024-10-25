from airflow import DAG
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from datetime import datetime

# Default args for DAG
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 10, 23),
    'retries': 1
}

# Define DAG
with DAG(
    dag_id= 'elt_dag',
    default_args=default_args,
    schedule_interval=None,  # You can set this based on your requirements
    catchup=False
) as dag:

    # Task 1: Create session_summary table under the analytics schema
    create_session_summary_table = SnowflakeOperator(
        task_id='create_session_summary_table',
        sql="""
        CREATE TABLE IF NOT EXISTS dev.analytics.session_summary (
            sessionId varchar(32) primary key,
            userId int,
            channel varchar(32),
            ts timestamp
        );
        """,
        snowflake_conn_id='snowflake_conn'
    )

    # Task 2: Insert joined data while checking for duplicates
    insert_joined_data = SnowflakeOperator(
        task_id='insert_joined_data',
        sql="""
        INSERT INTO dev.analytics.session_summary (sessionId, userId, channel, ts)
        SELECT
            usc.sessionId,
            usc.userId,
            usc.channel,
            st.ts
        FROM dev.raw_data.user_session_channel usc
        JOIN dev.raw_data.session_timestamp st
        ON usc.sessionId = st.sessionId
        WHERE usc.sessionId NOT IN (SELECT sessionId FROM analytics.session_summary);
        """,
        snowflake_conn_id='snowflake_conn'
    )

    # Set task dependencies
    create_session_summary_table >> insert_joined_data
