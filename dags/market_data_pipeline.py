from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from include.api_ingestion import fetch_data, save_data, upload_to_snowflake_stage
import os

def ingest_data():
    url = 'https://jsonplaceholder.typicode.com/posts'
    data = fetch_data(url)
    save_data(data, 'include/temp_data/posts.json')

def upload_to_stage():
    conn_id = 'snowflake_default'
    hook = SnowflakeHook(snowflake_conn_id=conn_id)
    conn = hook.get_conn()
    upload_to_snowflake_stage('include/temp_data/posts.json', conn)

with DAG(
    'market_data_pipeline',
    start_date=datetime(2024, 1, 1),
    schedule='@daily',
    catchup=False,
    description='A simple DAG to ingest market data from an API and load to Snowflake',
    tags=['ingestion', 'api', 'snowflake'],
) as dag:

    ingest_task = PythonOperator(
        task_id='ingest_market_data',
        python_callable=ingest_data
    )

    upload_task = PythonOperator(
        task_id='upload_to_snowflake',
        python_callable=upload_to_stage
    )

    copy_task = SQLExecuteQueryOperator(
        task_id='copy_to_table',
        conn_id='snowflake_default',
        sql="COPY INTO DE_LEARNING.RAW.RAW_POSTS (json_data) FROM (SELECT $1 FROM @DE_LEARNING.RAW.MY_API_STAGE) FILE_FORMAT = (TYPE = 'JSON')"
    )

    ingest_task >> upload_task >> copy_task
