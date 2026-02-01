from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from include.api_ingestion import fetch_data, save_data, upload_to_snowflake_stage, slack_alert, slack_success_alert, validate_api_response
import os
import json

def ingest_data():
    url = 'https://jsonplaceholder.typicode.com/posts'
    data = fetch_data(url)
    save_data(data, 'include/temp_data/posts.json')

def validate_data():
    with open('include/temp_data/posts.json', 'r') as f:
        data = json.load(f)
    validate_api_response(data)

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
    on_failure_callback=slack_alert,
    on_success_callback=slack_success_alert,
) as dag:

    ingest_task = PythonOperator(
        task_id='ingest_market_data',
        python_callable=ingest_data
    )

    validate_task = PythonOperator(
        task_id='validate_data',
        python_callable=validate_data
    )

    upload_task = PythonOperator(
        task_id='upload_to_snowflake',
        python_callable=upload_to_stage
    )

    copy_task = SQLExecuteQueryOperator(
        task_id='copy_to_table',
        conn_id='snowflake_default',
        sql=[
            "DELETE FROM DE_LEARNING.RAW.RAW_POSTS WHERE TO_DATE(ingested_at) = '{{ ds }}'",
            "COPY INTO DE_LEARNING.RAW.RAW_POSTS (json_data, ingested_at) FROM (SELECT $1, TO_TIMESTAMP_NTZ('{{ ts }}') FROM @DE_LEARNING.RAW.MY_API_STAGE) FILE_FORMAT = (TYPE = 'JSON')"
        ]
    )

    dbt_run = BashOperator(
        task_id='dbt_run',
        bash_command='dbt run',
        cwd='/usr/local/airflow/dbt'
    )

    dbt_test = BashOperator(
        task_id='dbt_test',
        bash_command='dbt test',
        cwd='/usr/local/airflow/dbt'
    )

    ingest_task >> validate_task >> upload_task >> copy_task >> dbt_run >> dbt_test
