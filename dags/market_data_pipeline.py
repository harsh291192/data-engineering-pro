from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from include.api_ingestion import fetch_data, save_data
import os

def ingest_data():
    url = 'https://jsonplaceholder.typicode.com/posts'
    data = fetch_data(url)
    # Using an absolute path or a path relative to the AIRFLOW_HOME usually works better, 
    # but based on the prompt, 'include/temp_data/' is requested.
    # In Astro/Airflow, 'include' is typically at the root.
    save_data(data, 'include/temp_data/posts.json')

with DAG(
    'market_data_pipeline',
    start_date=datetime(2024, 1, 1),
    schedule_interval='@daily',
    catchup=False,
    description='A simple DAG to ingest market data from an API',
    tags=['ingestion', 'api'],
) as dag:

    ingest_task = PythonOperator(
        task_id='ingest_market_data',
        python_callable=ingest_data
    )

    ingest_task
