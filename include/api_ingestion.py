import requests
import json
import os

def fetch_data(url):
    """
    Fetches JSON data from the given URL.
    """
    response = requests.get(url)
    response.raise_for_status()
    return response.json()

def save_data(data, path):
    """
    Saves the data to a local file.
    """
    # Ensure the directory exists
    os.makedirs(os.path.dirname(path), exist_ok=True)
    
    with open(path, 'w') as f:
        json.dump(data, f, indent=4)
    print(f"Data saved to {path}")


def upload_to_snowflake_stage(file_path, conn):
    """
    Uploads a local file to a Snowflake Internal Stage.
    """
    try:
        cursor = conn.cursor()
        put_query = f"PUT file://{file_path} @DE_LEARNING.RAW.MY_API_STAGE AUTO_COMPRESS=TRUE"
        cursor.execute(put_query)
        print(f"File {file_path} uploaded to @MY_API_STAGE")
    finally:
        cursor.close()

from airflow.providers.slack.hooks.slack import SlackHook

def slack_alert(context):
    """
    Sends a Slack alert on task failure.
    """
    ti = context.get('task_instance')
    dag_id = ti.dag_id
    task_id = ti.task_id
    execution_date = context.get('execution_date')
    
    message = f":red_circle: *Task Failed*\n*DAG*: {dag_id}\n*Task*: {task_id}\n*Execution Date*: {execution_date}"
    
    hook = SlackHook(slack_conn_id='slack_conn')
    # Using the client from SlackHook to post message
    hook.client.chat_postMessage(channel='#data-alerts', text=message)

def slack_success_alert(context):
    """
    Sends a Slack alert on DAG success.
    """
    ti = context.get('task_instance')
    dag_id = ti.dag_id
    execution_date = context.get('execution_date')
    
    message = f":white_check_mark: *Pipeline Succeeded*\n*DAG*: {dag_id}\n*Execution Date*: {execution_date}"
    
    hook = SlackHook(slack_conn_id='slack_conn')
    hook.client.chat_postMessage(channel='#data-alerts', text=message)

if __name__ == "__main__":
    url = 'https://jsonplaceholder.typicode.com/posts'
    data = fetch_data(url)
    save_data(data, 'include/temp_data/posts.json')
