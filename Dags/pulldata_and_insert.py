from airflow import DAG
from airflow.providers.google.cloud.sensors.pubsub import PubSubPullSensor
from airflow.operators.python_operator import PythonOperator
from airflow.providers.google.cloud.hooks.cloud_sql import CloudSQLDatabaseHook
import json
import logging
from datetime import datetime

# Define default_args
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 3, 14),
    'retries': 1,
}

# Define DAG
dag = DAG(
    'pubsub_to_sql_dag',
    default_args=default_args,
    schedule_interval='@once',  # Run on demand
    catchup=False
)

# Task 1: Pull messages from Pub/Sub
pull_messages = PubSubPullSensor(
    task_id='pull_messages',
    project_id='publishingengine',
    subscription='company-file-upload-topic-sub',
    max_messages=10,
    ack_messages=True,
    dag=dag
)

# Task 2: Decode Pub/Sub messages and extract data
def decode_and_log_message(ti):
    messages = ti.xcom_pull(task_ids='pull_messages')
    if not messages:
        logging.info('No messages received from Pub/Sub.')
        return []

    processed = []
    for msg in messages:
        try:
            decoded_data = json.loads(msg['message']['data'])
            logging.info(f'Decoded Message: {json.dumps(decoded_data, indent=2)}')

            # Extract relevant fields
            processed.append((
                decoded_data.get('bucketName', ''),
                decoded_data.get('fileSize', ''),
                decoded_data.get('project_id', ''),
                decoded_data.get('filePath', ''),
                decoded_data.get('contentType', '')
            ))
        except Exception as e:
            logging.error(f'Error decoding message: {e}')

    return processed

decode_and_log_message_task = PythonOperator(
    task_id='decode_and_log_message',
    python_callable=decode_and_log_message,
    provide_context=True,
    dag=dag
)

# Task 3: Insert extracted data into Cloud SQL
def insert_into_sql(ti):
    data = ti.xcom_pull(task_ids='decode_and_log_message')
    if not data:
        logging.info('No data to insert into Cloud SQL.')
        return

    hook = CloudSQLDatabaseHook(
        gcp_conn_id='google_cloud_default',
        database='pubsub_metadata',
        instance='pubsubdata'
    )

    sql = '''
    INSERT INTO pubsub_metadata (bucketName, fileSize, project_id, filePath, contentType, status)
    VALUES (%s, %s, %s, %s, %s, %s)
    '''

    hook.run(sql, parameters=data)
    logging.info('Data inserted into Cloud SQL successfully.')

insert_into_sql_task = PythonOperator(
    task_id='insert_into_sql',
    python_callable=insert_into_sql,
    provide_context=True,
    dag=dag
)

# Define task dependencies
pull_messages >> decode_and_log_message_task >> insert_into_sql_task
