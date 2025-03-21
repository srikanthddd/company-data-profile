from airflow import DAG
from airflow.providers.google.cloud.operators.dataproc import DataprocCreateBatchOperator
from airflow.providers.google.cloud.hooks.cloud_sql import CloudSQLDatabaseHook
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import logging

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 3, 14),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'dag2_dataproc_dag',
    default_args=default_args,
    schedule_interval='@once',
    catchup=False
)

# Fetch one pending file
def fetch_pending_file(**kwargs):
    hook = CloudSQLDatabaseHook(
        gcp_conn_id='google_cloud_default',
        database='pubsub_metadata',
        instance='pubsubdata'
    )

    sql = "SELECT filePath FROM pubsub_metadata WHERE status = 'PENDING' LIMIT 1"
    result = hook.get_records(sql)

    if result:
        file_path = result[0][0]
        logging.info(f"Fetched file: {file_path}")

        # Store the file path in XCom
        kwargs['ti'].xcom_push(key='file_path', value=file_path)
    else:
        logging.info("No pending files found.")
        kwargs['ti'].xcom_push(key='file_path', value='')

fetch_file = PythonOperator(
    task_id='fetch_pending_file',
    python_callable=fetch_pending_file,
    provide_context=True,
    dag=dag
)

# Create Dataproc Batch Job with the file path as an argument
create_batch = DataprocCreateBatchOperator(
    task_id='create_batch',
    project_id='publishingengine',
    region='us-central1',
    batch={
        "pyspark_batch": {
            "main_python_file_uri": "gs://dataproc-bucket/dataproc-jobs/scala-indexer.py",
            "args": ["{{ ti.xcom_pull(task_ids='fetch_pending_file', key='file_path') }}"]
        },
        "environment_config": {
            "peripherals_config": {
                "spark_history_server_config": {
                    "dataproc_cluster": "projects/publishingengine/regions/us-central1/clusters/on-demand-cluster-{{ ds_nodash }}"
                }
            }
        }
    },
    batch_id="batch-job-{{ ds_nodash }}",
    dag=dag
)

# Update the status of the processed file
def update_status_in_sql(**kwargs):
    file_path = kwargs['ti'].xcom_pull(task_ids='fetch_pending_file', key='file_path')

    if file_path:
        hook = CloudSQLDatabaseHook(
            gcp_conn_id='google_cloud_default',
            database='pubsub_metadata',
            instance='pubsubdata'
        )

        sql = f"""
        UPDATE pubsub_metadata
        SET status = 'PROCESSED'
        WHERE filePath = '{file_path}'
        """

        hook.run(sql)
        logging.info(f"Status updated for file: {file_path}")
    else:
        logging.info("No file to update.")

update_status_task = PythonOperator(
    task_id='update_status_in_sql',
    python_callable=update_status_in_sql,
    provide_context=True,
    dag=dag
)

# DAG Flow
fetch_file >> create_batch >> update_status_task
