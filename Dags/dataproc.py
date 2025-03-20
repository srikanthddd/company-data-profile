from airflow import DAG
from airflow.providers.google.cloud.operators.dataproc import (
    DataprocCreateBatchOperator,
    DataprocDeleteClusterOperator,
    DataprocSubmitJobOperator
)
from airflow.providers.google.cloud.hooks.cloud_sql import CloudSQLDatabaseHook
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import logging

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 3, 14),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'dag2_dataproc_batch_dag',
    default_args=default_args,
    schedule_interval='@once',
    catchup=False
)

# Task 1: Fetch pending files
def fetch_pending_files():
    hook = CloudSQLDatabaseHook(
        gcp_conn_id='google_cloud_default',
        database='pubsub_metadata',
        instance='pubsubdata'
    )

    sql = "SELECT filePath FROM pubsub_metadata WHERE status = 'PENDING' LIMIT 5"
    result = hook.get_records(sql)

    if not result:
        logging.info('No pending files found.')
        return []

    file_paths = [row[0] for row in result]
    logging.info(f'Pending Files: {file_paths}')
    return file_paths

fetch_pending_files_task = PythonOperator(
    task_id='fetch_pending_files',
    python_callable=fetch_pending_files,
    provide_context=True,
    dag=dag
)

# Task 2: Dataproc Batch Job
create_batch = DataprocCreateBatchOperator(
    task_id='create_batch',
    project_id='publishingengine',
    region='us-central1',
    batch={
        "pyspark_batch": {
            "main_python_file_uri": "gs://dataproc-bucket/dataproc-jobs/scala-indexer.py",
            "args": []
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

# Task 3: Update status in SQL
def update_status_in_sql(ti):
    """Update the status of processed files to 'PROCESSED'."""
    file_paths = ti.xcom_pull(task_ids='fetch_pending_files')

    if not file_paths:
        logging.info("No files to update.")
        return

    hook = CloudSQLDatabaseHook(
        gcp_conn_id='google_cloud_default',
        database='pubsub_metadata',
        instance='pubsubdata'
    )

    # Update the status to 'PROCESSED'
    sql = """
    UPDATE pubsub_metadata
    SET status = 'PROCESSED'
    WHERE filePath IN ({})
    """.format(', '.join(f"'{path}'" for path in file_paths))

    hook.run(sql)
    logging.info(f'Status updated for files: {file_paths}')

update_status_task = PythonOperator(
    task_id='update_status_in_sql',
    python_callable=update_status_in_sql,
    provide_context=True,
    dag=dag
)

fetch_pending_files_task >> create_batch >> update_status_task
