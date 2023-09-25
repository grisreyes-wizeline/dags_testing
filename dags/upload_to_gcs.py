import os
import logging
from datetime import datetime
from airflow import DAG
from airflow.decorators import dag
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator
from google.cloud import storage
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.operators.bash_operator import BashOperator
import pyarrow.csv as pv

# constants
PROJECT_ID = os.environ.get('GCP_PROJECT_ID')
BUCKET = 'africa-deb-bucket-second'
GCS_CONN_ID = 'gcp_conn'
dataset_url=f"https://data.montgomerycountymd.gov/resource/v76h-r7br.csv"
dataset_file= "warehouse_and_details_sales.csv"
path_to_local_home = "/opt/airflow/"
#LOCAL_FILE_PATH = '/Users/grisell.reyes/Google-Africa-DEB/session_06/resources/local_repository_file/warehouse_and_retail_sales.csv'

def upload_to_gcs(bucket, object_name, local_file):
    """
    Ref: https://cloud.google.com/storage/docs/uploading-objects#storage-upload-object-python
    """
    # WORKAROUND to prevent timeout for files > 6 MB on 800 kbps upload speed.
    # (Ref: https://github.com/googleapis/python-storage/issues/74)
    #storage.blob._MAX_MULTIPART_SIZE = 5 * 1024 * 1024  # 5 MB
    #storage.blob._DEFAULT_CHUNKSIZE = 5 * 1024 * 1024  # 5 MB
    # End of Workaround
    client = storage.Client()
    bucket = client.bucket(bucket)
    blob = bucket.blob(object_name)
    blob.upload_from_filename(local_file)
    print(
        f"File {local_file} uploaded to {bucket}."
    )

default_args = {
    "owner": "airflow",
    "start_date": days_ago(1),
    "depends_on_past": False,
    "retries": 1,
}

with DAG(
    dag_id="data_ingestion_gcs_dag",
    schedule_interval="@daily", # https://airflow.apache.org/docs/apache-airflow/1.10.1/scheduler.html
    default_args=default_args,
    catchup=False,
    max_active_runs=1,
    tags=['upload-gcs']
) as dag:

    download_dataset_task = BashOperator(
        task_id="download_dataset_task",
        bash_command=f"curl -sS {dataset_url} > {path_to_local_home}/{dataset_file}"
    )

    
    local_to_gcs_task = PythonOperator(
        task_id="local_to_gcs_task",
        python_callable=upload_to_gcs,
        op_kwargs={
            "bucket": BUCKET,
            "object_name": f"{dataset_file}",
            "local_file": f"{path_to_local_home}/{dataset_file}",
        },
    )

    # Workflow for task direction
    download_dataset_task >> local_to_gcs_task
