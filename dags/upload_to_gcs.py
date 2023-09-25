import os
import logging
from datetime import datetime
from airflow import DAG
from airflow.decorators import dag
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from google.cloud import storage
import pyarrow.csv as pv
from pathlib import Path
import requests
import tempfile

# constants
bucket = 'africa-deb-bucket-second'
dataset_url = (
    "https://data.montgomerycountymd.gov/resource/v76h-r7br.csv"
)
dataset_file= "warehouse_and_details_sales.csv"
# path_to_local_home = "/opt/airflow"
#credentials_file = Path("service_account.json")

def download_samples_from_url(path: str) -> None:
    """Downloads a set of samples into the specified path.

    Args:
        path (str): Path to output file.
    """
    with open(path, "wb") as out:
        response = requests.get(dataset_url)
        out.write(response.content)

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

    def upload_file_func():
        with tempfile.NamedTemporaryFile("wb+") as tmp:
            download_samples_from_url(tmp.name)
            hook = GCSHook(gcp_conn_id='google_cloud_default')
            bucket_name = bucket
            object_name = dataset_file
            filename = tmp
            hook.upload(bucket_name, object_name, filename)
            
    upload_file = PythonOperator(task_id='upload_file', python_callable=upload_file_func)

    # Workflow for task direction
    upload_file



