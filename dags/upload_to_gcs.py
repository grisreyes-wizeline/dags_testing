import os
import logging
from datetime import datetime
from airflow import DAG
from airflow.decorators import dag
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator
from google.cloud import storage
import pyarrow.csv as pv
from pathlib import Path
import requests
import tempfile

# constants
PROJECT_ID = os.environ.get('GCP_PROJECT_ID')
bucket = 'africa-deb-bucket-second'
gcs_conn_id = 'gcp_conn'
dataset_url = (
    "https://data.montgomerycountymd.gov/resource/v76h-r7br.csv"
)
dataset_file= "warehouse_and_details_sales.csv"
path_to_local_home = "/opt/airflow"
credentials_file = Path("service_account.json")

def download_samples_from_url(path: str) -> None:
    """Downloads a set of samples into the specified path.

    Args:
        path (str): Path to output file.
    """
    with open(path, "wb") as out:
        response = requests.get(dataset_url)
        out.write(response.content)

def upload_to_gcs(bucket_name, destination_blob_name, credentials_file):
    # Initialize the Google Cloud Storage client with the credentials
    storage_client = storage.Client.from_service_account_json(credentials_file)
    with tempfile.NamedTemporaryFile("wb+") as tmp:
        download_samples_from_url(tmp.name)
        # Get the target bucket
        bucket = storage_client.bucket(bucket_name)
        # Upload the file to the bucket
        blob = bucket.blob(destination_blob_name)
        source_file_path= blob.upload_from_filename(tmp.name)
        print(f"File {source_file_path} uploaded to gs://{bucket_name}/{destination_blob_name}")

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
   
    
    local_to_gcs_task = PythonOperator(
        task_id="upload_to_gcs_task",
        python_callable=upload_to_gcs,
        op_kwargs={
            "bucket_name": bucket,
            "destination_blob_name": dataset_file,
            "credentials_file": credentials_file,
        },
    )

    # Workflow for task direction
    local_to_gcs_task

