from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.contrib.operators.gcs_to_bq import GoogleCloudStorageToBigQueryOperator
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.contrib.hooks.gcs_hook import GoogleCloudStorageHook 
from airflow.operators.dummy_operator import DummyOperator
from airflow.contrib.operators.file_to_gcs import FileToGoogleCloudStorageOperator
import os
from datetime import timedelta
from datetime import datetime

# GCP constants
GCP_CONN_ID = "google_cloud_storage"
GCS_BUCKET_NAME = "africa-deb-bucket"
local_file_path = '/Users/grisell.reyes/Google-Africa-DEB/session_06/resources/local_repository_file/warehouse_and_retail_sales.csv'


def upload_to_gcs():
    upload_file = LocalFilesystemToGCSOperator(
        task_id="upload_file",
        src=local_file_path,
        dst='warehouse_and_retail_sales_2.csv,
        bucket=GCS_BUCKET_NAME,
        gcp_conn_id = GCP_CONN_ID)


default_args = {
    'owner': 'grisell.reyes',
    'depends_on_past': False,    
    'start_date': datetime(2023, 1, 1),
    'email': ['grisell.reyes@wizeline.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=1),
}
# Define your DAG
with DAG('uplod_to_gcs_2', schedule_interval='@once', default_args=default_args) as dag:
    start_pipeline = DummyOperator(
        task_id = 'start_pipeline',
        dag = dag
        )
        
    upload_data = PythonOperator(
        task_id="upload_to_gcs",
        python_callable=upload_to_gcs,
    )
    
    start_pipeline >> upload_to_gcs
    
