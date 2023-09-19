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

# Define the Python function to upload files to GCS
def upload_to_gcs():
    bucket_name = 'africa-deb-bucket'  # Your GCS bucket name
    gcs_conn_id = 'google_cloud_storage'
    local_file_path = '/Users/grisell.reyes/Google-Africa-DEB/session_06/resources/local_repository_file/warehouse_and_retail_sales.csv'
    localToGCS1 = FileToGoogleCloudStorageOperator(
        task_id='LocalToGCS',
        src= local_file_path,
        dst='warehouse_and_retail_sales_2.csv',
        bucket= bucket_name,
        google_cloud_storage_conn_id='google_cloud_storage',
        dag=dag
        )

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
with DAG('RecipeWarehouse', schedule_interval='@once', default_args=default_args) as dag:
    start_pipeline = DummyOperator(
        task_id = 'start_pipeline',
        dag = dag
        )
        
    upload_to_gcs = PythonOperator(
        task_id='upload_to_gcs',
        python_callable=upload_to_gcs,
        dag=dag  # Assign the DAG to the task
        )

# Define your DAG dependencies
start_pipeline >> upload_to_gcs
