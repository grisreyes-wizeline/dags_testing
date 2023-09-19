from datetime import datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.contrib.operators.gcs_to_bq import GoogleCloudStorageToBigQueryOperator
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
import os

# Define the Python function to upload files to GCS
def upload_to_gcs(data_folder, gcs_path,**kwargs):
    data_folder = /Users/grisell.reyes/Google-Africa-DEB/session_06/resources/local_repository_file
    bucket_name = 'africa-deb-bucket'  # Your GCS bucket name
    gcs_conn_id = 'gcs_conn'
    # List all CSV files in the data folder
    csv_files = [file for file in os.listdir(data_folder) if file.endswith('.csv')]

    # Upload each CSV file to GCS
    for csv_file in csv_files:
        local_file_path = os.path.join(data_folder, csv_file)
        gcs_file_path = f"{gcs_path}/{csv_file}"

        upload_task = LocalFilesystemToGCSOperator(
            task_id=f'upload_to_gcs',
            src=local_file_path,
            dst=gcs_file_path,
            bucket=bucket_name,
            gcp_conn_id=gcs_conn_id,
        )
        upload_task.execute(context=kwargs)
# Define your DAG
dag = DAG(
    'upload_files_to_gcs',
    start_date=datetime(2023, 1, 1),
    schedule_interval=None,  # Set your desired schedule interval or None for manual triggering
    catchup=False,  # Set to True if you want historical DAG runs upon creation
)


# Define the PythonOperator to run the upload_to_gcs function
upload_to_gcs = PythonOperator(
    task_id='upload_to_gcs',
    python_callable=upload_to_gcs,
    op_args=['/path/to/your/local/data/', 'gcs/destination'],
    provide_context=True,
    dag=dag,  # Assign the DAG to the task
)

# Define your DAG dependencies
upload_to_gcs
