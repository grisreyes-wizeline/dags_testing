import os
from datetime import datetime

from airflow import models
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator

# [START howto_gcs_environment_variables]
BUCKET_NAME = 'africa-deb-bucket'
PATH_TO_UPLOAD_FILE = '/Users/grisell.reyes/Google-Africa-DEB/session_06/resources/local_repository_file/warehouse_and_retail_sales.csv'
DESTINATION_FILE_LOCATION = 'warehouse_and_retail_sales_2.csv'
# [END howto_gcs_environment_variables]

with models.DAG(
    'example_local_to_gcs',
    schedule_interval='@once',
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=['example'],
) as dag:
    # [START howto_operator_local_filesystem_to_gcs]
    upload_file = LocalFilesystemToGCSOperator(
        task_id="upload_file",
        src=PATH_TO_UPLOAD_FILE,
        dst=DESTINATION_FILE_LOCATION,
        bucket=BUCKET_NAME,
    )
    # [END howto_operator_local_filesystem_to_gcs]
    upload_file
