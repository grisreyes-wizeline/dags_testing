from datetime import timedelta, datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.contrib.operators.file_to_gcs import FileToGoogleCloudStorageOperator

default_args = {
    'owner': 'Ilham Putra',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 5,
    'start_date':  days_ago(2),
    'retry_delay': timedelta(minutes=5),
}

with DAG('UploafToGCS', schedule_interval='@once', default_args=default_args) as dag:
    
    t1 = DummyOperator(task_id='op1', dag=dag)
    t2 = FileToGoogleCloudStorageOperator(
        task_id='fileToGCS',
        src='/Users/grisell.reyes/Google-Africa-DEB/session_06/resources/local_repository_file/warehouse_and_retail_sales.csv',
        dst='data/warehouse_and_retail_sales_bucket.csv',
        bucket='africa-deb-bucket',
        google_cloud_storage_conn_id='google_cloud_storage',
        dag=dag
        )
t1 >> t2
