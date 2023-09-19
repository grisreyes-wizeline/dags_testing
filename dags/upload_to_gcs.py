
from datetime import timedelta, datetime
from airflow import DAG
from airflow.contrib.sensors import file_sensor
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.contrib.operators.file_to_gcs import FileToGoogleCloudStorageOperator


seven_days_ago = datetime.combine(datetime.today() - timedelta(1),
                                  datetime.min.time())
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': seven_days_ago,
    'email': ['grisell.reyes@wizeline.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG('warehouse-and-retail-sales-dag', default_args=default_args, schedule_interval='@daily')


t1 = DummyOperator(task_id='op1', dag=dag)

t2 = FileToGoogleCloudStorageOperator(
    task_id='fileToGCS',
    src='/Users/grisell.reyes/Google-Africa-DEB/session_06/resources/local_repository_file/warehouse_and_retail_sales.csv'
    dst='',
    bucket='africa-deb-bucket',
    google_cloud_storage_conn_id='gcs_conn',
    dag=dag
)

t1 >> t2
