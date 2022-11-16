from datetime import timedelta, datetime
from airflow.utils.dates import days_ago
from airflow import DAG
# from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.operators.dummy import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
# from airflow.providers.google.cloud.transfers.bigquery_to_gcs import BigQueryToGCSOperator
# from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
import pandas as pd
# from scripts.pullFromBQ import etl
from airflow.models import Variable
import os
from scripts.pullFromBQ import etl


credential_path = Variable.get("etl_cred")


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,    
    'email': ['airflow@airflow.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    
}

schedule_interval = '@daily'

dag = DAG(
    'bigquery_github_trends', 
    default_args=default_args, 
    schedule_interval=schedule_interval,
    start_date=days_ago(1),
    catchup=False
)


start_task = DummyOperator(
        task_id='start', dag=dag
    )

run_etl = PythonOperator(
    task_id='run_etl', 
    python_callable = etl(credential_path=credential_path)

)

# run_elt = BashOperator(
#     task_id = 'run_elt',
#     bash_command = 'python /opt/airflow/scripts/pullFromBQ.py'
# )

# pull_from_BQ_TO_GCS = BigQueryToGCSOperator(
#     task_id='pull_from_BQ_TO_GCS',
#     source_project_dataset_table ='trips_data_all.to_pull',
#     destination_cloud_storage_uris ='gs://velvety_data_transfer_123445/data_transfer/fhv.csv',
#     # project_id = 'velvety-network-356911',
#     export_format='CSV', field_delimiter=',',
#     gcp_conn_id = 'google_conn'

# )

# from_GCS_TO_BQ = GCSToBigQueryOperator(
#     task_id='from_GCS_TO_BQ',
#     bucket='gs://velvety_data_transfer_123445',
#     source_objects='/data_transfer/fhv.csv',
#     destination_project_dataset_table='test_dataset.table1',
#     # gcp_conn_id='google_conn',
#     source_format='CSV',
#     field_delimiter=','
# )



start_task >> run_elt