import os
from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.models import Variable
from datetime import timedelta, datetime
from airflow.utils.dates import days_ago
from airflow.models import Variable
from airflow.providers.google.cloud.operators.dataproc import DataprocSubmitJobOperator


job_name = 'mongo_to_bigquery'
tempGCSbucket = Variable.get("tempGCSbucket")
mongoUri = Variable.get("mongoUri")
project_id = Variable.get("project")
jarfile_path = Variable.get("jarfile_path")
region = Variable.get("region")
cluster = Variable.get("cluster")
current_datetime = str(datetime.now().strftime("%Y%m%d_%H%M%S"))


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,    
    'email': ['airflow@airflow.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    
}

application_args = ['--mongoUri', mongoUri, '--database', 'sample_airbnb', '--collection', 'listingsAndReviews',
                    '--tempGCSbucket', tempGCSbucket, '--bigQueryDestination', 'test_dataset.table5']

spark_properties = {"spark.master": "local",
                    "spark.deploy.defaultCores": "1",
                    "spark.driver.memory": "512m",
                    "spark.executor.memory": "512m",
                    "spark.executor.cores": "1",
                    "spark.hadoop.fs.gs.implicit.dir.repair.enable": "false"}

schedule_interval = '@daily'

dag = DAG(
    dag_id = job_name, 
    default_args=default_args, 
    schedule_interval=schedule_interval,
    start_date=days_ago(1),
    catchup=False
)

SPARK_JOB = {
    "reference": {"project_id": project_id, "job_id": job_name + '_' + current_datetime},
    "placement": {"cluster_name": cluster},
    "spark_job": {
        "jar_file_uris": [jarfile_path],
        "main_class": "org.example.spark.mongoToBigquery",
        "args": application_args,
        "properties": spark_properties
    },
}

start_task = DummyOperator(
        task_id='start', dag=dag
    )

mongo_to_BQ = DataprocSubmitJobOperator(
    task_id="mongo_to_BQ", job=SPARK_JOB, region=region, project_id=project_id, gcp_conn_id="google_conn"
)



start_task >> mongo_to_BQ