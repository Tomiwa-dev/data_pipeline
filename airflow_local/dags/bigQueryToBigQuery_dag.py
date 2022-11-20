#using pyspark 
import os
from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.models import Variable
from datetime import timedelta, datetime
from airflow.utils.dates import days_ago
from airflow.models import Variable
from airflow.providers.google.cloud.operators.dataproc import DataprocSubmitJobOperator


job_name = 'bigQueryToBigQuery'
tempGCSbucket = Variable.get("tempGCSbucket")
project_id = Variable.get("project")
pyspark_script_path = Variable.get("pyspark_script")
region = Variable.get("region")
cluster = Variable.get("cluster")
current_datetime = str(datetime.now().strftime("%Y%m%d_%H%M%S"))
bigquery_jar = Variable.get("gcs_connector")

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,    
    'email': ['airflow@airflow.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=20),
    
}


application_args = ['--srcTable','velvety-network-356911:trips_data_all.to_pull' , '--destTable', 
'velvety-network-356911:test_dataset.table6', '--tempGCSbucket', tempGCSbucket]


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

PYSPARK_JOB = {"reference": {"project_id": project_id, "job_id": job_name + '_' + current_datetime},
    "placement": {"cluster_name": cluster}, 
    "pyspark_job": {"main_python_file_uri": pyspark_script_path,"jar_file_uris": [bigquery_jar],"args": application_args,
    "properties": spark_properties},
    
}

start_task = DummyOperator(
        task_id='start', dag=dag
    )

pyspark_task = DataprocSubmitJobOperator(
    task_id="pyspark_task", job=PYSPARK_JOB, region=region, project_id=project_id, gcp_conn_id="google_conn"
)


start_task >> pyspark_task