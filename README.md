##Data Pipeline
A repo for pulling data from different sources using Apache Spark written in Scala and Python. Apache Airflow is used to schedule the task on Google DataProc


Repo Structure

airflow_local - 
- dags/ : This directory contains the dags needed to schedule the jobs on dataproc. 

- docker-compose.yaml: this docker compose file can be used to set up airflow on docker.

- scripts/ : this directory contains python scripts that can be run on airflow using a BashOperator or PythonOperator.

etl_mongo/ - 
This directory contain the spark-scala scripts.

pyspark_scripts/ - 
This directory contains pyspark scripts.
