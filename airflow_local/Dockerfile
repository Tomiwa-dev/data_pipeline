FROM apache/airflow:2.2.3

ENV AIRFLOW_HOME=/opt/airflow

USER root

COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

WORKDIR $AIRFLOW_HOME

USER $AIRFLOW_UID