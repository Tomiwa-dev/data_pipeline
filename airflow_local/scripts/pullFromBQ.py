import os
import pandas as pd

credential_path = '/Users/emmanuel/airflow_local/velvety-network-356911-7c7f906de3e3.json'

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = credential_path


def etl(credential_path):
    query_string = "select * from velvety-network-356911.trips_data_all.to_pull"
    df = pd.read_gbq(query_string, dialect='standard',credentials=credential_path, project_id='velvety-network-356911')

    dest_table = "test_dataset.table1"

    df.to_gbq(dest_table, if_exists = 'replace', location ='europe-west2',credentials= credential_path,project_id='velvety-network-356911')
