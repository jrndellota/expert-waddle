import os
import logging
from datetime import datetime

from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateExternalTableOperator
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
import pyarrow.csv as pv
import pyarrow.parquet as pq

PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
BUCKET = os.environ.get("GCP_GCS_BUCKET")

dataset_file = "fhv_tripdata_{{ds[0:7]}}.csv"
dataset_url = "https://s3.amazonaws.com/nyc-tlc/trip+data/fhv_tripdata_{{ds[0:7]}}.csv"
path_to_local_home = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")
parquet_file = "fhv_tripdata_{{ds[0:7]}}.parquet"
BIGQUERY_DATASET = os.environ.get("BIGQUERY_DATASET", 'trips_data_all')

def format_to_parquet(src_file):
    if not src_file.endswith('.csv'):
        logging.error("Can only accept source files in CSV format, for the moment")
        return
    table = pv.read_csv(src_file)
    pq.write_table(table, src_file.replace('.csv', '.parquet'))

default_args = {
    "owner": "airflow",
    "start_date": datetime(2019,1,1),
    "end_date": datetime(2019, 12, 1),
    "depends_on_past": False,
    "retries": 1,
}

with DAG(
    dag_id="fhv_ingestion_dag",
    schedule_interval="@monthly",
    default_args=default_args,
    catchup=True,
    max_active_runs=1,
    tags=['dtc-de'],
) as dag:

    download_dataset_task = BashOperator(
        task_id="download_dataset_task",
        bash_command=f"curl -sS {dataset_url} > {path_to_local_home}/{dataset_file}"
    )

    format_to_parquet_task = PythonOperator(
        task_id="format_to_parquet_task",
        python_callable=format_to_parquet,
        op_kwargs={
            "src_file": f"{path_to_local_home}/{dataset_file}",
        },
    )

    local_to_gcs_task = LocalFilesystemToGCSOperator(
        task_id="local_to_gcs_task",
        src =f"{path_to_local_home}/{parquet_file}",
        dst=f"raw/fhv_tripdata/{parquet_file}",
        bucket = BUCKET,
        gcp_conn_id="gcp"
    )

    download_dataset_task >> format_to_parquet_task >> local_to_gcs_task
