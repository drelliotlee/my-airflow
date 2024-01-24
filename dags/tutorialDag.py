import os
import logging

from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator
from google.cloud import storage
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateExternalTableOperator
from airflow.operators.bash_operator import BashOperator
import pyarrow.csv as pv
import pyarrow.parquet as pq

# VARIABLES
PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
BUCKET = os.environ.get("GCP_GCS_BUCKET")
dataset_file = "winequality-red.csv"
dataset_url = f"https://archive.ics.uci.edu/ml/machine-learning-databases/wine-quality/{dataset_file}"
path_to_local_home = "/opt/airflow/" # we download in this location so file is is not deleted when the downloading task finishes
parquet_file = dataset_file.replace('.csv', '.parquet')
BIGQUERY_DATASET = 'winequality'


def format_to_parquet(src_file):
    if not src_file.endswith('.csv'):
        logging.error(
            "Can only accept source files in CSV format, for the moment")
        return
    parse_options = pv.ParseOptions(delimiter=";")
    table = pv.read_csv(src_file, parse_options=parse_options)
    pq.write_table(table, src_file.replace('.csv', '.parquet'))


def upload_to_gcs(bucket, object_name, local_file):
    """
    Ref: https://cloud.google.com/storage/docs/uploading-objects#storage-upload-object-python
    """
    # WORKAROUND to prevent timeout for files > 6 MB on 800 kbps upload speed.
    # (Ref: https://github.com/googleapis/python-storage/issues/74)
    storage.blob._MAX_MULTIPART_SIZE = 5 * 1024 * 1024  # 5 MB
    storage.blob._DEFAULT_CHUNKSIZE = 5 * 1024 * 1024  # 5 MB
    # End of Workaround
    client = storage.Client()
    bucket = client.bucket(bucket)
    blob = bucket.blob(object_name)
    blob.upload_from_filename(local_file)
    print(
        f"File {local_file} uploaded to {bucket}."
    )


default_args = {
    "owner": "airflow",
    "start_date": days_ago(1),
    "depends_on_past": False,
    "retries": 1,
}


with DAG(
    dag_id="data_ingestion_gcs_dag_v1.0",
    schedule_interval="@daily", # https://airflow.apache.org/docs/apache-airflow/1.10.1/scheduler.html
    default_args=default_args,
    catchup=False,
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

    local_to_gcs_task = PythonOperator(
        task_id="local_to_gcs_task",
        python_callable=upload_to_gcs,
        op_kwargs={
            "bucket": BUCKET,
            "object_name": f"raw/{parquet_file}",
            "local_file": f"{path_to_local_home}/{parquet_file}",
        },
    )

    bigquery_external_table_task = BigQueryCreateExternalTableOperator(
        task_id="bigquery_external_table_task",
        table_resource={
            "tableReference": {
                "projectId": PROJECT_ID,
                "datasetId": BIGQUERY_DATASET,
                "tableId": "wine_quality",
            },
            "externalDataConfiguration": {
                "sourceFormat": "PARQUET",
                "sourceUris": [f"gs://{BUCKET}/raw/{parquet_file}"],
            },
        },
    )

    # Workflow for task direction
    download_dataset_task >> format_to_parquet_task >> local_to_gcs_task >> bigquery_external_table_task