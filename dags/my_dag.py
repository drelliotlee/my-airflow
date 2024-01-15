import os
from datetime import datetime
import pandas as pd
import pyarrow.parquet as pq
from airflow import DAG
from airflow.decorators import task

from google.cloud import storage
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateExternalTableOperator

from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")

PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
BUCKET = os.environ.get("GCP_GCS_BUCKET")

FILE_URL = 'https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{{ execution_date.strftime(\'%Y-%m\') }}.parquet'
PARQUET_FILENAME = AIRFLOW_HOME + '/{{ execution_date.strftime(\'%Y-%m\') }}.parquet'
CSV_FILENAME = AIRFLOW_HOME + '/{{ execution_date.strftime(\'%Y-%m\') }}.csv'
with DAG(
	dag_id="elliots_first_dag",
	schedule_interval="@daily",
	start_date=datetime(2021,1,1),
	catchup=True
) as dag:
	
	download_parquet = BashOperator(
		task_id = "download_parquet",
		bash_command = f'curl -sSL {FILE_URL} > {PARQUET_FILENAME}'
	)

	@task
	def convert_to_csv():
		table = pq.ParquetDataset(PARQUET_FILENAME).read()
		df = table.to_pandas()
		df.to_csv(CSV_FILENAME, index=False)
	
	download_parquet >> convert_to_csv()


