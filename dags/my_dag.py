import os
import logging
from datetime import datetime
import pandas as pd
import pyarrow.parquet as pq
from airflow import DAG
from airflow.decorators import task

from google.cloud import storage
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateExternalTableOperator

from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

# AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")
# AIRFLOW_HOME = "/opt/airflow/"

PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
BUCKET = os.environ.get("GCP_GCS_BUCKET")

execution_date = datetime.now().strftime('%Y-%m')
FILE_URL = 'https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{{ execution_date.strftime(\'%Y-%m\') }}.parquet'
PARQUET_FILENAME = f'{execution_date}.parquet'
CSV_FILENAME = f'{execution_date}.csv'

with DAG(
	dag_id="elliots_first_dag10",
	schedule_interval="@monthly",
	# schedule_interval=None,
	start_date=datetime(2022,2,1),
	end_date=datetime(2022,3,1),
	catchup=True
) as dag:
	
	download_parquet = BashOperator(
		task_id = "download_parquet",
		bash_command = f'curl -sSL {FILE_URL} > {PARQUET_FILENAME}'
	)

	@task
	def convert_to_csv():
		# logging.info('airflow_home is:' + AIRFLOW_HOME)
		logging.info('parquet_filename is:' + PARQUET_FILENAME)
		logging.info('execution_date is:' + execution_date)
		dataset = pq.ParquetDataset(PARQUET_FILENAME)
		table = dataset.read()
		df = table.to_pandas()
		df.to_csv(CSV_FILENAME, index=False)
	
	download_parquet >> convert_to_csv()


