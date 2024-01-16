import os
import logging
from datetime import datetime
import pandas as pd
from sqlalchemy import create_engine
import pyarrow.parquet as pq

from google.cloud import storage

from airflow import DAG
from airflow.decorators import task
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateExternalTableOperator
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

# AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")
# AIRFLOW_HOME = "/opt/airflow/"

PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
BUCKET = os.environ.get("GCP_GCS_BUCKET")

execution_date = datetime.now().strftime('%Y-%m')
FILE_URL = 'https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{{ execution_date.strftime(\'%Y-%m\') }}.parquet'
PARQUET_FILENAME = '/opt/airflow/' + execution_date + '.parquet'
CSV_FILENAME = '/opt/airflow/' + execution_date + '.csv'

with DAG(
	dag_id="elliots_first_dag",
	schedule_interval="@monthly",
	# schedule_interval=None,
	start_date=datetime(2023,2,1),
	end_date=datetime(2023,2,1),
	catchup=True
) as dag:
	
	download_parquet = BashOperator(
		task_id = "download_parquet",
		bash_command = f'curl -sSL {FILE_URL} > {PARQUET_FILENAME}'
	)

	# @task
	# def convert_to_csv():
	# 	logging.info('*****************')
	# 	logging.info('parquet_filename is:' + PARQUET_FILENAME)
	# 	logging.info('execution_date is:' + execution_date)
	# 	logging.info('*****************')
	# 	dataset = pq.ParquetDataset(PARQUET_FILENAME)
	# 	table = dataset.read()
	# 	df = table.to_pandas()
	# 	df.to_csv(CSV_FILENAME, index=False)

	@task
	def parquet_to_postgres():
		engine = create_engine('postgresql://airflow:airflow@postgres/airflow')
		engine.connect()
		parquet_file = pq.ParquetFile(PARQUET_FILENAME)
		logging.info('*****************')
		logging.info('parquet_filename is:' + PARQUET_FILENAME)
		for i, batch in enumerate(parquet_file.iter_batches(batch_size=5)):
			logging.info(f'***iteration {i}***')
			chunk = batch.to_pandas()
			logging.info(chunk.to_string())
			chunk.tpep_pickup_datetime = pd.to_datetime(chunk.tpep_pickup_datetime)
			chunk.tpep_dropoff_datetime = pd.to_datetime(chunk.tpep_dropoff_datetime)
			if i==0:	
				chunk.head(n=0).to_sql(name='taxi_data', con=engine, if_exists='replace')
			chunk.to_sql('taxi_data', engine, if_exists='append', index=False)
	
	download_parquet >> parquet_to_postgres()


