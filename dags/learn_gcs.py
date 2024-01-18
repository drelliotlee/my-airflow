import os
import logging
import pandas as pd
from datetime import datetime
from airflow import DAG
from airflow.decorators import task

import pyarrow.parquet as pq
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateExternalTableOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateEmptyDatasetOperator


PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
BUCKET = os.environ.get("GCP_GCS_BUCKET")

with DAG(
	dag_id="learn_gcs2",
	schedule_interval="@monthly",
	# schedule_interval=None,
	start_date=datetime(2023,4,1),
	end_date=datetime(2023,4,1),
	catchup=True
) as dag:
	
	@task
	def make_parquet():
		df = pd.DataFrame([1,2,3], columns=['data'])
		df.to_parquet('fake.parquet')

	create_dataset = BigQueryCreateEmptyDatasetOperator(
		task_id="create_dataset", 
		dataset_id='some_name3',
		project_id = 'learning-gcs-411623'
	)

	make_parquet() >> create_dataset