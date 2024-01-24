import os
import logging
import pandas as pd
from datetime import datetime
from airflow import DAG
from airflow.decorators import task

import pyarrow.parquet as pq

from google.cloud import storage
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateExternalTableOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateEmptyDatasetOperator
from airflow.providers.google.cloud.operators.gcs import GCSCreateBucketOperator

with DAG(
	dag_id="learn_gcs8",
	schedule_interval=None,
	start_date=datetime.utcnow(),
	catchup=False
) as dag:
	
	create_bucket = GCSCreateBucketOperator(
		task_id = "create_bucket",
		bucket_name = 'test_bucket_learning-gcs-411623',
		project_id = 'learning-gcs-411623',
		location = 'us-central1',
		storage_class='STANDARD'
	)

	create_bucket