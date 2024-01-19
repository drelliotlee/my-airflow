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

PROJECT_ID = 'learning-gcs-411623'
FILE_NAME = 'fake.parquet'
BUCKET_NAME = PROJECT_ID + 'test-bucket'
DATASET_NAME = 'test_dataset'
TABLE_NAME = 'test-table'

with DAG(
	dag_id="learn_gcs9",
	schedule_interval=None,
	start_date=datetime.utcnow(),
	catchup=False
) as dag:
	
	@task
	def make_parquet():
		df = pd.DataFrame([1,2,3], columns=['data'])
		df.to_parquet(FILE_NAME)

	create_bucket = GCSCreateBucketOperator(
		task_id = "create_bucket",
		bucket_name = BUCKET_NAME,
		project_id = PROJECT_ID
	)

	@task
	def local_to_gcs():
		storage.blob._MAX_MULTIPART_SIZE = 5 * 1024 * 1024  # 5 MB
		storage.blob._DEFAULT_CHUNKSIZE = 5 * 1024 * 1024  # 5 MB
		client = storage.Client()
		bucket = client.bucket(BUCKET_NAME)
		blob = bucket.blob(f"raw/{FILE_NAME}")
		blob.upload_from_filename(FILE_NAME)
		print(
			f"File {FILE_NAME} uploaded to {bucket}."
		)

	create_bq_dataset = BigQueryCreateEmptyDatasetOperator(
		task_id = 'create_bq_dataset',
		dataset_id = DATASET_NAME,
		project_id = PROJECT_ID
	) 

	create_bq_external_table = BigQueryCreateExternalTableOperator(
        task_id="bigquery_external_table_task",
        table_resource={
            "tableReference": {
                "projectId": PROJECT_ID,
                "datasetId": DATASET_NAME,
                "tableId": TABLE_NAME,
            },
            "externalDataConfiguration": {
                "sourceFormat": "PARQUET",
                "sourceUris": [f"gs://{BUCKET_NAME}/raw/{FILE_NAME}"],
            },
        },
    )

	make_parquet() >> create_bucket >> local_to_gcs() >> create_bq_dataset >>create_bq_external_table
