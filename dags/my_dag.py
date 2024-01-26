import os
import logging
import pandas as pd
from datetime import datetime
import json
import urllib.parse
import subprocess

from sqlalchemy import create_engine
import pyarrow.parquet as pq

from google.cloud import storage

from airflow import DAG
from airflow.decorators import task
from airflow.decorators import branch_python
from airflow.hooks.base import BaseHook
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.google.cloud.operators.gcs import GCSCreateBucketOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateExternalTableOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateEmptyDatasetOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.http.sensors.http import HttpSensor
from airflow.providers.http.operators.http import SimpleHttpOperator


execution_date = datetime.now().strftime('%Y-%m')
PROJECT_ID = 'learning-gcs-411623'
BUCKET_NAME = PROJECT_ID + 'test-bucket'
DATASET_NAME = 'test_dataset'
TABLE_NAME = 'test-table'
PARQUET_URL = 'https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{{ execution_date.strftime(\'%Y-%m\') }}.parquet'
CSV_URL = 'https://d37ci6vzurychx.cloudfront.net/misc/taxi+_zone_lookup.csv'
PARQUET_FILENAME = '/opt/airflow/' + execution_date + '.parquet'
CSV_FILENAME = '/opt/airflow/' + execution_date + '.csv'

API_URI = 'https://' + urllib.parse.quote('gorest.co.in/public/v2/', safe='')

with DAG(
	dag_id="elliots_dag4",
	schedule_interval="@monthly",
	# schedule_interval=None,
	start_date=datetime(2023,4,1),
	end_date=datetime(2023,4,1),
	catchup=True
) as dag:
	
	entrypoint = BashOperator(
		task_id = 'entrypoint',
		bash_command = f'''
		airflow users create -e "admin@airflow.com" -f "airflow" -l "airflow" -p "airflow" -r "Admin" -u "airflow" && \
		airflow connections add --conn-uri "postgresql://airflow:airflow@postgres:5432" postgres_conn
		'''
	)

	@task.branch()
	def check_if_rest_api_conn_exists():
		command = "airflow connections list | grep -Eo '^[^|]*\|([^|]*)\|'"
		terminal_output = subprocess.run(command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
		if b'rest_api_conn' in terminal_output.stdout:
			logging.info("*** rest_api_conn already exists ***")
			return 'rest_api_to_xcom'
		else:
			logging.info("*** rest_api_conn doesn't exist ***")
			return 'create_rest_api_conn'


	create_rest_api_conn = BashOperator(
		task_id = 'create_rest_api_conn',
		bash_command='''
			echo "now creating connection" && \
			airflow connections add --conn-uri {API_URI} rest_api_conn
			'''
	)

	rest_api_to_xcom = SimpleHttpOperator(
		task_id = "rest_api_to_xcom",
		http_conn_id = 'rest_api_conn',
		endpoint = 'posts/',
		response_filter = lambda response: json.loads(response.text),
		log_response=True
	)

	@task
	def xcom_to_json(ti) -> None:
		posts = ti.xcom_pull(task_ids=['rest_api_to_xcom'])
		with open('rest_api_response.json', 'w') as f:
			json.dump(posts[0], f)

	drop_2nd_database = PostgresOperator(
		task_id = "drop_2nd_database",
		postgres_conn_id = "postgres_conn",
		autocommit = True,
		sql = '''
		DROP DATABASE IF EXISTS taxi_db;
		''',
	)
	
	create_2nd_database = PostgresOperator(
		task_id = "create_2nd_database",
		postgres_conn_id = "postgres_conn",
		autocommit = True,
		sql = '''
		CREATE DATABASE taxi_db;
		''',
	)

	download_parquet = BashOperator(
		task_id = "download_parquet",
		bash_command = f'curl -sSL {PARQUET_URL} > {PARQUET_FILENAME}'
	)

	create_trips_table = PostgresOperator(
		task_id = 'create_trips_table',
		postgres_conn_id = "postgres_conn",
		database = 'taxi_db',
		autocommit = True,
		sql = """
			DROP TABLE IF EXISTS trips_table;
			CREATE TABLE trips_table (
				"VendorID" INT,
				"PUtime" TIMESTAMP,
				"DOtime" TIMESTAMP,
				"Passenger_count" INT,
				"Trip_distance" NUMERIC,
				"RateCodeID" INT,
				"StoreFwdFlag" VARCHAR(2),
				"PUlocation" INT,
				"DOlocation" INT,
				"Payment_Type" INT,
				"Fare_amount" NUMERIC,
				"Extra" NUMERIC,
				"MTA_tax" NUMERIC,
				"Improve_surcharge" NUMERIC,
				"Tip_amount" NUMERIC,
				"Tolls_amount" NUMERIC,
				"Total_amount" NUMERIC,
				"Congest_surcharge" NUMERIC,
				"Airport_fee" NUMERIC
			);
		"""
	)

	@task
	def parquet_to_postgres():
		engine = create_engine('postgresql://airflow:airflow@postgres/taxi_db')
		connection = engine.connect()
		parquet_file = pq.ParquetFile(PARQUET_FILENAME)
		for i, batch in enumerate(parquet_file.iter_batches(batch_size=10)):
			if i<5:
				logging.info(f'***iteration {i}***')
				chunk = batch.to_pandas()
				chunk.columns = ['VendorID', 'PUtime', 'DOtime', 'Passenger_count', 'Trip_distance', 'RateCodeID', 'StoreFwdFlag', 'PUlocation', 'DOlocation', 'Payment_Type',
					 'Fare_amount', 'Extra', 'MTA_tax', 'Improve_surcharge', 'Tip_amount', 'Tolls_amount', 'Total_amount', 'Congest_surcharge', 'Airport_fee']
				logging.info(f'***{chunk.head(3)}***')
				chunk.PUtime = pd.to_datetime(chunk.PUtime)
				chunk.DOtime = pd.to_datetime(chunk.DOtime)
				# if i==0:	
				# 	chunk.head(n=0).to_sql(name='taxi_data', con=engine, if_exists='replace')
				chunk.to_sql('trips_table', engine, if_exists='append', index=False)
		connection.close()

	download_csv = BashOperator(
		task_id = "download_csv",
		bash_command = f'curl -sSL {CSV_URL} > {CSV_FILENAME}'
	)

	create_zones_table = PostgresOperator(
		task_id = 'create_zones_table',
		postgres_conn_id = "postgres_conn",
		database = 'taxi_db',
		autocommit = True,
		sql = """
			DROP TABLE IF EXISTS zones_table;
			CREATE TABLE zones_table(
				"LocationID" INT,
				"Borough" VARCHAR(80),
				"Zone" VARCHAR(80),
				"Service_zone" VARCHAR(80)
			);
		"""
	)

	@task
	def csv_to_postgres():
		hook = PostgresHook(
			postgres_conn_id='postgres_conn',
			schema = 'taxi_db')
		conn = hook.get_conn()
		cur = conn.cursor()
		sql_query = """
		COPY zones_table
		FROM STDIN WITH CSV HEADER DELIMITER AS ',' QUOTE '\"'
		"""
		with open(CSV_FILENAME, "r") as f:
			cur.copy_expert(sql_query, f)
		conn.commit()
		cur.close()
		conn.close()

	join_in_postgres = PostgresOperator(
		task_id = 'join_in_postgres',
		postgres_conn_id = "postgres_conn",
		database = 'taxi_db',
		autocommit = True,
		sql = """
		CREATE TABLE joined_table AS
		SELECT t.*, z1."Service_zone" AS "PUservice_zone", z2."Service_zone" AS "DOservice_zone"
		FROM trips_table t
		LEFT JOIN zones_table z1 ON t."PUlocation" = z1."LocationID"
		LEFT JOIN zones_table z2 ON t."DOlocation" = z2."LocationID";
		"""
	)

	@task
	def export_to_parquet():
		engine = create_engine('postgresql://airflow:airflow@postgres/taxi_db')
		connection = engine.connect()
		df = pd.read_sql_query("SELECT * FROM joined_table", engine)
		pq.write_table(df, 'joined_table.parquet')
		connection.close()

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
		blob = bucket.blob(f"raw/{PARQUET_FILENAME}")
		blob.upload_from_filename(PARQUET_FILENAME)

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
                "sourceUris": [f"gs://{BUCKET_NAME}/raw/{PARQUET_FILENAME}"],
            },
        },
    )

	check_if_rest_api_conn_exists_instance = check_if_rest_api_conn_exists()
	check_if_rest_api_conn_exists_instance >> rest_api_to_xcom
	check_if_rest_api_conn_exists_instance >> create_rest_api_conn
	drop_2nd_database >> create_2nd_database >> [ download_csv, download_parquet ]
	download_csv >> create_zones_table >> csv_to_postgres() >> join_in_postgres
	download_parquet >> create_trips_table >> parquet_to_postgres() >> join_in_postgres
	download_parquet >> create_bucket >> local_to_gcs() >> create_bq_dataset >> create_bq_external_table


