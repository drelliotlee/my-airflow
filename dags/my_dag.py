import os
import logging
from datetime import datetime
import pandas as pd
from sqlalchemy import create_engine
import pyarrow.parquet as pq

from google.cloud import storage

from airflow import DAG
from airflow.decorators import task
from airflow.hooks.base import BaseHook
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateExternalTableOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator


PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
BUCKET = os.environ.get("GCP_GCS_BUCKET")

execution_date = datetime.now().strftime('%Y-%m')
PARQUET_URL = 'https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{{ execution_date.strftime(\'%Y-%m\') }}.parquet'
CSV_URL = 'https://d37ci6vzurychx.cloudfront.net/misc/taxi+_zone_lookup.csv'
PARQUET_FILENAME = '/opt/airflow/' + execution_date + '.parquet'
CSV_FILENAME = '/opt/airflow/' + execution_date + '.csv'

with DAG(
	dag_id="elliots_dag",
	schedule_interval="@monthly",
	# schedule_interval=None,
	start_date=datetime(2023,4,1),
	end_date=datetime(2023,4,1),
	catchup=True
) as dag:
	
	drop_2nd_database = PostgresOperator(
		task_id = "drop_2nd_database",
		postgres_conn_id = "default_connection",
		autocommit = True,
		sql = '''
		DROP DATABASE IF EXISTS taxi_db;
		''',
	)
	
	create_2nd_database = PostgresOperator(
		task_id = "create_2nd_database",
		postgres_conn_id = "default_connection",
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
		postgres_conn_id = "default_connection",
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
		postgres_conn_id = "default_connection",
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
			postgres_conn_id='default_connection',
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
		postgres_conn_id = "default_connection",
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

	bigquery_external_table_task = BigQueryCreateExternalTableOperator(
        task_id="bigquery_external_table_task",
        table_resource={
            "tableReference": {
                "projectId": PROJECT_ID,
                "datasetId": 'taxi_data',
                "tableId": "wine_quality",
            },
            "externalDataConfiguration": {
                "sourceFormat": "PARQUET",
                "sourceUris": [f"gs://{BUCKET}/raw/joined_table.parquet"],
            },
        },
    )

	
	drop_2nd_database >> create_2nd_database >> [ download_csv, download_parquet ]
	download_csv >> create_zones_table >> csv_to_postgres() >> join_in_postgres
	download_parquet >> create_trips_table >> parquet_to_postgres() >> join_in_postgres
	join_in_postgres >> bigquery_external_table_task


