from airflow.providers.postgres.operators.postgres import PostgresOperator
from datetime import datetime
from airflow import DAG
import pandas as pd
from sqlalchemy import create_engine
from airflow.decorators import task
from airflow.hooks.base import BaseHook

with DAG(
	dag_id="test_dag",
	schedule_interval="@monthly",
	# schedule_interval=None,
	start_date=datetime(2023,2,1),
	end_date=datetime(2023,2,1),
	catchup=True
) as dag:
	
	create_2nd_database = PostgresOperator(
		task_id = "create_2nd_database",
		postgres_conn_id = "default_connection",
		sql = 'CREATE DATABASE airflow2;',
		autocommit = True
	)

	create_3rd_database = PostgresOperator(
		task_id = "create_3rd_database",
		postgres_conn_id = "default_connection",
		sql = 'CREATE DATABASE airflow3;',
		autocommit = True
	)

	@task
	def sqlalchemy_task():
		engine = create_engine('postgresql://airflow:airflow@postgres/airflow2')
		connection = engine.connect()
		df = pd.DataFrame([0,0,0])
		df.to_sql('alchemy_data', engine, if_exists='append', index=False)
		connection.close()

	
	pgOperator_task = PostgresOperator(
		task_id = 'pgOperator_task',
		postgres_conn_id = "default_connection",
		database = 'airflow3',
		sql = """
			DROP TABLE IF EXISTS pgoperator_data;
			CREATE TABLE pgoperator_data(
				LocationID INT,
				Borough VARCHAR(80),
				Zone VARCHAR(80),
				Service_zone VARCHAR(80)
			);
		"""
	)

	create_3rd_database >> create_2nd_database >> sqlalchemy_task() >> pgOperator_task
