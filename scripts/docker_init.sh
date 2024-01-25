#!/usr/bin/env bash
cd $AIRFLOW_HOME
airflow db init
airflow users create -e "admin@airflow.com" -f "airflow" -l "airflow" -p "airflow" -r "Admin" -u "airflow"

airflow connections add rest_api_conn --conn-type 'http' --conn-host 'https://gorest.co.in/public/v2'

# airflow connections add --conn-type 'fs' --conn-extra '{"path":"/opt/airflow/dags/files"}' forex_path

airflow connections add --conn-uri 'https://gist.github.com/' forex_api

# airflow connections add 'spark_conn' --conn-type 'spark' --conn-host 'spark://spark-master' --conn-port 7077

# airflow connections add 'slack_conn' --conn-uri 'https://hooks.slack.com/services/T06F573721J/B06F2L3DB8T/oqBmccKzwP7iKlk6a2'