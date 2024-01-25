# Things that got stuck on, and what I learned
Jan 21 - The most reliable way to make airflow reflect changes in your DAG is to change the DAG name.
Jan 22 - Bucket names in google cloud storage must be unique across ALL of GCS, not just your account and your projects
Jan 23 - Adding airflow connections via an environment variable or an entrypoint shell script are too mysterious and don't always work. The most reliable way is to simply create BashOperator tasks at the beginning of your DAG which execute the airflow configuration commands.
Jan 24 - How to properly encapsulate your entire project within a virtual environment AND how to make VScode's errors and warnings reflect your virtual environment, not your local python.
