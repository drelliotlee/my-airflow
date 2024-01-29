# Things that I got stuck on, frustrated me for hours, and what I learned
Jan 18 - Postgres wants double quotes (`"` not `'`) around names to preseve case, otherwise it will automatically change everything to lowercase.

Jan 19 - For some reason, you cannot use `psql` inside the postgres container. You need to run it from inside on of the airflow containers.

Jan 20 - The most reliable way to make docker reflect changes in your `dockerfile` is to use the `-t` flag and give your image an explicit name

Jan 21 - The most reliable way to make airflow reflect changes in your DAG is to change the DAG name.

Jan 22 - Bucket names in google cloud storage must be GLOBALLY unique across ALL of GCS, not just your account and your projects

Jan 23 - Adding airflow connections via an environment variable or an entrypoint shell script are too mysterious and don't always work. The most reliable way is to simply create `BashOperator` tasks at the beginning of your DAG which execute the airflow configuration commands.

Jan 24 - How to properly encapsulate your entire project within a virtual environment AND how to make VScode's errors and warnings reflect your virtual environment, not your local python.

Jan 25 - Connection URIs cannot have `/` or `:` so use `urlllib.parse` to convert to `%3A` and `%2F`