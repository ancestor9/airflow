import json
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
import requests
import requests.exceptions as requests_exceptions

dag = DAG(
    dag_id="makefolder_download_rocket_launches",
    description="Download rocket pictures of recently launched rockets.",
    start_date=days_ago(3),
    schedule_interval="@daily",
)

# Create the tmp folder if it doesn't exist
create_tmp_folder = BashOperator(
    task_id="create_tmp_folder",
    bash_command="mkdir -p /tmp",
    dag=dag,
)

# Download launches.json from the internet and save it in the tmp folder
download_launches = BashOperator(
    task_id="download_launches",
    bash_command="curl -o /tmp/launches.json -L 'https://ll.thespacedevs.com/2.0.0/launch/upcoming'",
    dag=dag,
)

# Set up the task dependencies
create_tmp_folder >> download_launches