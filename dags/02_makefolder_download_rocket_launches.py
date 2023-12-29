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

def _get_pictures():
    # Download all pictures in launches.json
    with open("/tmp/launches.json") as f:
        launches = json.load(f)
        image_urls = [launch["image"] for launch in launches["results"]]
        for image_url in image_urls:
            try:
                response = requests.get(image_url)
                image_filename = image_url.split("/")[-1]
                target_file = f"/tmp/images/{image_filename}"
                with open(target_file, "wb") as f:
                    f.write(response.content)
                print(f"Downloaded {image_url} to {target_file}")
            except requests_exceptions.MissingSchema:
                print(f"{image_url} appears to be an invalid URL.")
            except requests_exceptions.ConnectionError:
                print(f"Could not connect to {image_url}.")

get_pictures = PythonOperator(
    task_id="get_pictures", python_callable=_get_pictures, dag=dag
)

# Other tasks can be added as needed...

# Set up the task dependencies
create_tmp_folder >> download_launches >> get_pictures