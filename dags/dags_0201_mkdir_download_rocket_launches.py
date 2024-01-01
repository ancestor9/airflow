import airflow.utils.dates
from airflow import DAG
from airflow.operators.bash import BashOperator

dag = DAG(
    dag_id="0201_mkdir_download_rocket_launches",
    start_date=airflow.utils.dates.days_ago(5),
    schedule_interval="@daily",
)

# Create the tmp folder if it doesn't exist
create_tmp_folder = BashOperator(
    task_id="create_tmp_folder",
    bash_command="mkdir -p ./tmp",
    dag=dag,
)

download_launches = BashOperator(
    task_id="download_launches",
    bash_command="curl -o /tmp/launches.json -L 'https://ll.thespacedevs.com/2.0.0/launch/upcoming'",  # noqa: E501
    dag=dag,
)




create_tmp_folder >> download_launches