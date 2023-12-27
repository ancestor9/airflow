import datetime
import pendulum
from airflow.models.dag import DAG
from airflow.decorators import task

# copy from example_python_operator DAGS
with DAG(
    dag_id="dags_python_show_templates",
    schedule="30 9 * * *",
    start_date=pendulum.datetime(2023, 10, 1, tz="Asia/Seoul"),
    catchup=True
) as dag:

    @task(task_id="python_task")
    def show_templates(**kwargs):
        from pprint import pprint
        pprint(kwargs)

    @task(task_id="downstream_task")
    def downstream_task(**kwargs):
        print("Downstream task executed.")
        
    # Set up the task dependencies
    show_templates >> downstream_task
