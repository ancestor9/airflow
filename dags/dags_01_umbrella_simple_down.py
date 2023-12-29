import airflow.utils.dates
from airflow import DAG
from airflow.operators.bash import BashOperator

dag = DAG(
    dag_id="02_umbrella_simple_makedir",
    start_date=airflow.utils.dates.days_ago(5),
    schedule_interval="@daily",
)

# [START howto_operator_bash]
bash_t1 = BashOperator(
    task_id="bash_t1",
    bash_command="echo who are you",
    dag=dag,
)

bash_t2 = BashOperator(
    task_id="bash_t2",
    bash_command="echo $HOSTNAME",
    dag=dag,
)

# Create the tmp folder if it doesn't exist
create_tmp_folder = BashOperator(
    task_id="create_tmp_folder",
    bash_command="mkdir -p ./tmp",
    dag=dag,
)

bash_t1 >> bash_t2 >> create_tmp_folder