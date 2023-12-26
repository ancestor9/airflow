import datetime
import pendulum
from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator
from common.common_func import regist2

# copy from example_python_operator DAGS
with DAG(
    dag_id="dags_python_with_op_args",
    schedule="30 6 * * *",
    start_date=pendulum.datetime(2023, 3, 1, tz="Asia/Seoul"),
    catchup=False
) as dag:
    
    regist2_t1 = PythonOperator(
        task_id = 'regist2_t1',
        python_callable = regist2,
        op_args = ['zosanggoo', 'man', 'kr', 'seoul']
        op_args = ['email':ancestor9@naver.om, 'phone': '010-999-9999']
    )
    
    regist2_t1