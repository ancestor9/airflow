from datetime import datetime
from pathlib import Path

import pandas as pd
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

# 서울시 공공데이터(자전거)
url = 'http://openapi.seoul.go.kr:8088/614c50597a616e633131346e4b447142/json/bikeList/1/1000/'

dag = DAG(
    dag_id="0301_unscheduled", 
    start_date=datetime(2023, 1, 1), schedule_interval=None
)

fetch_events = BashOperator(
    task_id="fetch_events",
    bash_command=(
        f"curl -o /tmp/events.json {url}"
    ),
    env={'url': url},       # Pass the 'url' variable to the environment
    dag=dag,
)

def _calculate_stats(input_path, output_path):
    """Calculates event statistics."""

    Path(output_path).parent.mkdir(exist_ok=True)

    events = pd.read_json(input_path)
    display(events.head())
    #stats = events.groupby(["stationName", "stationId"]).size().reset_index()

    stats.to_csv(output_path, index=False)


calculate_stats = PythonOperator(
    task_id="calculate_stats",
    python_callable=_calculate_stats,
    op_kwargs={"input_path": "/tmp/events.json", "output_path": "/tmp/stats.csv"},
    dag=dag,
)

fetch_events >> calculate_stats