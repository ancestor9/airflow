from datetime import datetime
from pathlib import Path

import pandas as pd
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

# 서울시 공공데이터(자전거)
apikey_openapi_seoel_go_kr = '614c50597a616e633131346e4b447142'
url = f'http://openapi.seoul.go.kr:8088/{apikey_openapi_seoel_go_kr}/json/bikeList/1/1000/'

dag = DAG(
    dag_id="0301_unscheduled", 
    start_date=datetime(2016, 1, 1), schedule_interval=None
)

fetch_events = BashOperator(
    task_id="fetch_events",
    bash_command=(
        "mkdir -p /data/events && "
        "curl -o /data/events.json {url}"
    )
)


def _calculate_stats(input_path, output_path):
    """Calculates event statistics."""

    Path(output_path).parent.mkdir(exist_ok=True)

    events = pd.read_json(input_path).iloc[3:, 4:]
    stats = events.groupby(["stationName", "stationId"]).size().reset_index()

    stats.to_csv(output_path, index=False)


calculate_stats = PythonOperator(
    task_id="calculate_stats",
    python_callable=_calculate_stats,
    op_kwargs={"input_path": "/data/events.json", "output_path": "/data/stats.csv"},
    dag=dag,
)

fetch_events >> calculate_stats