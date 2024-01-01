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
    start_date=datetime(2023, 1, 1), schedule_interval=None
)

# mkdir -p /tmp/ 라고해야 permission denied error가 없음
fetch_events = BashOperator(
    task_id="fetch_events",
    bash_command=(
    #    "mkdir -p /tmp/events && "
        "curl -o /tmp/events.json {url}"
    )
)


def _calculate_stats(input_path, output_path):
    """Calculates event statistics."""

    Path(output_path).parent.mkdir(exist_ok=True)

    events = pd.read_json(input_path)
    stats = events.groupby(["stationName", "stationId"]).size().reset_index()

    stats.to_csv(output_path, index=False)


calculate_stats = PythonOperator(
    task_id="calculate_stats",
    python_callable=_calculate_stats,
    op_kwargs={"input_path": "/tmp/events.json", "output_path": "/tmp/stats.csv"},
    dag=dag,
)

fetch_events >> calculate_stats