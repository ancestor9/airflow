from datetime import datetime
from pathlib import Path

import pandas as pd
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

dag = DAG(
    dag_id="0301_unscheduled", start_date=datetime(2020, 1, 1), schedule_interval=None
)

# xml 파일이지만 맨 뒤에 '&_type=json'을 붙이면 json으로 변환 ! - No
url = "http://openapi.seoul.go.kr:8088/614c50597a616e633131346e4b447142/xml/CardSubwayStatsNew/1/1000/20161101"

fetch_events = BashOperator(
    task_id="fetch_events",
    bash_command=(
        "mkdir -p /data/events && "
        # "curl -o /data/events.json {url}"
        "curl {url} | xmlstarlet fo --omit-decl --noindent -R -H -J > /data/events.json"
    ),
    dag=dag,
)


def _calculate_stats(input_path, output_path):
    """Calculates event statistics."""

    Path(output_path).parent.mkdir(exist_ok=True)

    events = pd.read_json(input_path).iloc[3:, 4:]
    stats = events.groupby(["LINE_NUM", "SUB_STA_NM"]).size().reset_index()

    stats.to_csv(output_path, index=False)


calculate_stats = PythonOperator(
    task_id="calculate_stats",
    python_callable=_calculate_stats,
    op_kwargs={"input_path": "/data/events.json", "output_path": "/data/stats.csv"},
    dag=dag,
)

fetch_events >> calculate_stats