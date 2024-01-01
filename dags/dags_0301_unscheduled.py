from datetime import datetime
from pathlib import Path

import pandas as pd
import json
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
# Read JSON data from the file
    with open(input_path, 'r') as file:
        data = json.load(file)
    # Extract relevant data
    row_data = data['rentBikeStatus']['row']
    # Create DataFrame
    df = pd.DataFrame(row_data)
    # Group by and calculate statistics
    stats = df.groupby(["stationName", "stationId"]).size().reset_index()
    # Save to CSV
    stats.to_csv(output_path, index=False)


calculate_stats = PythonOperator(
    task_id="calculate_stats",
    python_callable=_calculate_stats,
    op_kwargs={"input_path": "/tmp/events.json", "output_path": "/tmp/stats.csv"},
    dag=dag,
)

fetch_events >> calculate_stats