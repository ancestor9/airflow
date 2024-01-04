from urllib import request
import airflow.utils.dates
from airflow import DAG
from airflow.operators.python import PythonOperator

dag = DAG(
    dag_id="04_listing_05",
    start_date=airflow.utils.dates.days_ago(2),
    schedule_interval="@hourly",
)


def _get_data(execution_date):
    year, month, day, hour, *_ = execution_date.timetuple()
    hour -= 3
    url = (
        "https://dumps.wikimedia.org/other/pageviews/"
        f"{year}/{year}-{month:0>2}/pageviews-{year}{month:0>2}{day:0>2}-{hour:0>2}0000.gz"
    )
    output_path = "/tmp/wikipageviews.gz"
    print(f"Constructed URL: {url}")
    request.urlretrieve(url, output_path)


get_data = PythonOperator(task_id="get_data", 
                          python_callable=_get_data, 
                          dag=dag)