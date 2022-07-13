import json
import pathlib
import xmltodict

import airflow
import requests
import requests.exceptions as request_exceptions
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

dag = DAG(
    dag_id="download_rss_feed",
    start_date=airflow.utils.dates.days_ago(14),
    schedule_interval="@daily",
)

download_rss_file = BashOperator(
    task_id="download_rsslaunches",
    bash_command="curl -o /tmp/rss.xml -L 'https://tokar.ua/feed'",
    dag=dag,
)


def _xml_to_json():
    # Download all pictures in launches.json
    with open("/tmp/rss.xml") as fi:
        with open("/tmp/rss.json", "w") as fo:
            json.dump(fi.read(), fo)


xml_to_json = PythonOperator(
    task_id="xml_to_json",
    python_callable=_xml_to_json,
    dag=dag,
)

notify = BashOperator(
    task_id="notify",
    bash_command='cat /tmp/rss.json',
    dag=dag,
)

download_rss_file >> xml_to_json >> notify

