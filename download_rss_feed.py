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
    task_id="download_rss_file",
    bash_command="curl -o /tmp/rss_{{ds}}.xml -L 'https://tokar.ua/feed'",
    dag=dag,
)


def _xml_to_json(**kwargs):
    # Download all pictures in launches.json
    input_file_name = kwargs["input_file"]
    output_file_name = kwargs["output_file"]
    with open(input_file_name) as fi:
        with open(output_file_name, "w") as fo:
            json.dump(fi.read(), fo)


xml_to_json = PythonOperator(
    task_id="xml_to_json",
    python_callable=_xml_to_json,
    op_kwargs={
        "input_file":"/tmp/rss_{{ ds }}.xml",
        "output_file":"/tmp/rss_{{ ds }}.json",
    },
    dag=dag,
)

notify = BashOperator(
    task_id="notify",
    bash_command='cat /tmp/rss_{{ds}}.json',
    dag=dag,
)

download_rss_file >> xml_to_json >> notify

