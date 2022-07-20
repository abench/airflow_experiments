import airflow.utils.dates
import datetime as dt
from airflow import DAG
from airflow.operators.bash import BashOperator

dag = DAG(
        dag_id="download_wiki_statistics",
        start_date = dt.datetime(2022,7,19),
        schedule_interval = "@hourly",
)

get_data = BashOperator(
        task_id = "get_data",
        bash_command=(
            "curl -o /tmp/wikipageviews.gz "
            "https://dumps.wikimedia.org/other/pageviews/"
            "{{ execution_date.year }}/"
            "{{ execution_date.year }}-"
            "{{ '{:02}'.format(execution_date.month) }}/"
            "pageviews-{{ execution_date.year }}"
            "{{ '{:02}'.format(execution_date.month) }}"
            "{{ '{:02}'.format(execution_date.day) }}-"
            "{{ '{:02}'.format(execution_date.hour-1) }}0000.gz"
            ),
        dag=dag,
        )


unzip_data = BashOperator(
        task_id = "unzip_data",
        bash_command = (
            "gunzip --force /tmp/wikipageviews.gz"
            ),
        dag=dag,
        )

extract_data = BashOperator(
        task_id = "extract_data",
        bash_command = (
            "grep \"^uk \" /tmp/wikipageviews | awk '{print $3 \" \" $2}'"            
            "| sort -r|head -n50 > "
            "/tmp/wikipageviews_statistics_{{execution_date.year}}_{{execution_date.month}}_{{execution_date.hour-1}}.txt"
            ),
        dag=dag,
        )

show_statistics = BashOperator(
        task_id = "show_statistics",
        bash_command = (
            "cat /tmp/wikipageviews_statistics_{{execution_date.year}}_{{execution_date.month}}_{{execution_date.hour-1}}.txt"
            ),
        dag = dag,
        )


get_data >> unzip_data >> extract_data >> show_statistics

