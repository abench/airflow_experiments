import airflow.utils.dates
import datetime as dt
import os
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
import sqlite3

data_directory = "/opt/data"
db_file_path = "/opt/data/wikipageviews.sqlite"
file_list = os.listdir(data_directory)
insert_sql = """
        insert into page_views (views_num, page_name, view_date, view_time) values( ?, ? ,?,?);
"""

select_sql = """
        select * from page_views;
"""




def _create_date_string(filename:str)->str:
    splitted_file_name = os.path.splitext(os.path.basename(filename))[0].split("_")
    print(splitted_file_name)
    # date time format for sqlite YYYY-MM-DD "{04:}-{02:}-{02:}".
    date_string = "{:04}-{:02}-{:02}".format(int(splitted_file_name[2]),
                                                      int(splitted_file_name[3]),
                                                      int(splitted_file_name[4]),
                                                      int(splitted_file_name[5]))
    print(date_string)
    return date_string

def _create_time_string(filename:str)->str:
    splitted_file_name = os.path.splitext(os.path.basename(filename))[0].split("_")
    print(splitted_file_name)
    # date time format for sqlite YYYY-MM-DD HH:MM "{04:}_{02:}_{02:} {02:}".
    time_string = "{:02}:00".format(int(splitted_file_name[5]))
    print(time_string)
    return time_string


dag = DAG(
        dag_id="download_wiki_statistics",
        start_date = dt.datetime(2022,7,19),
        schedule_interval = "@hourly",
)

get_data = BashOperator(
        task_id = "get_data",
        bash_command=(
            "curl -o /opt/data/wikipageviews.gz "
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
            "gunzip --force /opt/data/wikipageviews.gz"
            ),
        dag=dag,
        )

extract_data = BashOperator(
        task_id = "extract_data",
        bash_command = (
            "grep \"^uk \\|^uk.m \" /opt/data/wikipageviews | awk '{print $3 \" \" $2}'"            
            "| sort -r|head -n50 > "
            "/opt/data/wikipageviews_statistics_{{execution_date.year}}_{{execution_date.month}}_{{execution_date.day}}_{{execution_date.hour-1}}.txt"
            ),
        dag=dag,
        )


def _insert_data(**kwargs):
    input_file_name = kwargs["input_file"]
    date_string = _create_date_string(input_file_name)
    time_string = _create_time_string(input_file_name)
    try:
        conn = sqlite3.connect(db_file_path)
        cursor = conn.cursor()
    except:
        print(f'Database {db_file_path} connection error.')
    try:
        with open(input_file_name, "r") as f:
            file_data = f.readlines()
            for line in file_data:
                data_record = line.strip().split(" ")
                data_record.append(date_string)
                data_record.append(time_string)
                print(data_record)
                cursor.execute(insert_sql, data_record)
                print(cursor.rowcount)
        conn.commit()
        conn.close()
    except:
        print(f'File {input_file_name} open error ')


insert_data = PythonOperator(
    task_id="_insert_data",
    python_callable=_insert_data,
    op_kwargs={
        "input_file": "/opt/data/wikipageviews_statistics_{{execution_date.year}}_{{execution_date.month}}_{{execution_date.day}}_{{execution_date.hour-1}}.txt"
    },
    dag=dag,

)

show_statistics = BashOperator(
        task_id = "show_statistics",
        bash_command = (
            "cat /opt/data/wikipageviews_statistics_{{execution_date.year}}_{{execution_date.month}}_{{execution_date.day}}_{{execution_date.hour-1}}.txt"
            ),
        dag=dag,
        )


get_data >> unzip_data >> extract_data >> insert_data >> show_statistics

