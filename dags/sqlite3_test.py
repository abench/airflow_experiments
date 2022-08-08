from airflow.models import DAG
from airflow.providers.sqlite.operators.sqlite import SqliteOperator
from datetime import datetime

default_args = {
    'start_date' : datetime(2020,1,1)

}

with DAG('sqlite3_test', schedule_interval='@daily', default_args=default_args, catchup=False) as dag:

    dump_table = SqliteOperator(
        task_id='creating_table',
        sqlite_conn_id='db_wikipagewiews',
        sql='''
        insert into page_views (view_id, page_name, view_date, views_num) values (2, 'test','2022-08-03 17:20',1);
        '''
        )
