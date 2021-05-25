from airflow import DAG
from airflow.operators.python_operator import PythonOperator

from datetime import datetime, timedelta

from odbc.odbc import *
from auxiliary.mail_scrape import *

from pandas.tseries.offsets import BDay
import os
import pandas as pd
import shutil

today = pd.datetime.today() - BDay(1)
to_date = today.date().strftime("%Y-%m-%d")

default_args = {
    'owner': 'Akatov',
    'depends_on_past': False,
    'start_date': datetime(2020, 1, 1, 0, 0, 0),
    'email': ['vlad@aarna.capital'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
    'to_date': to_date
}

dag = DAG('php_data_upload',
          schedule_interval='0 5 * * 2-6',
          default_args=default_args,
          max_active_runs=1
          )

folder = '/php/'


def download_php_attachments(**context):
    try:
        ending = context['dag_run'].conf['to_date']
    except (KeyError, TypeError):
        ending = dag.default_args['to_date']
    context['ti'].xcom_push(key='to_date', value=ending)

    today_formatted = (datetime.strptime(ending, '%Y-%m-%d')).strftime("%d-%b-%Y")
    tomorrow_formatted = (datetime.strptime(ending, '%Y-%m-%d') + BDay(1)).strftime("%d-%b-%Y")

    if not os.path.exists(folder):
        os.mkdir(folder)

    download_attachments(folder, "pfpl_edp@phillip.com.sg", today_formatted, tomorrow_formatted)


def upload_php_pos(**context):
    db_cols = list(pd.read_sql_table("PhilipPosition", engine_aarna))
    td = context['ti'].xcom_pull(task_ids=None, key='to_date')
    yesterday = (datetime.strptime(td, '%Y-%m-%d') - BDay(1)).strftime('%Y-%m-%d')
    ds = pd.read_csv(folder + 'PHILLIP_Open_Position_ATL_{}.csv'.format(yesterday.replace('-', '')),
                     low_memory=False,
                     dtype=str)
    ds['RecDate'] = yesterday
    ds \
        .rename(columns=dict(zip(ds.columns, db_cols))) \
        .to_sql('PhilipPosition',
                engine_aarna,
                index=False,
                if_exists="append",
                schema="dbo")


def upload_php_trades(**context):
    db_cols = list(pd.read_sql_table("GetPhilipSettlement", engine_aarna))
    td = context['ti'].xcom_pull(task_ids=None, key='to_date')
    yesterday = (datetime.strptime(td, '%Y-%m-%d') - BDay(1)).strftime('%Y-%m-%d')
    ds = pd.read_csv(folder + 'PHILLIP_CloseOut_Trade_ATL_{}.csv'.format(yesterday.replace('-', '')),
                     low_memory=False,
                     dtype=str)
    ds['RecDate'] = yesterday
    ds \
        .rename(columns=dict(zip(ds.columns, db_cols))) \
        .to_sql('GetPhilipSettlement',
                engine_aarna,
                index=False,
                if_exists="append",
                schema="dbo")


def rm_r():
    shutil.rmtree(folder)


download_php_attachments = PythonOperator(
    task_id='download_php_attachments',
    provide_context=True,
    python_callable=download_php_attachments,
    dag=dag)

upload_php_pos = PythonOperator(
    task_id='upload_php_pos',
    provide_context=True,
    python_callable=upload_php_pos,
    dag=dag)

upload_php_trades = PythonOperator(
    task_id='upload_php_trades',
    provide_context=True,
    python_callable=upload_php_trades,
    dag=dag)

rm_r = PythonOperator(
    task_id='rm_r',
    python_callable=rm_r,
    dag=dag)

download_php_attachments >> upload_php_pos
download_php_attachments >> upload_php_trades
[upload_php_pos, upload_php_trades] >> rm_r
