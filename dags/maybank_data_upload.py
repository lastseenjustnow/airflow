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

dag = DAG('maybank_data_upload',
          schedule_interval='0 5 * * 1-5',
          default_args=default_args,
          max_active_runs=1
          )

folder = '/maybank/'


def download_maybank_attachments(**context):
    try:
        ending = context['dag_run'].conf['to_date']
    except (KeyError, TypeError):
        ending = dag.default_args['to_date']
    context['ti'].xcom_push(key='to_date', value=ending)

    today_formatted = (datetime.strptime(ending, '%Y-%m-%d') + BDay(1)).strftime("%d-%b-%Y")
    tomorrow_formatted = (datetime.strptime(ending, '%Y-%m-%d') + BDay(2)).strftime("%d-%b-%Y")

    if not os.path.exists(folder):
        os.mkdir(folder)

    download_attachments(folder, "pb-statement@maybank-ke.com.sg", today_formatted, tomorrow_formatted)


def upload_maybank_bal(**context):
    db_cols = list(pd.read_sql_table("MayBankBalances", engine_aarna))
    yesterday = context['ti'].xcom_pull(task_ids=None, key='to_date')
    ds = pd.read_csv(folder + 'KEUK440029_BALANCE_{}.csv'.format(yesterday.replace('-', '')),
                     low_memory=False,
                     dtype=str)
    ds['InsertingDate'] = yesterday
    ds \
        .rename(columns=dict(zip(ds.columns, db_cols))) \
        .to_sql('MayBankBalances',
                engine_aarna,
                index=False,
                if_exists="append",
                schema="dbo")


def rm_r():
    shutil.rmtree(folder)


download_maybank_attachments = PythonOperator(
    task_id='download_maybank_attachments',
    provide_context=True,
    python_callable=download_maybank_attachments,
    dag=dag)

upload_maybank_bal = PythonOperator(
    task_id='upload_maybank_bal',
    provide_context=True,
    python_callable=upload_maybank_bal,
    dag=dag)

rm_r = PythonOperator(
    task_id='rm_r',
    python_callable=rm_r,
    dag=dag)

download_maybank_attachments >> upload_maybank_bal >> rm_r
