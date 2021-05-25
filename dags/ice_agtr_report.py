from airflow import DAG
from airflow.contrib.hooks.ssh_hook import SSHHook
from airflow.operators.mssql_operator import MsSqlOperator
from airflow.contrib.operators.sftp_operator import SFTPOperator
from airflow.operators.python_operator import PythonOperator

from datetime import datetime, timedelta
from odbc.odbc import *

import pandas as pd
from pandas.tseries.offsets import BDay
import os

engine_aarna = getEngine(vlad_201, database_aarna)

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

dag = DAG('ice_agtr_report',
          schedule_interval='30 9 * * 2-6',
          default_args=default_args)

iceSSH = SSHHook(
    remote_host='mft.euclearing.theice.com',
    username='svc-lgtr-cck',
    key_file='/home/airflow/keys/ICEPrivateRSA.ppk'
)


def resolve_parameter(**context):
    try:
        td = context['dag_run'].conf['to_date']
    except (KeyError, TypeError):
        td = dag.default_args['to_date']
    context['task_instance'].xcom_push(key='to_date', value=td)


def download_txt(**context):
    td = context['task_instance'].xcom_pull(task_ids=None, key='to_date')
    ds = pd.read_sql_table("LGTRReport", engine_aarna)
    ds.to_csv('LGTR'+td.replace("-", "")+'.csv', index=False)
    os.rename('LGTR'+td.replace("-", "")+'.csv', 'LGTR'+td.replace("-", "")+'.txt')


def send_email(**context):
    td = context['task_instance'].xcom_pull(task_ids=None, key='to_date')

    to = ['amit@aarna.capital']

    sendMail(
        "reports@aarna.capital",
        to,
        "IceAGTR Report for " + td,
        "File has been submitted successfully to ICE FTP for date: " + td
    )


resolve_parameter = PythonOperator(
    task_id='resolve_parameter',
    provide_context=True,
    python_callable=resolve_parameter,
    dag=dag)

generate_report = MsSqlOperator(
    task_id='generate_report',
    mssql_conn_id='aarna_mssql_201',
    sql="""
    exec
        AGTRICEReporting 
        '{{ task_instance.xcom_pull(task_ids=None, key='to_date') }}'
        """,
    database='AarnaProcess',
    dag=dag)

download_txt = PythonOperator(
    task_id='download_txt',
    provide_context=True,
    python_callable=download_txt,
    dag=dag)

put_report = SFTPOperator(
    task_id="put_report",
    ssh_hook=iceSSH,
    local_filepath="./LGTR" + to_date.replace("-", '') + ".txt",
    remote_filepath="./LGTR" + to_date.replace("-", '') + ".txt",
    operation="put",
    create_intermediate_dirs=True,
    dag=dag
)

send_email = PythonOperator(
    task_id='send_email',
    provide_context=True,
    python_callable=send_email,
    dag=dag)

resolve_parameter >> generate_report >> download_txt >> put_report >> send_email
