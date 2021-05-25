from airflow import DAG
from airflow.operators.mssql_operator import MsSqlOperator
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from airflow.contrib.hooks.ssh_hook import SSHHook

from datetime import datetime, timedelta

from odbc.odbc import *

import pandas as pd
from pandas.tseries.offsets import BDay

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

dag = DAG('interest_sheet',
          schedule_interval='0 0 * * 1-5',
          default_args=default_args,
          max_active_runs=1
          )

edf_us_SSH = SSHHook(
    remote_host='sftp.edfmancapital.com',
    username='AARNA_GMI',
    key_file='/home/airflow/keys/EDFUSPrivateNewSSHOPEN.pem'
)


def resolve_parameter(**context):
    try:
        td = context['dag_run'].conf['to_date']
    except (KeyError, TypeError):
        td = dag.default_args['to_date']
    print(td)
    context['task_instance'].xcom_push(key='to_date', value=td)


def send_daily_report(**context):
    td = context['task_instance'].xcom_pull(task_ids=None, key='to_date')
    ds = pd.read_sql_table("ClientBalanacesCcywise", engine_js)
    ds = ds[ds['DATE'] == td]
    output = 'ClientBalanacesDaily_' + td + '.xlsx'
    writer = pd.ExcelWriter(output)
    ds.to_excel(writer, "ClientBalanacesDaily_", index=False)
    writer.save()

    to = ['brijesh@aarna.capital']

    sendMail(
        "reports@aarna.capital",
        to,
        "ClientBalanacesDaily " + td,
        "Date for ClientBalanacesDaily: " + td,
        [output]
    )


def is_last_day(**context):
    td = context['task_instance'].xcom_pull(task_ids=None, key='to_date')
    today = datetime.strptime(td, '%Y-%m-%d')
    next_day = datetime.strptime(td, '%Y-%m-%d') + BDay(1)
    if (today.month != next_day.month) & (context['dag_run'].external_trigger == False):
        context['task_instance'].xcom_push(key='start_date', value=today.replace(day=1).strftime("%Y-%m-%d"))
        return 'interest_sheet_monthly'
    else:
        return 'success'


def send_monthly_report(**context):
    td = context['task_instance'].xcom_pull(task_ids=None, key='to_date')
    sd = context['task_instance'].xcom_pull(task_ids=None, key='start_date')
    ds = pd.read_sql_table("ClientBalanacesCcywise", engine_js)
    ds = ds[(ds['DATE'] > sd) & (ds['datetime_col'] <= td)]
    output = 'ClientBalanacesMonthly_' + td + '.xlsx'
    writer = pd.ExcelWriter(output)
    ds.to_excel(writer, "ClientBalanacesDaily_", index=False)
    writer.save()

    to = ['brijesh@aarna.capital']

    sendMail(
        "reports@aarna.capital",
        to,
        "ClientBalanacesDaily " + td,
        "Date for ClientBalanacesDaily: " + td,
        [output]
    )


def success():
    print("End of DAG!")


resolve_parameter = PythonOperator(
    task_id='resolve_parameter',
    provide_context=True,
    python_callable=resolve_parameter,
    dag=dag)

interest_sheet_daily = MsSqlOperator(
    task_id='interest_sheet_daily',
    mssql_conn_id='aarna_mssql_201',
    sql="""
    EXEC
        InterestSheetGeneration 
        '{{ task_instance.xcom_pull(task_ids=None, key='to_date') }}',
        '{{ task_instance.xcom_pull(task_ids=None, key='to_date') }}'   
        """,
    database='Jsoham',
    dag=dag)

send_daily_report = PythonOperator(
    task_id='send_daily_report',
    provide_context=True,
    python_callable=send_daily_report,
    dag=dag)

is_last_day = BranchPythonOperator(
    task_id='is_last_day',
    provide_context=True,
    python_callable=is_last_day,
    dag=dag,
)

interest_sheet_monthly = MsSqlOperator(
    task_id='interest_sheet_monthly',
    mssql_conn_id='aarna_mssql_201',
    sql="""
    EXEC
        InterestSheetGeneration 
        '{{ task_instance.xcom_pull(task_ids=None, key='start_date') }}',
        '{{ task_instance.xcom_pull(task_ids=None, key='to_date') }}'   
        """,
    database='Jsoham',
    dag=dag)

send_monthly_report = PythonOperator(
    task_id='send_monthly_report',
    provide_context=True,
    python_callable=send_monthly_report,
    dag=dag)

success = PythonOperator(
    task_id='success',
    python_callable=success,
    dag=dag)

resolve_parameter >> interest_sheet_daily >> send_daily_report >> is_last_day
is_last_day >> interest_sheet_monthly >> send_monthly_report >> success
is_last_day >> success
