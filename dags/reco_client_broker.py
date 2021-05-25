from airflow import DAG
from airflow.operators.mssql_operator import MsSqlOperator
from airflow.operators.python_operator import PythonOperator

from datetime import datetime, timedelta
from odbc.odbc import *

import pandas as pd
from pandas.tseries.offsets import BDay
import logging
import time


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

dag = DAG('reco_client_broker',
          schedule_interval=None,
          default_args=default_args)


def resolve_parameter(**context):
    try:
        td = context['dag_run'].conf['to_date']
    except (KeyError, TypeError):
        td = dag.default_args['to_date']
    print(td)
    context['task_instance'].xcom_push(key='to_date', value=td)


def send_excel(**context):
    td = context['task_instance'].xcom_pull(task_ids=None, key='to_date')
    ds = pd.read_sql_table("RecoClientBrokerTable", engine_aarna)
    output = 'RecoClientBroker_'+td+'.xlsx'
    writer = pd.ExcelWriter(output)
    ds.to_excel(writer, "Reco", index=False)
    writer.save()

    to=[
        'vlad@aarna.capital'
      , 'dima@aarna.capital'
      , 'shakti@aarna.capital'
      , 'brijesh@aarna.capital'
      , 'annaleah@aarna.capital'
    ]

    sendMail(
        "reports@aarna.capital",
        to,
        "RecoClientBroker " + td,
        "Date for Reco: " + td,
        [output]
    )


resolve_parameter = PythonOperator(
    task_id='resolve_parameter',
    provide_context=True,
    python_callable=resolve_parameter,
    dag=dag)

execute_reco = MsSqlOperator(
    task_id='execute_reco',
    mssql_conn_id='aarna_mssql_201',
    sql="""
    EXEC 
        RecoClientBroker
        '{{ task_instance.xcom_pull(task_ids=None, key='to_date') }}'
        """,
    database='AarnaProcess',
    dag=dag)

send_excel = PythonOperator(
    task_id='send_email',
    provide_context=True,
    python_callable=send_excel,
    dag=dag)


resolve_parameter >> execute_reco >> send_excel