from airflow import DAG
from airflow.contrib.hooks.ftp_hook import FTPHook
from airflow.exceptions import AirflowNotFoundException
from airflow.operators.python_operator import PythonOperator

from datetime import datetime, timedelta

from odbc.odbc import *

import pandas as pd
from pandas.tseries.offsets import BDay
import logging
import time

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

dag = DAG('fcstone_data_upload',
          schedule_interval='0 3 * * 2-6',
          default_args=default_args,
          max_active_runs=1
          )


def lookup_fcstone_st4(**context):
    try:
        ending = context['dag_run'].conf['to_date']
    except (KeyError, TypeError):
        ending = dag.default_args['to_date']
    context['ti'].xcom_push(key='to_date', value=ending)

    filename = 'CSVST4F1_' + ending.replace("-", "") + '.csv'
    hook = FTPHook(ftp_conn_id='fcstone')
    initial_time = datetime.now() + timedelta(hours=6)

    while datetime.now() < initial_time:
        files = hook.list_directory('/')
        if filename in files:
            logging.info("File {} has been found at: {}.".format(filename, datetime.now()))
            context['ti'].xcom_push(key='filename_fcstone_st4', value=filename)
            return "Task has been completed."
        logging.warning("File {} has not yet been delivered at: {}.".format(filename, datetime.now()))
        time.sleep(300)

    raise AirflowNotFoundException("File {} has not been revealed during last 6 hours. Notification has been sent."
                                   .format(filename))


def lookup_fcstone_pos(**context):
    try:
        ending = context['dag_run'].conf['to_date']
    except (KeyError, TypeError):
        ending = dag.default_args['to_date']
    context['ti'].xcom_push(key='to_date', value=ending)

    filename = 'CSVPOSF1_' + ending.replace("-", "") + '.csv'
    hook = FTPHook(ftp_conn_id='fcstone')
    initial_time = datetime.now() + timedelta(hours=6)

    while datetime.now() < initial_time:
        files = hook.list_directory('/')
        if filename in files:
            logging.info("File {} has been found at: {}.".format(filename, datetime.now()))
            context['ti'].xcom_push(key='filename_fcstone_pos', value=filename)
            return "Task has been completed."
        logging.warning("File {} has not yet been delivered at: {}.".format(filename, datetime.now()))
        time.sleep(300)

    raise AirflowNotFoundException("File {} has not been revealed during last 6 hours. Notification has been sent."
                                   .format(filename))


def lookup_fcstone_mny(**context):
    try:
        ending = context['dag_run'].conf['to_date']
    except (KeyError, TypeError):
        ending = dag.default_args['to_date']
    context['ti'].xcom_push(key='to_date', value=ending)

    filename = 'CSVMNYF1_' + ending.replace("-", "") + '.csv'
    hook = FTPHook(ftp_conn_id='fcstone')
    initial_time = datetime.now() + timedelta(hours=6)

    while datetime.now() < initial_time:
        files = hook.list_directory('/')
        if filename in files:
            logging.info("File {} has been found at: {}.".format(filename, datetime.now()))
            context['ti'].xcom_push(key='filename_fcstone_mny', value=filename)
            return "Task has been completed."
        logging.warning("File {} has not yet been delivered at: {}.".format(filename, datetime.now()))
        time.sleep(300)

    raise AirflowNotFoundException("File {} has not been revealed during last 6 hours. Notification has been sent."
                                   .format(filename))


def get_fcstone_st4(**context):
    filename = context['ti'].xcom_pull(task_ids=None, key='filename_fcstone_st4')
    hook = FTPHook(ftp_conn_id='fcstone')
    hook.retrieve_file(filename, filename)
    return "FCStone ST4 files have been successfully downloaded."


def get_fcstone_pos(**context):
    filename = context['ti'].xcom_pull(task_ids=None, key='filename_fcstone_pos')
    hook = FTPHook(ftp_conn_id='fcstone')
    hook.retrieve_file(filename, filename)
    return "FCStone POS files have been successfully downloaded."


def get_fcstone_mny(**context):
    filename = context['ti'].xcom_pull(task_ids=None, key='filename_fcstone_mny')
    hook = FTPHook(ftp_conn_id='fcstone')
    hook.retrieve_file(filename, filename)
    return "FCStone MNY files have been successfully downloaded."


def insert_fcstone_st4(**context):
    filename = context['ti'].xcom_pull(task_ids=None, key='filename_fcstone_st4')
    input_data = pd.read_csv(
        filename,
        header=0,
        low_memory=False,
        dtype=str,
        na_filter=False
    )
    td = context['ti'].xcom_pull(task_ids=None, key='to_date')

    logging.info("Deleting rows for {} from database...".format(td))
    cur = getCursor(vlad_201, database_aarna)
    cur.execute("DELETE FROM GetFCESettlement WHERE RecDate = '{}'".format(td))
    cur.close()

    input_data['RecDate'] = td
    logging.info("Starting FCStone {} upload to a database...".format(filename))
    cursor = getCursor(vlad_201, database_aarna)
    input_data.to_sql('GetFCESettlement',
                      engine_aarna,
                      index=False,
                      if_exists="append",
                      schema="dbo")
    logging.info("Success!")
    cursor.close()
    return filename


def insert_fcstone_pos(**context):
    filename = context['ti'].xcom_pull(task_ids=None, key='filename_fcstone_pos')
    input_data = pd.read_csv(
        filename,
        header=0,
        low_memory=False,
        dtype=str,
        na_filter=False
    )
    td = context['ti'].xcom_pull(task_ids=None, key='to_date')

    logging.info("Deleting rows for {} from database...".format(td))
    cur = getCursor(vlad_201, database_aarna)
    cur.execute("DELETE FROM FCSTONEPosition WHERE RecDate = '{}'".format(td))
    cur.close()

    input_data['RecDate'] = td
    logging.info("Starting FCStone OpenPosition {} upload to a database...".format(filename))
    cursor = getCursor(vlad_201, database_aarna)
    input_data.to_sql('FCSTONEPosition',
                      engine_aarna,
                      index=False,
                      if_exists="append",
                      schema="dbo")
    logging.info("Success!")
    cursor.close()
    return filename


def insert_fcstone_mny(**context):
    filename = context['ti'].xcom_pull(task_ids=None, key='filename_fcstone_mny')
    input_data = pd.read_csv(
        filename,
        header=0,
        low_memory=False,
        dtype=str,
        encoding='cp1252'
    )
    td = context['ti'].xcom_pull(task_ids=None, key='to_date')

    logging.info("Deleting rows for {} from database...".format(td))
    cur = getCursor(vlad_201, database_aarna)
    cur.execute("DELETE FROM FcstoneBalances WHERE InsertingDate = '{}'".format(td))
    cur.close()

    input_data['InsertingDate'] = td
    logging.info("Starting FCStone {} upload to a database...".format(filename))
    cursor = getCursor(vlad_201, database_aarna)
    input_data.to_sql('FcstoneBalances',
                      engine_aarna,
                      index=False,
                      if_exists="append",
                      schema="dbo")
    logging.info("Success!")
    cursor.close()
    return "FCStone MNY files have been successfully uploaded to a database."


lookup_fcstone_st4 = PythonOperator(
    task_id='lookup_fcstone_st4',
    provide_context=True,
    python_callable=lookup_fcstone_st4,
    dag=dag)

lookup_fcstone_pos = PythonOperator(
    task_id='lookup_fcstone_pos',
    provide_context=True,
    python_callable=lookup_fcstone_pos,
    dag=dag)

lookup_fcstone_mny = PythonOperator(
    task_id='lookup_fcstone_mny',
    provide_context=True,
    python_callable=lookup_fcstone_mny,
    dag=dag)

get_fcstone_st4 = PythonOperator(
    task_id='get_fcstone_st4',
    provide_context=True,
    execution_timeout=timedelta(minutes=10),
    python_callable=get_fcstone_st4,
    dag=dag)

get_fcstone_pos = PythonOperator(
    task_id='get_fcstone_pos',
    provide_context=True,
    execution_timeout=timedelta(hours=1),
    python_callable=get_fcstone_pos,
    dag=dag)

get_fcstone_mny = PythonOperator(
    task_id='get_fcstone_mny',
    provide_context=True,
    execution_timeout=timedelta(minutes=10),
    python_callable=get_fcstone_mny,
    dag=dag)

insert_fcstone_st4 = PythonOperator(
    task_id='insert_fcstone_st4',
    provide_context=True,
    execution_timeout=timedelta(minutes=10),
    python_callable=insert_fcstone_st4,
    dag=dag)

insert_fcstone_pos = PythonOperator(
    task_id='insert_fcstone_pos',
    provide_context=True,
    execution_timeout=timedelta(hours=1),
    python_callable=insert_fcstone_pos,
    dag=dag)

insert_fcstone_mny = PythonOperator(
    task_id='insert_fcstone_mny',
    provide_context=True,
    execution_timeout=timedelta(minutes=10),
    python_callable=insert_fcstone_mny,
    dag=dag)


lookup_fcstone_st4 >> get_fcstone_st4 >> insert_fcstone_st4
lookup_fcstone_pos >> get_fcstone_pos >> insert_fcstone_pos
lookup_fcstone_mny >> get_fcstone_mny >> insert_fcstone_mny