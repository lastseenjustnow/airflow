from airflow import DAG
from airflow.contrib.hooks.ftp_hook import FTPHook
from airflow.contrib.hooks.ssh_hook import SSHHook
from airflow.exceptions import AirflowNotFoundException
from airflow.operators.python_operator import PythonOperator
from airflow.contrib.operators.sftp_operator import SFTPOperator

from datetime import datetime, timedelta

from odbc.odbc import *

import numpy as np
import pandas as pd
from pandas.tseries.offsets import BDay
import logging
import time
import paramiko
import re

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

dag = DAG('edfuk_data_upload',
          schedule_interval='0 3 * * 2-6',
          default_args=default_args,
          max_active_runs=1
          )


def lookup_edf_uk_trades(**context):
    try:
        ending = context['dag_run'].conf['to_date']
    except (KeyError, TypeError):
        ending = dag.default_args['to_date']
    context['ti'].xcom_push(key='to_date', value=ending)

    filename = ending.replace("-", "") + '_CoreTrades_10148190.csv'

    sftpURL = ''
    sftpUser = ''
    sftpPass = ''

    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())

    initial_time = datetime.now() + timedelta(hours=6)

    while datetime.now() < initial_time:
        ssh.connect(sftpURL, username=sftpUser, password=sftpPass)
        ftp = ssh.open_sftp()
        files = ftp.listdir()
        if filename in files:
            logging.info("File {} has been found at: {}.".format(filename, datetime.now()))
            context['ti'].xcom_push(key='filename_edf_uk_trades', value=filename)
            ssh.close()
            return ending
        logging.warning("File {} has not yet been delivered at: {}.".format(filename, datetime.now()))
        ssh.close()
        time.sleep(300)

    raise AirflowNotFoundException("File {} has not been revealed during last 6 hours. Notification has been sent."
                                   .format(filename))


def lookup_edf_uk_pos(**context):
    try:
        ending = context['dag_run'].conf['to_date']
    except (KeyError, TypeError):
        ending = dag.default_args['to_date']
    context['ti'].xcom_push(key='to_date', value=ending)

    filename = ending.replace("-", "") + '_CorePosition_10148190.csv'

    sftpURL = ''
    sftpUser = ''
    sftpPass = ''

    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())

    initial_time = datetime.now() + timedelta(hours=6)

    while datetime.now() < initial_time:
        ssh.connect(sftpURL, username=sftpUser, password=sftpPass)
        ftp = ssh.open_sftp()
        files = ftp.listdir()
        if filename in files:
            logging.info("File {} has been found at: {}.".format(filename, datetime.now()))
            context['ti'].xcom_push(key='filename_edf_uk_pos', value=filename)
            ssh.close()
            return ending
        logging.warning("File {} has not yet been delivered at: {}.".format(filename, datetime.now()))
        ssh.close()
        time.sleep(300)

    raise AirflowNotFoundException("File {} has not been revealed during last 6 hours. Notification has been sent."
                                   .format(filename))


def lookup_edf_uk_bal(**context):
    try:
        ending = context['dag_run'].conf['to_date']
    except (KeyError, TypeError):
        ending = dag.default_args['to_date']
    context['ti'].xcom_push(key='to_date', value=ending)

    filename = ending.replace("-", "") + '_CoreBalance_10148190.csv'

    sftpURL = ''
    sftpUser = ''
    sftpPass = ''

    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())

    initial_time = datetime.now() + timedelta(hours=6)

    while datetime.now() < initial_time:
        ssh.connect(sftpURL, username=sftpUser, password=sftpPass)
        ftp = ssh.open_sftp()
        files = ftp.listdir()
        if filename in files:
            logging.info("File {} has been found at: {}.".format(filename, datetime.now()))
            context['ti'].xcom_push(key='filename_edf_uk_bal', value=filename)
            ssh.close()
            return ending
        logging.warning("File {} has not yet been delivered at: {}.".format(filename, datetime.now()))
        ssh.close()
        time.sleep(300)

    raise AirflowNotFoundException("File {} has not been revealed during last 6 hours. Notification has been sent."
                                   .format(filename))


def insert_edf_uk_trades(**context):
    filename = context['ti'].xcom_pull(task_ids=None, key='filename_edf_uk_trades')
    input_data = pd.read_csv(
        filename,
        header=0,
        low_memory=False,
        dtype=str,
        na_filter=False
    )
    input_data['RecDate'] = context['ti'].xcom_pull(task_ids=None, key='to_date')
    logging.info("Starting EDF UK trades file {} upload to a database...".format(filename))
    cursor = getCursor(vlad_201, database_aarna)
    input_data.to_sql('GetEDFUKSettlement',
                      engine_aarna,
                      index=False,
                      if_exists="append",
                      schema="dbo")
    logging.info("Success!")
    cursor.close()
    return filename


def insert_edf_uk_pos(**context):
    filename = context['ti'].xcom_pull(task_ids=None, key='filename_edf_uk_pos')
    input_data = pd.read_csv(
        filename,
        header=0,
        low_memory=False,
        dtype=str,
        na_filter=False
    )
    input_data['RECDATE'] = context['ti'].xcom_pull(task_ids=None, key='to_date')
    logging.info("Starting EDF UK OpenPosition {} upload to a database...".format(filename))
    cursor = getCursor(vlad_201, database_aarna)
    input_data.to_sql('EDFUKPosition',
                      engine_aarna,
                      index=False,
                      if_exists="append",
                      schema="dbo")
    logging.info("Success!")
    cursor.close()
    return filename


def insert_edf_uk_bal(**context):
    filename = context['ti'].xcom_pull(task_ids=None, key='filename_edf_uk_bal')
    input_data = pd.read_csv(
        filename,
        header=0,
        low_memory=False,
        dtype=str,
        na_filter=False
    )
    input_data['InsertingDate'] = context['ti'].xcom_pull(task_ids=None, key='to_date')
    logging.info("Starting EDF UK trades file {} upload to a database...".format(filename))
    cursor = getCursor(vlad_201, database_aarna)
    input_data.to_sql('EDFUKBalances',
                      engine_aarna,
                      index=False,
                      if_exists="append",
                      schema="dbo")
    logging.info("Success!")
    cursor.close()
    return filename


lookup_edf_uk_trades = PythonOperator(
    task_id='lookup_edf_uk_trades',
    provide_context=True,
    python_callable=lookup_edf_uk_trades,
    dag=dag)

lookup_edf_uk_pos = PythonOperator(
    task_id='lookup_edf_uk_pos',
    provide_context=True,
    python_callable=lookup_edf_uk_pos,
    dag=dag)

lookup_edf_uk_bal = PythonOperator(
    task_id='lookup_edf_uk_bal',
    provide_context=True,
    python_callable=lookup_edf_uk_bal,
    dag=dag)

get_edf_uk_trades = SFTPOperator(
    task_id="get_edf_uk_trades",
    ssh_conn_id="edf_uk",
    local_filepath="./{{ task_instance.xcom_pull(task_ids=None, key='filename_edf_uk_trades') }}",
    remote_filepath="./{{ task_instance.xcom_pull(task_ids=None, key='filename_edf_uk_trades') }}",
    operation="get",
    create_intermediate_dirs=True,
    dag=dag
)

get_edf_uk_pos = SFTPOperator(
    task_id="get_edf_uk_pos",
    ssh_conn_id="edf_uk",
    local_filepath="./{{ task_instance.xcom_pull(task_ids=None, key='filename_edf_uk_pos') }}",
    remote_filepath="./{{ task_instance.xcom_pull(task_ids=None, key='filename_edf_uk_pos') }}",
    operation="get",
    create_intermediate_dirs=True,
    dag=dag
)

get_edf_uk_bal = SFTPOperator(
    task_id="get_edf_uk_bal",
    ssh_conn_id="edf_uk",
    local_filepath="./{{ task_instance.xcom_pull(task_ids=None, key='filename_edf_uk_bal') }}",
    remote_filepath="./{{ task_instance.xcom_pull(task_ids=None, key='filename_edf_uk_bal') }}",
    operation="get",
    create_intermediate_dirs=True,
    dag=dag
)

insert_edf_uk_trades = PythonOperator(
    task_id='insert_edf_uk_trades',
    provide_context=True,
    execution_timeout=timedelta(minutes=10),
    python_callable=insert_edf_uk_trades,
    dag=dag)

insert_edf_uk_pos = PythonOperator(
    task_id='insert_edf_uk_pos',
    provide_context=True,
    execution_timeout=timedelta(minutes=10),
    python_callable=insert_edf_uk_pos,
    dag=dag)

insert_edf_uk_bal = PythonOperator(
    task_id='insert_edf_uk_bal',
    provide_context=True,
    execution_timeout=timedelta(minutes=10),
    python_callable=insert_edf_uk_bal,
    dag=dag)


lookup_edf_uk_trades >> get_edf_uk_trades >> insert_edf_uk_trades
lookup_edf_uk_pos >> get_edf_uk_pos >> insert_edf_uk_pos
lookup_edf_uk_bal >> get_edf_uk_bal >> insert_edf_uk_bal