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

dag = DAG('edfus_data_upload',
          schedule_interval='0 3 * * 2-6',
          default_args=default_args,
          max_active_runs=1
          )


edf_us_SSH = SSHHook(
    remote_host='',
    username='',
    key_file='/home/airflow/keys/EDFUSPrivateNewSSHOPEN.pem'
)


def lookup_edf_us_trades(**context):
    try:
        ending = context['dag_run'].conf['to_date']
    except (KeyError, TypeError):
        ending = dag.default_args['to_date']
    context['ti'].xcom_push(key='to_date', value=ending)

    filename = 'PFDFST4_' + ending.replace("-", "") + '.CSV'

    sftpURL = ''
    sftpUser = ''
    sftpKey = '/home/airflow/keys/EDFUSPrivateNewSSHOPEN.pem'

    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())

    initial_time = datetime.now() + timedelta(hours=6)

    while datetime.now() < initial_time:
        ssh.connect(sftpURL, username=sftpUser, key_filename=sftpKey)
        ftp = ssh.open_sftp()
        files = ftp.listdir()
        if filename in files:
            logging.info("File {} has been found at: {}.".format(filename, datetime.now()))
            context['ti'].xcom_push(key='filename_edf_us_trades', value=filename)
            ssh.close()
            return ending
        logging.warning("File {} has not yet been delivered at: {}.".format(filename, datetime.now()))
        ssh.close()
        time.sleep(300)

    raise AirflowNotFoundException("File {} has not been revealed during last 6 hours. Notification has been sent."
                                   .format(filename))


def lookup_edf_us_pos(**context):
    try:
        ending = context['dag_run'].conf['to_date']
    except (KeyError, TypeError):
        ending = dag.default_args['to_date']
    context['ti'].xcom_push(key='to_date', value=ending)

    filename = 'PFDFPOS_' + ending.replace("-", "") + '.CSV'

    sftpURL = ''
    sftpUser = ''
    sftpKey = ''

    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())

    initial_time = datetime.now() + timedelta(hours=6)

    while datetime.now() < initial_time:
        ssh.connect(sftpURL, username=sftpUser, key_filename=sftpKey)
        ftp = ssh.open_sftp()
        files = ftp.listdir()
        if filename in files:
            logging.info("File {} has been found at: {}.".format(filename, datetime.now()))
            context['ti'].xcom_push(key='filename_edf_us_pos', value=filename)
            ssh.close()
            return ending
        logging.warning("File {} has not yet been delivered at: {}.".format(filename, datetime.now()))
        ssh.close()
        time.sleep(300)

    raise AirflowNotFoundException("File {} has not been revealed during last 6 hours. Notification has been sent."
                                   .format(filename))


def lookup_edf_us_mny(**context):
    try:
        ending = context['dag_run'].conf['to_date']
    except (KeyError, TypeError):
        ending = dag.default_args['to_date']
    context['ti'].xcom_push(key='to_date', value=ending)

    filename = 'PFDFMNY_' + ending.replace("-", "") + '.CSV'

    sftpURL = ''
    sftpUser = ''
    sftpKey = '/home/airflow/keys/EDFUSPrivateNewSSHOPEN.pem'

    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())

    initial_time = datetime.now() + timedelta(hours=6)

    while datetime.now() < initial_time:
        ssh.connect(sftpURL, username=sftpUser, key_filename=sftpKey)
        ftp = ssh.open_sftp()
        files = ftp.listdir()
        if filename in files:
            logging.info("File {} has been found at: {}.".format(filename, datetime.now()))
            context['ti'].xcom_push(key='filename_edf_us_mny', value=filename)
            ssh.close()
            return ending
        logging.warning("File {} has not yet been delivered at: {}.".format(filename, datetime.now()))
        ssh.close()
        time.sleep(300)

    raise AirflowNotFoundException("File {} has not been revealed during last 6 hours. Notification has been sent."
                                   .format(filename))


def insert_edf_us_trades(**context):
    filename = context['ti'].xcom_pull(task_ids=None, key='filename_edf_us_trades')
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
    cur.execute("DELETE FROM GetEefUSSettlement WHERE RecDate = '{}'".format(td))
    cur.close()

    input_data['RecDate'] = td

    f = np.vectorize(lambda x: re.sub('[\s\.&\/]', '', x.strip('.')))
    rename_dict = dict(zip(input_data.columns, f(np.array(input_data.columns))))
    input_data = input_data.rename(columns=rename_dict)

    logging.info("Starting EDF US Trades {} upload to a database...".format(filename))
    cursor = getCursor(vlad_201, database_aarna)
    input_data.to_sql('GetEefUSSettlement',
                      engine_aarna,
                      index=False,
                      if_exists="append",
                      schema="dbo")
    logging.info("Success!")
    cursor.close()
    return filename


def insert_edf_us_pos(**context):
    filename = context['ti'].xcom_pull(task_ids=None, key='filename_edf_us_pos')
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
    cur.execute("DELETE FROM EDFUSPosition WHERE RecDate = '{}'".format(td))
    cur.close()

    input_data['RecDate'] = td
    logging.info("Starting EDF US OpenPosition {} upload to a database...".format(filename))
    cursor = getCursor(vlad_201, database_aarna)
    input_data.to_sql('EDFUSPosition',
                      engine_aarna,
                      index=False,
                      if_exists="append",
                      schema="dbo")
    logging.info("Success!")
    cursor.close()
    return filename


def insert_edf_us_mny(**context):
    filename = context['ti'].xcom_pull(task_ids=None, key='filename_edf_us_mny')
    input_data = pd.read_csv(
        filename,
        header=0,
        low_memory=False,
        dtype=str,
        na_filter=False,
        index_col=False
    )
    td = context['ti'].xcom_pull(task_ids=None, key='to_date')

    logging.info("Deleting rows for {} from database...".format(td))
    cur = getCursor(vlad_201, database_aarna)
    cur.execute("DELETE FROM EDFUSBalances WHERE InsertingDate = '{}'".format(td))
    cur.close()

    input_data['InsertingDate'] = td

    f = np.vectorize(lambda x: re.sub('[\s\.&\/]', '', x.strip('.')))
    rename_dict = dict(zip(input_data.columns, f(np.array(input_data.columns))))
    input_data = input_data.rename(columns=rename_dict)

    logging.info("Starting EDF US OpenPosition {} upload to a database...".format(filename))
    cursor = getCursor(vlad_201, database_aarna)
    input_data.to_sql('EDFUSBalances',
                      engine_aarna,
                      index=False,
                      if_exists="append",
                      schema="dbo")
    logging.info("Success!")
    cursor.close()
    return filename


lookup_edf_us_trades = PythonOperator(
    task_id='lookup_edf_us_trades',
    provide_context=True,
    python_callable=lookup_edf_us_trades,
    dag=dag)

lookup_edf_us_pos = PythonOperator(
    task_id='lookup_edf_us_pos',
    provide_context=True,
    python_callable=lookup_edf_us_pos,
    dag=dag)

lookup_edf_us_mny = PythonOperator(
    task_id='lookup_edf_us_mny',
    provide_context=True,
    python_callable=lookup_edf_us_mny,
    dag=dag)

get_edf_us_trades = SFTPOperator(
    task_id="get_edf_us_trades",
    ssh_hook=edf_us_SSH,
    local_filepath="./{{ task_instance.xcom_pull(task_ids=None, key='filename_edf_us_trades') }}",
    remote_filepath="./{{ task_instance.xcom_pull(task_ids=None, key='filename_edf_us_trades') }}",
    operation="get",
    create_intermediate_dirs=True,
    dag=dag
)

get_edf_us_pos = SFTPOperator(
    task_id="get_edf_us_pos",
    ssh_hook=edf_us_SSH,
    local_filepath="./{{ task_instance.xcom_pull(task_ids=None, key='filename_edf_us_pos') }}",
    remote_filepath="./{{ task_instance.xcom_pull(task_ids=None, key='filename_edf_us_pos') }}",
    operation="get",
    create_intermediate_dirs=True,
    dag=dag
)

get_edf_us_mny = SFTPOperator(
    task_id="get_edf_us_mny",
    ssh_hook=edf_us_SSH,
    local_filepath="./{{ task_instance.xcom_pull(task_ids=None, key='filename_edf_us_mny') }}",
    remote_filepath="./{{ task_instance.xcom_pull(task_ids=None, key='filename_edf_us_mny') }}",
    operation="get",
    create_intermediate_dirs=True,
    dag=dag
)

insert_edf_us_trades = PythonOperator(
    task_id='insert_edf_us_trades',
    provide_context=True,
    execution_timeout=timedelta(minutes=10),
    python_callable=insert_edf_us_trades,
    dag=dag)

insert_edf_us_pos = PythonOperator(
    task_id='insert_edf_us_pos',
    provide_context=True,
    execution_timeout=timedelta(minutes=10),
    python_callable=insert_edf_us_pos,
    dag=dag)

insert_edf_us_mny = PythonOperator(
    task_id='insert_edf_us_mny',
    provide_context=True,
    execution_timeout=timedelta(minutes=10),
    python_callable=insert_edf_us_mny,
    dag=dag)

lookup_edf_us_trades >> get_edf_us_trades >> insert_edf_us_trades
lookup_edf_us_pos >> get_edf_us_pos >> insert_edf_us_pos
lookup_edf_us_mny >> get_edf_us_mny >> insert_edf_us_mny
