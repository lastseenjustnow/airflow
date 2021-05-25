from airflow import DAG
from airflow.contrib.hooks.ftp_hook import FTPHook
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


def create_kgi_dag(dag_id, schedule):
    def lookup_kgi_bal(**context):
        try:
            ending = context['dag_run'].conf['to_date'] if context['dag_run'].conf['to_date'] != '' else to_date
        except (KeyError, TypeError):
            ending = dag.default_args['to_date']
        context['ti'].xcom_push(key='to_date', value=ending)

        filename = 'CHE-102_CashBalance_' + ending.replace("-", "") + '.xlsx'
        hook = FTPHook(ftp_conn_id='kgi')
        initial_time = datetime.now() + timedelta(hours=6)

        while datetime.now() < initial_time:
            files = hook.list_directory('/')
            if filename in files:
                logging.info("File {} has been found at: {}.".format(filename, datetime.now()))
                context['ti'].xcom_push(key='filename_kgi_bal', value=filename)
                return ending
            logging.warning("File {} has not yet been delivered at: {}.".format(filename, datetime.now()))
            time.sleep(300)

        raise AirflowNotFoundException("File {} has not been revealed during last 6 hours. Notification has been sent."
                                       .format(filename))

    def insert_kgi_bal(**context):
        filename = context['ti'].xcom_pull(task_ids=None, key='filename_kgi_bal')
        input_data_kgi_bal = pd.read_excel(
            filename,
            header=0,
            dtype=str,
            sheet_name=[0, 1]
        )

        f = np.vectorize(lambda x: re.sub('-|\(USD\)', '', x.strip('.')))
        rename_dict_0 = dict(zip(input_data_kgi_bal[0].columns, f(np.array(input_data_kgi_bal[0].columns))))
        rename_dict_1 = dict(zip(input_data_kgi_bal[1].columns, f(np.array(input_data_kgi_bal[1].columns))))
        ds0 = input_data_kgi_bal[0].rename(columns=rename_dict_0)
        ds0['FutureorFX'] = 'FUT'
        ds1 = input_data_kgi_bal[1].rename(columns=rename_dict_1)
        ds1['FutureorFX'] = 'FX'
        out = pd.concat([ds0, ds1])
        ins_date = context['ti'].xcom_pull(task_ids=None, key='to_date')
        out['InsertingDate'] = ins_date

        logging.info("Starting KgieFutureandFXBalances {} upload to a database...".format(filename))
        cursor = getCursor(vlad_201, database_aarna)
        out.to_sql('KgieFutureandFXBalances',
                   engine_aarna,
                   index=False,
                   if_exists="append",
                   schema="dbo")
        logging.info("Success!")
        cursor.close()
        return filename

    dag = DAG(dag_id,
              schedule_interval=schedule,
              default_args=default_args,
              max_active_runs=1
              )

    with dag:
        lookup_kgi_bal = PythonOperator(
            task_id='lookup_kgi_bal',
            provide_context=True,
            python_callable=lookup_kgi_bal
        )

        get_kgi_bal = SFTPOperator(
            task_id="get_kgi_bal",
            ssh_conn_id='kgi',
            local_filepath="./{{ task_instance.xcom_pull(task_ids=None, key='filename_kgi_bal') }}",
            remote_filepath="./{{ task_instance.xcom_pull(task_ids=None, key='filename_kgi_bal') }}",
            operation="get",
            create_intermediate_dirs=True
        )

        insert_kgi_bal = PythonOperator(
            task_id='insert_kgi_bal',
            provide_context=True,
            execution_timeout=timedelta(minutes=10),
            python_callable=insert_kgi_bal
        )



    lookup_kgi_bal >> get_kgi_bal >> insert_kgi_bal

    return dag


globals()['kgi_bal_data_upload'] = create_kgi_dag('kgi_bal_data_upload', '0 5 * * 1-5')
