from airflow import DAG
from airflow.operators.python_operator import PythonOperator

from datetime import datetime, timedelta
from dateutil import tz

from odbc.odbc import *
from ftplib import FTP

import numpy as np
import pandas as pd
from pandas.tseries.offsets import BDay
import logging
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


def create_kgi_dag(dag_id, schedule):
    def get_and_insert_kgi_ps(**context):

        try:
            td = context['dag_run'].conf['to_date']
        except (KeyError, TypeError):
            td = dag.default_args['to_date']

        logging.info("Deleting rows for {} from database...".format(td))
        cur = getCursor(vlad_201, database_aarna)
        cur.execute("DELETE FROM GetKGISettlement WHERE RecDate = '{}'".format(td))
        cur.close()

        ending = td.replace("-", '')

        sftpURL = ''
        sftpUser = ''
        sftpPass = ''

        ftp = FTP(sftpURL)
        ftp.login(sftpUser, sftpPass)
        filenames = ftp.nlst()

        regexp = re.compile('.*PS.CSV$')
        ps = [s for s in filenames if regexp.match(s)]
        ps_trunc = list(filter(lambda x: re.findall('[0-9]{8}', x)[0] == ending, ps))

        before = ['Settlementdate',
                  'CLIENT CODE',
                  'RecordNo',
                  'EXCHANGE',
                  'CONTRACT',
                  'DEL.MONTH',
                  'C/P/F',
                  'STRIKE',
                  'CLOSEOUT LOT',
                  'LONG TRADE PRICE',
                  'SHORT TRADE PRICE',
                  'PL CCY',
                  'REALISED PL']

        after = ['Settlementdate',
                 'ShortClientCode',
                 'RecordNo',
                 'EXCode',
                 'ContractCode',
                 'ContractDelivery',
                 'CallPutFut',
                 'StrikePrice',
                 'CloseOutLot',
                 'LongPrice',
                 'ShortPrice',
                 'TradePLCcy',
                 'RealisedPL']

        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())

        ssh.connect(sftpURL, username=sftpUser, password=sftpPass)

        ftp = ssh.open_sftp()

        logging.info("Starting KGI settlement values files upload to a database...")

        for i in ps_trunc:
            with ftp.open(i) as file:
                ds = pd.read_csv(file)
                ds['Settlementdate'] = re.search('[0-9]{8}', i).group()
                ds['LONG TRADE PRICE'] = ds['LONG TRADE PRICE'].astype(str).str.replace(',', '')
                ds['SHORT TRADE PRICE'] = ds['SHORT TRADE PRICE'].astype(str).str.replace(',', '')
                ds['REALISED PL'] = ds['REALISED PL'].astype(str).str.replace(',', '')
                ds['RecordNo'] = None
                rename_dict = dict(zip(before, after))
                ds = ds.rename(columns=rename_dict)[after]
                ds['RecDate'] = datetime.strptime(re.search('[0-9]{8}', i).group(), "%Y%m%d").strftime("%Y-%m-%d")
                ds.to_sql('GetKGISettlement',
                          engine_aarna,
                          index=False,
                          if_exists="append",
                          schema="dbo")

        ps_trunc = "<br>".join(ps_trunc)

        context['ti'].xcom_push(key='kgi_ps_files', value=ps_trunc)

        return "Done!"

    def get_and_insert_kgi_op(**context):

        try:
            td = context['dag_run'].conf['to_date']
        except (KeyError, TypeError):
            td = dag.default_args['to_date']

        logging.info("Deleting rows for {} from database...".format(td))
        cur = getCursor(vlad_201, database_aarna)
        cur.execute("DELETE FROM KGIPosition WHERE RecDate = '{}'".format(td))
        cur.close()

        ending = td.replace("-", '')

        sftpURL = ''
        sftpUser = ''
        sftpPass = ''

        ftp = FTP(sftpURL)
        ftp.login(sftpUser, sftpPass)
        filenames = ftp.nlst()

        regexp = re.compile('.*OP.CSV$')
        ps = [s for s in filenames if regexp.match(s)]
        ps_trunc = list(filter(lambda x: re.findall('[0-9]{8}', x)[0] == ending, ps))

        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())

        ssh.connect(sftpURL, username=sftpUser, password=sftpPass)

        ftp = ssh.open_sftp()

        logging.info("Starting KGI Open Position files upload to a database...")

        f = np.vectorize(lambda x: re.sub('[\s\.&\/]', '', x.strip('.')))

        for i in ps_trunc:
            with ftp.open(i) as file:
                ds = pd.read_csv(file)
                rename_dict = dict(zip(ds.columns, f(np.array(ds.columns))))
                ds = ds.rename(columns=rename_dict)
                ds['TRADEPRICE'] = ds['TRADEPRICE'].astype(str).str.replace(',', '')
                ds['SETTLEMENTPRICE'] = ds['SETTLEMENTPRICE'].astype(str).str.replace(',', '')
                ds['UNREALISEDPL'] = ds['UNREALISEDPL'].astype(str).str.replace(',', '')
                ds['RecDate'] = datetime.strptime(re.search('[0-9]{8}', i).group(), "%Y%m%d").strftime("%Y-%m-%d")
                ds['InsertingDate'] = datetime.now(tz=tz.tzlocal()).strftime("%Y-%m-%d %H:%M:%S%z")
                ds.to_sql('KGIPosition',
                          engine_aarna,
                          index=False,
                          if_exists="append",
                          schema="dbo")

        ps_trunc = "<br>".join(ps_trunc)

        context['ti'].xcom_push(key='kgi_op_files', value=ps_trunc)

        return "Done!"

    dag = DAG(dag_id,
              schedule_interval=schedule,
              default_args=default_args,
              max_active_runs=1
              )

    with dag:
        get_and_insert_kgi_ps = PythonOperator(
            task_id='get_and_insert_kgi_ps',
            provide_context=True,
            python_callable=get_and_insert_kgi_ps
        )

        get_and_insert_kgi_op = PythonOperator(
            task_id='get_and_insert_kgi_op',
            provide_context=True,
            python_callable=get_and_insert_kgi_op
        )

    [get_and_insert_kgi_ps, get_and_insert_kgi_op]

    return dag


globals()['kgi_data_upload'] = create_kgi_dag('kgi_data_upload', '0 5 * * 1-5')
