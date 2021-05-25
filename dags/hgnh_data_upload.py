from airflow import DAG
from airflow.contrib.hooks.ftp_hook import FTPHook
from airflow.operators.python_operator import PythonOperator

from datetime import datetime, timedelta
from dateutil import tz
from ftplib import FTP

from odbc.odbc import *

import numpy as np
import pandas as pd
from pandas.tseries.offsets import BDay
import logging
import re
import subprocess
import os
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

dag = DAG('hgnh_data_upload',
          schedule_interval='0 3 * * 1-5',
          default_args=default_args,
          max_active_runs=1
          )

body = """
<body style="background-color:powderblue;">
<h1>HGNH data upload</h1>
<p>Hello, we have following files been uploaded to backoffice: </p>
<p> {{ task_instance.xcom_pull(task_ids=None, key='filenames') }}.</p>
</body>
"""

folder = '/HGNH/'


def collect_hgnh_files(**context):
    try:
        ending = context['dag_run'].conf['to_date'] if context['dag_run'].conf['to_date'] != '' else to_date
    except (KeyError, TypeError):
        ending = dag.default_args['to_date']
    context['ti'].xcom_push(key='to_date', value=ending)
    ending = datetime.strptime(ending, '%Y-%m-%d')
    ending_regexp1 = ending.date().strftime("%d%m%Y")
    ending_regexp2 = ending.date().strftime("%d%m%y")

    logging.info("Looking for files masked as {} or {}".format(ending_regexp1, ending_regexp2))

    sftpURL = ''
    sftpUser = ''
    sftpPass = ''

    ftp = FTP(sftpURL)
    ftp.login(sftpUser, sftpPass)
    filenames = ftp.nlst(folder)

    hook = FTPHook(ftp_conn_id='hgnh')

    regexp_1 = re.compile('.*{}.*'.format(ending_regexp1))
    regexp_2 = re.compile('.*{}.*'.format(ending_regexp2))
    ps = [s for s in filenames if
          ((regexp_1.match(s) or regexp_2.match(s)) and s.find('CLIENT A_C -MAIN') == -1 and s.find('(PATS) - MAIN') == -1)]
    if not os.path.exists(folder):
        os.mkdir(folder)
    for filename in ps:
        hook.retrieve_file(filename, filename)

    trade_files = [f for f in os.listdir(folder) if re.match(r'.*TradeFile.*', f)]

    for zip in trade_files:
        subprocess.run(["7z", "x", os.path.join(folder, zip), f"-o{folder}", "-p000000516", "-y"])

    return os.listdir(folder)


def unlock_statements(**context):
    xlss = list(filter(lambda x: re.findall('Daily Statement.*xlsx$', x), os.listdir(folder)))

    for xls in xlss:
        xls_path = os.path.join(folder, xls)
        subprocess.run(["msoffcrypto-tool", xls_path, os.path.join(folder, 'd' + xls), "-p", "000000516"])
        os.remove(xls_path)

    return "Unlocked!"


def insert_hgnh(filemask, table_name, **context):
    td = context['ti'].xcom_pull(task_ids=None, key='to_date')
    csv_filename = list(filter(lambda x: re.findall(filemask, x), os.listdir(folder)))

    logging.info("Deleting rows for {} from database...".format(td))
    cur = getCursor(vlad_201, database_aarna)
    cur.execute("DELETE FROM {} WHERE RecDate = '{}'".format(table_name, td))
    cur.close()

    for csv in csv_filename:
        input_data = pd.read_csv(
            os.path.join(folder, csv),
            header=0,
            low_memory=False,
            dtype=str
        )

        f = np.vectorize(lambda x: re.sub('[\s\.&\/\-/]', '', x.strip('.')))
        rename_dict = dict(zip(input_data.columns, f(np.array(input_data.columns))))
        input_data = input_data.rename(columns=rename_dict)
        input_data['Client'] = re.search(r'(\s\-\s)(\w+)', csv).group(2)
        input_data['RecDate'] = td
        current_ts = datetime.now(tz=tz.tzlocal()).strftime("%Y-%m-%d %H:%M:%S%z")
        input_data['InsertingDate'] = current_ts

        logging.info(f"Starting HGNH file {csv} upload to a database...")
        input_data.to_sql(table_name,
                          engine_aarna,
                          index=False,
                          chunksize=1000,
                          if_exists="append",
                          schema="dbo")

    return csv_filename


def insert_hgnh_trades(**context):
    return insert_hgnh('Newtrades_', 'HGNHTrades', **context)


def insert_hgnh_positions(**context):
    return insert_hgnh('Openpositions_', 'HGNHPosition', **context)


def insert_hgnh_purchase_sale(**context):
    td = context['ti'].xcom_pull(task_ids=None, key='to_date')
    xlss = list(filter(lambda x: re.findall('Daily Statement.*xlsx$', x), os.listdir(folder)))

    logging.info("Deleting rows for {} from database...".format(td))
    cur = getCursor(vlad_201, database_aarna)
    cur.execute("DELETE FROM GetHGNHSettlement WHERE InsertingDate = '{}'".format(td))
    cur.execute("DELETE FROM GetHGNHSettlementDefined WHERE RecDate = '{}'".format(td))
    cur.close()

    db_cols = list(pd.read_sql_table("GetHGNHSettlement", engine_aarna))

    for xls in xlss:
        ds = pd.read_excel(os.path.join(folder, xls),
                           header=None,
                           sheet_name='Purchase & Sale',
                           converters={0: str})
        ds = ds[ds.iloc[:, 0].str.match('[0-9]{1,}$', na=False)] \
            .dropna(axis=1)
        ds.insert(0, '0', datetime.strptime(td, "%Y-%m-%d").strftime("%d-%m-%Y"))
        ds = ds.rename(columns=dict(zip(ds.columns, db_cols)))
        ds['InsertingDate'] = datetime.now(tz=tz.tzlocal()).strftime("%Y-%m-%d %H:%M:%S%z")
        ds = ds[ds['PnL'] != 0]

        logging.info(f"Starting HGNH file {xls} to a GetHGNHSettlement...")
        if ds.size == 0:
            logging.info("Empty dataset!")
            continue
        ds.to_sql('GetHGNHSettlement',
                  engine_aarna,
                  index=False,
                  if_exists="append",
                  schema="dbo")

        attrs = ['TradeDate', 'Ticker', 'MarketCode', 'CallPut', 'StrikePrice', 'DelMonth', 'PnL', 'InsertingDate', 'RecDate']
        hgnh_native_codes = pd.read_sql_table("HGNHMapping", engine_aarna)
        hgnh_native_codes['key'] = 0
        ds['key'] = 0
        ds = ds.merge(hgnh_native_codes, how='outer')
        ds = ds[ds.apply(lambda x: x['UnderlyingAsset'] in x['SecurityDescription'], axis=1)]
        ds['Ticker'] = ds['NativeCode']
        ds['MarketCode'] = ds['SecurityDescription'].str.extract(r'^(?:[^\s]+\s){2}([^\s ]+)')
        ds['CallPut'] = ds['SecurityDescription'].str.extract(r'.*[0-9]{1,}([P|C])').fillna('F')
        ds['StrikePrice'] = ds['SecurityDescription'].str.extract(r'([0-9]+)[P|C]').fillna('0')
        ds['DelMonth'] = ds['SecurityDescription'].str.extract(r'^(\S* \S*).*').apply(pd.to_datetime, format='%b %y')
        ds['DelMonth'] = ds['DelMonth'].dt.strftime('%Y%m')
        ds['RecDate'] = datetime.strptime(td, "%Y-%m-%d")

        logging.info(f"Starting HGNH file {xls} to a GetHGNHSettlementDefined...")
        ds[attrs].to_sql('GetHGNHSettlementDefined',
                         engine_aarna,
                         index=False,
                         if_exists="append",
                         schema="dbo")

    return "Success!"


def insert_hgnh_financial_summary(**context):
    td = context['ti'].xcom_pull(task_ids=None, key='to_date')
    xlss = list(filter(lambda x: re.findall('Daily Statement.*xlsx$', x), os.listdir(folder)))

    logging.info("Deleting rows for {} from database...".format(td))
    cur = getCursor(vlad_201, database_aarna)
    cur.execute("DELETE FROM HGNHFinancialSummary WHERE RecDate = '{}'".format(td))
    cur.close()

    db_cols = list(pd.read_sql_table("HGNHFinancialSummary", engine_aarna))

    for xls in xlss:
        ds = pd.read_excel(os.path.join(folder, xls),
                           header=None,
                           sheet_name='Financial Summary',
                           converters={1: str, 2: str, 3: str})

        ds = ds[ds.iloc[:, 1].str.match('\d+', na=False)]
        ds = ds.rename(columns=dict(zip(ds.columns, db_cols)))
        ds['FileName'] = xls[1:]
        ds['RecDate'] = td
        ds['InsertingDate'] = datetime.now(tz=tz.tzlocal()).strftime("%Y-%m-%d %H:%M:%S%z")

        logging.info(f"Starting HGNH file {xls} to a HGNHFinancialSummary...")
        ds.to_sql('HGNHFinancialSummary',
                  engine_aarna,
                  index=False,
                  if_exists="append",
                  schema="dbo")
    return "Done"


def rm_r():
    shutil.rmtree(folder)


collect_hgnh_files = PythonOperator(
    task_id='collect_hgnh_files',
    provide_context=True,
    python_callable=collect_hgnh_files,
    dag=dag)

unlock_statements = PythonOperator(
    task_id='unlock_statements',
    provide_context=True,
    python_callable=unlock_statements,
    dag=dag)

insert_hgnh_trades = PythonOperator(
    task_id='insert_hgnh_trades',
    provide_context=True,
    python_callable=insert_hgnh_trades,
    dag=dag)

insert_hgnh_positions = PythonOperator(
    task_id='insert_hgnh_positions',
    provide_context=True,
    python_callable=insert_hgnh_positions,
    dag=dag)

insert_hgnh_purchase_sale = PythonOperator(
    task_id='insert_hgnh_purchase_sale',
    provide_context=True,
    python_callable=insert_hgnh_purchase_sale,
    dag=dag)

insert_hgnh_financial_summary = PythonOperator(
    task_id='insert_hgnh_financial_summary',
    provide_context=True,
    python_callable=insert_hgnh_financial_summary,
    dag=dag)

rm_r = PythonOperator(
    task_id='rm_r',
    python_callable=rm_r,
    dag=dag)

collect_hgnh_files >> insert_hgnh_trades
collect_hgnh_files >> insert_hgnh_positions
collect_hgnh_files >> unlock_statements
unlock_statements >> insert_hgnh_purchase_sale
unlock_statements >> insert_hgnh_financial_summary
[insert_hgnh_trades, insert_hgnh_positions, insert_hgnh_purchase_sale, insert_hgnh_financial_summary] >> rm_r
