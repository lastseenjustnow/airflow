# Important note: to comply with design of the system, one have to keep in mind that currency codes must be reversed,
# so USD always must take CurrencyCode column

from airflow import DAG
from airflow.operators.python_operator import PythonOperator

from datetime import datetime, timedelta

from odbc.odbc import *
import pdblp

import pandas as pd
from pandas.tseries.offsets import BDay


today = pd.datetime.today() - BDay(1)
to_date = today.date().strftime("%Y-%m-%d")

default_args = {
    'owner': 'Akatov',
    'depends_on_past': False,
    'start_date': datetime(2020, 1, 1, 0, 0, 0),
    'email': ['vlad@aarna.capital'
              ,'amit@aarna.capital'
              ],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
    'to_date': to_date
}

dag = DAG('bbg_curr_rates',
          schedule_interval='0 0 * * 2-6',
          default_args=default_args,
          max_active_runs=1
          )

def bbg_curr_rates(**context):
    curr_tickers = [
        "USDJPY Curncy",
        "USDAUD Curncy",
        "USDSGD Curncy",
        "USDTHB Curncy",
        "USDCHF Curncy",
        "USDCNY Curncy",
        "USDINR Curncy",
        "USDMYR Curncy",
        "USDCNH Curncy",
        "EURUSD Curncy",
        "GBPUSD Curncy",
        "CADUSD Curncy",
        "XAUUSD Curncy",
        "XAGUSD Curncy",
        "USDRUB Curncy",
        "USDHKD Curncy",
        "USDSEK Curncy"
    ]

    bcon = pdblp.BCon(debug=False, host='192.168.1.196', port=6970, timeout=5000)
    bcon.start()
    ds = bcon.ref(curr_tickers, "PX_LAST")
    bcon.stop()

    output = pd.DataFrame({
        "CurrencyCode": ds['ticker'].str[0:3],
        "CrossCurrencyCode": ds['ticker'].str[3:6],
        "CrossCurrencyRate": ds['value'].apply('{:,.2f}'.format)})
    output['CrossCurrencyCode'][output["CrossCurrencyCode"] == 'USD'] = \
        output['CurrencyCode'][output["CrossCurrencyCode"] == 'USD']
    output['CurrencyCode'] = 'USD'

    cursor = getCursor(vlad_201, database_js)
    data = [list(output.iloc[i].values) for i in range(0, len(output))]

    sql = """INSERT INTO [dbo].[ExchangeRateData]
        ([Date], [CurrencyCode], [CrossCurrencyCode], [CrossCurrencyRate]) 
        VALUES (
        FORMAT(DATEADD(DAY, -1, CURRENT_TIMESTAMP), 'yyyy-MM-dd 00:00:00'),
        ?,
        ?,
        ?)"""
    cursor.executemany(sql, data)


def success():
    print("End of DAG!")


bbg_curr_rates = PythonOperator(
    task_id='bbg_curr_rates',
    python_callable=bbg_curr_rates,
    dag=dag)

success = PythonOperator(
    task_id='success',
    python_callable=success,
    dag=dag)

bbg_curr_rates >> success