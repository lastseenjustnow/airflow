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
    'retry_delay': timedelta(minutes=1)
}

dag = DAG('bbg_indexes_interest_rates',
          schedule_interval='0 0 * * 1-5',
          default_args=default_args,
          max_active_runs=1
          )


def bbg_indexes_interest_rates():

    # at the moment chinese index failing with error. Raised ticket with Bloomberg
    bnch = pd.read_sql_table("Benchmark", engine_js)
    cny_idx = bnch[bnch['CurrencyCode'] == 'CNY'].index.tolist()[0]
    indexes_tickers = list(bnch['BenchmarkBloombergCode'].unique())
    indexes_tickers.pop(cny_idx)

    bcon = pdblp.BCon(debug=False, host='192.168.1.196', port=6970, timeout=5000)
    bcon.start()
    ds = bcon.ref(indexes_tickers, ["PX_LAST", "CRNCY"])
    bcon.stop()

    out = ds.set_index(['ticker', 'field']).unstack('field').reset_index()
    out.columns = ['BenchmarkBloombergCode', 'CurrencyCode', 'InterestRate']
    out['InterestRate'] = out['InterestRate'] / 100
    out['ValidTo'] = datetime.now().strftime("%Y-%m-%d")
    cnh = out[out['CurrencyCode'] == 'CNY']
    cnh['CurrencyCode'] = 'CNH'
    out = pd.concat([out, cnh])

    out.to_sql('CurrencyInterestRate',
               engine_js,
               index=False,
               if_exists="append",
               schema="dbo")

def success():
    print("End of DAG!")


bbg_indexes_interest_rates = PythonOperator(
    task_id='bbg_indexes_interest_rates',
    python_callable=bbg_indexes_interest_rates,
    dag=dag)

success = PythonOperator(
    task_id='success',
    python_callable=success,
    dag=dag)

bbg_indexes_interest_rates >> success