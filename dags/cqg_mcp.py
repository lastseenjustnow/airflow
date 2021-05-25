from airflow import DAG
from airflow.contrib.operators.sftp_operator import SFTPOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.mssql_operator import MsSqlOperator

from datetime import datetime, timedelta

from odbc.odbc import *

import pandas as pd
from pandas.tseries.offsets import BDay
import logging
import os

today = pd.datetime.today() - BDay(1)
to_date = today.date().strftime("%Y%m%d")

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

dag = DAG('cqg_mcp',
          schedule_interval='30 23 * * 2-6',
          default_args=default_args,
          max_active_runs=1
          )


def insert_source_data(**context):
    td = dag.default_args['to_date']
    filename = 'fill_' + td + '_2130_EOD.csv'
    os.rename('fill_' + td + '_2130_EOD.txt', filename)
    input_data = pd.read_csv(
        filename,
        header=None,
        low_memory=False,
        dtype=str
    )
    db_cols = list(pd.read_sql_table("CQGMCP", engine_aarna))
    logging.info("Starting data {} upload to a database...".format(filename))
    cursor = getCursor(vlad_201, database_aarna)
    cursor.execute("TRUNCATE TABLE dbo.CQGMCP")
    input_data \
        .rename(columns=dict(zip(input_data.columns, db_cols))) \
        .to_sql('CQGMCP',
                engine_aarna,
                index=False,
                if_exists="append",
                schema="dbo")
    logging.info("Success!")
    cursor.close()
    return "CQG data file has been successfully uploaded to a database."


def send_email(**context):
    td = dag.default_args['to_date']
    filename = 'fill_' + td + '_2230_EOD.csv'

    to = ['amit@aarna.capital']

    sendMail(
        "reports@aarna.capital",
        to,
        "CQG Mapping report for " + td,
        "File {} has been processed for updating mapping: ".format(filename)
    )


get_data = SFTPOperator(
    task_id="get_data",
    ssh_conn_id="cqg",
    remote_filepath="./fill_" + to_date.replace("-", '') + "_2130_EOD.txt",
    local_filepath="./fill_" + to_date.replace("-", '') + "_2130_EOD.txt",
    operation="get",
    create_intermediate_dirs=True,
    dag=dag
)

insert_source_data = PythonOperator(
    task_id='insert_source_data',
    provide_context=True,
    python_callable=insert_source_data,
    dag=dag)

update_mapping = MsSqlOperator(
    task_id='update_mapping',
    mssql_conn_id='aarna_mssql_201',
    sql="""
    INSERT INTO mCQGMCPDetails (cFCMAccountNumber,cExchangeKeyID,OmnibusFCMName,CQGRouteName,MCPCode)
    SELECT DISTINCT
    AccountNumber AS cFCMAccountNumber,
    CQGExchangeNumber AS cExchangeKeyID,
    cqg.OmnibusFCMName,
    cqg.CQGRouteName,
    COALESCE(map1.BrokerCode, map2.BrokerCode) AS MCPCode
    FROM CQGMCP cqg
    LEFT JOIN CQGMCPMapping map1 ON cqg.CQGRouteName = map1.BrokerName
    LEFT JOIN CQGMCPMapping map2 ON cqg.OmnibusFCMName = map2.BrokerName
    LEFT JOIN mCQGMCPDetails cm ON
    cqg.AccountNumber = CM.cFCMAccountNumber AND
    cqg.CQGExchangeNumber = cm.cExchangeKeyID AND
    cqg.OmnibusFCMName = cm.OmnibusFCMName AND
    cqg.CQGRouteName = cm.CQGRouteName
    WHERE
    CM.cFCMAccountNumber IS NULL AND
    cm.cExchangeKeyID IS NULL AND
    cm.OmnibusFCMName IS NULL AND
    cm.CQGRouteName IS NULL
    """,
    database='AarnaProcess',
    dag=dag)

send_email = PythonOperator(
    task_id='send_email',
    provide_context=True,
    execution_timeout=timedelta(minutes=10),
    python_callable=send_email,
    dag=dag)

get_data >> insert_source_data >> update_mapping >> send_email
