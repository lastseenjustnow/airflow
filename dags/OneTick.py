from airflow import DAG
from airflow.contrib.operators.sftp_operator import SFTPOperator
from airflow.contrib.hooks.ssh_hook import SSHHook
from airflow.operators.python_operator import PythonOperator

from datetime import datetime, timedelta

import pandas as pd
from pandas.tseries.offsets import BDay

today = pd.datetime.today() - BDay(1)
ending = today.date().strftime("%d.%m.%Y") + '.log'

default_args = {
    'owner': 'Akatov',
    'depends_on_past': False,
    'start_date': datetime(2020, 1, 1, 0, 0, 0),
    'email': ['vlad@aarna.capital'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

dag = DAG('OneTick',
          schedule_interval='0 3 * * 2-6',
          default_args=default_args
          )


get_CQG = SFTPOperator(
    task_id="get_CQG",
    ssh_conn_id="aarna",
    local_filepath="./CQG_FixData_" + ending,
    remote_filepath="D:/All Drop Copy/CQG/logs/FixData_" + ending,
    operation="get",
    create_intermediate_dirs=True,
    dag=dag
)

get_PATS = SFTPOperator(
    task_id="get_PATS",
    ssh_conn_id="aarna",
    local_filepath="./PATS_FixData_" + ending,
    remote_filepath="D:/All Drop Copy/PATS/logs/FixData_" + ending,
    operation="get",
    create_intermediate_dirs=True,
    dag=dag
)


def log_start_upload():
    print("Start uploading files to OneTick...")


log_upload = PythonOperator(
    task_id='log_start_upload',
    python_callable=log_start_upload,
    dag=dag)

oneTickSSH = SSHHook(
    remote_host='sftp.sol.onetick.com',
    username='aarna',
    key_file='/home/airflow/keys/OneTickSSHOPEN.ppk'
)

put_CQG = SFTPOperator(
    task_id="put_CQG",
    ssh_hook=oneTickSSH,
    local_filepath="./CQG_FixData_" + ending,
    remote_filepath="./CQG_FixData_" + ending,
    operation="put",
    create_intermediate_dirs=True,
    dag=dag
)

put_PATS = SFTPOperator(
    task_id="put_PATS",
    ssh_hook=oneTickSSH,
    local_filepath="./PATS_FixData_" + ending,
    remote_filepath="./PATS_FixData_" + ending,
    operation="put",
    create_intermediate_dirs=True,
    dag=dag
)

put_mapping = SFTPOperator(
    task_id="put_CQG_vs_Native_mapping",
    ssh_hook=oneTickSSH,
    local_filepath="./data/cqg_vs_native.csv",
    remote_filepath="./cqg_vs_native_" + ending.replace("log", "csv"),
    operation="put",
    create_intermediate_dirs=True,
    dag=dag
)


def success():
    print("End of DAG!")


success = PythonOperator(
    task_id='success',
    python_callable=success,
    dag=dag)

log_upload.set_upstream(get_CQG)
log_upload.set_upstream(get_PATS)
put_CQG.set_upstream(log_upload)
put_PATS.set_upstream(log_upload)
put_mapping.set_upstream(log_upload)
success.set_upstream(put_CQG)
success.set_upstream(put_PATS)
success.set_upstream(put_mapping)
