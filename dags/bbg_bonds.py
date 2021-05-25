from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.mssql_operator import MsSqlOperator
from airflow.operators.email_operator import EmailOperator

from datetime import datetime, timedelta

import numpy as np
import pandas as pd
from odbc.odbc import *
import pdblp

today = pd.datetime.today()
to_date = today.date().strftime("%Y-%m-%d")

body = """
<body style="background-color:powderblue;">
<h1>Bond parameters update</h1>
<br>Amount of added rows to backoffice: {{ task_instance.xcom_pull(task_ids=None, key='count') }} </br>
<br>Date: {{ task_instance.xcom_pull(task_ids=None, key='to_date') }}  </br>
</body>
"""

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

dag = DAG('bbg_bonds',
          schedule_interval='0 19 * * 1-5',
          default_args=default_args,
          max_active_runs=1
          )

params = ["ACCRUED_BASIS", "DAYS_ACC", "PX_LAST", "TKT_ACCRUED", "INT_ACC"]


def define_date(**context):
    try:
        ending = context['dag_run'].conf['to_date'] if context['dag_run'].conf['to_date'] != '' else to_date
    except (KeyError, TypeError):
        ending = dag.default_args['to_date']
    context['ti'].xcom_push(key='to_date', value=ending)

    return ending


def ETL_bond_parameters(**context):
    def getBondParameters(ser):
        con = pdblp.BCon(debug=False, host='192.168.1.196', port=6970, timeout=5000)
        con.start()
        try:
            data = con.ref(ser, params)
            data = data['value'].values
        except ValueError:
            data = np.array([np.nan] * len(params))
        con.stop()
        return data

    d = context['task_instance'].xcom_pull(task_ids='define_date', key=None)

    con = getConn(vlad_201, database_js)
    data = pd.read_sql_query("""
    select Bloomberg_Code, SecurityCode 
    from JSOHAM.dbo.securitymaster st    
    where (st.ProductCode = 'BON' OR st.ProductCode = 'EQT')
    """, con)
    data['values'] = data['Bloomberg_Code'].apply(getBondParameters)
    output = pd.concat([data[['Bloomberg_Code', 'SecurityCode']],
                        pd.DataFrame(np.vstack(data['values'].values), columns=params)],
                       axis=1)
    output['INT_ACC'] = output['INT_ACC'] / 100
    output['ts'] = d

    engine_js = getEngine(vlad_201, database_js)

    output.to_sql('BondParameters',
                  engine_js,
                  index=False,
                  if_exists="append",
                  schema="dbo")

    output = output[output['PX_LAST'].notna()]

    cur = getCursor(vlad_201, database_js)
    for index, row in output.iterrows():
        cur.execute("""
        INSERT INTO jsoham.dbo.securitypricesdata (
        [Date],
        [SecurityCode],
        [Rate],
        [oldvalue],
        [CPCode],
        [SpotRate]
        ) values ('{}','{}',{},0,'EDF',NULL)""".format(
            row['ts'],
            row['SecurityCode'],
            row['PX_LAST']
        ))
    cur.close()
    context['ti'].xcom_push(key='count', value=len(output))


def success():
    print("End of DAG!")


define_date = PythonOperator(
    task_id='define_date',
    provide_context=True,
    execution_timeout=timedelta(minutes=10),
    python_callable=define_date,
    dag=dag)

remove_existing_values_on_date = MsSqlOperator(
    task_id='remove_existing_values_on_date',
    mssql_conn_id='aarna_mssql_201',
    sql="""
delete w 
from Jsoham.dbo.SecurityPricesData w 
inner join Jsoham.dbo.securitymaster sm on sm.SecurityCode = w.SecurityCode
where
    date = '{{ task_instance.xcom_pull(task_ids=None, key='to_date') }} 00:00:00' 
    and (sm.ProductCode = 'BON' or sm.ProductCode = 'EQT')
        """,
    database='AarnaProcess',
    dag=dag)

ETL_bond_parameters = PythonOperator(
    task_id='ETL_bond_parameters',
    provide_context=True,
    execution_timeout=timedelta(minutes=10),
    python_callable=ETL_bond_parameters,
    dag=dag)

send_excel = EmailOperator(
    task_id='send_excel',
    to=[
        'brijesh@aarna.capital'
    ],
    subject="Bond parameters from Bloomberg {{ task_instance.xcom_pull(task_ids=None, key='to_date') }}",
    html_content=body,
    dag=dag
    )

t_final = PythonOperator(
    task_id='success',
    python_callable=success,
    dag=dag)

define_date >> remove_existing_values_on_date >> ETL_bond_parameters >> send_excel >> t_final
