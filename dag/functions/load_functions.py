import os.path

import psycopg2
from datetime import datetime
from airflow.operators.python_operator import PythonOperator
from hdfs import InsecureClient
from airflow.hooks.base_hook import BaseHook


hdfs_conn = BaseHook.get_connection('dshop_hdfs')
pg_conn = BaseHook.get_connection('dshop_postgres')
hdfs_url = 'http://' + hdfs_conn.host + ":" + hdfs_conn.port
hdfs_user = hdfs_conn.login
pg_creds = {
    'host': pg_conn.host,
    'port': pg_conn.port,
    'user': pg_conn.login,
    'password': pg_conn.password,
    'database': 'dshop_bu'
}

def upload_dims_operators(dag, dimension_dfs):
    operators = []
    for df in dimension_dfs:
        operator = PythonOperator(
            task_id="upload_" + df + "_dm",
            description=f"Upload {df} df from PostgresQL to bronze HDFS",
            python_callable=upload_dm_to_bronze,
            op_kwargs={"df_name": df, "pg_creds": pg_creds, "hdfs_url": hdfs_url},
            dag=dag)
        operators.append(operator)
    return operators


def upload_facts_operators(dag, fact_dfs):
    operators = []
    for df in fact_dfs:
        operator = PythonOperator(
            task_id="upload_" + df + "_fc",
            description=f"Upload {df} df from PostgresQL to bronze HDFS",
            python_callable=upload_fact_to_bronze,
            op_kwargs={"df_name": df, "pg_creds": pg_creds, "hdfs_url": hdfs_url},
            dag=dag)
        operators.append(operator)
    return operators


def upload_dm_to_bronze(df_name):
    client = InsecureClient(hdfs_url, hdfs_user)
    current_date = datetime.today().date()
    with psycopg2.connect(**pg_creds) as pg_connection:
        cursor = pg_connection.cursor()
        with client.write(os.path.join("/", 'dshop', 'bronze', str(current_date), df_name + '.csv'), overwrite=True) as csv_file:
            cursor.copy_expert(f"COPY {df_name} TO STDOUT WITH HEADER CSV", csv_file)


def upload_fact_to_bronze(df_name):
    client = InsecureClient(hdfs_url, hdfs_user)
    current_date_as_str = str(datetime.today().date())
    with psycopg2.connect(**pg_creds) as pg_connection:
        cursor = pg_connection.cursor()
        with client.write(os.path.join("/", 'dshop', 'bronze', current_date_as_str, df_name + '.csv'), overwrite=True) as csv_file:
            cursor.copy_expert(
                f"COPY (select * from {df_name} where order_date='{current_date_as_str}') TO STDOUT WITH HEADER CSV",
                csv_file)