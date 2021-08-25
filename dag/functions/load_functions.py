import os.path
import psycopg2
import logging
from datetime import datetime
from airflow.operators.python_operator import PythonOperator
from hdfs import InsecureClient
from airflow.hooks.base_hook import BaseHook


hdfs_conn = BaseHook.get_connection('dshop_hdfs')
pg_conn = BaseHook.get_connection('dshop_postgres')
hdfs_url = 'http://' + str(hdfs_conn.host) + ":" + str(hdfs_conn.port)
hdfs_user = hdfs_conn.login
pg_creds = {
    'host': pg_conn.host,
    'port': str(pg_conn.port),
    'user': pg_conn.login,
    'password': pg_conn.password,
    'database': 'dshop_bu'
}

def operators_load_dm(dag, dimension_dfs):
    operators = []
    for df in dimension_dfs:
        operator = PythonOperator(
            task_id="upload_" + df + "_dm",
            description=f"Upload {df} df from PostgresQL to bronze HDFS",
            python_callable=dm_to_bronze,
            op_kwargs={"df_name": df},
            dag=dag)
        operators.append(operator)
    return operators


def operators_load_fc(dag, fact_dfs):
    operators = []
    for df in fact_dfs:
        operator = PythonOperator(
            task_id="upload_" + df + "_fc",
            description=f"Upload {df} df from PostgresQL to bronze HDFS",
            python_callable=fc_to_bronze,
            op_kwargs={"df_name": df},
            dag=dag)
        operators.append(operator)
    return operators


def dm_to_bronze(df_name):
    client = InsecureClient(hdfs_url, hdfs_user)
    current_date = datetime.today().date()
    with psycopg2.connect(**pg_creds) as pg_connection:
        df_path = os.path.join("/", 'datalake', 'bronze', str(current_date), df_name + '.csv')
        logging.info(f'Load dataframe {df_name} to hdfs bronze | path: {df_path}')
        cursor = pg_connection.cursor()
        with client.write(df_path, overwrite=True) as csv_file:
            cursor.copy_expert(f"COPY {df_name} TO STDOUT WITH HEADER CSV", csv_file)
            logging.info('process success!')


def fc_to_bronze(df_name):
    client = InsecureClient(hdfs_url, hdfs_user)
    current_date_as_str = str(datetime.today().date())
    with psycopg2.connect(**pg_creds) as pg_connection:
        df_path = os.path.join("/", 'datalake', 'bronze', current_date_as_str, df_name + '.csv')
        logging.info(f'Load dataframe {df_name} to hdfs bronze | path: {df_path}')
        cursor = pg_connection.cursor()
        with client.write(df_path, overwrite=True) as csv_file:
            cursor.copy_expert(
                f"COPY (select * from {df_name} where order_date='{current_date_as_str}') TO STDOUT WITH HEADER CSV",
                csv_file)
            logging.info('process success!')
