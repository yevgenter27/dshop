import psycopg2
from datetime import datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from hdfs import InsecureClient

hdfs_url = "http://127.0.0.1:50070"

pg_creds = {
    "host": "192.168.1.105",
    "port": "5432",
    "user": "pguser",
    "password": "secret",
    "database": "dshop_bu"
}

dimension_dfs = [
    'aisles',
    'clients',
    'departments',
    'location_areas',
    'products',
    'store_types',
    'stores'
]

fact_df = 'orders'

def upload_dimension_dfs():
    client = InsecureClient(hdfs_url, user="user")
    date = datetime.today().date()

    for df in dimension_dfs:
        with psycopg2.connect(**pg_creds) as pg_connection:
            cursor = pg_connection.cursor()
            with client.write(f'/bronze/{str(date)}/{df}.csv') as csv_file:
                cursor.copy_expert(f"COPY {df} TO STDOUT WITH HEADER CSV", csv_file)


def upload_fact_df():
    client = InsecureClient(hdfs_url, user="user")
    date = datetime.today().date()

    with psycopg2.connect(**pg_creds) as pg_connection:
        cursor = pg_connection.cursor()
        with client.write(f'/bronze/{str(date)}/{fact_df}.csv') as csv_file:
            cursor.copy_expert(f"COPY {fact_df} TO STDOUT WITH HEADER CSV", csv_file)

dag = DAG(
    dag_id="dimensions",
    description="upload dimension tables",
    start_date=datetime(2021, 8, 1, 14, 30),
    end_date=datetime(2022, 8, 1, 14, 30),
    schedule_interval="@daily"
)

t1 = PythonOperator(
    task_id="upload dimension dfs to bronze",
    dag=dag,
    python_callable=upload_dimension_dfs,
    provide_context=True
)

t2 = PythonOperator(
    task_id="upload orders df to bronze",
    dag=dag,
    python_callable=upload_fact_df,
    provide_context=True
)

t3 = PythonOperator(
    task_id="silver preparation"

)

t4 = PythonOperator(

)

