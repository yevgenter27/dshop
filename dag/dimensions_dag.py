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

def upload_df(df_name):
    client = InsecureClient(hdfs_url, user="user")
    date = datetime.today().date()

    with psycopg2.connect(**pg_creds) as pg_connection:
        cursor = pg_connection.cursor()
        with client.write(f'/bronze/{str(date)}/{df_name}.csv') as csv_file:
            cursor.copy_expert(f"COPY {df_name} TO STDOUT WITH HEADER CSV", csv_file)


def upload_aisles():
    df_name = 'aisles'
    upload_df(df_name)

def upload_clients():
    df_name = 'clients'
    upload_df(df_name)

def upload_departments():
    df_name = 'departments'
    upload_df(df_name)

def upload_location_areas():
    df_name = 'location_areas'
    upload_df(df_name)

def upload_products():
    df_name = 'products'
    upload_df(df_name)

def upload_store_types():
    df_name = 'store_types'
    upload_df(df_name)

def upload_stores():
    df_name = 'stores'
    upload_df(df_name)


dag = DAG(
    dag_id="dimensions",
    description="upload dimension tables",
    start_date=datetime(2021, 8, 1, 14, 30),
    end_date=datetime(2022, 8, 1, 14, 30),
    schedule_interval="@daily"
)

t1 = PythonOperator(
    task_id="upload aisles",
    dag=dag,
    python_callable=upload_aisles,
    provide_context=True
)

t2 = PythonOperator(
    task_id="upload clients",
    dag=dag,
    python_callable=upload_clients,
    provide_context=True
)

t3 = PythonOperator(
    task_id="upload departments",
    dag=dag,
    python_callable=upload_departments,
    provide_context=True
)

t4 = PythonOperator(
    task_id="upload location_areas",
    dag=dag,
    python_callable=upload_location_areas,
    provide_context=True
)

t5 = PythonOperator(
    task_id="upload products",
    dag=dag,
    python_callable=upload_products,
    provide_context=True
)

t6 = PythonOperator(
    task_id="upload store_types",
    dag=dag,
    python_callable=upload_store_types,
    provide_context=True
)

t7 = PythonOperator(
    task_id="upload stores",
    dag=dag,
    python_callable=upload_stores,
    provide_context=True
)
