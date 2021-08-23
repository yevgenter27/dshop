import psycopg2
from datetime import datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from hdfs import InsecureClient

hdfs_url = "http://127.0.0.1:50070"

pg_creds = {
    "host": "192.168.1.105",
    "port": "5432",
    "user": "pguser",
    "password": "secret",
    "database": "dshop_bu"
}

def upload_df():
    client = InsecureClient(hdfs_url, user="user")
    date = datetime.today().date()

    with psycopg2.connect(**pg_creds) as pg_connection:
        cursor = pg_connection.cursor()
        with client.write(f'/bronze/{str(date)}/orders.csv') as csv_file:
            cursor.copy_expert(f"COPY orders TO STDOUT WITH HEADER CSV", csv_file)

dag = DAG(
    dag_id="orders",
    description="upload orders from postgresql to hdfs",
    start_date=datetime(2021, 8, 1, 14, 30),
    end_date=datetime(2022, 8, 1, 14, 30),
    schedule_interval="@daily"
)

t1 = PythonOperator(
    task_id="upload",
    dag=dag,
    python_callable=upload_df,
    provide_context=True
)