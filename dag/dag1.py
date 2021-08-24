import os.path
import json
import psycopg2
import pyspark.sql.functions as F
from datetime import datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from pyspark.sql.types import StringType, IntegerType, DateType
from hdfs import InsecureClient
from pyspark.sql import SparkSession
from functions.custom_spark import read_from_hdfs_with_spark, delete_duplicate, write_to_hdfs_with_spark


from functions.api_oos import download_from_api

hdfs_url = "http://127.0.0.1:50070"
gp_url = "jdbc:postgresql://172.20.10.10:5433/postgres"
gp_properties = {"user": "gpuser", "password": "secret"}

project_batch = 'dshop'
bronze_batch = 'bronze'
silver_batch = 'silver'
gold_batch = 'gold'

pg_creds = {
    "host": "192.168.88.69",
    "port": "5432",
    "user": "pguser",
    "password": "secret",
    "database": "dshop_bu"
}

dimension_dfs = [
    'departments',
    'products'
]

fact_dfs = [
    'out_of_stock'
]


def upload_dimension_dfs_to_bronze():
    client = InsecureClient(hdfs_url, user="user")
    current_date = datetime.today().date()
    for df in dimension_dfs:
        with psycopg2.connect(**pg_creds) as pg_connection:
            cursor = pg_connection.cursor()
            with client.write(os.path.join("/", bronze_batch, str(current_date), df + '.csv')) as csv_file:
                cursor.copy_expert(f"COPY {df} TO STDOUT WITH HEADER CSV", csv_file)


def upload_fact_dfs_to_bronze():
    date = datetime.today().date()
    data = download_from_api(date)
    current_date = datetime.today().date()
    client = InsecureClient(hdfs_url, user='user')

    for df in fact_dfs:
        client.makedirs(os.path.join("/", bronze_batch, str(current_date)))
        client.write(os.path.join("/", bronze_batch, str(current_date), df + '.json'), data=json.dumps(data),
                     encoding='utf-8', overwrite=True)


def silver_preparation():
    current_date = datetime.today().date()
    for df in dimension_dfs:
        bronze_df = read_from_hdfs_with_spark(hdfs_url, bronze_batch, current_date, df, '.csv')
        delete_duplicate(bronze_df)
        write_to_hdfs_with_spark(silver_batch, bronze_df)

    for df in fact_dfs:
        bronze_df = read_from_hdfs_with_spark(hdfs_url, bronze_batch, current_date, df, '.json')
        write_to_hdfs_with_spark(silver_batch, bronze_df)


def gold_preparation():
    spark = SparkSession.builder.master(hdfs_url).getOrCreate()
    fact_departments_sales_df_name = 'fact_departments_sales'
    oos_df = spark.read.parquet(os.path.join("/", silver_batch, 'out_of_stock'))
    products_df = spark.read.parquet(os.path.join("/", silver_batch, 'products'))
    departments_df = spark.read.parquet(os.path.join("/", silver_batch, 'departments'))

    oos_df = oos_df.join(products_df, oos_df['product_id'] == products_df['product_id'], 'left')\
        .select(oos_df['*'], products_df['department_id'])

    oos_df = oos_df.groupBy(F.col('department_id'), F.col('date')).count()

    fact_departments_df = oos_df.join(departments_df, oos_df['department_id'] == departments_df['department_id'], 'left')\
        .select(oos_df['*'], departments_df['department'])

    fact_departments_df_delta = fact_departments_df\
        .withColumn("id", F.col('department_id').cast(StringType()))\
        .withColumn("name", F.col('department').cast(StringType()))\
        .withColumn("products_qty", F.col('count').cast(IntegerType()))\
        .withColumn("date", F.col('date').cast(DateType()))

    try:
        fact_departments_sales_df = spark.read.parquet(os.path.join("/", silver_batch, fact_departments_sales_df_name))
        fact_departments_sales_df.unionByName(fact_departments_df_delta)
        fact_departments_sales_df.write.parquet(os.path.join("/", gold_batch, fact_departments_sales_df_name), mode='overwrite')
    except Exception:
        fact_departments_df_delta.write.parquet(os.path.join("/", gold_batch, fact_departments_sales_df_name), mode='overwrite')


dag = DAG(
    dag_id="departments sales dag",
    description="Define and upload daily departments sales information",
    start_date=datetime(2021, 8, 1, 14, 30),
    end_date=datetime(2022, 8, 1, 14, 30),
    schedule_interval="@daily"
)

t1 = PythonOperator(
    task_id="upload dim dfs to bronze",
    description="Upload dimension dfs from PostgresQL to HDFS",
    dag=dag,
    python_callable=upload_dimension_dfs_to_bronze,
    provide_context=True
)

t2 = PythonOperator(
    task_id="upload fact dfs to bronze",
    description="Upload fact dfs from PostgresQL to HDFS",
    dag=dag,
    python_callable=upload_fact_dfs_to_bronze,
    provide_context=True
)

t3 = PythonOperator(
    task_id="silver preparation",
    description="Formatting dataframes and upload to HDFS",
    dag=dag,
    python_callable=silver_preparation,
    provide_context=True
)

t4 = PythonOperator(
    task_id="gold preparation",
    description="Define and upload daily stores sales information",
    dag=dag,
    python_callable=gold_preparation,
    provide_context=True
)

dummy_start = DummyOperator(
    task_id="start",
    dag=dag
)

dummy_finish = DummyOperator(
    task_id="finish",
    dag=dag
)

dummy_start >> t1 >> t2 >> t3 >> t4 >> dummy_finish