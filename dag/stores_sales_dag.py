import os.path

import psycopg2
import pyspark.sql.functions as F
from datetime import datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from pyspark.sql.types import StringType, IntegerType, DateType
from pyspark.sql import SparkSession
from pyspark.sql import SQLContext
from functions.custom_spark import read_from_hdfs_with_spark, delete_duplicate, write_to_hdfs_with_spark
from functions.load_functions import upload_dims_operators, upload_facts_operators

project_batch = 'dshop'
bronze_batch = 'bronze'
silver_batch = 'silver'
gold_batch = 'gold'

hdfs_url = "http://127.0.0.1:50070"
gp_url = "jdbc:postgresql://172.20.10.10:5433/postgres"
gp_properties = {"user": "gpuser", "password": "secret"}

pg_creds = {
    "host": "192.168.88.69",
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

fact_dfs = [
    'orders'
]


def silver_preparation():
    current_date = datetime.today().date()
    all_dfs = dimension_dfs + fact_dfs
    for df in all_dfs:
        bronze_df = read_from_hdfs_with_spark(hdfs_url, bronze_batch, current_date, df, '.csv')
        delete_duplicate(bronze_df)
        write_to_hdfs_with_spark(silver_batch, bronze_df)


def gold_preparation():
    spark = SparkSession.builder.master('local').getOrCreate()
    fact_stores_sales_df_name = 'fact_stores_sales'
    orders_df = spark.read.parquet(os.path.join("/", silver_batch, 'orders'))
    stores_df = spark.read.parquet(os.path.join("/", silver_batch, 'stores'))
    store_types_df = spark.read.parquet(os.path.join("/", silver_batch, 'store_types'))
    location_areas_df = spark.read.parquet(os.path.join("/", silver_batch, 'location_areas'))

    stores_df = stores_df.join(store_types_df, stores_df['store_type_id'] == store_types_df['store_type_id'], 'left') \
        .select(stores_df['*'], store_types_df['type'])
    stores_df = stores_df.join(location_areas_df, stores_df['location_area_id'] == location_areas_df['area_id'], 'left') \
        .select(stores_df['*'], location_areas_df['area'])

    orders_qty_df = orders_df.dropDuplicates(F.col('order_id')).groupBy(F.col('store_id')).count()
    orders_qty_df = orders_qty_df \
        .withColumn("store_id", F.col('store_id').cast(StringType())) \
        .withColumn("quantity", F.col('quantity').cast(IntegerType()))
    products_qty_df = orders_qty_df.groupBy(F.col('store_id')).sum(F.col('quantity'))
    clients_qty_df = orders_df.dropDuplicates(F.col('client_id')).groupBy(F.col('store_id')).count()

    stores_df = stores_df.join(orders_qty_df, stores_df['store_id'] == orders_qty_df['store_id'], 'left') \
        .select(stores_df['*'], orders_qty_df['count'].alias('orders_qty'))
    stores_df = stores_df.join(products_qty_df, stores_df['store_id'] == products_qty_df['store_id'], 'left') \
        .select(stores_df['*'], products_qty_df['count'].alias('products_qty'))
    stores_sales_df_delta = stores_df.join(clients_qty_df, stores_df['store_id'] == clients_qty_df['store_id'], 'left') \
        .select(stores_df['*'], clients_qty_df['count'].alias('clients_qty'))

    fact_store_sales_delta = stores_sales_df_delta \
        .withColumn("store_id", F.col('store_id').cast(StringType())) \
        .withColumn("type", F.col('type').cast(StringType())) \
        .withColumn("area", F.col('area').cast(StringType())) \
        .withColumn("orders_qty", F.col('orders_qty').cast(IntegerType())) \
        .withColumn("clients_qty", F.col('clients_qty').cast(IntegerType())) \
        .withColumn("products_qty", F.col('products_qty').cast(IntegerType())) \
        .withColumn("date", F.col('date').cast(DateType()))

    fact_store_sales_delta.write.jdbc(gp_url, table=fact_stores_sales_df_name, properties=gp_properties, mode='append')
    fact_store_sales_delta.write.parquet(os.path.join("/", gold_batch, fact_stores_sales_df_name), mode='append')


dag = DAG(
    dag_id="stores_sales_dag",
    description="Define and upload daily stores sales information",
    start_date=datetime(2021, 8, 1, 14, 30),
    end_date=datetime(2022, 8, 1, 14, 30),
    schedule_interval="@daily"
)


silver_preparation_task = PythonOperator(
    task_id="silver_preparation",
    description="Formatting dataframes and upload to HDFS",
    dag=dag,
    python_callable=silver_preparation,
    # provide_context=True
)

gold_preparation_task = PythonOperator(
    task_id="gold_preparation",
    description="Define and upload daily stores sales information",
    dag=dag,
    python_callable=gold_preparation,
    # provide_context=True
)

dummy_start = DummyOperator(
    task_id="start_process",
    dag=dag
)

dummy_finish = DummyOperator(
    task_id="finish_process",
    dag=dag
)

dummy_start >> [*upload_dims_operators(dag, dimension_dfs, pg_creds, hdfs_url),
                *upload_facts_operators(dag, fact_dfs, pg_creds, hdfs_url)] \
            >> silver_preparation_task \
            >> gold_preparation_task >> \
dummy_finish
