import os.path
import json
import pyspark.sql.functions as F
from datetime import datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from pyspark.sql.types import StringType, IntegerType, DateType
from hdfs import InsecureClient
from pyspark.sql import SparkSession
from functions.spark_rw import read_from_hdfs_with_spark, delete_duplicate, write_to_hdfs_with_spark
from functions.load_functions import upload_dims_operators
from functions.api_oos import download_from_api
from airflow.hooks.base_hook import BaseHook

project_batch = 'dshop'
bronze_batch = 'bronze'
silver_batch = 'silver'
gold_batch = 'gold'

hdfs_conn = BaseHook.get_connection('dshop_hdfs')
pg_conn = BaseHook.get_connection('dshop_postgres')
gp_conn = BaseHook.get_connection('dshop_gp')

hdfs_url = 'http://' + hdfs_conn.host + ":" + str(hdfs_conn.port)
hdfs_user = hdfs_conn.login

gp_url = 'jdbc:postgresql://' + gp_conn.host + ':' + str(gp_conn.port) + '/' + gp_conn.schema
gp_properties = {
    'user': gp_conn.login,
    'password': gp_conn.password
}

dimension_dfs = [
    'departments',
    'products'
]

fact_oos_df = 'out_of_stock'


def upload_fact_df_to_bronze():
    date = datetime.today().date()
    data = download_from_api(date)
    current_date = datetime.today().date()
    client = InsecureClient(hdfs_url, hdfs_user)
    client.makedirs(os.path.join("/", 'datalake', bronze_batch, str(current_date)))
    client.write(os.path.join("/", 'datalake',  bronze_batch, str(current_date), fact_oos_df + '.json'), data=json.dumps(data),
                 encoding='utf-8', overwrite=True)


def silver_preparation():
    current_date = datetime.today().date()
    for df in dimension_dfs:
        bronze_df = read_from_hdfs_with_spark(hdfs_url, bronze_batch, current_date, df, '.csv')
        delete_duplicate(bronze_df)
        write_to_hdfs_with_spark(silver_batch, bronze_df)

    bronze_oos_df = read_from_hdfs_with_spark(hdfs_url, bronze_batch, current_date, fact_oos_df, '.json')
    write_to_hdfs_with_spark(silver_batch, bronze_oos_df)


def gold_preparation():
    spark = SparkSession.builder.master(hdfs_url).getOrCreate()
    fact_departments_sales_df_name = 'fact_departments_sales'
    oos_df = spark.read.parquet(os.path.join("/", 'datalake', silver_batch, 'out_of_stock'))
    products_df = spark.read.parquet(os.path.join("/", 'datalake', silver_batch, 'products'))
    departments_df = spark.read.parquet(os.path.join("/", 'datalake', silver_batch, 'departments'))

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

    fact_departments_df_delta.write.jdbc(gp_url, table=fact_departments_sales_df_name, properties=gp_properties, mode='append')
    fact_departments_df_delta.write.parquet(os.path.join("/", 'datalake', gold_batch, fact_departments_sales_df_name), mode='append')


dag = DAG(
    dag_id="departments_sales_dag",
    description="Define and upload daily departments sales information",
    start_date=datetime(2021, 8, 1, 14, 30),
    end_date=datetime(2022, 8, 1, 14, 30),
    schedule_interval="@daily"
)

upload_oos_to_bronze_task = PythonOperator(
    task_id="upload_oos_fc",
    description=f"Upload oos df from PostgresQL to bronze HDFS",
    dag=dag,
    python_callable=upload_fact_df_to_bronze,
)

silver_preparation_task = PythonOperator(
    task_id="silver_preparation",
    description="Formatting dataframes and upload to HDFS",
    dag=dag,
    python_callable=silver_preparation,
)

gold_preparation_task = PythonOperator(
    task_id="gold_preparation",
    description="Define and upload daily departments sales information",
    dag=dag,
    python_callable=gold_preparation,
)

dummy_start = DummyOperator(
    task_id="start_process",
    dag=dag
)

dummy_finish = DummyOperator(
    task_id="finish_process",
    dag=dag
)

dummy_start >> [*upload_dims_operators(dag, dimension_dfs), upload_oos_to_bronze_task] \
            >> silver_preparation_task \
            >> gold_preparation_task >> \
dummy_finish
