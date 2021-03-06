import os.path
import json
import logging
import pyspark.sql.functions as F
from datetime import datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from pyspark.sql.types import StringType, IntegerType, DateType
from hdfs import InsecureClient
from pyspark.sql import SparkSession
from functions.load_functions import operators_load_dm
from functions.api_oos import download_from_api
from airflow.hooks.base_hook import BaseHook

bronze_batch = 'bronze'
silver_batch = 'silver'
gold_batch = 'gold'

hdfs_conn = BaseHook.get_connection('dshop_hdfs')
gp_conn = BaseHook.get_connection('dshop_gp')

hdfs_url = 'http://' + hdfs_conn.host + ':' + str(hdfs_conn.port)
spark_driver_path = '/home/user/shared_folder/postgresql-42.2.23.jar'
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
    df_path = os.path.join("/", 'datalake',  bronze_batch, str(current_date), fact_oos_df + '.json')
    client.makedirs(os.path.join("/", 'datalake', bronze_batch, str(current_date)))
    logging.info(f'Load dataframe {fact_oos_df} to hdfs bronze | path: {df_path}')
    client.write(df_path, data=json.dumps(data),
                 encoding='utf-8', overwrite=True)
    logging.info('process success!')


def silver_preparation():
    logging.info('SILVER PREPARATION >>>')
    spark = SparkSession.builder.master(hdfs_url).getOrCreate()
    current_date = datetime.today().date()
    for df in dimension_dfs:
        df_path = os.path.join("/", 'datalake', bronze_batch, str(current_date), df + '.csv')
        bronze_df = spark.read.load(df_path
                           , header="true"
                           , inferSchema="true"
                           , format='.csv')
        bronze_df.distinct()
        logging.info(f'Load dataframe {df} to hdfs silver [.parquet]')
        bronze_df.write.parquet(os.path.join('/', 'datalake', silver_batch, df), mode='overwrite')
        logging.info('process success!')

    df_path = os.path.join("/", 'datalake', bronze_batch, str(current_date), fact_oos_df + '.json')
    logging.info(f'Load dataframe {fact_oos_df} to hdfs silver [.parquet]')
    bronze_oos_df = spark.read.load(df_path
                           , header="true"
                           , inferSchema="true"
                           , format='.json')
    bronze_oos_df.write.parquet(os.path.join('/', 'datalake', silver_batch, fact_oos_df), mode='overwrite')
    logging.info('process success!')


def gold_preparation():
    logging.info('GOLD PREPARATION >>>')
    spark = SparkSession.builder \
        .config('spark.driver.extraClassPath'
                , spark_driver_path) \
        .master('local')\
        .getOrCreate()
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

    logging.info(f'Load dataframe {fact_departments_df_delta} to greenplum [.parquet]')
    fact_departments_df_delta.write.jdbc(gp_url, table=fact_departments_sales_df_name, properties=gp_properties, mode='append')
    logging.info('process success!')

    logging.info(f'Load dataframe {fact_departments_df_delta} to hdfs gold [.parquet]')
    fact_departments_df_delta.write.parquet(os.path.join("/", 'datalake', gold_batch, fact_departments_sales_df_name), mode='append')
    logging.info('process success!')


dag = DAG(
    dag_id="dshop_departments_sales_dag",
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

dummy_start >> [*operators_load_dm(dag, dimension_dfs), upload_oos_to_bronze_task] \
            >> silver_preparation_task \
            >> gold_preparation_task >> \
dummy_finish
