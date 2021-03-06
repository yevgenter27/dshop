import os.path
import logging
import pyspark.sql.functions as F
from datetime import datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from pyspark.sql.types import StringType, IntegerType, DateType
from pyspark.sql import SparkSession
from functions.load_functions import operators_load_dm, operators_load_fc
from airflow.hooks.base_hook import BaseHook

bronze_batch = 'bronze'
silver_batch = 'silver'
gold_batch = 'gold'

hdfs_conn = BaseHook.get_connection('dshop_hdfs')
gp_conn = BaseHook.get_connection('dshop_gp')

hdfs_url = 'http://' + hdfs_conn.host + ':' + str(hdfs_conn.port)
spark_driver_path = '/home/user/shared_folder/postgresql-42.2.23.jar'

gp_url = 'jdbc:postgresql://' + gp_conn.host + ':' + str(gp_conn.port) + '/' + gp_conn.schema
gp_properties = {
    'user': gp_conn.login,
    'password': gp_conn.password
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
    logging.info('SILVER PREPARATION >>>')
    spark = SparkSession.builder.master(hdfs_url).getOrCreate()
    current_date = datetime.today().date()
    all_dfs = dimension_dfs + fact_dfs
    for df in all_dfs:
        bronze_df = spark.read.load(os.path.join("/", 'datalake', bronze_batch, str(current_date), df + '.csv')
                           , header="true"
                           , inferSchema="true"
                           , format='.csv')
        logging.info(f'Load dataframe {df} to hdfs silver [.parquet]')
        bronze_df.write.parquet(os.path.join('/', 'datalake', silver_batch, df), mode='overwrite')
        logging.info('process success!')


def gold_preparation():
    logging.info('GOLD PREPARATION >>>')
    spark = SparkSession.builder \
        .config('spark.driver.extraClassPath'
                , spark_driver_path) \
        .master('local')\
        .getOrCreate()

    fact_stores_sales_df_name = 'fact_stores_sales'
    orders_df = spark.read.parquet(os.path.join("/", 'datalake', silver_batch, 'orders'))
    stores_df = spark.read.parquet(os.path.join("/", 'datalake', silver_batch, 'stores'))
    store_types_df = spark.read.parquet(os.path.join("/", 'datalake', silver_batch, 'store_types'))
    location_areas_df = spark.read.parquet(os.path.join("/", 'datalake', silver_batch, 'location_areas'))

    stores_df = stores_df.join(store_types_df, stores_df['store_type_id'] == store_types_df['store_type_id'], 'left') \
        .select(stores_df['*'], store_types_df['type'])
    stores_df = stores_df.join(location_areas_df, stores_df['location_area_id'] == location_areas_df['area_id'], 'left') \
        .select(stores_df['*'], location_areas_df['area'])

    orders_qty_df = orders_df.dropDuplicates(orders_df.order_id).groupBy(F.col('store_id')).count()
    orders_qty_df = orders_qty_df \
        .withColumn("store_id", F.col('store_id').cast(StringType())) \
        .withColumn("quantity", F.col('quantity').cast(IntegerType()))
    products_qty_df = orders_qty_df.groupBy(F.col('store_id')).sum(orders_qty_df.quantity)
    clients_qty_df = orders_df.dropDuplicates(orders_df.client_id).groupBy(F.col('store_id')).count()

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

    logging.info(f'Load dataframe {fact_store_sales_delta} to greenplum [.parquet]')
    fact_store_sales_delta.write.jdbc(gp_url, table=fact_stores_sales_df_name, properties=gp_properties, mode='append')
    logging.info('process success!')

    logging.info(f'Load dataframe {fact_store_sales_delta} to hdfs gold [.parquet]')
    fact_store_sales_delta.write.parquet(os.path.join("/", 'datalake', gold_batch, fact_store_sales_delta), mode='append')
    logging.info('process success!')


dag = DAG(
    dag_id="dshop_stores_sales_dag",
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
)

gold_preparation_task = PythonOperator(
    task_id="gold_preparation",
    description="Define and upload daily stores sales information",
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

dummy_start >> [*operators_load_dm(dag, dimension_dfs),
                *operators_load_fc(dag, fact_dfs)] \
            >> silver_preparation_task \
            >> gold_preparation_task >> \
dummy_finish
