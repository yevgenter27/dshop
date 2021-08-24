import os.path
from pyspark.sql import SparkSession
from airflow.hooks.base_hook import BaseHook

gp_conn = BaseHook.get_connection('dshop_gp')
spark_driver_path = gp_conn.get_extra().get('extra__jdbc__drv_path')

def read_from_hdfs_with_spark(hdfs_url ,batch, current_date, df_name, df_format):
    spark = SparkSession.builder \
        .config('spark.driver.extraClassPath'
                , spark_driver_path) \
        .master('local') \
        .getOrCreate()

    spark = SparkSession.builder.master(hdfs_url).getOrCreate()
    return spark.read.load(os.path.join("/", 'datalale', batch, str(current_date), 'dshop', df_name + df_format)
                           , header="true"
                           , inferSchema="true"
                           , format=df_format)

def delete_duplicate(df):
    df.distinct()


def write_to_hdfs_with_spark(batch, df):
    df.write.parquet(os.path.join("/", 'datalake', batch, 'dshop', df), mode='overwrite')