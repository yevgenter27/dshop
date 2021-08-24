import os.path
from pyspark.sql import SparkSession



def read_from_hdfs_with_spark(hdfs_url ,root_batch, current_date, df_name, df_format):
    spark = SparkSession.builder.master(hdfs_url).getOrCreate()
    return spark.read.load(os.path.join("/", root_batch, str(current_date), df_name + df_format)
                           , header="true"
                           , inferSchema="true"
                           , format=df_format)

def delete_duplicate(df):
    df.distinct()


def write_to_hdfs_with_spark(root_batch, df):
    df.write.parquet(os.path.join("/", root_batch, df), mode='overwrite')