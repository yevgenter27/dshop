import os.path
import requests
import json
from datetime import datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from hdfs import InsecureClient
from pyspark.sql import SparkSession

def upload_df_to_hdfs():
    token = get_auth_token()
    date = datetime.today().date()
    data = download_from_api(token, date)
    print(data)
    client = InsecureClient('http://127.0.0.1:50070/', user='user')
    file_name = '/bronze/out_of_stock/{str(date)}'
    client.makedirs(file_name)
    client.write(os.path.join('/', file_name, 'oos.json'), data=json.dumps(data), encoding='utf-8', overwrite=True)

def download_from_api(token, date):
    req_url = 'https://robot-dreams-de-api.herokuapp.com' + '/out_of_stock'
    req_body = {'date': str(date)}
    headers = {'Content-type': 'application/json',
               'Authorization': 'JWT ' + token}
    response = requests.get(url=req_url, headers=headers, data=json.dumps(req_body))
    return response.json()

def get_auth_token():
    req_url = 'https://robot-dreams-de-api.herokuapp.com' + '/auth'
    req_body = {'username': 'rd_dreams', 'password': 'djT6LasE'}
    headers = {'content-type': 'application/json'}
    response = requests.post(url=req_url, headers=headers, data=json.dumps(req_body))
    print(response.json())
    return response.json()['access_token']

def define_oos_product_qty():
    date = datetime.today().date()
    spark = SparkSession.builder.master('local').getOrCreate()
    df = spark.read.json(f'/bronze/out_of_stock/{str(date)}/oos.json')

dag = DAG(
    dag_id="out_of_stock",
    description="define products out of stock",
    start_date=datetime(2021, 8, 1, 14, 30),
    end_date=datetime(2022, 8, 1, 14, 30),
    schedule_interval="@daily"
)

t1 = PythonOperator(
    task_id="upload_from_api_to_hdfs",
    dag=dag,
    python_callable=upload_df_to_hdfs,
    provide_context=True
)

t2 = PythonOperator(
    task_id="silver_preparation",
    dag=dag,
    python_callable=upload_df_to_hdfs,
    provide_context=True
)

t3 = PythonOperator(
    task_id="gold_preparation",
    dag=dag,
    python_callable=upload_df_to_hdfs,
    provide_context=True
)

t1 >> t2 >> t3