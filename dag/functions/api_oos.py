import requests
import json
from airflow.hooks.base_hook import BaseHook

api_conn = BaseHook.get_connection('dshop_hdfs')
api_base_url = "https://" + api_conn.host

def download_from_api(date):
    req_url = api_base_url + api_conn.get_extra().get('out_of_stock_method')
    req_body = {'date': str(date)}
    token = get_auth_token
    headers = {'Content-type': 'application/json',
               'Authorization': 'JWT ' + str(token)}
    response = requests.get(url=req_url, headers=headers, data=json.dumps(req_body))
    print(response)
    return response.json()

def get_auth_token():
    req_url = api_base_url + api_conn.get_extra().get('get_token_method')
    req_body = {'username': api_conn.login, 'password': api_conn.password}
    headers = {'content-type': 'application/json'}
    response = requests.post(url=req_url, headers=headers, data=json.dumps(req_body))
    print(response.json())
    return response.json()['access_token']
