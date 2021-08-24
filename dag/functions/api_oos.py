import requests
import json


def download_from_api(date):
    req_url = 'https://robot-dreams-de-api.herokuapp.com' + '/out_of_stock'
    req_body = {'date': str(date)}
    token = get_auth_token
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
