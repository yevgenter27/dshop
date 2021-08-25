import requests
import json
import logging

api_base_url = 'https://robot-dreams-de-api.herokuapp.com'
api_username = 'user'
api_password = 'djT6LasE'

def download_from_api(date):
    req_url = api_base_url + '/out_of_stock'
    req_body = {'date': str(date)}
    token = get_auth_token
    headers = {'Content-type': 'application/json',
               'Authorization': 'JWT ' + str(token)}
    logging.info(f'HTTP -> GET')
    logging.info(f'HTTP -> URL: {req_url}')
    logging.info(f'HTTP -> HEADERS: {headers}')
    logging.info(f'HTTP -> BODY: {req_body}')
    response = requests.get(url=req_url, headers=headers, data=json.dumps(req_body))
    logging.info(f'HTTP -> RESPONSE: {str(response)}')
    return response.json()

def get_auth_token():
    req_url = api_base_url + '/auth'
    req_body = {'username': api_username, 'password': api_password}
    headers = {'content-type': 'application/json'}
    response = requests.post(url=req_url, headers=headers, data=json.dumps(req_body))
    print(response.json())
    return response.json()['access_token']
