import requests
import json
import pandas as pd
import time
# 设置 pandas 以显示所有列
pd.set_option('display.max_columns', None)
pd.set_option('display.max_colwidth', None)

# Deribit OAuth2 token endpoint
TOKEN_URL = "https://www.deribit.com/api/v2/public/auth"

# Replace these with your actual CLIENT_ID and CLIENT_SECRET
CLIENT_ID = 'your ID here'
CLIENT_SECRET = 'your SECRET here'

def get_access_token(client_id, client_secret):
    data = {
        'client_id': client_id,
        'client_secret': client_secret,
        'grant_type': 'client_credentials'
    }
    response = requests.get(TOKEN_URL, params=data)
    response.raise_for_status()  # 检查请求是否成功
    token_info = response.json()
    return token_info['result']['access_token']

def get_withdrawals(access_token,currency):
    url = 'https://www.deribit.com/api/v2/private/get_withdrawals'
    headers = {
        'Authorization': f'Bearer {access_token}'
    }
    params = {
        'currency': currency
    }
    response = requests.get(url, params=params, headers=headers)
    response.raise_for_status()
    return response.json()['result']['data']

def get_deposits(access_token,currency):
    url = 'https://www.deribit.com/api/v2/private/get_deposits'
    headers = {
        'Authorization': f'Bearer {access_token}'
    }
    params = {
        'currency': currency
    }
    response = requests.get(url, params=params, headers=headers)
    response.raise_for_status()
    return response.json()['result']['data']

def get_transfers(access_token,currency):
    url = 'https://www.deribit.com/api/v2/private/get_transfers'
    headers = {
        'Authorization': f'Bearer {access_token}'
    }
    params = {
        'currency': currency
    }
    response = requests.get(url, params=params, headers=headers)
    response.raise_for_status()
    return response.json()['result']['data']

def get_user_trades_by_currency(access_token,currency):
    url = 'https://www.deribit.com/api/v2/private/get_user_trades_by_currency'
    headers = {
        'Authorization': f'Bearer {access_token}'
    }
    params = {
        'currency': currency
    }
    response = requests.get(url, params=params, headers=headers)
    response.raise_for_status()
    return response.json()['result']['trades']

def get_user_trades_by_currency_and_time(access_token, currency, start_timestamp, end_timestamp):
    url = 'https://www.deribit.com/api/v2/private/get_user_trades_by_currency_and_time'
    headers = {
        'Authorization': f'Bearer {access_token}'
    }
    params = {
        "currency": currency,
        "start_timestamp": start_timestamp,
        "end_timestamp": end_timestamp
    }
    response = requests.get(url, params=params, headers=headers)
    response.raise_for_status()
    return response.json()['result']['trades']

def get_user_trades_by_instrument(access_token,instrument_name):
    url = 'https://www.deribit.com/api/v2/private/get_user_trades_by_instrument'
    headers = {
        'Authorization': f'Bearer {access_token}'
    }
    # 设置请求参数
    params = {
        "instrument_name": instrument_name
    }
    response = requests.get(url, params=params, headers=headers)
    response.raise_for_status()
    return response.json()['result']['trades']


def get_user_trades_by_instrument_and_time(access_token, instrument_name, start_timestamp, end_timestamp):
    url = 'https://www.deribit.com/api/v2/private/get_user_trades_by_instrument_and_time'
    headers = {
        'Authorization': f'Bearer {access_token}'
    }
    params = {
        "instrument_name": instrument_name,
        "start_timestamp": start_timestamp,
        "end_timestamp": end_timestamp
    }
    response = requests.get(url, params=params, headers=headers)
    response.raise_for_status()
    return response.json()['result']['trades']

def get_account_summaries(access_token):
    url = 'https://www.deribit.com/api/v2/private/get_account_summaries'
    headers = {
        'Authorization': f'Bearer {access_token}'
    }
    
    response = requests.get(url, headers=headers)
    response.raise_for_status()
    return response.json()['result']['summaries']

def get_account_summary(access_token,currency):
    url = 'https://www.deribit.com/api/v2/private/get_account_summary'
    headers = {
        'Authorization': f'Bearer {access_token}'
    }
    # 设置请求参数
    params = {
        "currency": currency
    }
    response = requests.get(url, params=params, headers=headers)
    response.raise_for_status()
    return response.json()['result']

def get_position(access_token,instrument_name):
    url = 'https://www.deribit.com/api/v2/private/get_position'
    headers = {
        'Authorization': f'Bearer {access_token}'
    }
    # 设置请求参数
    params = {
        "instrument_name": instrument_name
    }
    response = requests.get(url, params=params, headers=headers)
    response.raise_for_status()
    return response.json()['result']

def get_positions(access_token):
    url = 'https://www.deribit.com/api/v2/private/get_positions'
    headers = {
        'Authorization': f'Bearer {access_token}'
    }
     
    response = requests.get(url, headers=headers)
    response.raise_for_status()
    return response.json()['result']

def get_subaccounts(access_token):
    url = 'https://www.deribit.com/api/v2/private/get_subaccounts'
    headers = {
        'Authorization': f'Bearer {access_token}'
    }
     
    response = requests.get(url, headers=headers)
    response.raise_for_status()
    return response.json()['result']

def get_subaccounts_details(access_token):
    url = 'https://www.deribit.com/api/v2/private/get_subaccounts_details'
    headers = {
        'Authorization': f'Bearer {access_token}'
    }
    params = {
        "currency": 'BTC'
    }
    response = requests.get(url, params=params, headers=headers)
    response.raise_for_status()
    return response.json()['result']

def get_transaction_log(access_token, start_timestamp, end_timestamp):
    url = 'https://www.deribit.com/api/v2/private/get_transaction_log'
    headers = {
        'Authorization': f'Bearer {access_token}'
    }
    params = {
        "currency": "BTC",
        "start_timestamp": start_timestamp,
        "end_timestamp": end_timestamp
    }
    response = requests.get(url, params=params, headers=headers)
    response.raise_for_status()
    return response.json()['result']['logs']
