import numpy as np
import pandas as pd
import pandasql as ps
from pandasql import sqldf
import json
import requests
import pytz
from datetime import datetime
import requests
import time
pd.options.mode.chained_assignment = None
pd.set_option('display.max_columns', 50)
pd.set_option('display.max_rows', 100)
def processData(apiKey, account_id, beginTime=None, endTime=None, timezone=None):
    function_startTime = time.time()
    # endpoint
    url = "https://sandbox-openapi.rakkardigital.com/v1/transactions"

    params = {
        "status": "completed",
        "start_date": beginTime,
        "end_date": endTime
    }
    headers = {
        "accept": "application/json",
        "X-API-KEY": apiKey,
        "account-id": account_id
    }

    response = requests.get(url, headers=headers, params=params)
    response.raise_for_status()
    
    df = pd.json_normalize(response.json()['transactions'])

    if df.empty:
        print("没有数据")
    
    else:
        # 格式修改
        df.rename(columns={'timestamp': 'datetime', 'transaction.asset_id': 'currency', 'transaction.txn_hash': 'txHash',
                       'destinationAddress': 'contactIdentity', 'transaction.amount': 'amount',
                          'transaction.destination.destination_address': 'contactIdentity',
                          'transaction.network_name':'contactPlatformSlug'}, inplace=True)
        df['datetime'] = pd.to_datetime(df['datetime']).dt.strftime('%Y-%m-%dT%H:%M:%SZ')
        
        df['direction'] = ''
        df['type'] = ''

        df['direction'] = df.apply(lambda row: 'OUT' if row['transaction.source.source_type'] == 'vault' else 'IN', axis=1)
        df.loc[(df['transaction.source.source_type'] == 'vault') & (df['transaction.destination.destination_type'] == 'vault'), 'direction'] = 0
        df['type'] = df.apply(lambda row: 'CUSTODY_WITHDRAW' if row['transaction.source.source_type'] == 'vault' else 'CUSTODY_DEPOSIT', axis=1)
        

        # EXCHANGE FEE
        new_rows = df.copy()
        new_rows['type'] = 'CUSTODY_FEE'
        new_rows['amount'] = new_rows['transaction.fee']
        new_rows.loc[(new_rows['direction'] == 'IN'), 'direction'] = 0
        new_rows['currency'] = df['transaction.network_id']
        new_rows['contactIdentity'] = ''

        # 将新的DataFrame与原始的DataFrame合并
        df = pd.concat([df, new_rows], ignore_index=True)
        
        # 将新数据列表转换为 DataFrame
        df = df[['type', 'txHash', 'datetime', 'contactIdentity',
                        'contactPlatformSlug', 'direction', 'currency', 'amount']]

    df = df[df['direction'] != 0] # 删去内部转账
    df.reset_index(drop=True, inplace=True)
    function_endTime = time.time()
    duration = function_endTime - function_startTime
    print(f"函数运行时间：{duration} 秒")
    
    print(f'共获取{len(df)}条数据')
    return df
