import pandas as pd
from pandasql import sqldf
import pytz
import time
import numpy as np
from fireblocks_sdk import FireblocksSDK, VAULT_ACCOUNT, PagedVaultAccountsRequestFilters
import json
from datetime import datetime, timedelta
pd.options.mode.chained_assignment = None
pd.set_option('display.max_columns', 50)
pd.set_option('display.max_rows', 100)

def processTime(beginTime, endTime, timezone):
    
    beginTime += 'T00:00:00'
    endTime += "T23:59:59"
    
    # 获取目标时区的时区对象
    target_timezone = pytz.timezone(timezone)
    
    # 将日期时间字符串转换为目标时区的时间
    dt_begin = target_timezone.localize(datetime.strptime(beginTime, "%Y-%m-%dT%H:%M:%S"))
    dt_end = target_timezone.localize(datetime.strptime(endTime, "%Y-%m-%dT%H:%M:%S"))

    # 将目标时区的时间转换为 UTC 时间
    utc_begin = dt_begin.astimezone(pytz.utc)
    utc_end = dt_end.astimezone(pytz.utc)

    # 转化为时间戳
    start_time = int(utc_begin.timestamp() * 1000)
    end_time = int(utc_end.timestamp() * 1000)
    
    return start_time, end_time 


def processData(apiKey, apiSecret, beginTime=None, endTime=None, timezone=None):
    function_startTime = time.time()
    # endpoint
    api_url = 'https://sandbox-api.fireblocks.io'
    fireblocks = FireblocksSDK(apiSecret, apiKey, api_base_url=api_url)

    start_time, end_time = processTime(beginTime, endTime, timezone)
    
    tx = fireblocks.get_transactions_with_page_info(status = 'COMPLETED', before = end_time, after = start_time)
    df = pd.json_normalize(tx['transactions'])

    if df.empty:
        print("没有数据")
    
    else:
        # 格式修改
        df['createdAt'] = pd.to_datetime(df['createdAt'], unit='ms').dt.strftime('%Y-%m-%dT%H:%M:%SZ')
        df.rename(columns={'transactionId': 'txHash', 'assetId': 'currency',
                       'createdAt': 'datetime', 'destinationAddress': 'contactIdentity',
                       'note': 'memo'}, inplace=True)
    
        df['contactPlatformSlug'] = ''
        # df['contactIdentity'] = ''
        # df["direction"] = ''
        df['amount'] = df['amount'].astype(float)
        df['direction'] = ''
        df['type'] = ''

        df['direction'] = df.apply(lambda row: 'OUT' if row['source.type'] == 'VAULT_ACCOUNT' else 'IN', axis=1)
        df.loc[(df['source.type'] == 'VAULT_ACCOUNT') & (df['destination.type'] == 'VAULT_ACCOUNT'), 'direction'] = 0
        df['type'] = df.apply(lambda row: 'CUSTODY_WITHDRAW' if row['source.type'] == 'VAULT_ACCOUNT' else 'CUSTODY_DEPOSIT', axis=1)
        

        # EXCHANGE FEE
        new_rows = df.copy()
        new_rows['type'] = 'CUSTODY_FEE'
        new_rows['amount'] = new_rows['fee']
        new_rows['currency'] = new_rows['feeCurrency']
        new_rows['direction'] = 'OUT'

        # 将新的DataFrame与原始的DataFrame合并
        df = pd.concat([df, new_rows], ignore_index=True)
        
        df['currency'] = df['currency'].str.split('_').str[0]
        
        # 将新数据列表转换为 DataFrame
        df = df[['type', 'txHash', 'datetime', 'contactIdentity',
                        'contactPlatformSlug', 'direction', 'currency', 'amount', 'memo']]

    df = df[df['direction'] != 0] # 删去内部转账

    function_endTime = time.time()
    duration = function_endTime - function_startTime
    print(f"函数运行时间：{duration} 秒")
    
    print(f'共获取{len(df)}条数据')
    return df

# 加载 API 密钥
api_key = 'f16cd7d7-54a9-4ea5-999d-6954dc4b8820'
api_secret = open('fireblocks_secret.key', 'r').read()

transfers = processData(api_key, api_secret, beginTime = '2024-02-01', endTime = '2024-05-25', timezone = 'Asia/Shanghai')
transfers
