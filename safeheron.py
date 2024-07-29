import yaml
import sys
from safeheron_api_sdk_python.api.transaction_api import *
from safeheron_api_sdk_python.api.coin_api import *
import requests
import json
import time
import hmac
import hashlib
import base64
import pandas as pd
import warnings
import pytz
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
# 设置 pandas 以显示所有列
pd.set_option('display.max_columns', None)
pd.set_option('display.max_colwidth', None)

def processData(apiKey, private_key, platform_public_key, beginTime=None, endTime=None, timezone=None):
    # Ensure the configurations are correctly set
    config = {
        'apiKey': apiKey,
        # 'privateKeyPemFile': private_key, # pem file格式
        'privateKey': private_key, # 直接字符串
        'safeheronPublicKey': platform_public_key,
        'baseUrl': 'https://api.safeheron.vip',
        'RequestTimeout': 20000
    }

    # Initialize the Transaction API client
    transaction_api = TransactionApi(config)

    # 处理时间
    beginTime = beginTime + 'T00:00:00'
    endTime = endTime + "T23:59:59"
        
    dt_begin = datetime.strptime(beginTime, "%Y-%m-%dT%H:%M:%S")
    dt_end = datetime.strptime(endTime, "%Y-%m-%dT%H:%M:%S")

    # 获取目标时区的时区对象
    target_timezone = pytz.timezone(timezone)
    # 将日期时间对象转换为目标时区的时间
    localized_dt_begin = target_timezone.localize(dt_begin)
    localized_dt_end = target_timezone.localize(dt_end)
    # 将本地时间转换为 UTC 时间
    utc_begin = localized_dt_begin.astimezone(pytz.utc)
    utc_end = localized_dt_end.astimezone(pytz.utc)
    # 使用 strftime 函数将 datetime 对象格式化为指定格式的字符串
    beginTime = utc_begin.strftime('%Y-%m-%dT%H:%M:%SZ')
    EndTime = utc_end.strftime('%Y-%m-%dT%H:%M:%SZ')

    # 转化为时间戳
    start_time = int(datetime.strptime(beginTime, '%Y-%m-%dT%H:%M:%SZ').timestamp() * 1000)
    end_time = int(datetime.strptime(EndTime, '%Y-%m-%dT%H:%M:%SZ').timestamp() * 1000)
    
    # Initialize request parameters
    param = ListTransactionsV2Request()
    param.transactionStatus = "COMPLETED"
    param.createTimeMin = start_time
    param.createTimeMax = end_time

    # Fetch transactions
    response = transaction_api.list_transactions_v2(param)
    
    # Normalize and convert response to DataFrame
    df = pd.json_normalize(response)

    # 修改格式
    if df.empty:
        print("没有数据")
    else:
        df.rename(columns={'coinKey': 'currency', 'completedTime': 'datetime', 
                           'txAmount': 'amount', 'destinationAddress': 'contactIdentity'}, inplace=True) # datetime是交易完成时间
        df['datetime'] = pd.to_datetime(df['datetime'], unit='ms').dt.strftime('%Y-%m-%dT%H:%M:%SZ')
        df['contactPlatformSlug'] = ''
        
        df['direction'] = df.apply(lambda row: 'OUT' if row['sourceAccountType'] == 'VAULT_ACCOUNT' else 'IN', axis=1)
        df.loc[(df['sourceAccountType'] == 'VAULT_ACCOUNT') & (df['destinationAccountType'] == 'VAULT_ACCOUNT'), 'direction'] = 0
        df['type'] = df.apply(lambda row: 'CUSTODY_WITHDRAW' if row['sourceAccountType'] == 'VAULT_ACCOUNT' else 'CUSTODY_DEPOSIT', axis=1)
        
        # EXCHANGE FEE
        new_rows = df.copy()
        new_rows['type'] = 'CUSTODY_FEE'
        new_rows['amount'] = new_rows['txFee']
        new_rows['currency'] = new_rows['feeCoinKey']
        new_rows['direction'] = 'OUT'

        # 将新的DataFrame与原始的DataFrame合并，确保不会有重复的行出现
        df = pd.concat([df, new_rows], ignore_index=True)
        df = df[df['direction'] != 0] # 删去内部转账

        # 注释掉下面两行可以看原始数据
        df = df[['type', 'txHash', 'datetime', 'contactIdentity',
                        'contactPlatformSlug', 'direction', 'currency', 'amount']]
        df.reset_index(drop=True, inplace=True)

    # 处理coin
    coin_api = CoinApi(config)
    response_coin = coin_api.list_coin()
    coin_result = pd.json_normalize(response_coin)
    merged_df = df.merge(coin_result, how='left', left_on='currency', right_on='coinKey')

    # 更新currency列
    df['currency'] = merged_df['symbol'].fillna(df['currency'])    

    return df

# Example usage:
apiKey = 'your apiKey here'
platform_public_key = 'your platform public key'
private_key = 'your private key here'

result = processData(apiKey, private_key, platform_public_key, beginTime = '2024-05-01', endTime = '2024-05-31', timezone = 'UTC')
result
