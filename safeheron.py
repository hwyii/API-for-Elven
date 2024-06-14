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
apiKey = '1359da04903643838265486d14243ff6'
platform_public_key = 'MIICIjANBgkqhkiG9w0BAQEFAAOCAg8AMIICCgKCAgEAl4Jvs6tNB9kd3f3DfSCRl/LntgY/8ziV3huBxCvYOGXPe9eMiuZZGFnnUaUL9ciEie2NYGIYd65RHse0YwIMpdVUfZO32NXHnxm3SUE3MKlWYcN9JhoVXuBQxHbP4PZyScUQOCblHd+Lh6IiLtU8vpKoSUEUd7bLcBQttlZWJ4slERdZElgBCEvLUgtcd28dOS/32ITntl5fN7Igz/ZiJRSgXh/gGZ6OdGg5Ud4U/fxPhhzA7Yqq5MW2+uLpnxUP/W7KDy/PvvHGTp3kUVQhK9z6miNxfmuQx8HO660C62l9DbcCkzE9yW9eryg1qdesNtzAxrGvsg5YVWWGk8pylADbqnRdlFU0xLchZCBax2wkP44RGkNhk8iDtnznOsTUF3hOACIEZc7SyBXUP5UkEvHEJPxF2EuKTKWopVlGgofy/Sf/B7IAK/EywzCD6DXQyFKdqwc6pM9ZRE9VrUDktOsogt+GKIwFdmgtAMknP8h6ykkIjN0lZMD9qNdCxwcU+2LheR7q/UO2w7ApP+8vlIOIRbdkHJOxy7siHsRAHKObgHRBxe79jmdG8qg2y7dArhnf2wrIf2/e7JUrabVs77yLm01pWVylG0B/kpyWX8dgCfWXCKkJk2fFjgebX43JGdq0TTgZNsZNISCBnkOdWgIR2kGmzjG/ryilBLCrPecCAwEAAQ=='
private_key = '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'

result = processData(apiKey, private_key, platform_public_key, beginTime = '2024-05-01', endTime = '2024-05-31', timezone = 'UTC')
result
