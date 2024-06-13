import yaml
import sys
from safeheron_api_sdk_python.api.transaction_api import *
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

def processData(apiKey, platform_public_key, beginTime=None, endTime=None, timezone=None):
    # Ensure the configurations are correctly set
    config = {
        'apiKey': apiKey,
        'privateKeyPemFile': './private_key.pem',
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

    return df

# Example usage:
apiKey = '1359da04903643838265486d14243ff6'
platform_public_key = 'MIICIjANBgkqhkiG9w0BAQEFAAOCAg8AMIICCgKCAgEAl4Jvs6tNB9kd3f3DfSCRl/LntgY/8ziV3huBxCvYOGXPe9eMiuZZGFnnUaUL9ciEie2NYGIYd65RHse0YwIMpdVUfZO32NXHnxm3SUE3MKlWYcN9JhoVXuBQxHbP4PZyScUQOCblHd+Lh6IiLtU8vpKoSUEUd7bLcBQttlZWJ4slERdZElgBCEvLUgtcd28dOS/32ITntl5fN7Igz/ZiJRSgXh/gGZ6OdGg5Ud4U/fxPhhzA7Yqq5MW2+uLpnxUP/W7KDy/PvvHGTp3kUVQhK9z6miNxfmuQx8HO660C62l9DbcCkzE9yW9eryg1qdesNtzAxrGvsg5YVWWGk8pylADbqnRdlFU0xLchZCBax2wkP44RGkNhk8iDtnznOsTUF3hOACIEZc7SyBXUP5UkEvHEJPxF2EuKTKWopVlGgofy/Sf/B7IAK/EywzCD6DXQyFKdqwc6pM9ZRE9VrUDktOsogt+GKIwFdmgtAMknP8h6ykkIjN0lZMD9qNdCxwcU+2LheR7q/UO2w7ApP+8vlIOIRbdkHJOxy7siHsRAHKObgHRBxe79jmdG8qg2y7dArhnf2wrIf2/e7JUrabVs77yLm01pWVylG0B/kpyWX8dgCfWXCKkJk2fFjgebX43JGdq0TTgZNsZNISCBnkOdWgIR2kGmzjG/ryilBLCrPecCAwEAAQ=='


result = processData(apiKey, platform_public_key, beginTime = '2024-05-01', endTime = '2024-05-31', timezone = 'UTC')
result
