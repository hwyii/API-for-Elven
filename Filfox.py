import requests
import pandas as pd
from datetime import datetime
import pytz
import time

def processData(address, beginTime=None, endTime=None, timezone=None):
    BASE_URL = 'https://filfox.info/api/v1'
    ENDPOINT = f'/address/{address}/transfers'
    
    # 构造请求URL
    url = f"{BASE_URL}{ENDPOINT}"
    
    # 构造请求头
    headers = {
        'accept': 'application/json',
    }
    
    df_list = []
    page_size = 50
    page = 0
    
    while True:
        # 构造请求参数
        params = {'page': page, 'pageSize': page_size}
        
        try:
            response = requests.get(url, headers=headers, params=params)
            response.raise_for_status() 
            data = response.json()
            
            if 'transfers' in data:
                df_list.append(pd.DataFrame(data['transfers']))
            
            if len(data['transfers']) < page_size:
                break  # 如果获取的数据少于每页大小，说明已到最后一页
                
            page += 1  # 获取下一页数据
        except requests.exceptions.RequestException as e:
            print("Error:", e)
            break
            
    if df_list:
        df = pd.concat(df_list, ignore_index=True)
        
        if not df.empty:
            df.rename(columns={'to': 'contactIdentity', 'value': 'amount', 
                               'message': 'txHash', 'timestamp': 'datetime'}, inplace=True) # message是交易的内容标识符（CID），用于唯一标识这笔交易
            df['datetime'] = pd.to_datetime(df['datetime'], unit='s').dt.strftime('%Y-%m-%dT%H:%M:%SZ')
            df['contactPlatformSlug'] = ''
            
            df['currency'] = 'FIL'
            df['direction'] = df['amount'].astype(float).apply(lambda x: "OUT" if x < 0 else "IN")
            df['amount'] = df['amount'].astype(float).abs() / 10**18 # 换算单位
            
            df['type'] = df['type'].replace({'send': 'CHAIN_TRANSFER_OUT', 'receive': 'CHAIN_TRANSFER_IN',
                                             'burn-fee': 'CHAIN_TRANSACTION_FEE', 'miner-fee': 'CHAIN_TRANSACTION_FEE'})
            
            df = df[['type', 'txHash', 'datetime', 'contactIdentity',
                      'contactPlatformSlug', 'direction', 'currency', 'amount']]
        # 过滤时间
        timezone = pytz.timezone(timezone)
        # 转换为 UTC 时间
        beginTime_tr = timezone.localize(datetime.strptime(beginTime, '%Y-%m-%d').replace(hour=0, minute=0, second=0)).astimezone(pytz.utc).strftime('%Y-%m-%dT%H:%M:%SZ')
        endTime_tr = timezone.localize(datetime.strptime(endTime, '%Y-%m-%d').replace(hour=23, minute=59, second=59)).astimezone(pytz.utc).strftime('%Y-%m-%dT%H:%M:%SZ')
        
        df = df[(df['datetime'] >= beginTime_tr) & (df['datetime'] <= endTime_tr)]

        # 过滤重复行
        df = df.drop_duplicates(subset=['txHash', 'contactIdentity', 'amount'])
        df.reset_index(drop=True, inplace=True)
        
        return df
        
    else:
        return pd.DataFrame()
