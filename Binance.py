import requests
import hmac
import hashlib
import pandas as pd
import pytz
from datetime import datetime, timedelta
import time

def processData(apiKey, apiSecret, beginTime=None, endTime=None, timezone=None):
    base_url = 'https://api.binance.com'
    endpoint_deposit = '/sapi/v1/capital/deposit/hisrec'
    endpoint_withdraw = '/sapi/v1/capital/withdraw/history'
    
    # 创建一个空的 DataFrame，用于存放结果
    all_transfers = pd.DataFrame()

    # 以每90天为间隔进行循环
    while (1):
        if beginTime > endTime:
            break
        # 获得当前循环的结束时间
        currentEndTime = datetime.strptime(beginTime, "%Y-%m-%d") + timedelta(days=90)
        # 如果当前结束时间超过了指定的结束时间，则将其设置为指定的结束时间
        if currentEndTime > datetime.strptime(endTime, "%Y-%m-%d"):
            currentEndTime = datetime.strptime(endTime, "%Y-%m-%d")
        
        # 处理当前时间段的数据
       
        ### 处理日期和时区
        beginTime = beginTime + 'T00:00:00'
        currentEndTime = currentEndTime.strftime("%Y-%m-%d") + "T23:59:59"
        
        dt_begin = datetime.strptime(beginTime, "%Y-%m-%dT%H:%M:%S")
        dt_end = datetime.strptime(currentEndTime, "%Y-%m-%dT%H:%M:%S")

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
    
        # 设置请求参数：
        params_de = {
            'status': 1, # 只查询成功的
            'startTime': start_time,
            'endTime': end_time
        }
        params_wi = {
            'status': 6, # 只查询提现完成的
            'startTime': start_time,
            'endTime': end_time
        }
        # 参数中加时间戳：
        timestamp = int(time.time() * 1000) # 以毫秒为单位的 UNIX 时间戳
        params_de['timestamp'] = timestamp
        params_wi['timestamp'] = timestamp
    
        # 参数中加签名：
        query_string_de = f'status={params_de["status"]}&startTime={params_de["startTime"]}&endTime={params_de["endTime"]}&timestamp={timestamp}'
        signature_de = hmac.new(apiSecret.encode('utf-8'), query_string_de.encode('utf-8'), hashlib.sha256).hexdigest()
        params_de['signature'] = signature_de

        query_string_wi = f'status={params_wi["status"]}&startTime={params_wi["startTime"]}&endTime={params_wi["endTime"]}&timestamp={timestamp}'
        signature_wi = hmac.new(apiSecret.encode('utf-8'), query_string_wi.encode('utf-8'), hashlib.sha256).hexdigest()
        params_wi['signature'] = signature_wi

        headers = {
            'X-MBX-APIKEY': apiKey,
        }    
   
        try:
            ### deposit history
            deposit_url = f'{base_url}{endpoint_deposit}'
            de_response = requests.get(deposit_url, headers=headers, params=params_de)
            de_response.raise_for_status()
            de_df = de_response.json()
            de_result = pd.DataFrame(de_df)
            # print(de_result)
            result = de_result[['amount', 'coin', 'address', 'txId', 'insertTime']].copy()
        
            result.rename(columns={'coin': 'currency', 'txId': 'txid', 'insertTime': 'datetime', 'address': 'contactIdentity'}, inplace=True)
            result['datetime'] = pd.to_datetime(result['datetime'], unit='ms').dt.strftime('%Y-%m-%dT%H:%M:%SZ')
            result['contactPlatformSlug'] = ''
            result['type'] = 'EXCHANGE_DEPOSIT'
            result['direction'] = 'IN'

            ### withdraw history
            withdraw_url = f'{base_url}{endpoint_withdraw}'
            wi_response = requests.get(withdraw_url, headers=headers, params=params_wi)
            wi_response.raise_for_status()
            wi_df = wi_response.json()
            wi_result = pd.DataFrame(wi_df)

            wi_result = wi_result[['amount', 'coin', 'address', 'txId', 'completeTime', 'transactionFee']].copy()
            wi_result.rename(columns={'coin': 'currency', 'txId': 'txid', 'completeTime': 'datetime', 'address': 'contactIdentity'}, inplace=True)

            wi_result['contactPlatformSlug'] = ''
            wi_result['type'] = 'EXCHANGE_WITHDRAW'
            wi_result['direction'] = 'OUT'

            # EXCHANGE_FEE
            new_rows = wi_result.copy()
            new_rows['type'] = 'EXCHANGE_FEE'
            new_rows['amount'] = new_rows['transactionFee']
        
            result = pd.concat([result, wi_result, new_rows], ignore_index=True)
            
            result = result[['type', 'txid', 'datetime', 'contactIdentity',
                        'contactPlatformSlug', 'direction', 'currency', 'amount']]
        
            current_transfers = result
        except requests.exceptions.RequestException as e:
            print("Error:", e)
            return None  

        if current_transfers is not None:
            # 将当前时间段的结果添加到总结果中
            all_transfers = pd.concat([all_transfers, current_transfers], ignore_index=True)
        
        # 更新下一次循环的开始时间为当前循环的结束时间的下一天
        currentEndTime = datetime.strptime(currentEndTime, "%Y-%m-%dT%H:%M:%S")
        beginTime = (currentEndTime + timedelta(days=1)).strftime("%Y-%m-%d")
        
    return all_transfers

apiKey = '0C9cLHlMnhilAvHKRI2XnWuAuC3tbzavNMUzN34ePNgkxj1W0WzOJzE4P0gen1fZ'
apiSecret = 'QdKKJKUS0fILSuLN5akEy7m7DkQv49IWP1QsoLSVQH4dZVuegzVybcUz2fzQdbjl'

transfers = processData(apiKey, apiSecret, beginTime = '2023-11-26', endTime = '2024-04-08', timezone = 'Asia/Shanghai')
transfers