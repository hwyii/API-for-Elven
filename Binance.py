# processData to get withdraw, deposit, trade history
import requests
import hmac
import hashlib
import pandas as pd
import pytz
from datetime import datetime, timedelta
import time

def processTime(beginTime, currentEndTime, timezone):
    
    beginTime += 'T00:00:00'
    currentEndTime = currentEndTime.strftime("%Y-%m-%d") + "T23:59:59"
    
    # 获取目标时区的时区对象
    target_timezone = pytz.timezone(timezone)
    
    # 将日期时间字符串转换为目标时区的时间
    dt_begin = target_timezone.localize(datetime.strptime(beginTime, "%Y-%m-%dT%H:%M:%S"))
    dt_end = target_timezone.localize(datetime.strptime(currentEndTime, "%Y-%m-%dT%H:%M:%S"))

    # 将目标时区的时间转换为 UTC 时间
    utc_begin = dt_begin.astimezone(pytz.utc)
    utc_end = dt_end.astimezone(pytz.utc)

    # 转化为时间戳
    start_time = int(utc_begin.timestamp() * 1000)
    end_time = int(utc_end.timestamp() * 1000)
    
    return start_time, end_time

# processData to get withdraw, deposit, trade history
import requests
import hmac
import hashlib
import pandas as pd
import pytz
from datetime import datetime, timedelta
import time

def processData(apiKey, apiSecret, beginTime=None, endTime=None, timezone=None):
    function_startTime = time.time()
    # endpoint
    base_url = 'https://api.binance.com'
    endpoint_deposit = '/sapi/v1/capital/deposit/hisrec'
    endpoint_withdraw = '/sapi/v1/capital/withdraw/history'
    endpoint_trade = '/api/v3/myTrades'
    endpoint_fiat = '/sapi/v1/fiat/orders'

    # 读入交易对名单
    pairs = pd.read_csv('pairs.csv')

    # 设置请求headers
    headers = {
        'X-MBX-APIKEY': apiKey,
    } 
    
    ##### trade
    print('开始获取Trade数据')
    # 创建一个空的 DataFrame 用于存储所有数据
    df_all_data = pd.DataFrame()

    # 遍历每个 symbol
    for symbol in pairs['test2']:
        if pd.isna(symbol):
            print('已经获取所有数据！')
            break
        print(f'开始获取 {symbol} Trade数据')
        temp = symbol # 存储带斜杠的交易对
        # 去除 symbol 列中的斜杠
        symbol = symbol.replace('/', '')
        time.sleep(1)  #避免请求过于频繁
    
        # 构造请求参数
        params = {
            'symbol': symbol,
            'fromId': 0,
            'limit': 1000,
            'recvWindow': 6000,
            'timestamp': int(time.time() * 1000)  # 以毫秒为单位的当前时间戳
        }

        # 计算签名
        query_string = '&'.join([f"{k}={v}" for k, v in params.items()])
        signature = hmac.new(apiSecret.encode('utf-8'), query_string.encode('utf-8'), hashlib.sha256).hexdigest()
        params['signature'] = signature

        try:
            response = requests.get(base_url + endpoint_trade, headers=headers, params=params)
            response.raise_for_status()  # 如果请求不成功，会抛出异常
            df = pd.DataFrame(response.json())
            trade_num = len(df)
            df_new = df
        except requests.exceptions.RequestException as e:
            print("Error:", e)
            
        if df.empty: # 如果没有这个交易对
            continue

        trade_num = len(df)
        df_new = df
            
        while trade_num == 1000: # 数据条数超出limit限制
            time.sleep(1)
            max_id = max(df_new['id'])
        
            params_in = {
                'symbol': symbol,
                'fromId': max_id,
                'recvWindow': 60000,
                'limit': 1000,
                'timestamp': int(time.time() * 1000)  # 以毫秒为单位的当前时间戳
            }

            query_string_in = '&'.join([f"{k}={v}" for k, v in params_in.items()])
            signature_in = hmac.new(apiSecret.encode('utf-8'), query_string_in.encode('utf-8'), hashlib.sha256).hexdigest()
            params_in['signature'] = signature_in

            try:
                response = requests.get(base_url + endpoint_trade, headers=headers, params=params_in)
                response.raise_for_status()
            except requests.exceptions.RequestException as e:
                print("Error:", e)
                break
                
            df_new = pd.DataFrame(response.json())
            trade_num = len(df_new)
            df = pd.concat([df, df_new], ignore_index=True)
            
        print(f'共获取{len(df)}条Trade数据')
        
        # 格式修改
        df.rename(columns={'symbol': 'currency', 'id': 'txHash', 'time': 'datetime'}, inplace=True)
        df['datetime'] = pd.to_datetime(df['datetime'], unit='ms').dt.strftime('%Y-%m-%dT%H:%M:%SZ')
        df['contactPlatformSlug'] = ''
        df['contactIdentity'] = ''
        df['currency'] = temp
        df["type"] = ''
        df["direction"] = ''
        df['amount'] = ''
        
        # 复制原始数据，准备拆分后的两条数据
        df_out = df.copy()
        df_in = df.copy()

        # 拆分后的第一条数据
        df_in['currency'] = df_in['currency'].apply(lambda x: x.split('/')[0])  # 取斜杠前的币种
        df_in['direction'] = df['isBuyer'].apply(lambda x: "IN" if x else "OUT")
        df_in['type'] = df['isBuyer'].apply(lambda x: "EXCHANGE_TRADE_IN" if x else "EXCHANGE_TRADE_OUT")
        df_in["amount"] = df_in['qty']

        # 拆分后的第二条数据
        df_out['currency'] = df_out['currency'].apply(lambda x: x.split('/')[1])  # 取斜杠后的币种
        df_out['direction'] = df_out['isBuyer'].apply(lambda x: "OUT" if x else "IN")
        df_out['type'] = df_out['isBuyer'].apply(lambda x: "EXCHANGE_TRADE_OUT" if x else "EXCHANGE_TRADE_IN")
        df_out['amount'] = df_out['quoteQty']


        # 合并两条拆分后的数据到原始数据框中
        df = pd.concat([df_in, df_out], ignore_index=True)
        
        # EXCHANGE FEE
        new_rows = df.copy()
        new_rows['type'] = 'EXCHANGE_FEE'
        new_rows['amount'] = new_rows['commission']
        new_rows['currency'] = new_rows['commissionAsset']
        new_rows['direction'] = 'OUT'

        # 将新的DataFrame与原始的DataFrame合并，确保不会有重复的行出现
        df = pd.concat([df, new_rows], ignore_index=True).drop_duplicates()
  
        df = df[['type', 'txHash', 'datetime', 'contactIdentity',
                        'contactPlatformSlug', 'direction', 'currency', 'amount']]
        
        # 将该 symbol 的数据添加到总的数据框中
        df_all_data = pd.concat([df_all_data, df], ignore_index=True)
        
        
        # 选择时间
        beginTime_tr = datetime.strptime(beginTime, '%Y-%m-%d').strftime('%Y-%m-%d %H:%M:%S')
        endTime_tr = str(datetime.strptime(endTime + "T23:59:59", "%Y-%m-%dT%H:%M:%S"))
        df_all_data = df_all_data[(df_all_data['datetime'] >= beginTime_tr) & (df_all_data['datetime'] <= endTime_tr)]
        
    # 创建一个空的 DataFrame，用于存放结果
    all_transfers = pd.DataFrame()

    ##### 加密货币withdraw和deposit
    # 以每90天为间隔进行循环
    while (1):
        time.sleep(1)
        if beginTime > endTime:
            break
        # 获得当前循环的结束时间
        currentEndTime = datetime.strptime(beginTime, "%Y-%m-%d") + timedelta(days=90)
        # 如果当前结束时间超过了指定的结束时间，则将其设置为指定的结束时间
        if currentEndTime > datetime.strptime(endTime, "%Y-%m-%d"):
            currentEndTime = datetime.strptime(endTime, "%Y-%m-%d")
        
        # 处理当前时间段的数据
        startTimestamp, endTimestamp = processTime(beginTime, currentEndTime, timezone)
        currentEndTime = currentEndTime.strftime("%Y-%m-%d") + "T23:59:59"
        beginTime = datetime.utcfromtimestamp(startTimestamp / 1000).strftime("%Y-%m-%dT%H:%M:%S")

        # 设置请求参数：
        params_de = {
            'status': 1, # 只查询成功的
            'startTime': startTimestamp,
            'endTime': endTimestamp
        }
        params_wi = {
            'status': 6, # 只查询提现完成的
            'startTime': startTimestamp,
            'endTime': endTimestamp
        }
        # 法定货币入金请求参数
        # params_fiat0 = {
        #     'transactionType': '0',  
        #     'timestamp': int(time.time() * 1000), 
        #     'beginTime': start_time,
        #     'endTime': end_time,
        #     'recvWindow': 60000
        # }
        # 法定货币出金请求参数
        # params_fiat1 = {
        #     'transactionType': '1',
        #     'timestamp': int(time.time() * 1000), 
        #     'beginTime': start_time,
        #     'endTime': end_time,
        #     'recvWindow': 60000
        # }
        
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
        '''
        query_string_fiat0 = '&'.join([f"{k}={v}" for k, v in params_fiat0.items()])
        signature_fiat0 = hmac.new(apiSecret.encode(), query_string_fiat0.encode(), hashlib.sha256).hexdigest()

        query_string_fiat1 = '&'.join([f"{k}={v}" for k, v in params_fiat1.items()])
        signature_fiat1 = hmac.new(apiSecret.encode(), query_string_fiat1.encode(), hashlib.sha256).hexdigest()
        '''
        try:
            ### deposit history
            print(f"开始获取 {beginTime} 到 {currentEndTime} 的Deposit数据")
            deposit_url = f'{base_url}{endpoint_deposit}'
            de_response = requests.get(deposit_url, headers=headers, params=params_de)
            de_response.raise_for_status()
            de_df = de_response.json()
            de_result = pd.DataFrame(de_df)

            trade_num = len(de_result)
            df_new = de_result
            limit = 1000
            offset = 0
            while trade_num == limit: # 数据条数超出limit限制
                time.sleep(1)
                offset += limit
        
                params_in = {
                    'status': 1, # 只查询提现完成的
                    'startTime': startTimestamp,
                    'endTime': endTimestamp,
                    'offset': offset,
                    'timestamp': int(time.time() * 1000)  # 以毫秒为单位的当前时间戳
                }

                query_string_in = '&'.join([f"{k}={v}" for k, v in params_in.items()])
                signature_in = hmac.new(apiSecret.encode('utf-8'), query_string_in.encode('utf-8'), hashlib.sha256).hexdigest()
                params_in['signature'] = signature_in

                try:
                    response = requests.get(base_url + endpoint_deposit, headers=headers, params=params_in)
                    response.raise_for_status()
                    df_new = pd.DataFrame(response.json())
                    trade_num = len(df_new)
                    de_result = pd.concat([de_result, df_new], ignore_index=True)
            
                except requests.exceptions.RequestException as e:
                    print("Error:", e)
                    break
            
            print(f'共获取该时间内{len(de_result)}条Deposit数据')
            
            if de_result.empty:
                print("没有Deposit数据")
                result = de_result
            else:
                result = de_result[['amount', 'coin', 'address', 'txId', 'insertTime', 'network']].copy()
        
                result.rename(columns={'coin': 'currency', 'txId': 'txHash', 'insertTime': 'datetime', 'address': 'contactIdentity',
                                       'network': 'contactPlatformSlug'}, inplace=True)
                result['datetime'] = pd.to_datetime(result['datetime'], unit='ms').dt.strftime('%Y-%m-%dT%H:%M:%SZ')
                # result['contactPlatformSlug'] = ''
                result['type'] = 'EXCHANGE_DEPOSIT'
                result['direction'] = 'IN'

            time.sleep(3)
            
            ### withdraw history
            print(f"开始获取 {beginTime} 到 {currentEndTime} 的Withdraw数据")
            withdraw_url = f'{base_url}{endpoint_withdraw}'
            wi_response = requests.get(withdraw_url, headers=headers, params=params_wi)
            time.sleep(3)
            wi_response.raise_for_status()
            wi_df = wi_response.json()
            wi_result = pd.DataFrame(wi_df)

            trade_num = len(wi_result)
            df_new = wi_result
            limit = 1000
            offset = 0
            while trade_num == limit: # 数据条数超出limit限制
                time.sleep(1)
                offset += limit
        
                params_in = {
                    'status': 6, # 只查询提现完成的
                    'startTime': startTimestamp,
                    'endTime': endTimestamp,
                    'offset': offset,
                    'timestamp': int(time.time() * 1000)  # 以毫秒为单位的当前时间戳
                }

                query_string_in = '&'.join([f"{k}={v}" for k, v in params_in.items()])
                signature_in = hmac.new(apiSecret.encode('utf-8'), query_string_in.encode('utf-8'), hashlib.sha256).hexdigest()
                params_in['signature'] = signature_in

                try:
                    response = requests.get(base_url + endpoint_withdraw, headers=headers, params=params_in)
                    response.raise_for_status()
                    df_new = pd.DataFrame(response.json())
                    trade_num = len(df_new)
                    wi_result = pd.concat([wi_result, df_new], ignore_index=True)
            
                except requests.exceptions.RequestException as e:
                    print("Error:", e)
                    break
            
            print(f'共获取该时间内{len(wi_result)}条Withdraw数据')

            if wi_result.empty:
                print("没有Withdraw数据")
            else: 
                wi_result = wi_result[['amount', 'coin', 'address', 'txId', 'completeTime', 'transactionFee', 'network']].copy()
                wi_result.rename(columns={'coin': 'currency', 'txId': 'txHash', 'completeTime': 'datetime', 'address': 'contactIdentity',
                                          'network': 'contactPlatformSlug'}, inplace=True)
                wi_result['datetime'] = wi_result['datetime'].apply(lambda x: datetime.strptime(x, "%Y-%m-%d %H:%M:%S").strftime('%Y-%m-%dT%H:%M:%SZ'))

                
                # wi_result['contactPlatformSlug'] = ''
                wi_result['type'] = 'EXCHANGE_WITHDRAW'
                wi_result['direction'] = 'OUT'

                # 加密货币EXCHANGE_FEE
                new_rows = wi_result.copy()
                new_rows['type'] = 'EXCHANGE_FEE'
                new_rows['amount'] = new_rows['transactionFee']
        
            result = pd.concat([result, wi_result, new_rows], ignore_index=True)
            
            result = result[['type', 'txHash', 'datetime', 'contactIdentity',
                        'contactPlatformSlug', 'direction', 'currency', 'amount']]
            '''
            ### 法币出入金
            print('开始获取Fiat数据')
            fiat_response0 = requests.get(base_url + endpoint_fiat, headers=headers, params={**params_fiat0, 'signature': signature_fiat0})
            time.sleep(20) # 避免访问过于频繁
            fiat_response1 = requests.get(base_url + endpoint_fiat, headers=headers, params={**params_fiat1, 'signature': signature_fiat1})
            fiat_response0.raise_for_status()
            fiat_response1.raise_for_status()
    
            fiat_df0 = fiat_response0.json()['data']
            fiat_df1 = fiat_response1.json()['data']
    
            fiat_result0 = pd.DataFrame(fiat_df0)
            fiat_result1 = pd.DataFrame(fiat_df1)
    
            if not fiat_result0.empty: # 入金
                fiat_result0 = fiat_result0[fiat_result0['status'] == 'Successful']
                fiat_result0 = fiat_result0[['status', 'fiatCurrency', 'totalFee', 
                                             'orderNo', 'updateTime']].copy()
                fiat_result0.rename(columns={'fiatCurrency': 'currency', 'orderNo': 'txid', 
                                             'updateTime': 'datetime'}, inplace=True)
                fiat_result0['contactPlatformSlug'] = ''
                fiat_result0['contactIdentity'] = ''
                fiat_result0['type'] = 'EXCHANGE_DEPOSIT'
                fiat_result0['direction'] = 'IN'

            if not fiat_result1.empty: # 出金
                fiat_result1 = fiat_result1[fiat_result1['status'] == 'Successful']
                fiat_result1 = fiat_result1[['status', 'fiatCurrency', 'totalFee', 
                                     'orderNo', 'updateTime']].copy()
                fiat_result1.rename(columns={'fiatCurrency': 'currency', 'orderNo': 'txid', 
                                             'updateTime': 'datetime'}, inplace=True)
                fiat_result1['contactPlatformSlug'] = ''
                fiat_result1['contactIdentity'] = ''
                fiat_result1['type'] = 'EXCHANGE_WITHDRAW'
                fiat_result1['direction'] = 'OUT'
        
            if not fiat_result0.empty or not fiat_result1.empty:
                fiat_result = pd.concat([fiat_result0, fiat_result1], ignore_index=True) # 出入金数据
            else:
                fiat_result = pd.DataFrame()
                
            if not fiat_result.empty: # 法币的EXCHAGE_FEE
                new_rows = fiat_result.copy()
                new_rows['type'] = 'EXCHANGE_FEE'
                new_rows['direction'] = 'OUT'
                new_rows['amount'] = new_rows['totalFee']
        
                result_fiat_all = pd.concat([fiat_result, new_rows], ignore_index=True)
            
                result_fiat_all = result_fiat_all[['type', 'txid', 'datetime', 'contactIdentity',
                                                   'contactPlatformSlug', 'direction', 'currency', 'amount']]
            else:
                result_fiat_all = pd.DataFrame()
                print("没有Fiat数据")
                
            result = pd.concat([result, result_fiat_all], ignore_index=True)
            '''
            current_transfers = result
            
        except requests.exceptions.RequestException as e:
            print("Error:", e)
            continue

        if current_transfers is not None:
            # 将当前时间段的结果添加到总结果中
            all_transfers = pd.concat([all_transfers, current_transfers], ignore_index=True)
        
        # 更新下一次循环的开始时间为当前循环的结束时间的下一天
        currentEndTime = datetime.strptime(currentEndTime, "%Y-%m-%dT%H:%M:%S")
        beginTime = (currentEndTime + timedelta(days=1)).strftime("%Y-%m-%d")

    all_transfers = pd.concat([df_all_data, all_transfers], ignore_index=True).drop_duplicates()

    all_transfers = all_transfers[(all_transfers['datetime'] >= beginTime_tr) & (all_transfers['datetime'] <= endTime_tr)]
    all_transfers.reset_index(drop=True, inplace=True)
    
    function_endTime = time.time()
    duration = function_endTime - function_startTime
    print(f"函数运行时间：{duration} 秒")
    return all_transfers

apiKey = 'cPRKGQV6QGTOwpQUTK6VhbungH5rTy6xKL4TZQgipr6oPqPAgtnE5gfGpI2BIu0K'
apiSecret = 'LbYip4Polxsnv2pMGoRdt3QGLmvD43555XH2iuXdsDs9V5r0C7SL9CDhzWbk9d1l'

transfers = processData(apiKey, apiSecret, beginTime = '2023-12-01', endTime = '2024-01-01', timezone = 'Asia/Shanghai')
