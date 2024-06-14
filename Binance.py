# Binance.py
import pandas as pd
import requests
import hmac
import hashlib
import pandas as pd
import pytz
from datetime import datetime, timedelta
import time

class BinanceProcessor:
    def __init__(self, pairs, apiKey, apiSecret, beginTime=None, endTime=None, timezone=None):
        self.apiKey = apiKey
        self.apiSecret = apiSecret
        self.pairs = pairs
        self.beginTime = beginTime
        self.endTime = endTime
        self.timezone = timezone
        

    def processTime(self, beginTime, endTime, timezone):
        """
        处理时间的函数
        """
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

    def processDataTrade(self):
        """
        获取trade history
        """
        beginTime = self.beginTime
        endTime = self.endTime
        
        function_startTime = time.time()
        # endpoint
        base_url = 'https://api.binance.com'
        endpoint_trade = '/api/v3/myTrades'
    
        # 设置请求headers
        headers = {
            'X-MBX-APIKEY': self.apiKey,
        } 
    
        print('开始获取Trade数据')
        # 创建一个空的 DataFrame 用于存储所有数据
        df_all_data = pd.DataFrame()

        # 遍历每个 symbol
        for symbol in self.pairs['binanceTest']:
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
            signature = hmac.new(self.apiSecret.encode('utf-8'), query_string.encode('utf-8'), hashlib.sha256).hexdigest()
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
                signature_in = hmac.new(self.apiSecret.encode('utf-8'), query_string_in.encode('utf-8'), hashlib.sha256).hexdigest()
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
        
            
            # 将该 symbol 的数据添加到总的数据框中
            df_all_data = pd.concat([df_all_data, df], ignore_index=True)
        
            # 选择时间
            startTimestamp, endTimestamp = self.processTime(beginTime, endTime, self.timezone)
            df_all_data = df_all_data[(df_all_data['time'] >= startTimestamp) & (df_all_data['time'] <= endTimestamp)]
    
        function_endTime = time.time()
        duration = function_endTime - function_startTime
        print(f"函数运行时间：{duration} 秒")

        return df_all_data

    def processDataWithdraw(self):
        """
        处理提现数据的函数
        """

        beginTime = self.beginTime
        endTime = self.endTime
        
        function_startTime = time.time()
        # endpoint
        base_url = 'https://api.binance.com'
        endpoint_withdraw = '/sapi/v1/capital/withdraw/history'

        # 设置请求headers
        headers = {
            'X-MBX-APIKEY': self.apiKey,
        } 
    
        # 创建一个空的 DataFrame，用于存放结果
        all_transfers = pd.DataFrame()

        # 以每90天为间隔进行循环
        while (1):
            time.sleep(1)
            if beginTime > endTime:
                break
            # 获得当前循环的结束时间
            currentEndTime = datetime.strptime(beginTime, "%Y-%m-%d") + timedelta(days=90) # 为了用timedelta增加90天，将currentEndTime转换为datetime的年月日格式
            # 如果当前结束时间超过了指定的结束时间，则将其设置为指定的结束时间
            if currentEndTime > datetime.strptime(endTime, "%Y-%m-%d"):
                currentEndTime = datetime.strptime(endTime, "%Y-%m-%d")
        
            # 处理当前时间段的数据
            currentEndTime = currentEndTime.strftime("%Y-%m-%d") # 为了下一步传入processTime，将currentEndTime转成字符串年月日格式
            startTimestamp, endTimestamp = self.processTime(beginTime, currentEndTime, self.timezone)
            beginTimehms = datetime.utcfromtimestamp(startTimestamp / 1000).strftime("%Y-%m-%dT%H:%M:%S")
            endTimehms = datetime.utcfromtimestamp(endTimestamp / 1000).strftime("%Y-%m-%dT%H:%M:%S") # 为了便于按时间print，这两步是年月日时分秒的字符串格式

    
            # 设置请求参数：
            params_wi = {
                'status': 6, # 只查询提现完成的
                'startTime': startTimestamp,
                'endTime': endTimestamp
            }
        
            # 参数中加时间戳：
            timestamp = int(time.time() * 1000) # 以毫秒为单位的 UNIX 时间戳
            params_wi['timestamp'] = timestamp
    
            # 参数中加签名：
            query_string_wi = f'status={params_wi["status"]}&startTime={params_wi["startTime"]}&endTime={params_wi["endTime"]}&timestamp={timestamp}'
            signature_wi = hmac.new(self.apiSecret.encode('utf-8'), query_string_wi.encode('utf-8'), hashlib.sha256).hexdigest()
            params_wi['signature'] = signature_wi

            try:
                ### withdraw history
                print(f"开始获取 {beginTimehms} 到 {endTimehms} 的Withdraw数据")
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
                    signature_in = hmac.new(self.apiSecret.encode('utf-8'), query_string_in.encode('utf-8'), hashlib.sha256).hexdigest()
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
                current_transfers = wi_result
            
            except requests.exceptions.RequestException as e:
                print("Error:", e)
                continue

            if current_transfers is not None:
                # 将当前时间段的结果添加到总结果中
                all_transfers = pd.concat([all_transfers, current_transfers], ignore_index=True)
        
            # 更新下一次循环的开始时间为当前循环的结束时间的下一天
            currentEndTime = datetime.strptime(currentEndTime, "%Y-%m-%d")
            beginTime = (currentEndTime + timedelta(days=1)).strftime("%Y-%m-%d")

        all_transfers.reset_index(drop=True, inplace=True)
    
        function_endTime = time.time()
        duration = function_endTime - function_startTime
        print(f"函数运行时间：{duration} 秒")
        return all_transfers

    def processDataDeposit(self):
        """
        获得deposit history
        """
        beginTime = self.beginTime
        endTime = self.endTime
        
        function_startTime = time.time()
        # endpoint
        base_url = 'https://api.binance.com'
        endpoint_deposit = '/sapi/v1/capital/deposit/hisrec'

        # 设置请求headers
        headers = {
            'X-MBX-APIKEY': self.apiKey,
        } 
    
        # 创建一个空的 DataFrame，用于存放结果
        all_transfers = pd.DataFrame()
    
        # 以每90天为间隔进行循环
        while (1):
            time.sleep(1)
            if beginTime > endTime:
                break
            # 获得当前循环的结束时间
            currentEndTime = datetime.strptime(beginTime, "%Y-%m-%d") + timedelta(days=90) # 为了用timedelta增加90天，将currentEndTime转换为datetime的年月日格式
            # 如果当前结束时间超过了指定的结束时间，则将其设置为指定的结束时间
            if currentEndTime > datetime.strptime(endTime, "%Y-%m-%d"):
                currentEndTime = datetime.strptime(endTime, "%Y-%m-%d")
        
            # 处理当前时间段的数据
            currentEndTime = currentEndTime.strftime("%Y-%m-%d") # 为了下一步传入processTime，将currentEndTime转成字符串年月日格式
            startTimestamp, endTimestamp = self.processTime(beginTime, currentEndTime, self.timezone)
            beginTimehms = datetime.utcfromtimestamp(startTimestamp / 1000).strftime("%Y-%m-%dT%H:%M:%S")
            endTimehms = datetime.utcfromtimestamp(endTimestamp / 1000).strftime("%Y-%m-%dT%H:%M:%S") # 为了便于按时间print，这两步是年月日时分秒的字符串格式
    
            # 设置请求参数：
            params_de = {
                'status': 1, # 只查询成功的
                'startTime': startTimestamp,
                'endTime': endTimestamp
            }
        
            # 参数中加时间戳：
            timestamp = int(time.time() * 1000) # 以毫秒为单位的 UNIX 时间戳
            params_de['timestamp'] = timestamp

            # 参数中加签名：
            query_string_de = f'status={params_de["status"]}&startTime={params_de["startTime"]}&endTime={params_de["endTime"]}&timestamp={timestamp}'
            signature_de = hmac.new(self.apiSecret.encode('utf-8'), query_string_de.encode('utf-8'), hashlib.sha256).hexdigest()
            params_de['signature'] = signature_de

            try:
                ### deposit history
                print(f"开始获取 {beginTimehms} 到 {endTimehms} 的Deposit数据")
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
                    signature_in = hmac.new(self.apiSecret.encode('utf-8'), query_string_in.encode('utf-8'), hashlib.sha256).hexdigest()
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
                current_transfers = de_result
            
            except requests.exceptions.RequestException as e:
                print("Error:", e)
                continue

            if current_transfers is not None:
                 # 将当前时间段的结果添加到总结果中
                all_transfers = pd.concat([all_transfers, current_transfers], ignore_index=True)
        
            # 更新下一次循环的开始时间为当前循环的结束时间的下一天
            currentEndTime = datetime.strptime(currentEndTime, "%Y-%m-%d")
            beginTime = (currentEndTime + timedelta(days=1)).strftime("%Y-%m-%d")

        all_transfers.reset_index(drop=True, inplace=True)
        
        function_endTime = time.time()
        duration = function_endTime - function_startTime
        print(f"函数运行时间：{duration} 秒")
        return all_transfers

    def processDatac2c(self):
        '''
        C2C history
        '''
        beginTime = self.beginTime
        endTime = self.endTime
        
        function_startTime = time.time()
        # endpoint
        base_url = 'https://api.binance.com'
        endpoint = '/sapi/v1/pay/transactions'

        # 设置请求headers
        headers = {
            'X-MBX-APIKEY': self.apiKey,
        } 

        # 创建一个空的 DataFrame，用于存放结果
        all_transfers = pd.DataFrame()
    
        # 以每30天为间隔进行循环
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
       
            ### 处理日期和时区
            beginTime = beginTime + 'T00:00:00'
            currentEndTime = currentEndTime.strftime("%Y-%m-%d") + "T23:59:59"
        
            dt_begin = datetime.strptime(beginTime, "%Y-%m-%dT%H:%M:%S")
            dt_end = datetime.strptime(currentEndTime, "%Y-%m-%dT%H:%M:%S")

            # 获取目标时区的时区对象
            target_timezone = pytz.timezone(self.timezone)
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

            print('开始获取数据')
            # 构造请求参数
            params = {
                'startTime': start_time,
                'endTime': end_time,
                'recvWindow': 6000,
                'timestamp': int(time.time() * 1000)  # 以毫秒为单位的当前时间戳
            }

            # 计算签名
            query_string = '&'.join([f"{k}={v}" for k, v in params.items()])
            signature = hmac.new(self.apiSecret.encode('utf-8'), query_string.encode('utf-8'), hashlib.sha256).hexdigest()
            params['signature'] = signature

            try:
                response = requests.get(base_url + endpoint, headers=headers, params=params)
                response.raise_for_status()  # 如果请求不成功，会抛出异常
                df = pd.DataFrame(response.json()['data'])
            
            except requests.exceptions.RequestException as e:
                print("Error:", e)
            
            if df.empty:
                print("没有数据")

            current_transfers = df
            if current_transfers is not None:
                # 将当前时间段的结果添加到总结果中
                all_transfers = pd.concat([all_transfers, current_transfers], ignore_index=True)
        
            # 更新下一次循环的开始时间为当前循环的结束时间的下一天
            currentEndTime = datetime.strptime(currentEndTime, "%Y-%m-%dT%H:%M:%S")
            beginTime = (currentEndTime + timedelta(days=1)).strftime("%Y-%m-%d")    

        function_endTime = time.time()
        duration = function_endTime - function_startTime
        print(f"函数运行时间：{duration} 秒")
    
        print(f'共获取{len(df)}条数据')
        return all_transfers

    def processDataEarn(self):
        '''
        获取Simple Earn Flexible Subscription数据
        '''
        beginTime = self.beginTime
        endTime = self.endTime
        
        function_startTime = time.time()
        # endpoint
        base_url = 'https://api.binance.com'
        endpoint = '/sapi/v1/simple-earn/flexible/history/subscriptionRecord'

        # 设置请求headers
        headers = {
            'X-MBX-APIKEY': self.apiKey,
        } 

        # 创建一个空的 DataFrame，用于存放结果
        all_transfers = pd.DataFrame()

        print('开始获取数据')
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
       
            ### 处理日期和时区
            beginTime = beginTime + 'T00:00:00'
            currentEndTime = currentEndTime.strftime("%Y-%m-%d") + "T23:59:59"
        
            dt_begin = datetime.strptime(beginTime, "%Y-%m-%dT%H:%M:%S")
            dt_end = datetime.strptime(currentEndTime, "%Y-%m-%dT%H:%M:%S")

            # 获取目标时区的时区对象
            target_timezone = pytz.timezone(self.timezone)
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
        
            # 构造请求参数
            params = {
                'startTime': start_time,
                'endTime': end_time,
                'size': 100,
                'timestamp': int(time.time() * 1000)  # 以毫秒为单位的当前时间戳
            }

            # 计算签名
            query_string = '&'.join([f'{key}={value}' for key, value in sorted(params.items())])
            signature = hmac.new(self.apiSecret.encode(), query_string.encode(), hashlib.sha256).hexdigest()

            try:
                url = f'{base_url}{endpoint}?{query_string}&signature={signature}'
                response = requests.get(url, headers=headers)
                response.raise_for_status()
                df = pd.DataFrame(response.json()['rows'])
                current = 1
                num = len(df)
            
            except requests.exceptions.RequestException as e:
                print("Error:", e)

            while num == 100: # 数据条数超出限制
                time.sleep(1)
                current += 1
        
                params_in = {
                    'current': current,
                    'startTime': start_time,
                    'endTime': end_time,
                    'size': 100,
                    'timestamp': int(time.time() * 1000)  # 以毫秒为单位的当前时间戳
                }

                query_string_in = '&'.join([f'{key}={value}' for key, value in sorted(params_in.items())])
                signature = hmac.new(self.apiSecret.encode(), query_string_in.encode(), hashlib.sha256).hexdigest()

                try:
                    url = f'{base_url}{endpoint}?{query_string}&signature={signature}'
                    response = requests.get(url, headers=headers)
                    response.raise_for_status()
            
                except requests.exceptions.RequestException as e:
                    print("Error:", e)
                    break
                
                df_new = pd.DataFrame(response.json()['rows'])
                num = len(df_new)
                df = pd.concat([df, df_new], ignore_index=True)

            if df.empty:
                print("没有数据")

            current_transfers = df

            if current_transfers is not None:
                # 将当前时间段的结果添加到总结果中
                all_transfers = pd.concat([all_transfers, current_transfers], ignore_index=True)
        
            # 更新下一次循环的开始时间为当前循环的结束时间的下一天
            currentEndTime = datetime.strptime(currentEndTime, "%Y-%m-%dT%H:%M:%S")
            beginTime = (currentEndTime + timedelta(days=1)).strftime("%Y-%m-%d")

    
        function_endTime = time.time()
        duration = function_endTime - function_startTime
        print(f"函数运行时间：{duration} 秒")
    
        print(f'共获取{len(all_transfers)}条数据')
        return all_transfers

    def processDataBNB(self):
        '''
        获得Small Assests Exchange BNB数据
        '''

        beginTime = self.beginTime
        endTime = self.endTime
        
        function_startTime = time.time()
        # endpoint
        base_url = 'https://api.binance.com'
        endpoint = '/sapi/v1/asset/dribblet'

        # 设置请求headers
        headers = {
            'X-MBX-APIKEY': self.apiKey,
        } 

        # 创建一个空的 DataFrame，用于存放结果
        all_transfers = pd.DataFrame()

        start_time, end_time = self.processTime(beginTime, endTime, self.timezone)
    
        print('开始获取数据')
        # 构造请求参数
        params = {
            'accountType': 'SPOT',
            'startTime': start_time,
            'endTime': end_time,
            'timestamp': int(time.time() * 1000)  # 以毫秒为单位的当前时间戳
        }

        # 计算签名
        query_string = '&'.join([f'{key}={value}' for key, value in sorted(params.items())])
        signature = hmac.new(self.apiSecret.encode(), query_string.encode(), hashlib.sha256).hexdigest()

        try:
            url = f'{base_url}{endpoint}?{query_string}&signature={signature}'
            response = requests.get(url, headers=headers)
            response.raise_for_status()
            if response.json()['userAssetDribblets'] == []:
                df = pd.DataFrame()
            else:
                df = pd.DataFrame(response.json()['userAssetDribblets'][0]["userAssetDribbletDetails"])
        
        except requests.exceptions.RequestException as e:
            print("Error:", e)
            
        if df.empty:
            print("没有数据")
    
        function_endTime = time.time()
        duration = function_endTime - function_startTime
        print(f"函数运行时间：{duration} 秒")
    
        print(f'共获取{len(df)}条数据')
        return df

    def processDataFuture(self):
        '''
        获取future数据
        '''
        beginTime = self.beginTime
        endTime = self.endTime
        
        function_startTime = time.time()
        # endpoint
        base_url = 'https://fapi.binance.com'
        endpoint = '/fapi/v1/userTrades'

        # 设置请求headers
        headers = {
            'X-MBX-APIKEY': self.apiKey,
        } 
    
        print('开始获取数据')
        # 创建一个空的 DataFrame 用于存储所有数据
        df_all_data = pd.DataFrame()

        # 遍历每个 symbol
        for symbol in ['BTC/USDT', 'XRP/USDT', 'ETH/USDT']:#pairs['binanceTest']:
            if pd.isna(symbol):
                print('已经获取所有数据！')
                break
            print(f'开始获取 {symbol} 数据')
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
            signature = hmac.new(self.apiSecret.encode('utf-8'), query_string.encode('utf-8'), hashlib.sha256).hexdigest()
            params['signature'] = signature

            try:
                response = requests.get(base_url + endpoint, headers=headers, params=params)
                response.raise_for_status()  # 如果请求不成功，会抛出异常
                df = pd.DataFrame(response.json())
                trade_num = len(df)
                df_new = df
            except requests.exceptions.RequestException as e:
                print("Error:", e)
                df = pd.DataFrame()
            
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
                signature_in = hmac.new(self.apiSecret.encode('utf-8'), query_string_in.encode('utf-8'), hashlib.sha256).hexdigest()
                params_in['signature'] = signature_in

                try:
                    response = requests.get(base_url + endpoint, headers=headers, params=params_in)
                    response.raise_for_status()
                except requests.exceptions.RequestException as e:
                    print("Error:", e)
                    break
                
                df_new = pd.DataFrame(response.json())
                trade_num = len(df_new)
                df = pd.concat([df, df_new], ignore_index=True)
                
            print(f'共获取{len(df)}条Future数据')
        
            # 将该 symbol 的数据添加到总的数据框中
            df_all_data = pd.concat([df_all_data, df], ignore_index=True)
        
            # 选择时间
            startTimestamp, endTimestamp = self.processTime(beginTime, endTime, self.timezone)
            df_all_data = df_all_data[(df_all_data['time'] >= startTimestamp) & (df_all_data['time'] <= endTimestamp)]
    
        function_endTime = time.time()
        duration = function_endTime - function_startTime
        print(f"函数运行时间：{duration} 秒")
    
        print(f'共获取{len(df_all_data)}条数据')
        return df_all_data

    def processDataIncome(self):
        '''
        获取Income数据
        '''
        beginTime = self.beginTime
        endTime = self.endTime
        
        function_startTime = time.time()
        # endpoint
        base_url = 'https://fapi.binance.com'
        endpoint = '/fapi/v1/income'

        # 设置请求headers
        headers = {
            'X-MBX-APIKEY': self.apiKey,
        } 

        # 创建一个空的 DataFrame，用于存放结果
        all_transfers = pd.DataFrame()
        print('开始获取数据')
    
        while (1):
            time.sleep(1)
            if beginTime > endTime:
                break
            # 获得当前循环的结束时间
            currentEndTime = datetime.strptime(beginTime, "%Y-%m-%d") + timedelta(days=7)
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
            target_timezone = pytz.timezone(self.timezone)
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
            
            beginTimehms = datetime.utcfromtimestamp(start_time / 1000).strftime("%Y-%m-%dT%H:%M:%S")
            endTimehms = datetime.utcfromtimestamp(end_time / 1000).strftime("%Y-%m-%dT%H:%M:%S")
            # 构造请求参数
            params = {
                'page': 1,
                'startTime': start_time,
                'endTime': end_time,
                'limit': 1000,
                'timestamp': int(time.time() * 1000)  # 以毫秒为单位的当前时间戳
            }

            # 计算签名
            query_string = '&'.join([f'{key}={value}' for key, value in sorted(params.items())])
            signature = hmac.new(self.apiSecret.encode(), query_string.encode(), hashlib.sha256).hexdigest()
        
            print(f"开始获取 {beginTimehms} 到 {endTimehms} 的数据")
            try:
                url = f'{base_url}{endpoint}?{query_string}&signature={signature}'
                response = requests.get(url, headers=headers)
                response.raise_for_status()
                df = pd.DataFrame(response.json()) 
                df_num = len(df)
                if df.empty:
                    print("没有数据")
                
            except requests.exceptions.RequestException as e:
                print("Error:", e)
        
            page = 1
    
            while df_num == 1000:
                time.sleep(1)
                page += 1
                params_in = {
                    'page': page,
                    'startTime': start_time,
                    'endTime': end_time,
                    'limit': 1000,
                    'timestamp': int(time.time() * 1000)  # 以毫秒为单位的当前时间戳
                }

                query_string_in = '&'.join([f"{k}={v}" for k, v in params_in.items()])
                signature_in = hmac.new(self.apiSecret.encode('utf-8'), query_string_in.encode('utf-8'), hashlib.sha256).hexdigest()
                params_in['signature'] = signature_in

                try:
                    response = requests.get(base_url + endpoint, headers=headers, params=params_in)
                    response.raise_for_status()
                except requests.exceptions.RequestException as e:
                    print("Error:", e)
                    break
                
                df_new = pd.DataFrame(response.json())
                df_num = len(df_new)
                df = pd.concat([df, df_new], ignore_index=True)
        
            current_transfers = df

            if current_transfers is not None:
                # 将当前时间段的结果添加到总结果中
                all_transfers = pd.concat([all_transfers, current_transfers], ignore_index=True)
        
            # 更新下一次循环的开始时间为当前循环的结束时间的下一天
            currentEndTime = datetime.strptime(currentEndTime, "%Y-%m-%dT%H:%M:%S")
            beginTime = (currentEndTime + timedelta(days=1)).strftime("%Y-%m-%d")
                
        function_endTime = time.time()
        duration = function_endTime - function_startTime
        print(f"函数运行时间：{duration} 秒")
    
        print(f'共获取{len(all_transfers)}条数据')
        return all_transfers
