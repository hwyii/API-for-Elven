# processData to get withdraw, deposit and trade history
# https://www.okx.com/docs-v5/zh/?python#funding-account-rest-api-get-deposit-history
import requests
import json
import time
import hmac
import hashlib
import base64
import pandas as pd
import warnings
import pytz
import warnings
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
warnings.filterwarnings("ignore", category=FutureWarning)

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

def processData(apiKey, SecretKey, passphrase, beginTime=None, endTime=None, timezone=None):
    function_startTime = time.time()
    # endpoint
    base_url = 'https://aws.okx.com'

    # 创建一个空的 DataFrame，用于存放结果
    all_transfers = pd.DataFrame()
        
    if beginTime > endTime:
        print("时间输入错误")

    ##### withdraw和deposit
    
    currentEndTime = datetime.strptime(beginTime, "%Y-%m-%d") + timedelta(days=90)
    # 如果当前结束时间超过了指定的结束时间，则将其设置为指定的结束时间
    if currentEndTime > datetime.strptime(endTime, "%Y-%m-%d"):
        currentEndTime = datetime.strptime(endTime, "%Y-%m-%d")
        
    # 处理当前时间段的数据
    currentEndTime = currentEndTime.strftime("%Y-%m-%d") # 为了下一步传入processTime，将currentEndTime转成字符串年月日格式
    startTimestamp, endTimestamp = processTime(beginTime, currentEndTime, timezone)
    beginTimehms = datetime.utcfromtimestamp(startTimestamp / 1000).strftime("%Y-%m-%dT%H:%M:%S")
    endTimehms = datetime.utcfromtimestamp(endTimestamp / 1000).strftime("%Y-%m-%dT%H:%M:%S") # 为了便于按时间print，这两步是年月日时分秒的字符串格式

    method = "GET"
    body_wi = ""
    body_de = ""
    try:
        print(f"开始获取 {beginTimehms} 到 {endTimehms} 的Withdraw数据")
        ## withdraw history

        # 计算时间戳，使用 UTC 时间并按照 ISO 格式
        timestamp = time.strftime("%Y-%m-%dT%H:%M:%S.000Z", time.gmtime())
        # state=2表示操作成功
        endpoint_withdraw = '/api/v5/asset/withdrawal-history?state=2&type=4&before=' + str(startTimestamp) + "&after=" + str(endTimestamp)
        # 拼接需要签名的字符串
        sign_str_wi = timestamp + method + endpoint_withdraw + body_wi
        # 使用 HMAC SHA256 方法对字符串进行加密
        signature_wi = hmac.new(SecretKey.encode('utf-8'), sign_str_wi.encode('utf-8'), hashlib.sha256)
        # 对加密后的结果进行 Base64 编码
        OK_ACCESS_SIGN_wi = base64.b64encode(signature_wi.digest()).decode('utf-8')
        # 设置请求headers
        headers_wi = {
            "OK-ACCESS-KEY": apiKey,
            "OK-ACCESS-SIGN": OK_ACCESS_SIGN_wi,
            "OK-ACCESS-TIMESTAMP": timestamp,
            "OK-ACCESS-PASSPHRASE": passphrase
        }
            
        response_wi = requests.get(base_url + endpoint_withdraw, headers=headers_wi)
        response_wi.raise_for_status()
        df_wi = pd.DataFrame(response_wi.json()['data'])

        if df_wi.empty:
            print("没有Withdraw数据")
        else: 
            trade_num = len(df_wi)
            df_new = df_wi
            # 如果数据超过limit(100条)

            while trade_num == 100: # 数据条数超出limit限制
                time.sleep(2)
                min_date = min(df_new['ts'].astype(int)) # 由于时间按倒序排列，所以这里需要获取最小（最旧）的时间
                min_date_hms = datetime.utcfromtimestamp(min_date / 1000).strftime("%Y-%m-%dT%H:%M:%S")
                print(f"开始获取{min_date_hms}之前的Withdraw数据")
                # 计算时间戳，使用 UTC 时间并按照 ISO 格式
                timestamp = time.strftime("%Y-%m-%dT%H:%M:%S.000Z", time.gmtime())
                # state=2表示操作成功
                endpoint_withdraw = '/api/v5/asset/withdrawal-history?state=2&type=4&before=' + str(startTimestamp) + "&after=" + str(min_date)
                # 拼接需要签名的字符串
                sign_str_wi = timestamp + method + endpoint_withdraw + body_wi
                # 使用 HMAC SHA256 方法对字符串进行加密
                signature_wi = hmac.new(SecretKey.encode('utf-8'), sign_str_wi.encode('utf-8'), hashlib.sha256)
                # 对加密后的结果进行 Base64 编码
                OK_ACCESS_SIGN_wi = base64.b64encode(signature_wi.digest()).decode('utf-8')
                # 设置请求headers
                headers_wi = {
                    "OK-ACCESS-KEY": apiKey,
                    "OK-ACCESS-SIGN": OK_ACCESS_SIGN_wi,
                    "OK-ACCESS-TIMESTAMP": timestamp,
                    "OK-ACCESS-PASSPHRASE": passphrase
                }

                try:
                    response = requests.get(base_url + endpoint_withdraw, headers=headers_wi)
                    response.raise_for_status()
                    df_new = pd.DataFrame(response.json()['data'])
                    trade_num = len(df_new)
                    df_wi = pd.concat([df_wi, df_new], ignore_index=True)
            
                except requests.exceptions.RequestException as e:
                    print("Error:", e)
                    break
                    
            print(f'共获取该时间内{len(df_wi)}条Withdraw数据')
            
            # 格式修改
            df_wi.rename(columns={'ccy': 'currency', 'amt': 'amount', 'txId': 'txHash', 
                               'to':'contactIdentity', 'ts':'datetime'}, inplace=True) # ts只是提币申请时间
            df_wi['datetime'] = pd.to_datetime(df_wi['datetime'], unit='ms').dt.strftime('%Y-%m-%dT%H:%M:%SZ')
            df_wi['contactPlatformSlug'] = ''
            df_wi['type'] = 'EXCHANGE_WITHDRAW'
            df_wi['direction'] = 'OUT'
                
            ## EXCHANGE_FEE
            new_rows = df_wi.copy()
            new_rows['type'] = 'EXCHANGE_FEE'
            new_rows['amount'] = new_rows['fee']
            new_rows['currency'] = new_rows['feeCcy']
            
            df_wi = pd.concat([df_wi, new_rows], ignore_index=True)
            
            df_wi = df_wi[['type', 'txHash', 'datetime', 'contactIdentity',
                                'contactPlatformSlug', 'direction', 'currency', 'amount']]

            
        # deposit history
        print(f"开始获取 {beginTimehms} 到 {endTimehms} 的Deposit数据")
            
        # 计算时间戳，使用 UTC 时间并按照 ISO 格式
        timestamp = time.strftime("%Y-%m-%dT%H:%M:%S.000Z", time.gmtime())
        endpoint_deposit = '/api/v5/asset/deposit-history?state=2&before=' + str(startTimestamp) + "&after=" + str(endTimestamp)
        sign_str_de = timestamp + method + endpoint_deposit + body_de
        signature_de = hmac.new(SecretKey.encode('utf-8'), sign_str_de.encode('utf-8'), hashlib.sha256)
        OK_ACCESS_SIGN_de = base64.b64encode(signature_de.digest()).decode('utf-8')
        headers_de = {
            "OK-ACCESS-KEY": apiKey,
            "OK-ACCESS-SIGN": OK_ACCESS_SIGN_de,
            "OK-ACCESS-TIMESTAMP": timestamp,
            "OK-ACCESS-PASSPHRASE": passphrase
        }
            
        response_de = requests.get(base_url + endpoint_deposit, headers=headers_de)
        response_de.raise_for_status()
        df_de = pd.DataFrame(response_de.json()['data'])

        if df_de.empty:
            print("没有Deposit数据")
        else:
            trade_num = len(df_de)
            df_new = df_de
            # 如果数据超过limit(100条)

            while trade_num == 100: # 数据条数超出limit限制
                time.sleep(1)
                min_date = min(df_new['ts'].astype(int)) # 由于时间按倒序排列，所以这里需要获取最小（最旧）的时间
                min_date_hms = datetime.utcfromtimestamp(min_date / 1000).strftime("%Y-%m-%dT%H:%M:%S")
                print(f"开始获取{min_date_hms}之前的Deposit数据")
                # 计算时间戳，使用 UTC 时间并按照 ISO 格式
                timestamp = time.strftime("%Y-%m-%dT%H:%M:%S.000Z", time.gmtime())
                # state=2表示操作成功
                endpoint_deposit = '/api/v5/asset/deposit-history?state=2&type=4&before=' + str(startTimestamp) + "&after=" + str(min_date)
                # 拼接需要签名的字符串
                sign_str_de = timestamp + method + endpoint_deposit + body_de
                # 使用 HMAC SHA256 方法对字符串进行加密
                signature_de = hmac.new(SecretKey.encode('utf-8'), sign_str_de.encode('utf-8'), hashlib.sha256)
                # 对加密后的结果进行 Base64 编码
                OK_ACCESS_SIGN_de = base64.b64encode(signature_de.digest()).decode('utf-8')
                # 设置请求headers
                headers_de = {
                    "OK-ACCESS-KEY": apiKey,
                    "OK-ACCESS-SIGN": OK_ACCESS_SIGN_de,
                    "OK-ACCESS-TIMESTAMP": timestamp,
                    "OK-ACCESS-PASSPHRASE": passphrase
                    }

                try:
                    response = requests.get(base_url + endpoint_deposit, headers=headers_de)
                    response.raise_for_status()
                    df_new = pd.DataFrame(response.json()['data'])
                    trade_num = len(df_new)
                    df_de = pd.concat([df_de, df_new], ignore_index=True)
            
                except requests.exceptions.RequestException as e:
                    print("Error:", e)
                    break
            print(f'共获取该时间内{len(df_de)}条Deposit数据')
            # 格式修改
            df_de.rename(columns={'ccy': 'currency', 'amt': 'amount', 'txId': 'txHash', 
                            'to':'contactIdentity', 'ts':'datetime'}, inplace=True) # ts只是提币申请时间
            df_de['datetime'] = pd.to_datetime(df_de['datetime'], unit='ms').dt.strftime('%Y-%m-%dT%H:%M:%SZ')
            df_de['contactPlatformSlug'] = ''
            df_de['type'] = 'EXCHANGE_DEPOSIT'
            df_de['direction'] = 'IN'
            
            df_de = df_de[['type', 'txHash', 'datetime', 'contactIdentity',
                            'contactPlatformSlug', 'direction', 'currency', 'amount']]
            
        result = pd.concat([df_wi, df_de], ignore_index=True)
            
        
    except requests.exceptions.RequestException as e:
        print("Error:", e)

    ##### trade
    while True:
        time.sleep(1)
        if beginTime > endTime:
            break
        # 获得当前循环的结束时间
        currentEndTime = datetime.strptime(beginTime, "%Y-%m-%d") + relativedelta(months=3) # 为了用relativedelta增加3个月，将currentEndTime转换为datetime的年月日格式
        # 如果当前结束时间超过了指定的结束时间，则将其设置为指定的结束时间
        if currentEndTime > datetime.strptime(endTime, "%Y-%m-%d"):
            currentEndTime = datetime.strptime(endTime, "%Y-%m-%d")
        
        # 处理当前时间段的数据
        currentEndTime = currentEndTime.strftime("%Y-%m-%d") # 为了下一步传入processTime，将currentEndTime转成字符串年月日格式
        startTimestamp, endTimestamp = processTime(beginTime, currentEndTime, timezone)
        beginTimehms = datetime.utcfromtimestamp(startTimestamp / 1000).strftime("%Y-%m-%dT%H:%M:%S")
        endTimehms = datetime.utcfromtimestamp(endTimestamp / 1000).strftime("%Y-%m-%dT%H:%M:%S") # 为了便于按时间print，这两步是年月日时分秒的字符串格式

    
        # 计算时间戳，使用 UTC 时间并按照 ISO 格式
        timestamp = time.strftime("%Y-%m-%dT%H:%M:%S.000Z", time.gmtime())

        # 请求方法
        method = "GET"

        # 请求路径
        endpoint_trade ="/api/v5/trade/fills-history?instType=SPOT&begin=" + str(startTimestamp) + "&end=" + str(endTimestamp)

        # 请求主体（如果有的话，这里以示例为准，实际请求中请填写具体内容）
        body_trade = ""
        # 拼接需要签名的字符串
        sign_str_trade = timestamp + method + endpoint_trade + body_trade

        # 使用 HMAC SHA256 方法对字符串进行加密
        signature_trade = hmac.new(SecretKey.encode('utf-8'), sign_str_trade.encode('utf-8'), hashlib.sha256)

        # 对加密后的结果进行 Base64 编码
        OK_ACCESS_SIGN_trade = base64.b64encode(signature_trade.digest()).decode('utf-8')

        # 构造请求头
        headers_trade = {
            "OK-ACCESS-KEY": apiKey,
            "OK-ACCESS-SIGN": OK_ACCESS_SIGN_trade,
            "OK-ACCESS-TIMESTAMP": timestamp,
            "OK-ACCESS-PASSPHRASE": passphrase
        }
        
        # 发起请求
        try:
            print(f"开始获取 {beginTimehms} 到 {endTimehms} 的Trade数据")
            response = requests.get(base_url + endpoint_trade, headers=headers_trade)
            response.raise_for_status()
            df = response.json()['data']
            df = pd.DataFrame(df)

            if df.empty:
                print("没有Trade数据")
                # 直接更新时间
                currentEndTime = datetime.strptime(currentEndTime, "%Y-%m-%d") 
                beginTime = (currentEndTime + timedelta(days=1)).strftime("%Y-%m-%d") 
                continue
                
            else: 
                trade_num = len(df)
                df_new = df
                # 如果三个月的数据超过limit(100条)

                while trade_num == 100: # 数据条数超出limit限制
                    time.sleep(1)
                    min_id = min(df_new['billId'])

                    endpoint_trade_in ="/api/v5/trade/fills-history?instType=SPOT&begin=" + str(startTimestamp) + "&end=" + str(endTimestamp) + "&after=" + str(min_id)
                    body_trade_in = ""
                    sign_str_trade_in = timestamp + method + endpoint_trade_in + body_trade_in
                    signature_trade_in = hmac.new(SecretKey.encode('utf-8'), sign_str_trade_in.encode('utf-8'), hashlib.sha256)
                    OK_ACCESS_SIGN_trade_in = base64.b64encode(signature_trade_in.digest()).decode('utf-8')

                    headers_trade_in = {
                        "OK-ACCESS-KEY": apiKey,
                        "OK-ACCESS-SIGN": OK_ACCESS_SIGN_trade_in,
                        "OK-ACCESS-TIMESTAMP": timestamp,
                        "OK-ACCESS-PASSPHRASE": passphrase
                    }

                    try:
                        response = requests.get(base_url + endpoint_trade, headers=headers_trade)
                        response.raise_for_status()
                    except requests.exceptions.RequestException as e:
                        print("Error:", e)
                        break
                
                    df_new = pd.DataFrame(response.json())
                    trade_num = len(df_new)
                    df = pd.concat([df, df_new], ignore_index=True)
            
                print(f'共获取该时间内{len(df)}条Trade数据')
                
                # 格式修改
                df.rename(columns={'instId': 'currency', 'tradeId': 'txHash', 
                               'fillTime':'datetime'}, inplace=True)
                df['datetime'] = pd.to_datetime(df['datetime'], unit='ms').dt.strftime('%Y-%m-%dT%H:%M:%SZ')
                df['contactPlatformSlug'] = ''
                df['contactIdentity'] = ''
                df['type'] = ''
                df['direction'] = ''
        
                # 复制原始数据，准备拆分后的两条数据
                df_out = df.copy()
                df_in = df.copy()

                # 拆分后的第一条数据
                df_in['currency'] = df_in['currency'].apply(lambda x: x.split('-')[0])  # 取-前的币种
                df_in['direction'] = df_in['side'].apply(lambda x: "IN" if x == 'buy' else "OUT")
                df_in['type'] = df_in['side'].apply(lambda x: "EXCHANGE_TRADE_IN" if x == 'buy' else "EXCHANGE_TRADE_OUT")
                df_in["amount"] = df_in['fillSz']

                # 拆分后的第二条数据
                df_out['currency'] = df_out['currency'].apply(lambda x: x.split('-')[1])  # 取-后的币种
                df_out['direction'] = df_out['side'].apply(lambda x: "OUT" if x == 'buy' else "IN")
                df_out['type'] = df_out['side'].apply(lambda x: "EXCHANGE_TRADE_OUT" if x == 'buy' else "EXCHANGE_TRADE_IN")
                df_out['amount'] = df_out['fillPx'].astype(float) * df_out['fillSz'].astype(float)


                # 合并两条拆分后的数据到原始数据框中
                df = pd.concat([df_in, df_out], ignore_index=True)
        
                # EXCHANGE FEE
                new_rows = df_in.copy()
                new_rows['type'] = 'EXCHANGE_FEE'
                new_rows['amount'] = new_rows['fee'].astype(float) # fee是手续费金额或者返佣金额
                new_rows['currency'] = new_rows['feeCcy'] # 交易手续费币种或者返佣金币种
                new_rows['direction'] = new_rows['amount'].apply(lambda x: 'IN' if x >= 0 else 'OUT')
                new_rows['amount'] = new_rows['amount'].abs()
            
                df = pd.concat([df, new_rows], ignore_index=True)
            
                df = df[['type', 'txHash', 'datetime', 'contactIdentity',
                            'contactPlatformSlug', 'direction', 'currency', 'amount']]
                
            current_transfers = df
        except requests.exceptions.RequestException as e:
            print("Error:", e)
            break

        if current_transfers is not None:
            # 将当前时间段的结果添加到总结果中
            all_transfers = pd.concat([all_transfers, current_transfers], ignore_index=True)
        
        # 更新下一次循环的开始时间为当前循环的结束时间的下一天
        currentEndTime = datetime.strptime(currentEndTime, "%Y-%m-%d")
        beginTime = (currentEndTime + timedelta(days=1)).strftime("%Y-%m-%d")
        
    # 拼接deposit，withdraw和trade的结果
    all_transfers = pd.concat([all_transfers, result], ignore_index=True)

    function_endTime = time.time()
    duration = function_endTime - function_startTime
    print(f"函数运行时间：{duration} 秒")
    return all_transfers

apiKey = "c943b0c5-3337-451f-8df6-618231aa753a"
SecretKey = "74FE2DCF0891C02BB0772718404FF748"
passphrase = "20240220aA!"

transfers = processData(apiKey, SecretKey, passphrase, beginTime = '2023-12-01', endTime = '2024-03-01', timezone = 'Asia/Shanghai')
transfers
