import requests
import time
import hmac
import hashlib
import json
import pandas as pd
from datetime import datetime
import pytz

pd.set_option('display.max_columns', 50)

def processData(key, secret, beginTime=None, endTime=None, timezone=None):
    base_url = 'https://api.bit.com'
    endpoint = '/um/v1/transactions'
    
    ### 处理日期和时区
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
    endTime = utc_end.strftime('%Y-%m-%dT%H:%M:%SZ')
    # 转化为时间戳
    start_time = int(datetime.strptime(beginTime, '%Y-%m-%dT%H:%M:%SZ').timestamp() * 1000)
    end_time = int(datetime.strptime(endTime, '%Y-%m-%dT%H:%M:%SZ').timestamp() * 1000)
    # print(start_time, end_time)
    
    # 构建 timestamp
    timestamp = int(time.time() * 1000)

    
    # 签名算法，来源于API文档示例
    class SignatureGenerator:
        def encode_list(self, item_list):
            list_val = []
            for item in item_list:
                obj_val = self.encode_object(item)
                list_val.append(obj_val)
            sorted_list = sorted(list_val)
            output = '&'.join(sorted_list)
            output = '[' + output + ']'
            return output

        def encode_object(self, obj):
            if isinstance(obj, (str, int)):
                return str(obj)

            # treat obj as dict
            sorted_keys = sorted(obj.keys())
            ret_list = []
            for key in sorted_keys:
                val = obj[key]
                if isinstance(val, list):
                    list_val = self.encode_list(val)
                    ret_list.append(f'{key}={list_val}')
                elif isinstance(val, dict):
                    # call encode_object recursively
                    dict_val = self.encode_object(val)
                    ret_list.append(f'{key}={dict_val}')
                elif isinstance(val, bool):
                    bool_val = str(val).lower()
                    ret_list.append(f'{key}={bool_val}')
                else:
                    general_val = str(val)
                    ret_list.append(f'{key}={general_val}')

            sorted_list = sorted(ret_list)
            output = '&'.join(sorted_list)
            return output

        def get_signature(self, http_method, api_path, param_map, secret_key):
            str_to_sign = api_path + '&' + self.encode_object(param_map)
            # print('str_to_sign = ' + str_to_sign)
            sig = hmac.new(secret_key.encode('utf-8'), str_to_sign.encode('utf-8'), digestmod=hashlib.sha256).hexdigest()
            return sig


    signature_generator = SignatureGenerator()

    http_method = 'GET'

    param_map = {
        'start_time': start_time,
        'end_time': end_time,
        'timestamp': timestamp
    }

    # 使用密钥生成签名
    signature = signature_generator.get_signature(http_method, endpoint, param_map, secret)
    # print(signature)

    # 构建请求头部
    headers = {
        'X-Bit-Access-Key': key
    }
    
    # 构建查询参数
    params = {
        'start_time': start_time,
        'end_time': end_time,
        'timestamp': timestamp,
        'signature': signature
    }
   
    try:
        transfer_url = f'{base_url}{endpoint}'

        response = requests.get(transfer_url, headers=headers, params=params)
        response.raise_for_status()
        df = response.json()['data']
        info = response.json()['page_info']
        # print(info)
        result = pd.DataFrame(df)
        
        # 对手方信息
        result['contactIdentity'] = ''
        result['contactPlatformSlug'] = ''
        result.rename(columns={'ccy': 'currency', 'qty': 'amount',
                                'trade_id': 'txid', 'tx_time': 'datetime', 'tx_type': 'type'}, inplace=True)
        
        # 对时间戳进行格式转换
        result['datetime'] = pd.to_datetime(result['datetime'], unit='ms').dt.strftime('%Y-%m-%dT%H:%M:%SZ')
        
        # 选出type为withdraw, deposit, transfer-in, transfer-out的数据，注释掉可以看到所有type的数据
        result = result[result['type'].isin(['withdraw', 'deposit', 'transfer-in', 'transfer-out'])].reset_index(drop=True)
        
        # 修改withdraw和deposit
        result['type'] = result['type'].replace({'withdraw': 'EXCHANGE_WITHDRAW', 'deposit': 'EXCHANGE_DEPOSIT',
                                                 'transfer-in': 'EXCHANGE_TRADE_IN', 'transfer-out': 'EXCHANGE_TRADE_OUT'})

        # 对应修改direction
        result.loc[result['type'] == 'EXCHANGE_DEPOSIT', 'direction'] = 'IN'
        result.loc[result['type'] == 'EXCHANGE_WITHDRAW', 'direction'] = 'OUT'
        result.loc[result['type'] == 'EXCHANGE_TRADE_IN', 'direction'] = 'IN'
        result.loc[result['type'] == 'EXCHANGE_TRADE_OUT', 'direction'] = 'OUT'

        ## 加入EXCHANGE_FEE
        # 创建一个筛选条件
        condition = (result['type'] == 'EXCHANGE_WITHDRAW') | (result['type'] == 'EXCHANGE_TRADE_IN') | (result['type'] == 'EXCHANGE_TRADE_OUT') 

        # 根据筛选条件获取满足条件的行
        filtered_rows = result[condition]

        # 复制这些行并更新type为 'EXCHANGE_FEE', amount为fee_paid
        new_rows = filtered_rows.copy()
        new_rows['type'] = 'EXCHANGE_FEE'
        new_rows['amount'] = new_rows['fee_paid']

        # 将更新后的行添加回原始数据框
        result = pd.concat([result, new_rows], ignore_index=True)

        # 将amount改为正数
        result['amount'] = result['amount'].astype(float).abs()

        result = result[['type', 'txid', 'datetime', 'contactIdentity',
                        'contactPlatformSlug', 'direction', 'currency', 'amount']]
        
        return result
    except requests.exceptions.RequestException as e:
        print("Error:", e)
        return None   

key = 'your key here'
secret = 'your secret here'

transfers = processData(key, secret, beginTime = '2024-01-25', endTime = '2024-04-05', timezone = 'Asia/Shanghai')
transfers
