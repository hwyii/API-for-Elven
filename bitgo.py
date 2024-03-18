import requests
import pandas as pd
import json
from datetime import datetime
import pytz

pd.set_option('display.max_columns', 50)

def processData(access_token, beginTime=None, endTime=None, timezone=None, prevId=None):
    all_transfers = []
    base_url = 'https://app.bitgo-test.com/api/v2'
    
    # 首先获取enterpriseID
    url = f'{base_url}/wallets'
    headers = {'Authorization': f'Bearer {access_token}'}
    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        data = response.json()
        enterpriseID = data['wallets'][0]['enterprise']
        # print(enterpriseID)
    except requests.exceptions.RequestException as e:
        print("Error:", e)
        return None

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
    # print(beginTime, endTime)
    
    while True:
        try:
            # 获取转账数据
            transfer_url = f'{base_url}/enterprise/{enterpriseID}/transfer'
            
            if prevId is None:
                transfer_url += f'?dateGte={beginTime}&dateLt={endTime}&state={"confirmed"}'
            else:
                transfer_url += f'?dateGte={beginTime}&dateLt={endTime}&prevId={prevId}&state={"confirmed"}'
            
            response = requests.get(transfer_url, headers=headers)
            response.raise_for_status()
            transfer_data = response.json()['transfers']

            # 提取转账数据并加入总列表
            transfers = pd.json_normalize(transfer_data)
            all_transfers.append(transfers)

            # 获取下一页的 prevId
            response_data = response.json()
            prevId = response_data.get('nextBatchPrevId')

            if prevId is None: # 如果没有下一页，退出循环
                break

        except requests.exceptions.RequestException as e:
            print("Error:", e)
            return None

    # 合并所有转账数据并返回
    if all_transfers:
        result = pd.concat(all_transfers, ignore_index=True)
        ## 修改输出格式
        # print(result['entries'][20])
 
        # 对手方信息
        result['contactIdentity'] = ''
        result['contactPlatformSlug'] = ''
        
        for i in range(len(result)):
            if result.iloc[i]['type'] == 'send':
                for entry in result['entries'][i]:
                    if int(entry['valueString']) > 0: # 防止'value'不存在
                        result.loc[i, 'contactIdentity'] = entry['address']
                        break
            else:
                for entry in result['entries'][i]:
                    if int(entry['valueString']) < 0:
                        result.loc[i, 'contactIdentity'] = entry['address']
                        break                       
        result.rename(columns={'coin': 'currency', 'date': 'datetime',
                                'type': 'direction', 'usd': 'amount'}, inplace=True)
        result['direction'] = result['direction'].replace({'send': 'out', 'receive': 'in'})
        result['datetime'] = pd.to_datetime(result['datetime']).dt.strftime('%Y-%m-%dT%H:%M:%SZ')
        
        result['type'] = result['direction'].replace({'out': 'CUSTODY_WITHDRAW', 'in': 'CUSTODY_DEPOSIT'})
        result['amount'] = result['value'].astype(float).abs()

        result['currency'] = result['currency'].apply(lambda x: x.split(':')[1] if ':' in x else x) # 处理token命名，保留冒号后的部分
        
        result = result[['type', 'txid', 'datetime', 'contactIdentity',
                        'contactPlatformSlug', 'direction', 'currency', 'amount']]

        return result
    else:
        return None


def processData_Wallet(access_token, beginTime=None, endTime=None, timezone=None, prevId=None):
    all_transfers = []
    base_url = 'https://app.bitgo-test.com/api/v2'
    
    # 首先获取这个access token可以取到的所有walletID和对应的coin
    headers = {'Authorization': f'Bearer {access_token}'}
    all_wallets = []
    # 通过nextBatchPrevId翻页
    while True:
        try:
            url = f'{base_url}/wallets'
            
            if prevId is not None:
                url += f'?prevId={prevId}'
            
            response = requests.get(url, headers=headers)
            response.raise_for_status()
            data = response.json()
    
            # 提取所有钱包信息
            wallets = data.get('wallets',[])
            wallet_coin_info = [{'walletID': wallet.get('id'), 'coin': wallet.get('coin')} for wallet in wallets]
            df = pd.DataFrame(wallet_coin_info)
            all_wallets.append(df)

            # 获取下一页的 prevId
            response_data = response.json()
            prevId = response_data.get('nextBatchPrevId')

            if prevId is None: # 如果没有下一页，退出循环
                break
    
        except requests.exceptions.RequestException as e:
            print("Error:", e)
            return None

    combined_df = pd.concat(all_wallets, ignore_index=True) # 为可以取到的所有walletID和对应的coin
    
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

    for index, row in combined_df.iterrows():
        coin = row['coin']
        wallet_id = row['walletID']
        # 对每个wallet先将prevId置空
        prevId = None
        # 通过nextBatchPrevId翻页
        while True:
            try:
                # 获取转账数据
                transfer_url = f'{base_url}/{coin}/wallet/{wallet_id}/transfer'
            
                if prevId is None:
                    transfer_url += f'?dateGte={beginTime}&dateLt={endTime}&state={"confirmed"}'
                else:
                    transfer_url += f'?dateGte={beginTime}&dateLt={endTime}&prevId={prevId}&state={"confirmed"}'
            
                response = requests.get(transfer_url, headers=headers)
                response.raise_for_status()
                transfer_data = response.json()['transfers']

                # 提取转账数据并加入总列表
                transfers = pd.json_normalize(transfer_data)
                all_transfers.append(transfers)

                # 获取下一页的 prevId
                response_data = response.json()
                prevId = response_data.get('nextBatchPrevId')

                if prevId is None: # 如果没有下一页，退出循环
                    break

            except requests.exceptions.RequestException as e:
                print("Error:", e)
                return None
    
        # 合并所有转账数据并返回
    if all_transfers:
        result = pd.concat(all_transfers, ignore_index=True)
        ## 修改输出格式
 
        # 对手方信息
        result['contactIdentity'] = ''
        result['contactPlatformSlug'] = ''
        
        for i in range(len(result)):
            if result.iloc[i]['type'] == 'send':
                for entry in result['entries'][i]:
                    if int(entry['valueString']) > 0: # 防止'value'不存在
                        result.loc[i, 'contactIdentity'] = entry['address']
                        break
            else:
                for entry in result['entries'][i]:
                    if int(entry['valueString']) < 0:
                        result.loc[i, 'contactIdentity'] = entry['address']
                        break                       
        result.rename(columns={'coin': 'currency', 'date': 'datetime',
                                'type': 'direction', 'usd': 'amount'}, inplace=True)
        result['direction'] = result['direction'].replace({'send': 'out', 'receive': 'in'})
        result['datetime'] = pd.to_datetime(result['datetime']).dt.strftime('%Y-%m-%dT%H:%M:%SZ')
        
        result['type'] = result['direction'].replace({'out': 'CUSTODY_WITHDRAW', 'in': 'CUSTODY_DEPOSIT'})
        result['amount'] = result['value'].astype(float).abs()

        result['currency'] = result['currency'].apply(lambda x: x.split(':')[1] if ':' in x else x) # 处理token命名，保留冒号后的部分
        
        result = result[['type', 'txid', 'datetime', 'contactIdentity',
                        'contactPlatformSlug', 'direction', 'currency', 'amount']]

        return result
    else:
        return None
