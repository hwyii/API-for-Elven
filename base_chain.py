import requests
import pandas as pd
import pytz
from datetime import datetime, timedelta
import time
pd.options.mode.chained_assignment = None
pd.set_option('display.max_columns', 50)
pd.set_option('display.max_rows', 100)

# 普通交易
def processDataNormal(apiKey, address, beginTime=None, endTime=None, timezone=None):
    # Base URL and API endpoint
    base_url = "https://www.oklink.com/api/v5/explorer/address/normal-transaction-list"
    if beginTime is None:
        beginTime = '2010-01-01'
    if endTime is None:
        endTime = (datetime.utcnow() + timedelta(days=1)).strftime('%Y-%m-%d')
    if timezone is None:
        timezone = 'UTC'
    # Initialize a DataFrame to hold all transactions
    all_transactions = pd.DataFrame()

    page = 1
    limit = 100  # limit to 100 transactions at maximum per request
    
    while True:
        # Construct query parameters
        query_params = {
            "chainShortName": "base",
            "address": address,
            "page": page,
            "limit": limit
        }

        # Send the API request
        response = requests.get(base_url, headers={'Ok-Access-Key': apiKey}, params=query_params)

        # Ensure the request was successful
        if response.status_code == 200:
            response_data = response.json()
            # Extract the transactions data
            if "data" in response_data and len(response_data["data"]) > 0:
                transactions = response_data["data"][0].get("transactionList", [])
            else:
                transactions = []
            
            if not transactions:
                break  # Exit the loop if there are no transactions
            
            # Convert the transactions data to a DataFrame
            df = pd.json_normalize(transactions)
            num = len(df)
            
            # Adding custom columns based on specific data
            if not df.empty:
                df.rename(columns={'txId': 'txHash', 'symbol': 'currency', 'transactionTime': 'datetime'}, inplace=True)
                df['datetime'] = pd.to_datetime(pd.to_numeric(df['datetime'], errors='coerce'), unit='ms').dt.strftime('%Y-%m-%dT%H:%M:%SZ')
                df['contactPlatformSlug'] = ''
                df['direction'] = df.apply(lambda row: 'IN' if row['to'] == address else 'OUT', axis=1)
                df['amount'] = df['amount'].astype(float)
                df['contactIdentity'] = df.apply(lambda row: row['to'] if row['from'] == address else row['from'], axis=1)
                df['type'] = df.apply(lambda row: 'CHAIN_TRANSFER_IN' if row['to'] == address else 'CHAIN_TRANSFER_OUT', axis=1)
                df = df[df['state'] == 'success']
                df['txFee'] = df['txFee'].astype(float)
                # FEE
                new_rows = df[df['type'] == 'CHAIN_TRANSFER_OUT'].copy()
                new_rows['type'] = 'CHAIN_TRANSACTION_FEE'
                new_rows['amount'] = new_rows['txFee']
                new_rows['currency'] = new_rows['currency']
                new_rows['direction'] = 'OUT'
                
                # 将新的DataFrame与原始的DataFrame合并，确保不会有重复的行出现
                df = pd.concat([df, new_rows], ignore_index=True)
                
                df = df[['type', 'txHash', 'datetime', 'contactIdentity',
                      'contactPlatformSlug', 'direction', 'currency', 'amount']]
                
                all_transactions = pd.concat([all_transactions, df], ignore_index=True)
        else:
            print(f"Error: {response.status_code} - {response.text}")
            return pd.DataFrame()  # return an empty DataFrame in case of an error

        page += 1  # Move to the next page

    # 过滤时间
    if not all_transactions.empty:
        if beginTime and endTime and timezone:
            timezone = pytz.timezone(timezone)
            beginTime_tr = timezone.localize(datetime.strptime(beginTime, '%Y-%m-%d').replace(hour=0, minute=0, second=0)).astimezone(pytz.utc).strftime('%Y-%m-%dT%H:%M:%SZ')
            endTime_tr = timezone.localize(datetime.strptime(endTime, '%Y-%m-%d').replace(hour=23, minute=59, second=59)).astimezone(pytz.utc).strftime('%Y-%m-%dT%H:%M:%SZ')
        
            all_transactions = all_transactions[(all_transactions['datetime'] >= beginTime_tr) & (all_transactions['datetime'] <= endTime_tr)]
        
        # Reset index and drop duplicates just in case
        all_transactions.drop_duplicates(subset=['type', 'txHash', 'amount', 'contactIdentity'], inplace=True)
        all_transactions.reset_index(drop=True, inplace=True)
    
    return all_transactions

# 内部交易
def processDataIn(apiKey, address, beginTime=None, endTime=None, timezone=None):
    # Base URL and API endpoint
    base_url = "https://www.oklink.com/api/v5/explorer/address/internal-transaction-list"
    if beginTime is None:
        beginTime = '2010-01-01'
    if endTime is None:
        endTime = (datetime.utcnow() + timedelta(days=1)).strftime('%Y-%m-%d')
    if timezone is None:
        timezone = 'UTC'
    # Initialize a DataFrame to hold all transactions
    all_transactions = pd.DataFrame()

    page = 1
    limit = 100  # limit to 100 transactions at maximum per request
    
    while True:
        # Construct query parameters
        query_params = {
            "chainShortName": "base",
            "address": address,
            "page": page,
            "limit": limit
        }

        # Send the API request
        response = requests.get(base_url, headers={'Ok-Access-Key': apiKey}, params=query_params)

        # Ensure the request was successful
        if response.status_code == 200:
            response_data = response.json()
            # Extract the transactions data
            if "data" in response_data and len(response_data["data"]) > 0:
                transactions = response_data["data"][0].get("transactionList", [])
            else:
                transactions = []
            
            if not transactions:
                break  # Exit the loop if there are no transactions
            
            # Convert the transactions data to a DataFrame
            df = pd.json_normalize(transactions)
            
            # Adding custom columns based on specific data
            if not df.empty:
                df.rename(columns={'txId': 'txHash', 'symbol': 'currency', 'transactionTime': 'datetime'}, inplace=True)
                df['datetime'] = pd.to_datetime(pd.to_numeric(df['datetime'], errors='coerce'), unit='ms').dt.strftime('%Y-%m-%dT%H:%M:%SZ')
                df['contactPlatformSlug'] = ''
                df['direction'] = df['to'].apply(lambda x: 'IN' if x == address else 'OUT')
                df['amount'] = df['amount'].astype(float)
                df['contactIdentity'] = df.apply(lambda row: row['to'] if row['from'] == address else row['from'], axis=1)
                df['type'] = df.apply(lambda row: 'CHAIN_TRANSFER_IN' if row['to'] == address else 'CHAIN_TRANSFER_OUT', axis=1)
                
                df = df[['type', 'txHash', 'datetime', 'contactIdentity',
                      'contactPlatformSlug', 'direction', 'currency', 'amount']]
                
                all_transactions = pd.concat([all_transactions, df], ignore_index=True)
        else:
            print(f"Error: {response.status_code} - {response.text}")
            return pd.DataFrame()  # return an empty DataFrame in case of an error

        page += 1  # Move to the next page

    # 过滤时间
    if not all_transactions.empty:
        if beginTime and endTime and timezone:
            timezone = pytz.timezone(timezone)
            beginTime_tr = timezone.localize(datetime.strptime(beginTime, '%Y-%m-%d').replace(hour=0, minute=0, second=0)).astimezone(pytz.utc).strftime('%Y-%m-%dT%H:%M:%SZ')
            endTime_tr = timezone.localize(datetime.strptime(endTime, '%Y-%m-%d').replace(hour=23, minute=59, second=59)).astimezone(pytz.utc).strftime('%Y-%m-%dT%H:%M:%SZ')
        
            all_transactions = all_transactions[(all_transactions['datetime'] >= beginTime_tr) & (all_transactions['datetime'] <= endTime_tr)]
        
        # Reset index and drop duplicates just in case
        all_transactions.drop_duplicates(subset=['type', 'txHash', 'contactIdentity', 'amount'])
        all_transactions.reset_index(drop=True, inplace=True)
    
    return all_transactions

# 代币转账
def processDataToken(apiKey, address, beginTime=None, endTime=None, timezone=None):
    # Base URL and API endpoint
    base_url = "https://www.oklink.com/api/v5/explorer/address/token-transaction-list"
    if beginTime is None:
        beginTime = '2010-01-01'
    if endTime is None:
        endTime = (datetime.utcnow() + timedelta(days=1)).strftime('%Y-%m-%d')
    if timezone is None:
        timezone = 'UTC'
    # Initialize a DataFrame to hold all transactions
    all_transactions = pd.DataFrame()

    page = 1
    limit = 100  # limit to 100 transactions at maximum per request
    
    while True:
        # Construct query parameters
        query_params = {
            "chainShortName": "base",
            "address": address,
            "protocolType": "token_20",
            "page": page,
            "limit": limit
        }

        # Send the API request
        response = requests.get(base_url, headers={'Ok-Access-Key': apiKey}, params=query_params)

        # Ensure the request was successful
        if response.status_code == 200:
            response_data = response.json()
            # Extract the transactions data
            if "data" in response_data and len(response_data["data"]) > 0:
                transactions = response_data["data"][0].get("transactionList", [])
            else:
                transactions = []
            
            if not transactions:
                break  # Exit the loop if there are no transactions
            
            # Convert the transactions data to a DataFrame
            df = pd.json_normalize(transactions)
        
            # Adding custom columns based on specific data
            if not df.empty:
                df.rename(columns={'txId': 'txHash', 'symbol': 'currency', 'transactionTime': 'datetime'}, inplace=True)
                df['datetime'] = pd.to_datetime(pd.to_numeric(df['datetime'], errors='coerce'), unit='ms').dt.strftime('%Y-%m-%dT%H:%M:%SZ')
                df['contactPlatformSlug'] = ''
                df['direction'] = df['to'].apply(lambda x: 'IN' if x == address else 'OUT')
                df['amount'] = df['amount'].astype(float)
                df['contactIdentity'] = df.apply(lambda row: row['to'] if row['from'] == address else row['from'], axis=1)
                df['type'] = df.apply(lambda row: 'CHAIN_TRANSFER_IN' if row['to'] == address else 'CHAIN_TRANSFER_OUT', axis=1)
                
                df = df[['type', 'txHash', 'datetime', 'contactIdentity',
                      'contactPlatformSlug', 'direction', 'currency', 'amount']]
                
                all_transactions = pd.concat([all_transactions, df], ignore_index=True)
                
        else:
            print(f"Error: {response.status_code} - {response.text}")
            return pd.DataFrame()  # return an empty DataFrame in case of an error

        page += 1  # Move to the next page

    # 过滤时间
    if not all_transactions.empty:
        if beginTime and endTime and timezone:
            timezone = pytz.timezone(timezone)
            beginTime_tr = timezone.localize(datetime.strptime(beginTime, '%Y-%m-%d').replace(hour=0, minute=0, second=0)).astimezone(pytz.utc).strftime('%Y-%m-%dT%H:%M:%SZ')
            endTime_tr = timezone.localize(datetime.strptime(endTime, '%Y-%m-%d').replace(hour=23, minute=59, second=59)).astimezone(pytz.utc).strftime('%Y-%m-%dT%H:%M:%SZ')
        
            all_transactions = all_transactions[(all_transactions['datetime'] >= beginTime_tr) & (all_transactions['datetime'] <= endTime_tr)]
        
        # Reset index and drop duplicates just in case
        all_transactions.drop_duplicates(subset=['type', 'txHash', 'contactIdentity', 'amount'])
        all_transactions.reset_index(drop=True, inplace=True)
    
    return all_transactions

def processData(apiKey, address, beginTime=None, endTime=None, timezone=None):
    if beginTime is None:
        beginTime = '2010-01-01'
    if endTime is None:
        endTime = (datetime.utcnow() + timedelta(days=1)).strftime('%Y-%m-%d')
    if timezone is None:
        timezone = 'UTC'
    # normal
    df = pd.DataFrame()
    df = processDataNormal(apiKey, address, beginTime=beginTime, endTime=endTime, timezone=timezone)

    # internal
    df_in = pd.DataFrame()
    df_in = processDataIn(apiKey, address, beginTime=beginTime, endTime=endTime, timezone=timezone)

    # token
    df_token = pd.DataFrame()
    df_token = processDataToken(apiKey, address, beginTime=beginTime, endTime=endTime, timezone=timezone)
    
    result = pd.concat([df, df_in, df_token], ignore_index=True)
    return df_token

apiKey = "your apiKey here"
address = ""

result = processData(apiKey, address.lower(), beginTime = '2023-01-01', endTime = '2024-07-13', timezone = 'UTC')

# 获取原生代币和其它代币余额
def processBalance(address):  
    base_url_token = "https://www.oklink.com/api/v5/explorer/address/token-balance"  
    balance_all = pd.DataFrame(columns=['balance', 'currency'])  

    success = 1   
    # 其它代币  
    page = 1  
    limit = 50  
    more_pages = True  

    apiKey = 'd4a4be25-10a1-486a-ac67-7dcd9fa1d7db'  
    
    while more_pages:  
        query_params = {  
            "chainShortName": "BASE",  
            "address": address,  
            "protocolType": 'token_20',  
            "page": page,  
            "limit": limit  
        }  
        
        try:  
            response = requests.get(base_url_token, headers={'Ok-Access-Key': apiKey}, params=query_params)  
            response.raise_for_status()  
            data = response.json()['data'][0]['tokenList']  
            
            if not data:  
                more_pages = False  
                break  
            
            result_df = pd.json_normalize(data)  
            page += 1  
            
            if len(result_df) > 0:  
                balance_df = result_df[['symbol', 'holdingAmount']]  
                balance_df.loc[:, 'symbol'] = balance_df['symbol'].str.upper()  
                balance_df = balance_df.rename(columns={'holdingAmount': 'balance', 'symbol': 'currency'})  
                balance_all = pd.concat([balance_all, balance_df])  
        except requests.exceptions.RequestException as e:  
            print("Error:", e)  
            success = 0  
            more_pages = False  

    # 原生代币  
    base_url_summary = "https://www.oklink.com/api/v5/explorer/address/address-summary"  
    query_params = {  
        "chainShortName": "BASE",  
        "address": address,  
    }  

    try:  
        response = requests.get(base_url_summary, headers={'Ok-Access-Key': apiKey}, params=query_params)  
        response.raise_for_status()  
        df = pd.json_normalize(response.json()['data'])  
        
        if len(df) > 0:  
            df = df.rename(columns={'balanceSymbol': 'currency'})  
            df = df[['balance', 'currency']]  
            balance_all = pd.concat([balance_all, df], ignore_index=True)  
            balance_all['chain'] = ''  
            balance_all['address'] = ''  
        
    except requests.exceptions.RequestException as e:  
        print("Error:", e)  
        success = 0  

    if success == 1:  
        json_balance = balance_all.to_json(orient='records')  
        result_json = {  
            'action': "REALTIME_TREASURY_BALANCE_READY",  
            'status': 'SUCCESS',  
            'error': {  
                'code': -1,  
                'message': "无错误"  
            },  
            'tokenList': json.loads(json_balance)  
        }  
    else:  
        result_json = {  
            'action': "REALTIME_TREASURY_BALANCE_READY",  
            'status': 'FAILED',  
            'error': {  
                'code': 10001,  
                'message': "无数据"  
            }  
        }  
                
    return result_json

address= '0x63DFE4e34A3bFC00eB0220786238a7C6cEF8Ffc4'
processBalance(address)
