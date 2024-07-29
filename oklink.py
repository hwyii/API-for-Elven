import requests
import pandas as pd
import pytz
from datetime import datetime
import time
pd.options.mode.chained_assignment = None
pd.set_option('display.max_columns', 50)
pd.set_option('display.max_rows', 100)


def processDataNormal(apiKey, chainShortName, address, beginTime=None, endTime=None, timezone=None):
    # Base URL and API endpoint
    base_url = "https://www.oklink.com/api/v5/explorer/address/normal-transaction-list"

    # Initialize a DataFrame to hold all transactions
    all_transactions = pd.DataFrame()

    page = 1
    limit = 100  # limit to 100 transactions at maximum per request
    
    while True:
        # Construct query parameters
        query_params = {
            "chainShortName": chainShortName,
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
        all_transactions.drop_duplicates(subset=['txHash', 'amount', 'contactIdentity'], inplace=True)
        all_transactions.reset_index(drop=True, inplace=True)
    
    return all_transactions

def processDataIn(apiKey, chainShortName, address, beginTime=None, endTime=None, timezone=None):
    # Base URL and API endpoint
    base_url = "https://www.oklink.com/api/v5/explorer/address/internal-transaction-list"

    # Initialize a DataFrame to hold all transactions
    all_transactions = pd.DataFrame()

    page = 1
    limit = 100  # limit to 100 transactions at maximum per request
    
    while True:
        # Construct query parameters
        query_params = {
            "chainShortName": chainShortName,
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
        all_transactions.drop_duplicates(subset=['txHash', 'contactIdentity', 'amount'])
        all_transactions.reset_index(drop=True, inplace=True)
    
    return all_transactions

def processDataToken(apiKey, chainShortName, address, protocolType, beginTime=None, endTime=None, timezone=None):
    # Base URL and API endpoint
    base_url = "https://www.oklink.com/api/v5/explorer/address/token-transaction-list"

    # Initialize a DataFrame to hold all transactions
    all_transactions = pd.DataFrame()

    page = 1
    limit = 100  # limit to 100 transactions at maximum per request
    
    while True:
        # Construct query parameters
        query_params = {
            "chainShortName": chainShortName,
            "address": address,
            "protocolType": protocolType,
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
        all_transactions.drop_duplicates(subset=['txHash', 'contactIdentity', 'amount'])
        all_transactions.reset_index(drop=True, inplace=True)
    
    return all_transactions

def processData(apiKey, chainShortName, address, protocolType, beginTime=None, endTime=None, timezone=None):

    # normal
    df = pd.DataFrame()
    df = processDataNormal(apiKey, chainShortName, address, beginTime=beginTime, endTime=endTime, timezone=timezone)

    # internal
    df_in = pd.DataFrame()
    df_in = processDataIn(apiKey, chainShortName, address, beginTime=beginTime, endTime=endTime, timezone=timezone)

    # token
    df_token = pd.DataFrame()
    df_token = processDataToken(apiKey, chainShortName, address, protocolType, beginTime=beginTime, endTime=endTime, timezone=timezone)

    result = pd.concat([df, df_in, df_token], ignore_index=True)

    return result

apiKey = "your apiKey here"
chainShortName = "base"
address = "0x9b6899D37D3200a9EAe9F0E24765E8cC2057856D"
protocolType = "token_20"

result = processData(apiKey, chainShortName, address.lower(), protocolType, beginTime = '2024-01-01', endTime = '2024-07-10', timezone = 'UTC')
result
