from moralis import evm_api
import pandas as pd
import pytz
import time
from datetime import datetime
pd.options.mode.chained_assignment = None
pd.set_option('display.max_columns', 50)
pd.set_option('display.max_rows', 100)

def processData(apiKey, chain, address, beginTime=None, endTime=None, timezone=None):
    params = {
        "chain": chain,
        "order": "DESC",
        "address": address
    }

    result = evm_api.wallets.get_wallet_history(
        api_key=apiKey,
        params=params,
    )

    transactions = result.get('result', [])
    data = []
    
    for tx in transactions:
        for native_transfer in tx.get('native_transfers', []):
            direction = 'IN' if native_transfer['direction'] == 'receive' else 'OUT'
            tx_data = {
                'direction': direction,
                'type': 'CHAIN_TRANSFER_IN' if direction == 'IN' else 'CHAIN_TRANSFER_OUT',
                'txHash': tx['hash'],
                'txFee': float(tx['transaction_fee']),
                'datetime': tx['block_timestamp'],
                'contactPlatformSlug': '',
                'currency': native_transfer['token_symbol'],
                'amount': float(native_transfer['value']) / (10 ** 18),
                'contactIdentity': native_transfer['from_address'] if direction == 'IN' else native_transfer['to_address']
            }
            data.append(tx_data)
    
    df = pd.DataFrame(data)
    
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

# 使用示例
api_key = 'your api key here'
chain = "base"
address = "0x9b6899D37D3200a9EAe9F0E24765E8cC2057856D"

df = processData(api_key, chain, address, beginTime = '2024-01-01', endTime = '2024-07-10', timezone = 'UTC')
df
