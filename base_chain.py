import http.client
import urllib.parse
import pandas as pd
import json
pd.options.mode.chained_assignment = None
pd.set_option('display.max_columns', 50)
pd.set_option('display.max_rows', 100)



def processData(apiKey, blockchain, network, address, beginTime=None, endTime=None, timezone=None):
    # Base URL and API endpoint
    base_url = "rest.cryptoapis.io"
    endpoint = f"/blockchain-data/{blockchain}/{network}/addresses/{address}/transactions"

    # Construct query parameters
    query_params = {
        "limit": 50,  # Fetch 50 transactions per request
        "offset": 0,  # Starting offset
    }

    # Encode the query parameters
    querystring = f"{endpoint}?{urllib.parse.urlencode(query_params)}"
    # print(querystring)
    # Configuring headers
    headers = {
        'Content-Type': "application/json",
        'x-api-key': apiKey  # Replace with your actual API key
    }

    # Create a connection to the server
    conn = http.client.HTTPSConnection(base_url)

    # Send GET request
    conn.request("GET", querystring, headers=headers)

    # Get and read response
    res = conn.getresponse()
    data = res.read()

    # Decoding the response data
    response_data = data.decode("utf-8")
    # Convert the JSON response to a dictionary
    response_dict = json.loads(response_data)

    # Extract the transactions data
    transactions = response_dict.get('data', {}).get('items', [])

    # Convert the transactions data to a DataFrame
    df = pd.json_normalize(transactions)
    
    if not df.empty:
        df['recipients.address'] = df['recipients'].apply(lambda x: x[0]['address'])
        df['recipients.amount'] = df['recipients'].apply(lambda x: x[0]['amount'])
        df['senders.address'] = df['senders'].apply(lambda x: x[0]['address'])
        df['senders.amount'] = df['senders'].apply(lambda x: x[0]['amount'])

        df.rename(columns={'unit': 'amount', 
                               'transactionHash': 'txHash', 'timestamp': 'datetime'}, inplace=True) 
        #df['datetime'] = pd.to_datetime(df['datetime'], unit='s').dt.strftime('%Y-%m-%dT%H:%M:%SZ')
        df['contactPlatformSlug'] = ''
        df['direction'] = df['recipients.address'].apply(lambda x: 'IN' if x == address else 'OUT')
        df['currency'] = ''
        df['amount'] = df['amount'].astype(float)
        df['contactIdentity'] = df.apply(lambda row: row['recipients.address'] if row['senders.address'] == address else row['recipients.address'], axis=1)
        df['type'] = df.apply(lambda row: 'CHAIN_TRANSFER_IN' if row['recipients.address'] == address else 'CHAIN_TRANSFER_OUT', axis=1)
        
        df = df[['type', 'txHash', 'datetime', 'contactIdentity',
                      'contactPlatformSlug', 'direction', 'currency', 'amount']]

        # 过滤重复行
        df = df.drop_duplicates(subset=['txHash', 'contactIdentity', 'amount'])
        df.reset_index(drop=True, inplace=True)
        
    return df
