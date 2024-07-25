import pandas as pd  
import pytz
import json
data = pd.read_csv('chainup.csv')
def processData(df, timezone):
    df = pd.DataFrame(df)
    if df.empty:
        print('没有数据')
    else:
        df['txHash'] = ''
        df.rename(columns={'时间/Time': 'datetime', '交易数量/Transaction quantity': 'amount'}, inplace=True)
        # 将 'datetime' 列转换为日期时间类型，自动解析格式  
        df['datetime'] = pd.to_datetime(df['datetime'])  

        # 使用 lambda 函数在 apply 中进行时区转换和格式化  
        def convert_to_utc(dt, tz):  
            if dt.tzinfo is None:  
                # 如果没有时区信息，使用 tz_localize  
                localized_dt = dt.tz_localize(tz)  
            else:  
                # 如果已有时区信息，使用 tz_convert  
                localized_dt = dt.tz_convert(tz)  
            return localized_dt.astimezone(pytz.utc).strftime('%Y-%m-%dT%H:%M:%SZ')  

        df['datetime'] = df['datetime'].apply(lambda dt: convert_to_utc(dt, timezone))  
        # 提取括号外的内容  
        df['currency'] = df['币种/Currency'].apply(lambda x: x.split('(')[0])

        # 提取括号内的内容并去掉最后一个字符  
        df['contactPlatformSlug'] = df['币种/Currency'].apply(lambda x: x.split('(')[1][:-1] if '(' in x else '')  

        df['contactIdentity'] = ''
        df['amount'] = df['amount'].astype(float).abs()
        df['direction'] = df['交易方向/Transaction direction'].apply(lambda row: 'OUT' if row == '支出' else 'IN')
        df['type'] = df['业务类型/Business Type'].apply(lambda x: 'CUSTODY_FEE' if '矿工费' in x else None)
        df['type'] = df.apply(lambda row: 'CUSTODY_DEPOSIT' if row['type'] is None and row['交易方向/Transaction direction'] == '收入'   
                      else ('CUSTODY_WITHDRAW' if row['type'] is None and row['交易方向/Transaction direction'] == '支出'   
                            else row['type']), axis=1)
        df['订单备注/Order Remark'] = df['订单备注/Order Remark'].fillna('')  
        df['流水备注/Notes on running water'] = df['流水备注/Notes on running water'].fillna('')
        # 使用 ensure_ascii=False 确保中文字符不被转义  
        df['memo'] = df.apply(lambda row: json.dumps({  
            '业务类型/Business Type': row['业务类型/Business Type'],  
            '订单备注/Order Remark': row['订单备注/Order Remark'],  
            '流水备注/Notes on running water': row['流水备注/Notes on running water']  
        }, ensure_ascii=False), axis=1) 
        df = df[['type', 'txHash', 'datetime', 'contactIdentity',
                            'contactPlatformSlug', 'direction', 'currency', 'amount', 'memo']]
    return df
result = processData(data, timezone = 'Asia/Shanghai' )
result
