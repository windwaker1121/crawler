import pandas as pd
import requests
from io import StringIO
import re
import numpy as np
def fetch_stock_data():
    # 目標URL
    url = "https://hist.tpex.org.tw/Hist/STOCK/AFTERTRADING/DAILY_CLOSE_QUOTES/RSTA3104_931022.HTML"
    
    try:
        # 發送請求並獲取內容
        response = requests.get(url)
        # 設置正確的編碼
        response.encoding = 'big5'
        columns = [
            "股票代號",
            "證券名稱",
            "收盤價",
            "漲跌",
            "開盤價",
            "最高價",
            "最低價",
            "均價",
            "成交股數",
            "成交金額",
            "成交筆數",
            "最後委買價",
            "最後委賣價",
        ]

        # 使用pandas讀取HTML表格
        dfs = pd.read_html(StringIO(response.text))
        for i, df in enumerate(dfs):
            df = df[8:-3]
            
            df = df[[0, 6, 10, 16, 21, 26, 31, 35, 39, 44, 49, 54, 60]]
            df.columns = columns
            print(df)
            dfs[i] = df
        for i in dfs[1:]:
            dfs[0] = pd.concat((dfs[0], i))
        df = dfs[0]
        df['均價'][df['均價']=='註'] = '0.0'
        
        # print(df['均價'].dtype);exit()
        df.index = np.arange(len(df)) 
        # df = df.reset_index().set_index('股票代號')
        # print(df);exit()
        exc_index = []
        for r, row in enumerate(df.iterrows()):
            if df.loc[row[0]].values[0] in ["＊＊＊＊＊ 二類股票 ＊＊＊＊＊", "＊＊＊＊＊ 管理股票 ＊＊＊＊＊", np.nan]:
                print(r, row)
                exc_index.append(r)
                # delete_rows +=1
            # print(df.loc[row[0]].values)
        print(df.index)
        index = list(df.index.values.copy())
        for i in exc_index:
            index.remove(i)
        df = df[df['股票代號']!="＊＊＊＊＊ 二類股票 ＊＊＊＊＊"]
        df = df[df['股票代號']!="＊＊＊＊＊ 管理股票 ＊＊＊＊＊"]
        df = df[df['股票代號']==df['股票代號']]
        for col in [
            "收盤價",
            "開盤價",
            "最高價",
            "最低價",
            '均價',
            "最後委買價",
            "最後委賣價",
        ]:
            df[col] = df[col].values.astype(float)

        for col in [
            "成交股數",
            "成交金額",
            "成交筆數",
        ]:
            df[col] = df[col].values.astype(str)
        # print(df[456]['股票代號'])
        print(df);exit()
        # 通常會抓取到多個表格，我們需要的是主要的交易數據表格
        # 找到最大的那個表格（通常是主要的交易數據）
        main_df = max(dfs, key=len)
        # 設置列名
        
        
        main_df.columns = columns
        
        # 清理數據
        # 移除包含 "..." 或空值的行
        main_df = main_df[~main_df['股票代號'].isna()]
        main_df = main_df[main_df['股票代號'].astype(str).str.contains(r'^\d+$')]
        
        # 轉換數據類型
        numeric_columns = ['收盤價', '漲跌', '開盤價', '最高價', '最低價', '均價', 
                         '成交股數', '成交金額', '成交筆數', '最後買價', '最後賣價']
        
        for col in numeric_columns:
            main_df[col] = pd.to_numeric(main_df[col].astype(str).str.replace(',', ''), errors='coerce')
            
        return main_df
        
    except Exception as e:
        print(f"發生錯誤: {e}")
        return None

# 使用函數
df = fetch_stock_data()

# 顯示結果
if df is not None:
    print(df.head())
    print(f"\n總共有 {len(df)} 筆資料")