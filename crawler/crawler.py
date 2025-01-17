from io import StringIO
import psycopg2
import os
import csv
import time
import pandas as pd
import numpy as np
from header import generate_random_header, o2tp, o2tp_b2007, name2colname
import requests
from util import combine_index, preprocess
import pickle
import base64
from postgre_fun import update_data, query_by_SQL, str2db_byte, update_file, insert, check_table, create_table, delete_data, query_data, get_file, db_byte2str
import re
from joblib import Parallel, delayed
import argparse
from bs4 import BeautifulSoup

parser = argparse.ArgumentParser()
parser.add_argument("--csv", default="", help="Use exist csv file for dir.")
parser.add_argument("--date", default=None, help="Start date")
args = parser.parse_args()

'''
上市:
~2008/12/31
2009/01/01~
'''
twe_url = 'https://www.twse.com.tw/exchangeReport/MI_INDEX?response=csv&type=ALLBUT0999&date='

'''
上櫃:
~2007/01/01
2007/01/02-2007/04/20
2007/04/21~
'''
otc_url = 'https://www.tpex.org.tw/www/zh-tw/afterTrading/dailyQuotes?id=&response=csv&date='
old_2007_otc_url = "https://www.tpex.org.tw/www/zh-tw/afterTrading/dailyQuotesHis?id=&response=csv&date="
old_b2007_otc_url = "https://hist.tpex.org.tw/Hist/STOCK/AFTERTRADING/DAILY_CLOSE_QUOTES/RSTA3104_%s.HTML"
# host = "172.128.0.2"
dbname = "stock"
# user = "admin"
# password = "admin"
# sslmode = "allow"

# # Construct connection string
# connect_succes = False
# while not connect_succes:
#     try:
#         conn_string = "host={0} user={1} dbname={2} password={3} sslmode={4}".format(host, user, dbname, password, sslmode)
#         conn = psycopg2.connect(conn_string)
#         print("Connection established")
#         cursor = conn.cursor()
#         connect_succes = True
#     except:
        
#         pass
#     print(connect_succes)
#     time.sleep(1)

class stock_crawler(object):
    def __init__(self, stock_id):
        pass

class csv_parser(object):
    ses_twse = None
    ses_tpex = None
    price = None
    def __init__(self, date, twse_byte, tpex_byte, csv=False):
        self.date = date
        self.date_time = datetime.datetime(date.year, date.month, date.day)
        self.timeout = 60
        self.twse_byte = twse_byte
        self.tpex_byte = tpex_byte
        self.csv = csv
        pass

    def find_best_session(self, stock_type="twse"):
        for i in range(100):
            try:
                # print('獲取新的Session 第', i, '回合')
                headers = generate_random_header()
                if stock_type=="twse":
                    self.ses_twse = requests.Session()
                    self.ses_twse.get('https://www.twse.com.tw/zh/', headers=headers, timeout=self.timeout)
                    self.ses_twse.headers.update(headers)
                    print('獲取新的Session 第', i, '回合成功！')
                    return
                else:
                    self.ses_tpex = requests.Session()
                    self.ses_tpex.get('https://www.tpex.org.tw/www/zh-tw/', headers=headers, timeout=self.timeout)
                    self.ses_tpex.headers.update(headers)
                    print('獲取新的Session 第', i, '回合成功！')
                    return
            except Exception as error:
                print(error)
                print('失敗，10秒後重試')
                time.sleep(15)

        print('您的網頁IP已經被證交所封鎖，請更新IP來獲取解鎖')
        print("　手機：開啟飛航模式，再關閉，即可獲得新的IP")
        print("數據機：關閉然後重新打開數據機的電源")

    def requests_get(self, *args1, **args2):
        # get current session
        stock_type = args1[1]
        args1 = (args1[0],)
        # if stock_type == 'twse':
        #     self.find_best_session(stock_type)
        # if stock_type == 'tpex':
        self.find_best_session(stock_type)
        # download data
        i = 10
        sleep = 30

        while i >= 0:
            try:
                if stock_type == 'twse' and self.ses_twse is not None:
                    return self.ses_twse.get(*args1, timeout=self.timeout, **args2)
                if stock_type == 'tpex' and self.ses_tpex is not None:
                    return self.ses_tpex.get(*args1, timeout=self.timeout, **args2)
            except Exception as error:
                print(error)
                print('retry one more time after ',sleep,'s', i, 'times left')
            time.sleep(sleep)
            if stock_type == 'twse' and self.ses_twse is not None:
                self.find_best_session('twse')
            if stock_type == 'tpex' and self.ses_tpex is not None:
                self.find_best_session('tpex')

            i -= 1
        Update_Queue.put(None)
        return pd.DataFrame()

    def price_twse(self):
        date_str = self.date.strftime('%Y%m%d')
        # twse_csv = get_file('date', {'date': self.date.strftime('%Y-%m-%d')}, 'twse')
        self.twse_url = link = twe_url+date_str
        if self.twse_byte is None:
            print(link)
            while True:
                res = self.requests_get(link, "twse")
                if res.status_code != 200:
                    print(res.status_code)
                    time.sleep(10)
                    continue
                elif res.status_code == 200 and len(res.text.replace(" ", "")) == 0:
                    # update_file('date', {'date': self.date.strftime('%Y-%m-%d')}, {'twse': str2db_byte("")})
                    return pd.DataFrame()
                if type(res) is not requests.models.Response: return pd.DataFrame()
                res_text = res.text.replace('\r\n', '\n')
                break
                
            self.twse_res_text = res_text
            # update_file('date', {'date': self.date.strftime('%Y-%m-%d')}, {'twse': str2db_byte(res_text)})
        else:
            self.twse_res_text = res_text = db_byte2str(self.twse_byte)
            # if self.csv:
            #     update_file('date', {'date': self.date.strftime('%Y-%m-%d')}, {'twse': str2db_byte(res_text)})
            # print(res_text)

        if res_text == '':
            return pd.DataFrame()
        string_date = res_text.split('\n')[0]
        string_date = string_date.replace('"', '')
        string_date = string_date.split(' ')[0]
        date_year = int(string_date.split('年')[0])
        if date_year < 1911:
            date_year = 1911 + date_year
        else:
            date_year = date_year
        if self.date_time.year != date_year:
            print("the csv year is not match crawler date")
            return pd.DataFrame()
        
        # res_text = res_text.split('"備註:"')[1]
        res_text_list = res_text.split('"備註:"')
        if len(res_text_list) > 2:
            for i in range(len(res_text_list)):
                if '"證券代號"' in res_text_list[i]:
                    res_text = res_text_list[i].replace('="','"').replace(',\n', '\n')
                    header = np.where(list(map(lambda l: '"證券代號"' in l, res_text.split('\n')[:500])))[0][0]

                    print("header", header)
                    break

        # res_text = res_text.replace('="','"').replace(',\r\n', '\r\n')
        else:
            res_text = res_text.split('"備註:"')[0]
            res_text = res_text.replace('="','"').replace(',\n', '\n')
            header = np.where(list(map(lambda l: '"證券代號"' in l, res_text.split('\n')[:500])))[0][0]
        
        df = pd.read_csv(StringIO(res_text), header=header)
        return df      

    def price_twse_2008(self):
        date_str = self.date.strftime('%Y%m%d')
        # twse_csv = get_file('date', {'date': self.date.strftime('%Y-%m-%d')}, 'twse')
        print(twe_url+date_str)
        self.twse_url = link = twe_url+date_str
        if self.twse_byte is None:
            print(link)
            while True:
                res = self.requests_get(link, "twse")
                if res.status_code != 200:
                    print(res.status_code)
                    time.sleep(10)
                    continue
                elif res.status_code == 200 and len(res.text.replace(" ", "")) == 0:
                    # update_file('date', {'date': self.date.strftime('%Y-%m-%d')}, {'twse': str2db_byte("")})
                    return pd.DataFrame()
                if type(res) is not requests.models.Response: return pd.DataFrame()
                res_text = res.text.replace('\r\n', '\n')
                break
                
            self.twse_res_text = res_text
            # update_file('date', {'date': self.date.strftime('%Y-%m-%d')}, {'twse': str2db_byte(res_text)})
        else:
            self.twse_res_text = res_text = db_byte2str(self.twse_byte)
            # if self.csv:
            #     update_file('date', {'date': self.date.strftime('%Y-%m-%d')}, {'twse': str2db_byte(res_text)})
            # print(res_text)

        if res_text == '':
            return pd.DataFrame()
        string_date = res_text.split('\n')[0]
        string_date = string_date.replace('"', '')
        string_date = string_date.split(' ')[0]
        date_year = int(string_date.split('年')[0])
        if date_year < 1911:
            date_year = 1911 + date_year
        else:
            date_year = date_year
        if self.date_time.year != date_year:
            print("the csv year is not match crawler date")
            return pd.DataFrame()

        res_text_list = res_text.split('"備註:"')
        if len(res_text_list) > 2:
            for i in range(len(res_text_list)):
                if '"證券代號"' in res_text_list[i]:
                    res_text = res_text_list[i].replace('="','"').replace(',\n', '\n')
                    print(res_text)
                    header = np.where(list(map(lambda l: '"證券代號"' in l, res_text.split('\n')[:500])))[0][0]
                    # res_text = res_text_list[i]
                    print("header", header)
                    break
        else:
        # res_text = res_text.replace('="','"').replace(',\r\n', '\r\n')
            res_text = res_text.split('"備註:"')[0]
            res_text = res_text.replace('="','"').replace(',\n', '\n')
            header = np.where(list(map(lambda l: '"證券代號"' in l, res_text.split('\n')[:500])))[0][0]
        df = pd.read_csv(StringIO(res_text), header=header)
        return df      

    def price_tpex(self):
        date_str = self.date.strftime('%Y/%m/%d')
        # tpex_csv = get_file('date', {'date': self.date.strftime('%Y-%m-%d')}, 'tpex')
        print(otc_url+date_str)
        self.tpex_url = link = otc_url+date_str
        if self.tpex_byte is None:
            while True:
                res = self.requests_get(link, "tpex")
                if res.status_code != 200:
                    time.sleep(10)
                    continue
                elif res.status_code == 200 and len(res.text.replace(" ", "")) == 0:
                    # update_file('date', {'date': self.date.strftime('%Y-%m-%d')}, {'tpex': str2db_byte("")})
                    return pd.DataFrame()
                if type(res) is not requests.models.Response: return pd.DataFrame()
                res_text = res.text
                break
            self.tpex_res_text = res_text
            # update_file('date', {'date': self.date.strftime('%Y-%m-%d')}, {'tpex': str2db_byte(res_text)})
        else:
            self.tpex_res_text = res_text = db_byte2str(self.tpex_byte)
            # if self.csv:
            #     update_file('date', {'date': self.date.strftime('%Y-%m-%d')}, {'tpex': str2db_byte(res_text)})

            # print(res_text)


        if '上櫃家數,"0"' in res_text: return pd.DataFrame()
        try:
            df = pd.read_csv(StringIO(res_text.split('管理股票')[0]), header=2)
        except Exception as e:
            print(e, res_text)
            return pd.DataFrame()

        return df

    def price_tpex_b20041027(self):
        # date_str = self.date.strftime('%Y/%m/%d')
        date_str = str(self.date.year - 1911)+str(self.date.month).zfill(2)+str(self.date.day).zfill(2)
        # tpex_csv = get_file('date', {'date': self.date.strftime('%Y-%m-%d')}, 'tpex')
        print(old_b2007_otc_url % date_str)
        self.tpex_url = link = old_b2007_otc_url % date_str
        if self.tpex_byte is None:
            while True:
                res = self.requests_get(link, "tpex")
                res.encoding = 'big5'
                
                res_text = res.text               
                if res.status_code != 200:
                    time.sleep(10)
                    continue
                elif res.status_code == 200 and len(res_text.replace(" ", "")) == 0:
                    # update_file('date', {'date': self.date.strftime('%Y-%m-%d')}, {'tpex': str2db_byte("")})
                    return pd.DataFrame()
                if type(res) is not requests.models.Response: return pd.DataFrame()
                # res_text = res.text
                break
            
            self.tpex_res_text = res_text
            # update_file('date', {'date': self.date.strftime('%Y-%m-%d')}, {'tpex': str2db_byte(res_text)})
        else:
            self.tpex_res_text = res_text = db_byte2str(self.tpex_byte)
            # if self.csv:
            #     update_file('date', {'date': self.date.strftime('%Y-%m-%d')}, {'tpex': str2db_byte(res_text)})
        

        if '上櫃家數,"0"' in res_text: return pd.DataFrame()
        try:
            
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
            dfs = pd.read_html(StringIO(res_text))
            for i, df in enumerate(dfs):
                df = df[8:-3]
                
                df = df[[0, 6, 10, 16, 21, 26, 31, 35, 39, 44, 49, 54, 60]]
                df.columns = columns
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
                    exc_index.append(r)
                    # delete_rows +=1
                # print(df.loc[row[0]].values)
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


        except Exception as e:
            print(e)
            return pd.DataFrame()
        
        return df

    def price_tpex_b2004(self):
        # date_str = self.date.strftime('%Y/%m/%d')
        date_str = str(self.date.year - 1911)+str(self.date.month).zfill(2)+str(self.date.day).zfill(2)
        # tpex_csv = get_file('date', {'date': self.date.strftime('%Y-%m-%d')}, 'tpex')
        self.tpex_url = link = old_b2007_otc_url % date_str
        if self.tpex_byte is None:
            while True:
                res = self.requests_get(link, "tpex")
                res.encoding = 'big5'
                
                res_text = res.text
                res_text = res_text.replace("♁", "")
                res_text = res_text.replace("☉", "")
                res_text = res_text.replace("▽", "")
                res_text = res_text.replace("△", "")

                if res.status_code != 200:
                    time.sleep(10)
                    continue
                elif res.status_code == 200 and len(res_text.replace(" ", "")) == 0:
                    # update_file('date', {'date': self.date.strftime('%Y-%m-%d')}, {'tpex': str2db_byte("")})
                    return pd.DataFrame()
                if type(res) is not requests.models.Response: return pd.DataFrame()
                # res_text = res.text
                break
            
            self.tpex_res_text = res_text
            # update_file('date', {'date': self.date.strftime('%Y-%m-%d')}, {'tpex': str2db_byte(res_text)})
        else:
            self.tpex_res_text = res_text = db_byte2str(self.tpex_byte)
            # if self.csv:
            #     update_file('date', {'date': self.date.strftime('%Y-%m-%d')}, {'tpex': str2db_byte(res_text)})

        if '上櫃家數,"0"' in res_text: return pd.DataFrame()
        try:
            # df = pd.read_csv(StringIO(res_text.split('管理股票')[0]), header=2)
            soup = BeautifulSoup(res_text, 'html.parser')
            # print(res.status_code, res_text)
            title = soup.find_all('table')[0]
            if not title:
                raise ValueError("找不到目標表格，請檢查網頁結構")
            # 找到目標表格 (通常需要根據網頁的結構調整)
            table = soup.find('table')  # 假設目標表格是 <table> 標籤
            if not table:
                raise ValueError("找不到目標表格，請檢查網頁結構")

            # 解析表格內容
            rows = table.find_all('tr')  # 獲取所有的列
            data = []

            for row in rows:
                cols = row.find_all('td')  # 找到每一列的所有欄位
                cols = [col.text.strip() for col in cols]  # 提取文字並移除多餘的空白
                if cols:  # 排除空列
                    data.append(cols)

            # 將資料轉為 Pandas DataFrame
            columns = list(data[0])  # 假設第一列是表頭
            # columns.insert(2, "收盤價sign_")
            # columns.insert(2, "收盤價sign")
            columns.insert(3, "漲跌sign")
            # print(columns)
            df = pd.DataFrame(data[1:], columns=columns)  # 從第二列開始為數據
            # columns.remove("收盤價sign")
            columns.remove("漲跌sign")
            df = df[columns]

        except Exception as e:
            print(e)
            return pd.DataFrame()
        return df

    def price_tpex_b2007(self):
        # date_str = self.date.strftime('%Y/%m/%d')
        date_str = str(self.date.year - 1911)+str(self.date.month).zfill(2)+str(self.date.day).zfill(2)
        # tpex_csv = get_file('date', {'date': self.date.strftime('%Y-%m-%d')}, 'tpex')
        self.tpex_url = link = old_b2007_otc_url % date_str
        if self.tpex_byte is None:
            while True:
                res = self.requests_get(link, "tpex")
                res.encoding = 'big5'
                
                res_text = res.text
                res_text = res_text.replace("♁", "")
                res_text = res_text.replace("☉", "")
                res_text = res_text.replace("▽", "")
                res_text = res_text.replace("△", "")

                if res.status_code != 200:
                    time.sleep(10)
                    continue
                elif res.status_code == 200 and len(res_text.replace(" ", "")) == 0:
                    # update_file('date', {'date': self.date.strftime('%Y-%m-%d')}, {'tpex': str2db_byte("")})
                    return pd.DataFrame()
                if type(res) is not requests.models.Response: return pd.DataFrame()
                # res_text = res.text
                break
            
            self.tpex_res_text = res_text
            # update_file('date', {'date': self.date.strftime('%Y-%m-%d')}, {'tpex': str2db_byte(res_text)})
        else:
            self.tpex_res_text = res_text = db_byte2str(self.tpex_byte)
            # if self.csv:
            #     update_file('date', {'date': self.date.strftime('%Y-%m-%d')}, {'tpex': str2db_byte(res_text)})

        if '上櫃家數,"0"' in res_text: return pd.DataFrame()
        try:
            # df = pd.read_csv(StringIO(res_text.split('管理股票')[0]), header=2)
            soup = BeautifulSoup(res_text, 'html.parser')
            # print(res.status_code, res_text)
            title = soup.find_all('table')
            if not title:
                print("找不到目標表格，請檢查網頁結構")
                return pd.DataFrame()

            title = title[0]
            # 找到目標表格 (通常需要根據網頁的結構調整)
            table = soup.find('table')  # 假設目標表格是 <table> 標籤
            if not table:
                print("找不到目標表格，請檢查網頁結構")
                return pd.DataFrame()

            # 解析表格內容
            rows = table.find_all('tr')  # 獲取所有的列
            data = []

            for row in rows:
                cols = row.find_all('td')  # 找到每一列的所有欄位
                cols = [col.text.strip() for col in cols]  # 提取文字並移除多餘的空白
                if cols:  # 排除空列
                    data.append(cols)

            # 將資料轉為 Pandas DataFrame
            columns = list(data[0])  # 假設第一列是表頭
            columns.insert(2, "收盤價sign")
            columns.insert(4, "漲跌sign")
            # print(columns)
            df = pd.DataFrame(data[1:], columns=columns)  # 從第二列開始為數據
            columns.remove("收盤價sign")
            columns.remove("漲跌sign")
            df = df[columns]
        except Exception as e:
            print(e)
            return pd.DataFrame()
        return df

    def price_tpex_2007(self):
        date_str = self.date.strftime('%Y/%m/%d')
        # tpex_csv = get_file('date', {'date': self.date.strftime('%Y-%m-%d')}, 'tpex')

        self.tpex_url = link = old_2007_otc_url+date_str
        if self.tpex_byte is None:
            while True:
                res = self.requests_get(link, "tpex")
                res_text = eval(res.text)
                # print(res.status_code, res_text)
                if res.status_code != 200:
                    time.sleep(10)
                    continue
                elif res.status_code == 200 and 'html' not in res_text:
                    # update_file('date', {'date': self.date.strftime('%Y-%m-%d')}, {'tpex': str2db_byte("")})
                    return pd.DataFrame()
                if type(res) is not requests.models.Response: return pd.DataFrame()
                # res_text = res.text
                if 'html' in res_text:
                    res_text = res_text['html']
                    res_text = res_text.replace("\r\n", "")
                    res_text = res_text.replace("\t", "")
                    res_text = res_text.replace("♁", "")
                    res_text = res_text.replace("☉", "")
                    break
                else:
                    res_text = "holiday"
                
            self.tpex_res_text = res_text
            # update_file('date', {'date': self.date.strftime('%Y-%m-%d')}, {'tpex': str2db_byte(res_text)})
        else:
            self.tpex_res_text = res_text = db_byte2str(self.tpex_byte)
            # if self.csv:
            #     update_file('date', {'date': self.date.strftime('%Y-%m-%d')}, {'tpex': str2db_byte(res_text)})

            # print(res_text)


        if '上櫃家數,"0"' in res_text: return pd.DataFrame()
        try:
            # df = pd.read_csv(StringIO(res_text.split('管理股票')[0]), header=2)
            
            soup = BeautifulSoup(res_text, 'html.parser')
            title = soup.find_all('table')[0]
            if not title:
                raise ValueError("找不到目標表格，請檢查網頁結構")
            title_rows = title.find_all('tr')  # 獲取所有的列
            data = []
            for row in title_rows:
                cols = row.find_all('td')  # 找到每一列的所有欄位
                cols = [col.text.strip() for col in cols]  # 提取文字並移除多餘的空白
                if cols:  # 排除空列
                    data.append(cols)
            columns = data[1]
            table = soup.find_all('table')[1]  # 假設目標表格是 <table> 標籤
            if not table:
                raise ValueError("找不到目標表格，請檢查網頁結構")

            # 解析表格內容
            rows = table.find_all('tr')  # 獲取所有的列
            data = []

            for row in rows:
                # print(rows)
                cols = row.find_all('td')  # 找到每一列的所有欄位
                cols = [col.text.strip() for col in cols]  # 提取文字並移除多餘的空白
                if cols:  # 排除空列
                    data.append(cols)

            df = pd.DataFrame(data, columns=columns)
        except Exception as e:
            print(e, res_text)
            return pd.DataFrame()

        return df

    def merge(self, twe, otc, t2o):
        if self.date_time <= datetime.datetime(2007,1,1):
            t2o2_b2007 = {k:v for k,v in o2tp_b2007.items() if k in otc.columns}
            otc = otc[list(t2o2_b2007.keys())]
            otc = otc.rename(columns=t2o2_b2007)
        else:
            t2o2 = {k:v for k,v in t2o.items() if k in otc.columns}
            otc = otc[list(t2o2.keys())]
            otc = otc.rename(columns=t2o2)
        
        join = set(otc.columns).intersection(set(twe.columns))
        twe = twe[list(join)]
        # print(twe.columns)

        return pd.concat([twe, otc[otc.columns]], ignore_index=True)

    def create_stock(self):
        res = query_by_SQL("\
                        SELECT substring(table_name, 3, 100) \
                        FROM information_schema.tables where table_name like 's\_%';")
        id2Name = {}
        stock_id = self.price["證券代號"].values.astype(str)
        stock_name = self.price["證券名稱"].values
        for id, name in zip(stock_id, stock_name):
            id2Name[id] = name
        new_stock = set(self.price["證券代號"].values).difference(set(res))
        if len(new_stock) > 0:
            for stock in new_stock:
                if type(stock) is not str: continue
                if len(stock)>=5 and stock[0] == '7': continue
                stock_name = id2Name[stock]
                if len(re.findall(r'[A-Z]{1}', stock))>0: continue
                if type(stock_name) is not str: continue
                if len(re.findall(r'[A-Z0-9]{2}[售|購]+[A-Z0-9]{2}$', stock_name))>0: continue
                print("create table", stock, stock_name)
                create_table("s_"+stock.lower(), 
                        [
                            ["date", "date", "PRIMARY KEY UNIQUE"],
                            [name2colname["開盤價"], "double precision", ],
                            [name2colname["最高價"], "double precision", ],
                            [name2colname["最低價"], "double precision", ],
                            [name2colname["收盤價"], "double precision", ],
                            [name2colname["成交股數"], "double precision", ],
                            [name2colname["成交筆數"], "double precision", ],
                            [name2colname["成交金額"], "double precision", ],
                            [name2colname["最後揭示買價"], "double precision", ],
                            [name2colname["最後揭示賣價"], "double precision", ],
                            ["rev", "double precision"],
                            ["pe", "double precision"],
                            ["pn", "double precision"],
                            ["dy", "double precision"],
                        ]
                    )
                insert("stock_name", ["stock_id", "name"], [stock, stock_name], "name")

    def crawl_price(self):
        print("begin crawl ...")
        if self.date_time <= datetime.datetime(2008,12,31):
            dftwe_func = self.price_twse_2008
        elif datetime.datetime(2009,1,1) <= self.date_time:
            dftwe_func = self.price_twse
        self.dftwe = dftwe_func()
        # time.sleep(1)
        if self.date_time <= datetime.datetime(2004,10,27):
            dfotc_func = self.price_tpex_b20041027
        elif datetime.datetime(2004,10,28) <= self.date_time and self.date_time <= datetime.datetime(2004,11,24):
            dfotc_func = self.price_tpex_b2004
        elif datetime.datetime(2004,11,25) <= self.date_time and self.date_time <= datetime.datetime(2007,1,1):
            dfotc_func = self.price_tpex_b2007
        elif datetime.datetime(2007,1,2) <= self.date_time and self.date_time <= datetime.datetime(2007,4,20):
            dfotc_func = self.price_tpex_2007
        else:
            dfotc_func = self.price_tpex
        self.dfotc = dfotc_func()
        # print(self.dftwe)
        # print(dfotc)
        # exit()
        if len(self.dftwe) != 0 and len(self.dfotc) != 0:
            price = self.merge(self.dftwe, self.dfotc, o2tp)
            self.price = price
            # print(price)
            # print(type(price.loc[0]), price.loc[0]["證券名稱"])
            self.create_stock()    
        else:
            if len(self.dftwe) != 0:
                print("上櫃資料有問題", dfotc_func.__name__, self.date, self.tpex_url)
                print("dftwe", self.dftwe, "\ndfotc", self.dfotc)
                exit()
            if len(self.dfotc) != 0:
                print("上市資料有問題", dftwe_func.__name__, self.date, self.twse_url)
                print("dftwe", self.dftwe, "\ndfotc", self.dfotc)
                exit()

    def update_file_data(self):
        if len(self.dftwe) == 0:
            print("上市資料有問題", self.date, self.twse_url)
            print("self.twse_res_text", self.twse_res_text)
            exit()
        elif len(self.dftwe) != 0:
            update_file('date', {'date': self.date.strftime('%Y-%m-%d')}, {'twse': str2db_byte(self.twse_res_text)})

        if len(self.dfotc) == 0:
            print("上櫃資料有問題", self.date, self.tpex_url)
            print("self.tpex_res_text", self.tpex_res_text)
            exit()
        elif len(self.dfotc) != 0:
            update_file('date', {'date': self.date.strftime('%Y-%m-%d')}, {'tpex': str2db_byte(self.tpex_res_text)})

    def iterfunc(self, i_price):
        # if len(re.findall(r'[A-Z]{1}[0-9]{1}$', i_price["證券代號"]))>0: return
        if type(i_price["證券代號"]) is not str: return
        if len(i_price["證券代號"])>=5 and i_price["證券代號"][0] == '7': return
        if len(re.findall(r'[A-Z]{1}', i_price["證券代號"]))>0: return
        if type(i_price["證券名稱"]) is not str: return
        if len(re.findall(r'[A-Z0-9]{2}[售|購]+[A-Z0-9]{2}$', i_price["證券名稱"]))>0: return
        
        property_name = ['date']
        property_value = [str(self.date.strftime('%Y-%m-%d'))]

        for c in self.price_columns:
            # print(c)
            if c in ["成交金額", "成交股數", "成交筆數"]:
                try:
                    value = float(i_price[c].replace(',',""))
                except Exception as e:
                    print(e, 'set to nan')
                    value = float('nan')
                property_value.append(value)
            elif c in ["證券名稱", "證券代號"]: continue
            else:
                try:
                    value = float(i_price[c].replace(',',''))
                except:
                    try:
                        value = float(i_price[c])
                    except Exception as e:
                        print("else:", e, 'set to nan')
                        value = float('nan')
                property_value.append(value)

            property_name.append(name2colname[c])
        # print(idx)
        # print(property_name)
        # print(property_value)
        # print(["date"]+idx, [str(self.date.strftime('%Y/%m/%d'))] + list(map(lambda x: price.loc[i][x] ,idx)))
        res = insert("s_"+i_price["證券代號"].lower(), property_name, property_value, 'date')
        
        return

    def update(self):
        self.price_columns = self.price.columns.tolist().copy()
        ret = Parallel(n_jobs=4, backend='threading')(delayed(self.iterfunc)(self.price.loc[i].copy()) for i in range(len(self.price)))
        return
        

import queue
Update_Queue = queue.Queue()
def cosumer():
    obj = 0
    while obj is not None:
        if Update_Queue.qsize() > 0:
            obj = Update_Queue.get_nowait()
            if obj is not None:
                obj.update()
                update_data('date', {'done': "true"}, {"date": str(obj.date.strftime('%Y-%m-%d'))})
               
            else:
                obj = 0
        # else:
            # print("empty queue wait 1 sec")    
            # time.sleep(2)
    print("cosumer end")
    return
import datetime
import threading
if __name__ == "__main__":
    date_time = datetime.datetime.now()
    # date_time = datetime.datetime(2009,4,17)
    # date_time = datetime.datetime(2007,4,25)
    date_time = datetime.datetime(2004,11,3)
    # date_time = datetime.datetime(2011,8,9)
    # date_time = datetime.datetime(2024,5,18)
    if args.date is not None:
        if "/" in args.date:
            date = args.date.split('/')
            date_time = datetime.datetime(int(date[0]),int(date[1]),int(date[2]))
        elif "-" in args.date:
            date = args.date.split('-')
            date_time = datetime.datetime(int(date[0]),int(date[1]),int(date[2]))
        else:
            date_time = datetime.datetime(int(args.date[:4]),int(args.date[4:6]),int(args.date[6:]))
    print(date_time)
    t = threading.Thread(target=cosumer)
    # t.start()
    while date_time >= datetime.datetime(1990,1,1):
        q_res = []
        twse_byte, tpex_byte, done = None, None, False
        # while q_res is None or len(q_res)<1:

        q_res = query_data("date", ['twse', 'tpex', 'done'], {'date':[str(date_time.date())]})
        if q_res is not None:
            try:
                twse_byte, tpex_byte, done = q_res[0]
                twse_test = db_byte2str(twse_byte)
                if len(twse_test.replace(" ", "")) ==0:
                    twse_byte = None
                tpex_test = db_byte2str(tpex_byte)
                if len(tpex_test.replace(" ", "")) ==0:
                    tpex_byte = None
            except:
                print(q_res)
        else:
            update_file('date', {'date': str(date_time.date())}, {'twse': str2db_byte(""), 'tpex': str2db_byte("")})
        #     insert("date", ['date'], [str(date_time.date())], "date")
        # print("twse_byte, tpex_byte, done", twse_byte, tpex_byte, done)#;exit()
        if done: 
            print(date_time, " crawled!")
            date_time = date_time - datetime.timedelta(days=1)
            continue

        if len(args.csv) > 0:
            date_str = str(date_time).split(' ')[0].replace("-", "_")
            history_name_twse = date_str + "_twse.txt"
            if twse_byte is None and history_name_twse in os.listdir(args.csv):
                with open(os.path.join(args.csv, history_name_twse), "r") as f:
                    twse_byte = str2db_byte(f.read()) 
                print('twse history file exist.')

            history_name_tpex = date_str + "_tpex.txt"
            if tpex_byte is None and history_name_tpex in os.listdir(args.csv):
                with open(os.path.join(args.csv, history_name_tpex), "r") as f:
                    tpex_byte = str2db_byte(f.read())
                print('tpex history file exist.')
        
        obj = csv_parser(datetime.date(date_time.year, date_time.month, date_time.day), twse_byte, tpex_byte, csv=len(args.csv)>0)
        if not done:
            if twse_byte is None or tpex_byte is None:
                obj.crawl_price()
            elif twse_byte is not None and tpex_byte is not None:
                obj.crawl_price()
        
        if obj.price is not None:
            # Update_Queue.put(obj)
            obj.update()
            obj.update_file_data()
            update_data('date', {'done': "true"}, {"date": str(obj.date.strftime('%Y-%m-%d'))})


            # time.sleep(2)
            # exit()
        # else:
        #     print(obj, str(obj.date), "沒有開盤")
        #     try:
        #         delete_data("date", {"date": str(obj.date)})
        #     except Exception as e:
        #         print("Exception delete_data date", e)
        
            # time.sleep(2)
            # exit()
        # else:
        #     print(obj, str(obj.date), "沒有開盤")
        #     try:
        #         delete_data("date", {"date": str(obj.date)})
        #     except Exception as e:
        #         print("Exception delete_data date", e)
        date_time = date_time - datetime.timedelta(days=1)
    Update_Queue.put(None)
    
    # df.head()
    print("exit")
    t.join()