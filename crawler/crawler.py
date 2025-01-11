from io import StringIO
import psycopg2
import os
import csv
import time
import pandas as pd
import numpy as np
from header import generate_random_header, o2tp, name2colname
import requests
from util import combine_index, preprocess
import pickle
import base64
from postgre_fun import update_data, query_by_SQL, str2db_byte, update_file, insert, check_table, create_table, delete_data, query_data, get_file, db_byte2str
import re
from joblib import Parallel, delayed
import argparse
parser = argparse.ArgumentParser()
parser.add_argument("--csv", default="", help="Use exist csv file for dir.")
args = parser.parse_args()

otc_url = 'https://www.tpex.org.tw/www/zh-tw/afterTrading/dailyQuotes?id=&response=csv&date='
twe_url = 'https://www.twse.com.tw/exchangeReport/MI_INDEX?response=csv&type=ALLBUT0999&date='
old_otc_url = "https://hist.tpex.org.tw/Hist/STOCK/AFTERTRADING/DAILY_CLOSE_QUOTES/RSTA3104_%s.HTML"
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
        if self.twse_byte is None:
            link = twe_url+date_str
            print(link)
            while True:
                res = self.requests_get(link, "twse")
                if res.status_code != 200:
                    time.sleep(10)
                    continue
                if type(res) is not requests.models.Response: return pd.DataFrame()
                res_text = res.text
                break
                
            update_file('date', {'date': self.date.strftime('%Y-%m-%d')}, {'twse': str2db_byte(res_text)})
        else:
            res_text = db_byte2str(self.twse_byte)
            if self.csv:
                update_file('date', {'date': self.date.strftime('%Y-%m-%d')}, {'twse': str2db_byte(res_text)})
            # print(res_text)

        if res_text == '':
            return pd.DataFrame()
        
        header = np.where(list(map(lambda l: '證券代號' in l, res_text.split('\n')[:500])))[0][0]
        df = pd.read_csv(StringIO(res_text.replace('=','')), header=header-1)
        # print(df[df['證券名稱']=='聯發科']['開盤價']);exit()
        return df      
    
    def price_tpex(self):
        date_str = self.date.strftime('%Y/%m/%d')
        # tpex_csv = get_file('date', {'date': self.date.strftime('%Y-%m-%d')}, 'tpex')

        if self.tpex_byte is None:
            link = otc_url+date_str
            while True:
                res = self.requests_get(link, "tpex")
                if res.status_code != 200:
                    time.sleep(10)
                    continue
                if type(res) is not requests.models.Response: return pd.DataFrame()
                res_text = res.text
                break
                
            update_file('date', {'date': self.date.strftime('%Y-%m-%d')}, {'tpex': str2db_byte(res_text)})
        else:
            res_text = db_byte2str(self.tpex_byte)
            if self.csv:
                update_file('date', {'date': self.date.strftime('%Y-%m-%d')}, {'tpex': str2db_byte(res_text)})

            # print(res_text)


        if '上櫃家數,"0"' in res_text: return pd.DataFrame()
        try:
            df = pd.read_csv(StringIO(res_text.split('管理股票')[0]), header=2)
        except Exception as e:
            print(e, res_text)
            return pd.DataFrame()

        return df

    def merge(self, twe, otc, t2o):
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
                if len(stock)>=6 and stock[0] == '7': continue
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
        dftwe = self.price_twse()
        # print(dftwe)
        # time.sleep(1)
        dfotc = self.price_tpex()
        # print(dfotc)
        if len(dftwe) != 0 and len(dfotc) != 0:
            price = self.merge(dftwe, dfotc, o2tp)
            self.price = price
            # print(price)
            # print(type(price.loc[0]), price.loc[0]["證券名稱"])
            self.create_stock()    

    def iterfunc(self, i_price):
        # if len(re.findall(r'[A-Z]{1}[0-9]{1}$', i_price["證券代號"]))>0: return
        if type(i_price["證券代號"]) is not str: return
        if len(i_price["證券代號"])>=6 and i_price["證券代號"][0] == '7': return
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
                except:
                    value = float('nan')
                property_value.append(value)
            elif c in ["證券名稱", "證券代號"]: continue
            else:
                try:
                    value = float(i_price[c].replace(',',''))
                except:
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
    # date_time = datetime.datetime(2025,1,9)
    date_time = datetime.datetime(2011,8,9)
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
            except:
                print(q_res)
        # else:
        #     insert("date", ['date'], [str(date_time.date())], "date")
        # print("twse_byte, tpex_byte, done", twse_byte, tpex_byte, done)#;exit()
        if len(args.csv) > 0:
            if done: 
                print(date_time, " crawled!")
                date_time = date_time - datetime.timedelta(days=1)
                continue

            date_str = str(date_time).split(' ')[0].replace("-", "_")
            history_name_twse = date_str + "_twse.csv"
            if twse_byte is None and history_name_twse in os.listdir(args.csv):
                with open(os.path.join(args.csv, history_name_twse), "r") as f:
                    twse_byte = str2db_byte(f.read()) 
                print('twse history file exist.')

            history_name_tpex = date_str + "_tpex.csv"
            if tpex_byte is None and history_name_tpex in os.listdir(args.csv):
                with open(os.path.join(args.csv, history_name_tpex), "r") as f:
                    tpex_byte = str2db_byte(f.read())
                print('tpex history file exist.')
            
            # if twse_byte is None or tpex_byte is None:
            #     q_res = query_data("date", ['twse', 'tpex', 'done'], {'date':[str(date_time.date())]})
            #     if q_res is not None:
            #         try:
            #             tmp_twse_byte, tmp_tpex_byte, done = q_res[0]
            #         except:
            #             print(q_res)
            #     if twse_byte is None:
            #         twse_byte = tmp_twse_byte
            #     if tpex_byte is None:
            #         tpex_byte = tmp_tpex_byte
            
            # print(q_res)
            # time.sleep(0.5)
        if done:
            print(date_time, " crawled!")
            date_time = date_time - datetime.timedelta(days=1)
            continue
        # if twse_byte is not None and tpex_byte is not None:
        # # if query_data("date", ['twse', 'tpex'], {'date':[str(date_time.date())]}) is not None: 
        #     print(date_time, " crawled!")
        #     date_time = date_time - datetime.timedelta(days=1)
        #     continue
        
        obj = csv_parser(datetime.date(date_time.year, date_time.month, date_time.day), twse_byte, tpex_byte, csv=len(args.csv)>0)
        obj.crawl_price()
        if obj.price is not None:
            # Update_Queue.put(obj)
            obj.update()
            update_data('date', {'done': "true"}, {"date": str(obj.date.strftime('%Y-%m-%d'))})

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