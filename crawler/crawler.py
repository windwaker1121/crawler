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
from postgre_fun import update_data, insert, check_table, create_table, delete_data, query_data
import re
otc_url = 'https://www.tpex.org.tw/www/zh-tw/afterTrading/dailyQuotes?id=&response=csv&date='
twe_url = 'https://www.twse.com.tw/exchangeReport/MI_INDEX?response=csv&type=ALLBUT0999&date='
host = "172.128.0.2"
dbname = "stock"
user = "admin"
password = "admin"
sslmode = "allow"

# Construct connection string
connect_succes = False
while not connect_succes:
    try:
        conn_string = "host={0} user={1} dbname={2} password={3} sslmode={4}".format(host, user, dbname, password, sslmode)
        conn = psycopg2.connect(conn_string)
        print("Connection established")
        cursor = conn.cursor()
        connect_succes = True
    except:
        
        pass
    print(connect_succes)
    time.sleep(1)

class stock_crawler(object):
    def __init__(self, stock_id):
        pass

class csv_parser(object):
    ses = None
    price = None
    def __init__(self, date):
        self.date = date
        self.date_time = datetime.datetime(date.year, date.month, date. day)
        self.timeout = 60
        pass

    def find_best_session(self):
        for i in range(10):
            try:
                # print('獲取新的Session 第', i, '回合')
                headers = generate_random_header()
                self.ses = requests.Session()
                self.ses.get('https://www.twse.com.tw/zh/', headers=headers, timeout=self.timeout)
                self.ses.headers.update(headers)
                print('獲取新的Session 第', i, '回合成功！')
                return
            except Exception as error:
                print(error)
                print('失敗，10秒後重試')
                time.sleep(10)

        print('您的網頁IP已經被證交所封鎖，請更新IP來獲取解鎖')
        print("　手機：開啟飛航模式，再關閉，即可獲得新的IP")
        print("數據機：關閉然後重新打開數據機的電源")

    def requests_get(self, *args1, **args2):
        # get current session
        if self.ses == None:
            self.find_best_session()

        # download data
        i = 10
        sleep = 30

        while i >= 0:
            try:
                if self.ses is not None:
                    return self.ses.get(*args1, timeout=self.timeout, **args2)
            except Exception as error:
                print(error)
                print('retry one more time after ',sleep,'s', i, 'times left')
            time.sleep(sleep)
            self.ses = self.find_best_session()

            i -= 1
        Update_Queue.put(None)
        return pd.DataFrame()

    def price_twe(self):
        date_str = self.date.strftime('%Y%m%d')
        res = self.requests_get(twe_url+date_str+'')

        if res.text == '':
            return pd.DataFrame()

        header = np.where(list(map(lambda l: '證券代號' in l, res.text.split('\n')[:500])))[0][0]

        df = pd.read_csv(StringIO(res.text.replace('=','')), header=header-1)
        return df
    
    def price_otc(self):
        date_str = self.date.strftime('%Y/%m/%d')
        # datestr = otc_date_str(date)
        link = otc_url + date_str
        res = self.requests_get(link)
        if type(res) is not requests.models.Response: return pd.DataFrame()
        if '上櫃家數,"0"' in res.text: return pd.DataFrame()
        try:
            df = pd.read_csv(StringIO(res.text.split('管理股票')[0]), header=2)
        except Exception as e:
            print(e, res.text)
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

    def crawl_price(self):

        dftwe = self.price_twe()
        # time.sleep(1)
        # print(dftwe)
        dfotc = self.price_otc()
        # print(dfotc)
        if len(dftwe) != 0 and len(dfotc) != 0:
            price = self.merge(dftwe, dfotc, o2tp)
            self.price = price
            # print(price)
            # print(type(price.loc[0]), price.loc[0]["證券名稱"])

    def update(self):
        idx = self.price.columns.tolist().copy()
        for i in range(len(self.price)):
            if len(re.findall(r'[A-Z]{1}$', self.price.loc[i]["證券代號"]))>0: continue
            if len(re.findall(r'[A-Z0-9]{2}[售|購]+[A-Z0-9]{2}$', self.price.loc[i]["證券名稱"]))>0: continue
            
            # print(not check_table(dbname, "s_"+price.loc[i]["證券代號"]))
            check = None
            while check is None:
                try:
                    check = check_table(dbname, "s_"+self.price.loc[i]["證券代號"].lower())
                except:
                    time.sleep(1)

            if not check:
                # time.sleep(0.2)
                print("股票 ", self.price.loc[i]["證券代號"].lower(), self.price.loc[i]["證券名稱"], " 不在資料庫中，創立資料表")
                create_table("s_"+self.price.loc[i]["證券代號"].lower(), 
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
            property_name = ['date']
            property_value = [str(self.date.strftime('%Y-%m-%d'))]

            for c in idx:
                # print(c)
                if c in ["成交金額", "成交股數", "成交筆數"]:
                    try:
                        value = float(self.price.loc[i][c].replace(',',""))
                    except:
                        value = float('nan')
                    property_value.append(value)
                elif c in ["證券名稱", "證券代號"]: continue
                else:
                    try:
                        value = float(self.price.loc[i][c])
                    except:
                        value = float('nan')
                    property_value.append(value)

                property_name.append(name2colname[c])
            # print(idx)
            # print(property_name)
            # print(property_value)
            # print(["date"]+idx, [str(self.date.strftime('%Y/%m/%d'))] + list(map(lambda x: price.loc[i][x] ,idx)))
            insert("s_"+self.price.loc[i]["證券代號"].lower(), property_name, property_value, 'date')
            # if insert("s_"+self.price.loc[i]["證券代號"].lower(), property_name, property_value, 'date') is not None:
            #     print(property_value)
        print(self.date, "更新完成")

import queue
Update_Queue = queue.Queue()
def cosumer():
    obj = 0
    while obj is not None:
        if Update_Queue.qsize() > 0:
            obj = Update_Queue.get_nowait()
            if obj is not None:
                obj.update()
                res = insert('date', ["date"], [str(obj.date)], PK='date')
                if res is not None:
                    print("insert new date in date", res)
                
            # exit()
                
            else:
                
                obj = 0
        else:
            # print("empty queue wait 1 sec")    
            time.sleep(1)
    print("cosumer end")
    return
import datetime
import threading
if __name__ == "__main__":
    date_time = datetime.datetime.now()
    date_time = datetime.datetime(2022,6,20)
    print(date_time)
    t = threading.Thread(target=cosumer)
    t.start()
    while date_time >= datetime.datetime(1990,1,1):
        if query_data("date", ['date'], {'date':[str(date_time.date())]}) is not None: 
            print(date_time, " crawled!")
            date_time = date_time - datetime.timedelta(days=1)
            continue
        
        obj = csv_parser(datetime.date(date_time.year, date_time.month, date_time.day))
        obj.crawl_price()
        if obj.price is not None:
            # if obj.price is not None:
            #     obj.update()
            #     print(obj.date_time.date(), "資料已更新")
            #     res = insert('date', ["date"], [str(obj.date)], PK='serial')
            #     if res is not None:
            #         print("insert new date in date", res)
            
            # # exit()
                
            # else:
            #     print(obj, str(obj.date), "沒有開盤")
            #     try:
            #         delete_data("date", {"date": str(obj.date)})
            #     except Exception as e:
            #         print("Exception delete_data date", e)

            Update_Queue.put(obj)
            time.sleep(2)
        else:
            print(obj, str(obj.date), "沒有開盤")
            try:
                delete_data("date", {"date": str(obj.date)})
            except Exception as e:
                print("Exception delete_data date", e)
                # exit()
        date_time = date_time - datetime.timedelta(days=1)
    Update_Queue.put(None)
    
    # df.head()
    print("exit")
    t.join()