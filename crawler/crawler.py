from io import StringIO
import psycopg2
import os
import csv
import time
import pandas as pd
import numpy as np
from header import generate_random_header
import requests
from util import combine_index, preprocess
host = "172.128.0.2"
dbname = "stock"
user = "stadmin"
password = "stadmin"
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
    def __init__(self, date):
        self.date = date
        pass

    def find_best_session(self):
        for i in range(10):
            try:
                print('獲取新的Session 第', i, '回合')
                headers = generate_random_header()
                self.ses = requests.Session()
                self.ses.get('https://www.twse.com.tw/zh/', headers=headers, timeout=10)
                self.ses.headers.update(headers)
                print('成功！')
                return
            except (ConnectionError, requests.exceptions.ReadTimeout) as error:
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
        i = 3
        while i >= 0:
            try:
                return self.ses.get(*args1, timeout=10, **args2)
            except (ConnectionError, requests.exceptions.ReadTimeout) as error:
                print(error)
                print('retry one more time after 60s', i, 'times left')
                time.sleep(60)
                self.ses = self.find_best_session()

            i -= 1
        return pd.DataFrame()

    def price_twe(self, date):
        date_str = date.strftime('%Y%m%d')
        res = self.requests_get('https://www.twse.com.tw/exchangeReport/MI_INDEX?response=csv&date='+date_str+'&type=ALLBUT0999')

        if res.text == '':
            return pd.DataFrame()

        header = np.where(list(map(lambda l: '證券代號' in l, res.text.split('\n')[:500])))[0][0]

        df = pd.read_csv(StringIO(res.text.replace('=','')), header=header-1)
        df = combine_index(df, '證券代號', '證券名稱')
        df = preprocess(df, date)
        return df
    
    def crawl_price(self):

        dftwe = self.price_twe(self.date)
        time.sleep(5)
        print(dftwe)
        # dfotc = price_otc(date)
        # if len(dftwe) != 0 and len(dfotc) != 0:
        #     df = merge(dftwe, dfotc, o2tp)
        #     return df
        # else:
        #     return pd.DataFrame()


import datetime
if __name__ == "__main__":
    date = datetime.datetime.now()
    date = datetime.datetime(2024,11,8)
    print(date)
    while date >= datetime.datetime(1990,1,1):
        df = csv_parser(datetime.date(date.year, date.month, date.day))
        df.crawl_price()
        print(date.date())
        time.sleep(2)
        exit()
        date = date - datetime.timedelta(days=1)
    # df.head()
    print("exit")