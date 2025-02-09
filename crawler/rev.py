from io import StringIO
import pandas as pd
import psycopg2
import requests
import re
import random
import copy
from requests.exceptions import ReadTimeout
from header import generate_random_header, name2colname
from postgre_fun import update_data, query_by_SQL, str2db_byte, update_file, insert, check_table, create_table, delete_data, query_data, get_file, db_byte2str
import time
import numpy as np
import datetime
import argparse
from bs4 import BeautifulSoup
from joblib import Parallel, delayed

parser = argparse.ArgumentParser()
parser.add_argument("--csv", default="", help="Use exist csv file for dir.")
parser.add_argument("--date", default=None, help="Start date")
parser.add_argument("--daily", action='store_true', help="Start date")
args = parser.parse_args()
def find_best_session():

    for i in range(10):
        try:
            print('獲取新的Session 第', i, '回合')
            headers = generate_random_header()
            ses = requests.Session()
            ses.get('https://www.twse.com.tw/zh/', headers=headers, timeout=10)
            ses.headers.update(headers)
            print('成功！')
            return ses
        except (ConnectionError, ReadTimeout) as error:
            print(error)
            print('失敗，10秒後重試')
            time.sleep(10)

    print('您的網頁IP已經被證交所封鎖，請更新IP來獲取解鎖')
    print("　手機：開啟飛航模式，再關閉，即可獲得新的IP")
    print("數據機：關閉然後重新打開數據機的電源")

ses = None
def requests_get(*args1, **args2):

    # get current session
    global ses
    if ses == None:
        ses = find_best_session()

    # download data
    i = 3
    while i >= 0:
        try:
            return ses.get(*args1, timeout=10, **args2)
        except (ConnectionError, ReadTimeout) as error:
            print(error)
            print('retry one more time after 60s', i, 'times left')
            time.sleep(60)
            ses = find_best_session()

        i -= 1
    return pd.DataFrame()

def combine_index(df, n1, n2):

    """將dataframe df中的股票代號與股票名稱合併

    Keyword arguments:

    Args:
        df (pandas.DataFrame): 此dataframe含有column n1, n2
        n1 (str): 股票代號
        n2 (str): 股票名稱

    Returns:
        df (pandas.DataFrame): 此dataframe的index為「股票代號+股票名稱」
    """

    return df.set_index(df[n1].astype(str).str.replace(' ', '') + \
        ' ' + df[n2].astype(str).str.replace(' ', '')).drop([n1, n2], axis=1)

def preprocess(df, date):
    df = df.dropna(axis=1, how='all').dropna(axis=0, how='all')
    if len(df.columns) > 0:
        print(df.columns, type(df.columns.str))
        df.columns = df.columns.str.replace(' ', '')
    df.index.name = 'stock_id'
    df.columns.name = ''
    df['date'] = pd.to_datetime(date)
    df = df.reset_index().set_index(['stock_id', 'date'])
    df = df.apply(lambda s: s.astype(str).str.replace(',',''))

    return df

def get_trade_day():
    # TODO
    return ['2024-05-02', '2024-05-03', '2024-05-06', '2024-05-07', '2024-05-08', '2024-05-09']

def update_to_database(twe, otc, db):
    # TODO
    # print(twe)
    print(otc)
    return

def otc_date_str(date):
    """將datetime.date轉換成民國曆

    Args:
        date (datetime.date): 西元歷的日期

    Returns:
        str: 民國歷日期 ex: 109/01/01
    """
    date_list = date.split('-')
    return '/'.join([str(int(date_list[0]) - 1911), date_list[1], date_list[2]]) 


def pe_twe(date, db=None):
    # datestr = date.strftime('%Y%m%d')
    # res = requests_get('https://www.twse.com.tw/exchangeReport/BWIBBU_d?response=csv&date='+datestr+'&selectType=ALL')
    dates = get_trade_day()
    for date in dates:
        res = requests_get('https://www.twse.com.tw/exchangeReport/BWIBBU_d?response=csv&date='+date.replace('-', '')+'&selectType=ALL')
        
        # print(res.text)
        try:
            twe = pd.read_csv(StringIO(res.text), header=1)
            twe = combine_index(twe, '證券代號', '證券名稱')
            twe = preprocess(twe, date)
        except:
            twe = pd.DataFrame()
            # return pd.DataFrame()

        otcdate = otc_date_str(date)
        res = requests_get('https://www.tpex.org.tw/web/stock/aftertrading/peratio_analysis/pera_result.php?l=zh-tw&o=csv&charset=UTF-8&d='+otcdate+'&c=&s=0,asc')
        # try:
        otc = pd.read_csv(StringIO(res.text), header=3)
        otc = combine_index(otc, '股票代號', '名稱')
        otc = preprocess(otc, date)
        # except Exception as e:
        #     print(e)
        #     otc = pd.DataFrame()
            # return pd.DataFrame()
        update_to_database(twe, otc, db)

def get_profit(cursor, stock_list, ACyear):
    ACyear
    year = ACyear - 1911
    marketTypes = [
        'sii',
        'otc',
    ]
    for marketType in marketTypes:
        url = "https://mops.twse.com.tw/server-java/t05st09sub?step=1&TYPEK="+marketType+"&YEAR="+str(year)+"&first="
        res = requests_get(url)
        # res = requests.get(url)
        res.encoding = 'big5'

        tables = pd.read_html(StringIO(res.text), encoding='big-5')[2:]
        # asdas = res.content.split(b'2353')
        
        # print( asdas[-1][:1000].decode("cp950"));exit()
        # tables = pd.io.html.read_html(res.content)[2:]
        # sadiu = sad[0]
        # print(sadiu.columns)
        for table in  tables:
            print(table.columns)
            # dpsd = table['摘錄公司章程-股利分派部分'].iloc[4] 
            # for c in dpsd:
            #     print(c)
            # exit()
            table = table.set_index('公司代號 名稱')
            if '董事會決議\t通過股利\t分派日' in table:
                date_string = '董事會決議\t通過股利\t分派日'
            elif '董事會決議\t（擬議）股\t利分派日' in table:
                date_string = '董事會決議\t（擬議）股\t利分派日'

            董事會決議擬議股利分派日 = table[date_string]
            # print(董事會決議擬議股利分派日)
            if '股東配發內容' in table:
                owner_content = '股東配發內容'
            elif '股東股利' in table:
                owner_content = '股東股利'

            content = table[owner_content]

            content[date_string] = 董事會決議擬議股利分派日
            print(content)
            print(content.groupby('公司代號 名稱')[date_string].max())
            content = content.groupby('公司代號 名稱')[date_string].max()
            if owner_content == '股東配發內容':
                盈餘分配之現金股利 = table[owner_content].groupby('公司代號 名稱')['盈餘分配\t之現金股利\t(元/股)'].sum()
                法定盈餘公積資本公積發放之現金 = table[owner_content].groupby('公司代號 名稱')['法定盈餘\t公積、資本\t公積發放 之現金(元/股)'].sum()
                盈餘轉增資配股 = table[owner_content].groupby('公司代號 名稱')['盈餘轉\t增資配股\t(元/股)'].sum()
                # 法定盈餘公積資本公積轉增資配股 = table[owner_content].groupby('公司代號 名稱')['法定盈餘\t公積、資本\t公積轉增資\t配股(元/股)'].sum()
                每股股利 = 盈餘分配之現金股利+法定盈餘公積資本公積發放之現金+盈餘轉增資配股#+法定盈餘公積資本公積轉增資配股
            elif owner_content == '股東股利':
                現金股利元股 = table[owner_content].groupby('公司代號 名稱')['現金股利 (元/股)'].sum()
                盈餘配股元股 = table[owner_content].groupby('公司代號 名稱')['盈餘配股 (元/股)'].sum()
                每股股利 = 現金股利元股+盈餘配股元股
            
            print(每股股利)
            print(table[owner_content])
            print()
            print("=========================")
            
            for stock_info, date in zip(每股股利.items(), content):
                data = stock_info[1]
                stock_id = stock_info[0][0].split('-')[0].strip()
                date = str(date).split('/')
                if len(date) <2:
                    continue
                date = '-'.join([str(int(date[0]) + 1911).zfill(4), str(date[1]).zfill(2), str(date[2]).zfill(2)])
                if stock_id in stock_list:
                    cursor.execute("select * from dividend where stock_id=%s and year=%s ;", (stock_id, ACyear))  
                    rows = cursor.fetchall()
                    if len(rows) > 0:
                        cursor.execute("update dividend set dividend=%s, date=%s where stock_id=%s and year=%s;", (data, date, stock_id, ACyear))
                    else:
                        cursor.execute("INSERT INTO dividend (stock_id, year, dividend, date) VALUES (%s, %s, %s, %s);", (stock_id, ACyear, data, date))

                    print(stock_id, ACyear, data, date)


        

    # print(sad[1])
    # print("=========================")
    # print(sad[2])
    # print("=========================")
    # print(3, sad[3])
    # print("=========================")
    # print(sad[4])
    # twe = pd.read_csv(StringIO(res.text), header=1)
    # print(twe)
# from finlab.data import Data
# data = Data()
# data.cache = True
# pe = data.get('本益比')
# print(pe['2330'])

def month_revenue(name, date):

    year = date.year - 1911
    month = date.month
    if month == 12:
        year -= 1
    url = 'https://mops.twse.com.tw/nas/t21/%s/t21sc03_%d_%d.html' % (name, year, month)
    print(url)
    res = requests_get(url, verify=False)
    res.encoding = 'big5'

    try:
        dfs = pd.read_html(StringIO(res.text), encoding='big-5')
    except:
        print('MONTH ' + name + ': cannot parse ' + str(date))
        return pd.DataFrame()
    
    if date == datetime.datetime(year=2012, month=1, day=1):
        df = pd.concat([df for df in dfs if df.shape[1] <= 11 and df.shape[1] > 5 and 'levels' in dir(df.columns)])
        df.columns = df.columns.get_level_values(1)
    else:
        df = pd.concat([df for df in dfs if df.shape[1] <= 11 and df.shape[1] > 5])
    
    if 'levels' in dir(df.columns):
        df.columns = df.columns.get_level_values(1)
    else:
        df = pd.concat([df for df in dfs if df.shape[1] <= 11 and df.shape[1] > 5 and 'levels' in dir(df.columns)])
        df.columns = df.columns.get_level_values(1)
            
    column_list = df.columns.values
    for i, col in enumerate(column_list):
        column_list[i] = col.replace(" ", "")
    df.columns = column_list
    


    df = df.loc[:,~df.columns.isnull()]
    df = df.loc[~pd.to_numeric(df['當月營收'], errors='coerce').isnull()]
    df = df[df['公司代號'] != '合計']
    # df = combine_index(df, '公司代號', '公司名稱')
    
    df = preprocess(df, datetime.date(date.year, date.month, 10))
    df = df.reset_index().set_index(['公司代號'])
    # print(df);exit()
    return df.drop_duplicates()

def get_stock_list(cursor):
    cursor.execute("select * from stock_list where securitytype = '01';")
    stock_list = cursor.fetchall()
    if len(stock_list)>0:
        stock_list = np.array(stock_list)[:,0].tolist()
    else:
        return []
    return sorted(stock_list)
    
def iterfunc(i_price):
    try:
        int(i_price._name)
        res = insert("s_"+i_price._name.lower(), ['date', 'rev'], [str(i_price['date'].strftime('%Y-%m-%d')), i_price['當月營收']], 'date')
    except:
        print(i_price._name, i_price['當月營收'], i_price['date'].strftime('%Y-%m-%d'))
        return
    # print(i_price.__dict__)

if __name__ == "__main__":
    date_time = datetime.datetime.now()
    daily_limit = date_time - datetime.timedelta(days=92)
    # date_time = datetime.datetime(2025,2,11)
    # date_time = datetime.datetime(2007,4,25)
    # date_time = datetime.datetime(2004,11,3)
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
    print("crawler rev for", date_time)
    start_date_time = datetime.datetime(date_time.year, date_time.month, date_time.day)
    while start_date_time >= datetime.datetime(1990,1,1):
        print("now crawling:", start_date_time)
        mouth = start_date_time.month
        # print(datetime.datetime(1990,1,1))
        
        if mouth == 1:
            mouth = 12
            year = start_date_time.year - 1
        else:
            mouth = mouth - 1
            year = start_date_time.year
        start_date_time = datetime.datetime(year, mouth, 1)
        # print(start_date_time);exit()
        if args.daily and start_date_time < daily_limit:
            # check_rev = query_by_SQL("select rev from s_2330 where date = '2024-12-10' and nullif(rev, 'NaN') is not null;")
            # print(date_time>=datetime.datetime(year, date_time.month, 10))
            # check_rev = query_by_SQL("select rev from s_2330 where date = '"+str(datetime.datetime(year, mouth, 10))+"' and nullif(rev, 'NaN') is not null;")
            # if len(check_rev) != 0:
            print(datetime.datetime(year, mouth, 10), "daily rev crawler crawlered break")
            break

        res = query_by_SQL("\
                            SELECT substring(table_name, 3, 100) \
                            FROM information_schema.tables where table_name like 's\_%';")
            
        # Construct connection string
        rev = month_revenue('sii', start_date_time)

        dfotc = month_revenue('otc', start_date_time)
        rev = pd.concat((rev, dfotc))

        id2Name = {}
        
        stock_id = rev.index.values.astype(str)
        stock_name = rev["公司名稱"].values
        # print(stock_id, stock_name);exit()
        for id, name in zip(stock_id, stock_name):
            id2Name[id] = name
        new_stock = set(rev.index.values.astype(str)).difference(set(res))
        print(new_stock)
        if len(new_stock) > 0:
            for stock in new_stock:
                try:
                    int(stock)
                except:
                    continue
                if type(stock) not in [str, np.str_]: continue
                if len(stock)>=5 and stock[0] == '7': continue
                stock_name = id2Name[stock]
                if len(re.findall(r'[A-Z]{1}', stock))>0: continue
                if type(stock_name) not in [str, np.str_]: continue
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

        ret = Parallel(n_jobs=4, backend='threading')(delayed(iterfunc)(rev.iloc[i]) for i in range(len(rev)))


