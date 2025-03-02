import requests
from bs4 import BeautifulSoup
import pandas as pd

# 目標網址
# url = "https://hist.tpex.org.tw/Hist/STOCK/AFTERTRADING/DAILY_CLOSE_QUOTES/RSTA3104_%s.HTML"
url = "https://www.tpex.org.tw/www/zh-tw/afterTrading/dailyQuotesHis?id=&response=csv&date="
url = url + '2007/04/20'

# url = url % '951228'
# print(url);exit()
# 發送 HTTP GET 請求
response = requests.get(url)
response.encoding = 'utf-8'  # 設定編碼以避免亂碼
html_content = eval(response.text)['html']
html_content = html_content.replace("\r\n", "")
html_content = html_content.replace("\t", "")
# print(html_content)
# 使用 BeautifulSoup 解析 HTML
soup = BeautifulSoup(html_content, 'html.parser')


title = soup.find_all('table')[0]
if not title:
    raise ValueError("找不到目標表格，請檢查網頁結構")
title_rows = title.find_all('tr')  # 獲取所有的列
data = []
for row in title_rows:
    print(row)
    cols = row.find_all('td')  # 找到每一列的所有欄位
    cols = [col.text.strip() for col in cols]  # 提取文字並移除多餘的空白
    if cols:  # 排除空列
        data.append(cols)
columns = data[1]
# print(data[1])
# exit()

# 找到目標表格 (通常需要根據網頁的結構調整)
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

print(data)
# 將資料轉為 Pandas DataFrame
# columns = list(data[0])  # 假設第一列是表頭
# # columns.insert(2, "收盤價sign")
# # columns.insert(4, "漲跌sign")
# print(columns)
df = pd.DataFrame(data, columns=columns)  # 從第二列開始為數據

# 輸出 DataFrame
print(df)