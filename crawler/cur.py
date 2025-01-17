import requests
from bs4 import BeautifulSoup
import csv
from datetime import datetime

def scrape_website(url):
    # 發送 HTTP GET 請求
    try:
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        }
        response = requests.get(url, headers=headers)
        response.raise_for_status()  # 檢查是否請求成功
    except requests.RequestException as e:
        print(f"請求失敗: {e}")
        return None

    # 解析 HTML
    soup = BeautifulSoup(response.text, 'html.parser')
    
    # 這裡需要根據目標網站的具體結構來提取數據
    # 這是一個示例，您需要根據實際網站修改選擇器
    data = []
    articles = soup.find_all('article')
    
    for article in articles:
        title = article.find('h2').text.strip() if article.find('h2') else 'N/A'
        link = article.find('a')['href'] if article.find('a') else 'N/A'
        data.append({
            'title': title,
            'link': link,
            'timestamp': datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        })
    
    return data

def save_to_csv(data, filename):
    if not data:
        print("沒有數據可保存")
        return
    
    # 保存到 CSV 文件
    try:
        with open(filename, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=data[0].keys())
            writer.writeheader()
            writer.writerows(data)
        print(f"數據已保存到 {filename}")
    except IOError as e:
        print(f"保存文件時出錯: {e}")

def main():
    # 設置要爬取的網址
    target_url = "https://www.tpex.org.tw/www/zh-tw/afterTrading/dailyQuotesHis?id=&response=csv&date=2007/04/20"  # 替換為您要爬取的網站
    
    # 爬取數據
    scraped_data = scrape_website(target_url)
    print(scraped_data)
    if scraped_data:
        # 保存數據
        save_to_csv(scraped_data, f'scraped_data_{datetime.now().strftime("%Y%m%d_%H%M%S")}.csv')

if __name__ == "__main__":
    main()