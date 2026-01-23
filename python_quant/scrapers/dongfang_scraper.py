# 获取东方财富的精选新闻

import requests
from bs4 import BeautifulSoup
from fake_useragent import UserAgent
import time

class GetInformationFromDongfang:

    def __init__(self):
        self.urlList = []
        self.pages = 5
        self.urlList = self.get_urlList(self.pages)

    def get_urlList(self, pages):
        for page in range(1, pages + 1):
            if page == 1:
                url = "https://finance.eastmoney.com/a/ccjdd.html"
            else:
                url = f"https://finance.eastmoney.com/a/ccjdd_{page}.html"
            self.urlList.append(url)
        return self.urlList

    def get_news(self):
        urlList = self.urlList
        news_data = []
        headers = {
            'User-Agent': UserAgent().random
        }

        for url in urlList:
            try:
                res = requests.get(url, headers=headers)
                res.raise_for_status()  # 检查响应状态是否正常
                html = res.text
                self.parse_news_ccjdd(html, news_data)
                time.sleep(1)  # 暂停1秒以避免频繁请求
            except requests.exceptions.RequestException as e:
                print(f"Request error occurred: {e}")
        return news_data

    def parse_news_ccjdd(self, html, news_data):
        soup = BeautifulSoup(html, 'html.parser')
        news_list = soup.select('.artitleList ul li')
        for item in news_list:
            title_element = item.select_one('.title a')
            if title_element:
                title = title_element.get_text(strip=True)  # 提取并清理标题文本
                link = title_element['href']  # 获取链接
                news_data.append({
                    'title': title,
                    'link': link
                })

if __name__ == '__main__':
    getInformation = GetInformationFromDongfang()
    urlList = getInformation.get_urlList(pages=10)
    # print('urlList is:', urlList)  # 如果需要查看 URL 列表，可以取消注释

    news_data = getInformation.get_news()  # 获取新闻数据
    for news in news_data:  # 打印获取到的新闻数据
        print(news)
