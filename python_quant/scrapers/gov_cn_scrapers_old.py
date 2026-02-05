import requests
import pandas as pd  # 建议使用缩写 pd
from bs4 import BeautifulSoup
from fake_useragent import UserAgent
from datetime import datetime
import time


class GovernmentPolicyScraper:
    def __init__(self, pages=2):  # 默认抓2页通常足够覆盖最新政策
        self.pages = pages
        self.ua = UserAgent()
        # 预先生成 URL 列表
        self.url_list = self._generate_url_list()

    def _generate_url_list(self):
        urls = []
        for page in range(0, self.pages):
            if page == 0:
                urls.append("https://www.gov.cn/zhengce/zuixin/home.htm")
            else:
                urls.append(f"https://www.gov.cn/zhengce/zuixin/home_{page}.htm")
        return urls

    def parse_news_gov(self, html):
        soup = BeautifulSoup(html, 'html.parser')
        news_items = soup.select('.news_box h4')
        news_data = []

        for item in news_items:
            a_tag = item.select_one('a')
            if not a_tag: continue

            title = a_tag.get_text(strip=True)
            link = a_tag['href']

            # 自动补全绝对路径
            if link.startswith('./'):
                full_link = f"https://www.gov.cn/zhengce/zuixin/{link.replace('./', '')}"
            elif link.startswith('/'):
                full_link = f"https://www.gov.cn{link}"
            else:
                full_link = link

            date_span = item.select_one('.date')
            date = date_span.get_text(strip=True) if date_span else None

            # 统一列名，与之前的 Harvester 保持风格一致
            news_data.append({
                '发布日期': date,
                '政策标题': title,
                '详情链接': full_link,
                '来源': '中国政府网'
            })
        return news_data

    def get_news_df(self):
        """
        核心优化：返回 DataFrame 格式，方便后续合并
        """
        all_news = []
        headers = {'User-Agent': self.ua.random}

        for url in self.url_list:
            try:
                # 增加超时控制，防止程序卡死
                res = requests.get(url, headers=headers, timeout=10)
                res.encoding = 'utf-8'  # 政府网强制 utf-8 比较稳妥
                res.raise_for_status()

                page_data = self.parse_news_gov(res.text)
                all_news.extend(page_data)

                # 礼貌爬取，避免请求过快
                time.sleep(0.5)

            except Exception as e:
                print(f"爬取 {url} 时出错: {e}")

        # 转换为 DataFrame
        df = pd.DataFrame(all_news)

        # 简单清洗：确保日期格式统一或去除空值
        if not df.empty:
            df = df.dropna(subset=['政策标题'])
            # 可以在这里做进一步的关键词过滤，例如只看“印发”、“通知”类文件

        return df


if __name__ == '__main__':
    scraper = GovernmentPolicyScraper(pages=2)
    df_policy = scraper.get_news_df()

    #file_path = 'market_report.csv'
    # 保存文件
    #df_policy.to_csv(file_path, index=False, encoding='utf-8-sig')
    if not df_policy.empty:
        print(f"\n[成功抓取 {len(df_policy)} 条政策快讯]:")
        print(df_policy[['发布日期', '政策标题']].head(10).to_string(index=False))