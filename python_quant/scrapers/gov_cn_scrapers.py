import requests
import pandas as pd
import time
from fake_useragent import UserAgent


class GovernmentPolicyScraper:
    def __init__(self):
        self.ua = UserAgent()
        # 直接使用你发现的那个“金矿”接口
        self.api_url = "https://www.gov.cn/zhengce/zuixin/ZUIXINZHENGCE.json"

    def get_news_df(self):
        headers = {
            'User-Agent': self.ua.random,
            'Referer': 'https://www.gov.cn/zhengce/zuixin.htm'
        }

        try:
            res = requests.get(self.api_url, headers=headers, timeout=15)
            res.encoding = 'utf-8'
            raw_data = res.json()

            news_data = []
            for item in raw_data:
                # --- 精确匹配你提供的键名 ---
                title = item.get('TITLE', '').strip()
                date = item.get('DOCRELPUBTIME', '').strip()
                link = item.get('URL', '').strip()

                if not title: continue

                # 补全链接逻辑
                full_link = f"https://www.gov.cn{link}" if link.startswith('/') else link

                news_data.append({
                    '发布日期': date,
                    '政策标题': title,
                    '详情链接': full_link,
                    '来源': '中国政府网'
                })

            df = pd.DataFrame(news_data)

            # 格式化与清理
            if not df.empty:
                # 统一重置索引，看起来更整齐
                df = df.sort_values(by='发布日期', ascending=False).reset_index(drop=True)
                print(f"✅ 解析成功！获取到 {len(df)} 条政策。")

            return df

        except Exception as e:
            print(f"❌ 解析 JSON 出错: {e}")
            return pd.DataFrame()

# --- 使用示例 ---
if __name__ == '__main__':
    scraper = GovernmentPolicyScraper()
    df = scraper.get_news_df()
    print(df.head())