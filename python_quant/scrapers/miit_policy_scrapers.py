import requests
from bs4 import BeautifulSoup
import pandas as pd
from fake_useragent import UserAgent
import time


class MiitPolicyScraper:
    def __init__(self):
        self.ua = UserAgent()
        # 针对你发现的规律，直接访问政策文件频道
        self.url = "https://www.miit.gov.cn/zwgk/zcwj/wjfb/index.html"
        self.root_url = "https://www.miit.gov.cn"


    def fetch_miit_policies(self):
        print("[爬虫源] 正在抓取工信部(MIIT)最新政策文件...")

        headers = {
            "User-Agent": self.ua.random,
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
            "Referer": "https://www.miit.gov.cn/",
            "Cookie": "js_path=/zwgk/zcwj/wjfb/"  # 有些政府网站会检查访问路径Cookie
        }

        try:
            # 这里的请求如果还是 403，通常是因为 WAF 防火墙检测到了 TLS 指纹
            # 我们可以增加一个简易的 Session 维持
            session = requests.Session()
            response = session.get(self.url, headers=headers, timeout=15)
            response.encoding = 'utf-8'

            soup = BeautifulSoup(response.text, "html.parser")

            # 根据你提供的元素结构进行精准匹配
            # 查找所有 class 为 'fl' 的 a 标签，且 href 包含 art
            items = soup.find_all("a", class_="fl")
            policy_data = []

            for a_tag in items:
                title = a_tag.get("title") or a_tag.get_text(strip=True)
                href = a_tag.get("href")

                # 过滤并补全链接
                if "/art/" in href:
                    full_link = href if href.startswith("http") else f"{self.root_url}{href}"

                    # 尝试从父级或同级寻找日期（通常在 <li> 或紧跟的 <span> 中）
                    date_tag = a_tag.find_next("span") or a_tag.parent.find("span")
                    date = date_tag.get_text(strip=True) if date_tag else "2026-01-19"  # 兜底当前日期

                    policy_data.append({
                        "发布日期": date.strip("[] "),
                        "政策标题": title,
                        "详情链接": full_link,
                        "来源": "工信部文件库"
                    })

            return pd.DataFrame(policy_data).head(10)
        except Exception as e:
            print(f"工信部深度爬取失败: {e}")
            return pd.DataFrame()


# --- 测试部分 ---
if __name__ == "__main__":
    miit = MiitPolicyScraper()
    df = miit.fetch_miit_policies()
    print(df.to_string(index=False))