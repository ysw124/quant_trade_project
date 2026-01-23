# market_radar.py
# 用于整合不同网站的实时信息

from harvesters.akshare_harvesters import QuantDataHarvester
from scrapers.gov_cn_scrapers import GovernmentPolicyScraper
from datetime import datetime
import pandas as pd


class MarketRadar:
    """
    中台类：负责整合全市场的‘面’数据（快讯、板块、人气、研报、热搜、政策）
    """

    def __init__(self):
        self.harvester = QuantDataHarvester()
        self.policy_scraper = GovernmentPolicyScraper(pages=2)

    def get_full_intelligence(self):
        """获取所有维度的原始数据包"""
        print(f"[{datetime.now().strftime('%H:%M:%S')}] 正在执行全维度市场扫描...")

        # 获取 AkShare 端的 5 个维度
        intel = self.harvester.get_all_raw_data()

        # 补充爬虫端的 政策维度
        intel["政策面"] = self.policy_scraper.get_news_df()

        return intel

    def format_to_text(self, intel, top_n=10):
        """将数据字典转化为易读的 Markdown 文本（为 LLM 准备）"""
        output = [f"# 市场全景扫描报告 ({datetime.now().strftime('%Y-%m-%d %H:%M')})\n"]

        # 定义展示顺序和标题映射
        mapping = {
            "快讯面": "📰 实时快讯 (财联社)",
            "板块面": "🔥 热门赛道 (板块行情)",
            "人气面": "📈 市场人气个股",
            "研报面": "💡 机构研报共识",
            "热搜面": "🔍 题材热搜关键词",
            "政策面": "⚖️ 政府最新政策"
        }
        # 针对不同维度，选择不同的敏感度, top_n 作为限制
        limits = {
            "快讯面": 15,  # 快讯多一点，方便 LLM 找关联
            "板块面": 8,  # 赛道不需要太多，只看最热的
            "人气面": 10,
            "研报面": 10,
            "热搜面": 5,  # 关键词只要最火的几个
            "政策面": 10
        }
        for key, title in mapping.items():
            if key in intel and not intel[key].empty:
                limit = limits.get(key, top_n)  # 获取自定义限制，默认用 top_n
                output.append(f"## {title}")
                output.append(intel[key].head(limit).to_markdown(index=False))

        return "\n".join(output)


if __name__ == "__main__":
    # 单独测试 Radar 模块
    radar = MarketRadar()
    data = radar.get_full_intelligence()
    text = radar.format_to_text(data)
    print(text)