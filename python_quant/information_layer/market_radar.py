# market_radar.py
# 用于整合不同网站的实时信息
from python_quant.harvesters.akshare_harvesters import QuantDataHarvester
from python_quant.scrapers.gov_cn_scrapers import GovernmentPolicyScraper
from datetime import datetime
import pandas as pd
from utils.db_helper import QuantumDB
from python_quant.logic_layer.analyst_agent import AnalystAgent


class MarketRadar:
    """
    中台类：负责整合全市场的‘面’数据（快讯、板块、人气、研报、热搜、政策）
    """

    def __init__(self):
        self.harvester = QuantDataHarvester()
        self.policy_scraper = GovernmentPolicyScraper(pages=2)
        self.db_client = QuantumDB()
        self.agent = AnalystAgent()  # 新增：用于预分析新闻

    # def get_full_intelligence(self):
    #     """获取所有维度的原始数据包"""
    #     print(f"[{datetime.now().strftime('%H:%M:%S')}] 正在执行全维度市场扫描...")
    #
    #     # 获取 AkShare 端的 5 个维度
    #     intel = self.harvester.get_all_raw_data()
    #
    #     # 补充爬虫端的 政策维度
    #     intel["政策面"] = self.policy_scraper.get_news_df()
    #
    #     return intel

    def get_full_intelligence(self):
        print(f"[{datetime.now().strftime('%H:%M:%S')}] 正在执行全维度市场扫描...")

        # 1. 获取原始数据包
        intel = self.harvester.get_all_raw_data()
        policy_df = self.policy_scraper.get_news_df()

        # 2. 处理政策面：去重 -> 分析 -> 入库
        if not policy_df.empty:
            print("⚖️ 正在处理政府政策...")
            # 过滤掉数据库里已有的标题
            new_policies = self._filter_new_items(policy_df, '政策标题','policy')
            if not new_policies.empty:
                # 预分析：将 '政策标题' 翻译成 分数和板块
                print(f"✨ 发现 {len(new_policies)} 条新政策，正在请求 AI 分析...")
                analyzed_df = self._analyze_and_store(new_policies, '政策标题', 'policy')
                intel["政策面"] = analyzed_df
            else:
                print("✅ 政策库已是最新，无新增。")
                intel["政策面"] = pd.DataFrame(columns=['日期', '政策标题', 'ai_score', 'sectors'])  # 全是旧闻

        # 3. 处理快讯面：去重 -> 分析 -> 入库
        flash_df = intel.get("快讯面", pd.DataFrame())
        if not flash_df.empty:
            print("📰 正在处理实时快讯...")
            # 假设快讯的列名是 'content' 或 '标题'，请根据你的 fetch_cls_news 结果调整
            flash_col = '标题' if '标题' in flash_df.columns else flash_df.columns[0]
            new_flash = self._filter_new_items(flash_df, flash_col,'flash')
            if not new_flash.empty:
                print(f"🔥 发现 {len(new_flash)} 条新快讯，同步 AI 评分中...")
                self._analyze_and_store(new_flash, flash_col, 'flash')
            else:
                print("✅ 快讯库已同步。")
        return intel

    def _filter_new_items(self, df, col_name, source_type):
        """
        针对性去重：
        - 如果是 policy：对比库中所有历史政策。
        - 如果是 flash：只对比库中最近 30 天的快讯。
        """
        cursor = self.db_client.conn.cursor()

        if source_type == 'policy':
            # 政策类：全量提取标题
            cursor.execute("SELECT title FROM news_items WHERE source='policy'")
        else:
            # 快讯类：只提取最近30天的标题
            cursor.execute("""
                SELECT title FROM news_items 
                WHERE source='flash' 
                AND created_at > datetime('now', '-30 days')
            """)

        existing_titles = set(res[0] for res in cursor.fetchall())

        # 返回不在库中的新鲜新闻
        return df[~df[col_name].isin(existing_titles)]

    def _analyze_and_store(self, df, col_name, source_type):
        """调用 AI 分析并存入数据库"""
        titles = df[col_name].tolist()
        # 调用你的 agent：输入标题列表，返回 {标题: {'score': 90, 'sectors': {...}}}
        analysis_results = self.agent.batch_analyze(titles)

        # 组装入库数据
        to_db_list = []
        for _, row in df.iterrows():
            title = row[col_name]
            analysis = analysis_results.get(title, {'score': 0, 'sectors': {}})

            to_db_list.append({
                'date': row.get('日期', datetime.now().strftime('%Y-%m-%d')),
                'title': title,
                'ai_score': analysis['score'],
                'sectors': analysis['sectors']
            })
            # 存入数据库
        to_db_df = pd.DataFrame(to_db_list)
        self.db_client.save_news_batch(to_db_df, source_type)
        return to_db_df

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