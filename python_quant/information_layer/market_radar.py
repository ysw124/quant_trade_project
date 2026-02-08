# market_radar.py
# 用于整合不同网站的实时信息
from python_quant.harvesters.akshare_harvesters import QuantDataHarvester
from python_quant.scrapers.gov_cn_scrapers import GovernmentPolicyScraper
from datetime import datetime
import pandas as pd
from utils.db_helper import QuantumDB
from python_quant.logic_layer.analyst_agent import AnalystAgent
import time


class MarketRadar:
    """
    中台类：负责整合全市场的‘面’数据（快讯、板块、人气、研报、热搜、政策）
    """

    def __init__(self):
        self.harvester = QuantDataHarvester()
        self.policy_scraper = GovernmentPolicyScraper()
        self.db_client = QuantumDB()
        self.agent = AnalystAgent()  # 新增：用于预分析新闻



    def get_full_intelligence(self):
        print(f"[{datetime.now().strftime('%H:%M:%S')}] 正在执行全维度市场扫描...")

        # 1. 获取原始数据包
        intel = self.harvester.get_all_raw_data()
        policy_df = self.policy_scraper.get_news_df()

        # 2. 处理政策面：去重 -> 分析 -> 入库
        if not policy_df.empty:
            print("⚖️ 正在处理政府政策...")

            # 入库前重命名，对齐数据库字段 (pub_date, title, source)
            policy_df = policy_df.rename(columns={'发布日期': 'pub_date', '政策标题': 'title', '详情链接':'link_url'})

            # 直接把爬到的 1000 条往库里塞，save_news_batch 内部的 INSERT OR IGNORE 会自动去重
            self.db_client.save_news_batch(policy_df, source_type='policy')

            # 找出库里最近一年、且 ai_score=0 的新政策
            pending_policies = self.db_client.get_pending_news(days=365, source_type='policy')

            if not pending_policies.empty:
                print(f"✨ 发现 {len(pending_policies)} 条新政策，请求 AI 批量分析...")
                # 调用你提供的分析函数
                self._analyze_and_store(pending_policies, 'title', 'policy')
            else:
                print("政策库无打分新闻")

            intel["政策面"] = self.db_client.get_today_analyzed_news('policy')

        # 3. 处理快讯面：去重 -> 分析 -> 入库
        flash_df = intel.get("快讯面", pd.DataFrame())
        if not flash_df.empty:
            print("📰 正在处理实时快讯...")

            # 核心修复 2：快讯通常内容参差不齐，必须清洗
            # 假设快讯对应的列名在 DataFrame 里叫 'title' 或 'content'
            # df[["发布时间", "标题", "内容"]]
            print("📰 正在处理实时快讯...")
            flash_df = flash_df.rename(columns={'标题': 'title', '发布时间': 'pub_date','内容':'content'})
            self.db_client.save_news_batch(flash_df, source_type='flash')

            pending_flash = self.db_client.get_pending_news(days=3, source_type='flash')
            if not pending_flash.empty:
                self._analyze_and_store(pending_flash, 'title', 'flash')

            intel["快讯面"] = self.db_client.get_today_analyzed_news('flash')

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
        """调用 AI 分析并更新数据库中的评分"""
        if df.empty:
            return df
        titles = df[col_name].tolist()

        # 1. 批量调用 AI 代理
        # analysis_results 格式: {标题: {'score': 90, 'sectors': [...]}}
        chunk_size = 15

        for i in range(0, len(titles), chunk_size):
            chunk = titles[i: i + chunk_size]
            print(f" - 正在分析第 {i // chunk_size + 1} 组数据 ({len(chunk)} 条)...")

            try:
                # 批量调用 AI
                analysis_results = self.agent.batch_analyze(chunk)

                # 更新数据库
                for title in chunk:
                    analysis = analysis_results.get(title)
                    if analysis:
                        if analysis['score'] == 0:
                            analysis['score'] = 1
                        self.db_client.update_news_score(
                            title=title,
                            score=analysis['score'],
                            sectors=analysis['sectors']
                        )
                        if analysis['score'] >= 85:
                            self.db_client.record_strategy_hit(title, analysis['sectors'])

                # 稍微停顿一下，防止 API 限制频率
                time.sleep(1)

            except Exception as e:
                print(f"❌ 这一组 AI 分析失败: {e}")
                continue  # 这一组失败了跳过，不影响后面

        return df



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
    print("🚀 开始运行市场雷达测试...")
    start_time = datetime.now()
    data = radar.get_full_intelligence()
    # text = radar.format_to_text(data)
    # print(text)
    # --- 验证逻辑 ---
    print("\n" + "=" * 30)
    print("📊 运行结果验证：")

    for key in ["政策面", "快讯面"]:
        df = data[key]
        print(f"\n[{key}] 模块:")
        print(f" - 总计可用情报数: {len(df)} 条")

        if not df.empty:
            # 验证分数是否都已打上
            zero_scores = df[df['ai_score'] == 0]
            if zero_scores.empty:
                print(f" ✅ 成功：所有数据均已打分。")
            else:
                print(f" ❌ 警告：仍有 {len(zero_scores)} 条数据未打分。")

            # 验证高分分布
            high_score_count = len(df[df['ai_score'] >= 80])
            print(f" - 发现高价值情报 (>=80分): {high_score_count} 条")

            # 打印最新的一条看看
            print(f" - 最新情报样例: {df.iloc[0]['政策标题'][:30]}...")
        else:
            print(" ⚠️ 提示：今日无新增或有效情报。")

    end_time = datetime.now()
    print("\n" + "=" * 30)
    print(f"⏱️ 总耗时: {end_time - start_time}")
    print("✅ 测试结束。数据已同步至数据库。")