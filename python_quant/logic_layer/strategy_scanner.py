# 2026-1-28 created

# 使用AI进行恩熙market_radar抓取的字典

# 人气面股票 与 政策，新闻的碰撞

import pandas as pd
import os
import pickle
from datetime import datetime
import math
import json


class StrategyScanner:
    def __init__(self, intel_data, linker, agent, db_client):
        """
        :param intel_data: MarketRadar 抓取的字典数据
        :param linker: 已经 load_linker() 过的 IndustryLinker 实例
        """
        self.intel = intel_data
        self.linker = linker
        self.agent = agent
        self.db = db_client
        self.decay_lambda = 0.005  # 时间衰减系数，数值越大衰减越快

    def scan(self):
        print(f"[{datetime.now().strftime('%H:%M:%S')}] 🧠 启动 V2.0 联想记忆扫描模式...")

        # 1. 从数据库提取“活跃利好池”
        raw_intelligence = self.db.get_active_intelligence()
        if not raw_intelligence:
            print("📭 记忆库中暂无活跃利好，本次扫描结束。")
            return pd.DataFrame()

        # 2. 预处理利好（计算时间衰减）
        active_sectors = self._process_active_sectors(raw_intelligence)

        # 3. 获取实时人气榜
        popularity_df = self.intel.get('人气面', pd.DataFrame())
        if popularity_df.empty:
            print("⚠️ 未获取到实时人气榜，无法完成共振。")
            return pd.DataFrame()

        # 4. 执行共振交叉匹配
        return self._cross_resonance(active_sectors, popularity_df)

    def _process_active_sectors(self, raw_data):
        """
        核心逻辑：将多条新闻的利好叠加，并根据时间衰减
        返回格式: { "板块名": {"total_score": 150, "reasons": [...] } }
        """
        sector_pool = {}
        for title, ai_score, sectors_json, pub_date, source, age_hours in raw_data:
            sectors = json.loads(sectors_json)

            # 时间衰减公式: Score * e^(-λ * t)
            # 政策类衰减慢，快讯类衰减快
            lambda_val = 0.002 if source == 'policy' else 0.02
            decay_factor = math.exp(-lambda_val * age_hours)
            adjusted_score = ai_score * decay_factor

            for s_name, _ in sectors.items():
                if s_name not in sector_pool:
                    sector_pool[s_name] = {"total_score": 0, "reasons": [], "max_score": 0}

                sector_pool[s_name]["total_score"] += adjusted_score
                sector_pool[s_name]["max_score"] = max(sector_pool[s_name]["max_score"], adjusted_score)
                sector_pool[s_name]["reasons"].append(f"({source}){title[:10]}")

        return sector_pool

    def _cross_resonance(self, sector_pool, popularity_df):
        """
        将板块池与人气榜进行撞击
        """
        candidates = []
        # 确保列名对齐
        pop_col = '代码' if '代码' in popularity_df.columns else popularity_df.columns[0]
        popularity_df['short_code'] = popularity_df[pop_col].astype(str).str.extract(r'(\d{6})')

        for s_name, info in sector_pool.items():
            # 模糊匹配板块个股
            sector_stocks = self._get_stocks_by_sector_name(s_name)
            if not sector_stocks: continue

            # 取交集
            matched = popularity_df[popularity_df['short_code'].isin(sector_stocks)]

            for _, stock in matched.iterrows():
                # 综合分 = 历史利好叠加分 + (100 - 人气排名)
                # 排名越靠前（index越小），热度越高
                market_hot = (100 - stock.name)  # 假设人气榜给了100名
                final_score = info['total_score'] + market_hot

                candidates.append({
                    "代码": stock['short_code'],
                    "名称": stock['名称'] if '名称' in stock else "未知",
                    "驱动赛道": s_name,
                    "综合强度": round(final_score, 2),
                    "逻辑支撑": " | ".join(list(set(info['reasons']))[:2]),  # 取前两个理由
                    "热度来源": "政策记忆+实时人气" if info['max_score'] > 50 else "短线脉冲"
                })

        result_df = pd.DataFrame(candidates)
        if not result_df.empty:
            # 去重：如果一封股票被多个板块命中，保留分数最高的
            result_df = result_df.sort_values('综合强度', ascending=False).drop_duplicates('代码')
        return result_df

    def _get_stocks_by_sector_name(self, name):
        """
        升级版：支持模糊匹配的辅助函数
        策略：1. 先试精准匹配 2. 失败后尝试关键词包含匹配 3. 失败后尝试简化匹配
        """
        # 提前加载字典（建议在 __init__ 中加载一次，避免频繁读取 pkl）
        ind_map = pd.read_pickle(self.linker.industry_file)
        cp_map = pd.read_pickle(self.linker.concept_file)
        all_sectors = {**ind_map, **cp_map}

        # 1. 第一阶段：精准匹配 (最快)
        if name in all_sectors:
            return all_sectors[name]

        # 2. 第二阶段：模糊包含逻辑
        # 比如 AI 给的是 "人工智能"，字典里是 "人工智能概念"，这时候 in 操作符立功
        matches = []
        for sector_key, stocks in all_sectors.items():
            if name in sector_key or sector_key in name:
                matches.extend(stocks)

        if matches:
            return list(set(matches))  # 去重

        # 3. 第三阶段：关键词降维 (兜底)
        # 比如 "半导体概念" -> 变成 "半导体" 再搜一遍
        clean_name = name.replace("概念", "").replace("板块", "").replace("行业", "")
        if clean_name != name:
            for sector_key, stocks in all_sectors.items():
                if clean_name in sector_key:
                    matches.extend(stocks)

        return list(set(matches))

    def _align_sector_names(self, ai_sector_name):
        """
        将 AI 返回的行业名映射到东财官方行业名
        """
        # 这是一个简单的对齐字典，以后可以根据报错日志不断补充
        ALIGN_MAP = {
            "芯片": "半导体",
            "集成电路": "半导体",
            "飞行汽车": "航天航空",
            "无人机": "航天航空",
            "白酒": "酿酒行业",
            "AI": "计算机设备"
        }

        # 1. 直接匹配
        if ai_sector_name in self.linker.industry_map:
            return ai_sector_name

        # 2. 别名匹配
        return ALIGN_MAP.get(ai_sector_name, None)


    # def scan_version1(self):
    #     """
    #     扫描策略：寻找‘政策/热搜’与‘热门板块’的共振
    #     """
    #     candidates = []
    #
    #     # 1. 提取政策面和热搜面的关键词
    #     # 实际开发中，这里可以接入 LLM 提取，现在我们用简单的包含匹配
    #     policy_titles = " ".join(self.intel['政策面']['政策标题'].tolist())
    #     hot_search_topics = " ".join(self.intel['人气面']['股票名称'].head(10).tolist())  # 前10人气股名
    #
    #
    #     # 2. 获取当前最强的前 10 个板块 (来自板块面数据)
    #     hot_sectors_df = self.intel['板块面'].head(10)
    #
    #     print(f"🔎 正在扫描共振信号...")
    #
    #     for _, sector_row in hot_sectors_df.iterrows():
    #         sector_name = sector_row['板块名称']
    #
    #         # 判断逻辑：如果热门板块名称出现在政策新闻中，认为该赛道被点火
    #         # 例如：新闻有“半导体”，板块里刚好有“半导体”
    #         if sector_name in policy_titles or any(keyword in sector_name for keyword in ["自主可控", "高新"]):
    #             print(f"🔥 发现赛道共振: {sector_name}")
    #
    #             # 从 linker 中获取该板块的所有个股
    #             # 注意：IndustryLinker 里的 industry_map 和 concept_map 已经合并在 stock_to_tags 里了
    #             # 这里我们直接根据板块名称反向提取
    #             sector_stocks = self._get_stocks_by_sector_name(sector_name)
    #
    #             # 3. 与“人气个股”求交集，找出该板块里的领涨龙头
    #             popularity_stocks = self.intel['人气面']['代码'].tolist()
    #             print(f"调试：当前人气股示例: {popularity_stocks[:3]}")  # 查看格式
    #
    #
    #             clean_popularity_stocks = [code[-6:] for code in popularity_stocks]
    #             leader_stocks = list(set(sector_stocks) & set(clean_popularity_stocks))
    #
    #             # 在循环内部
    #             if sector_name in policy_titles:
    #                 sector_stocks = self._get_stocks_by_sector_name(sector_name)
    #                 clean_popularity_stocks = [code[-6:] for code in popularity_stocks]
    #                 print(f"调试：匹配到板块 {sector_name}，包含股票数: {len(sector_stocks)}")
    #                 # 打印一下交集前的比对
    #                 leader_stocks = list(set(sector_stocks) & set(clean_popularity_stocks))
    #                 print(f"调试：交集结果数: {len(leader_stocks)}")
    #
    #             for code in leader_stocks:
    #                 name = self.intel['人气面'].loc[self.intel['人气面']['代码'] == code, '名称'].values[0]
    #                 candidates.append({
    #                     "代码": code,
    #                     "名称": name,
    #                     "赛道": sector_name,
    #                     "推荐理由": f"政策共振 + 板块走强({sector_row['涨跌幅']}%) + 人气前排"
    #                 })
    #
    #     return pd.DataFrame(candidates)

    # def scan(self):
    #     print("🧠 正在启动多维联合共振扫描...")
    #
    #     # --- 第一步：从数据库拉取“联合利好池” ---
    #     # 我们不再依赖 self.intel['政策面']，因为那是“新增”，我们要看的是“有效期内”的所有利好
    #     # 获取过去 7 天，评分大于 70 的所有新闻（包含政策和快讯）
    #     high_value_news = self.db.get_recent_high_value_news(days=7, min_score=70)
    #
    #     if not high_value_news:
    #         print("⚠️ 数据库中近期无高分利好，跳过语义分析。")
    #         return pd.DataFrame()
    #
    #     # 将数据库记录转为可处理的格式
    #     # 数据库字段顺序：0:title, 1:ai_score, 2:sectors_json, 3:pub_date, 4:source
    #     combined_news = []
    #     for item in high_value_news:
    #         try:
    #             combined_news.append({
    #                 "title": item[0],
    #                 "score": item[1],
    #                 "sectors": json.loads(item[2]),
    #                 "date": item[3],
    #                 "type": item[4]
    #             })
    #         except:
    #             continue
    #
    #     # --- 第二步：准备人气榜数据 ---
    #     candidates = []
    #     if '人气面' not in self.intel or self.intel['人气面'].empty:
    #         print("❌ 缺失人气榜数据，无法进行共振。")
    #         return pd.DataFrame()
    #
    #     popularity_df = self.intel['人气面'].copy()
    #     # 统一代码格式，确保能和映射表匹配
    #     popularity_df['short_code'] = popularity_df['代码'].astype(str).str.extract(r'(\d{6})')
    #
    #     # --- 第三步：共振匹配 (Cross-Resonance) ---
    #     print(f"🔎 正在对 {len(combined_news)} 条有效利好进行板块穿透...")
    #
    #     for news in combined_news:
    #         news_title = news['title']
    #         sectors_data = news['sectors']  # 这是一个 {板块名: 分数} 的字典
    #
    #         for s_name, sector_score in sectors_data.items():
    #             # 使用我们之前写的模糊匹配函数获取个股
    #             sector_stocks = self._get_stocks_by_sector_name(s_name)
    #
    #             # 寻找人气股交集
    #             matched = popularity_df[popularity_df['short_code'].isin(sector_stocks)]
    #
    #             for _, stock in matched.iterrows():
    #                 # 综合热度算法：AI分 + 人气加权
    #                 # 可以加入时间衰减：如果是几天前的政策，权重稍微降低
    #                 total_score = sector_score + (20 - stock.name)
    #
    #                 candidates.append({
    #                     "代码": stock['short_code'],
    #                     "名称": stock.get('名称') or stock.get('股票名称'),
    #                     "赛道": s_name,
    #                     "综合热度": total_score,
    #                     "推荐理由": f"[{news['type']}] {news_title[:15]}...",
    #                     "发现时间": news['date']
    #                 })
    #
    #     # --- 第四步：去重与排序 ---
    #     df_result = pd.DataFrame(candidates)
    #     if not df_result.empty:
    #         # 如果同一只股票命中多个板块，取最高分那个
    #         df_result = df_result.sort_values(by="综合热度", ascending=False)
    #         df_result = df_result.drop_duplicates(subset=['代码'], keep='first')
    #
    #     return df_result