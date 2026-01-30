# 2026-1-28 created

# need to be done --- get into LLM to help analysis


import pandas as pd
import os
import pickle



class StrategyScanner:
    def __init__(self, intel_data, linker):
        """
        :param intel_data: MarketRadar 抓取的字典数据
        :param linker: 已经 load_linker() 过的 IndustryLinker 实例
        """
        self.intel = intel_data
        self.linker = linker

    def scan_version1(self):
        """
        扫描策略：寻找‘政策/热搜’与‘热门板块’的共振
        """
        candidates = []

        # 1. 提取政策面和热搜面的关键词
        # 实际开发中，这里可以接入 LLM 提取，现在我们用简单的包含匹配
        policy_titles = " ".join(self.intel['政策面']['政策标题'].tolist())
        hot_search_topics = " ".join(self.intel['人气面']['股票名称'].head(10).tolist())  # 前10人气股名


        # 2. 获取当前最强的前 10 个板块 (来自板块面数据)
        hot_sectors_df = self.intel['板块面'].head(10)

        print(f"🔎 正在扫描共振信号...")

        for _, sector_row in hot_sectors_df.iterrows():
            sector_name = sector_row['板块名称']

            # 判断逻辑：如果热门板块名称出现在政策新闻中，认为该赛道被点火
            # 例如：新闻有“半导体”，板块里刚好有“半导体”
            if sector_name in policy_titles or any(keyword in sector_name for keyword in ["自主可控", "高新"]):
                print(f"🔥 发现赛道共振: {sector_name}")

                # 从 linker 中获取该板块的所有个股
                # 注意：IndustryLinker 里的 industry_map 和 concept_map 已经合并在 stock_to_tags 里了
                # 这里我们直接根据板块名称反向提取
                sector_stocks = self._get_stocks_by_sector_name(sector_name)

                # 3. 与“人气个股”求交集，找出该板块里的领涨龙头
                popularity_stocks = self.intel['人气面']['代码'].tolist()
                print(f"调试：当前人气股示例: {popularity_stocks[:3]}")  # 查看格式


                clean_popularity_stocks = [code[-6:] for code in popularity_stocks]
                leader_stocks = list(set(sector_stocks) & set(clean_popularity_stocks))

                # 在循环内部
                if sector_name in policy_titles:
                    sector_stocks = self._get_stocks_by_sector_name(sector_name)
                    clean_popularity_stocks = [code[-6:] for code in popularity_stocks]
                    print(f"调试：匹配到板块 {sector_name}，包含股票数: {len(sector_stocks)}")
                    # 打印一下交集前的比对
                    leader_stocks = list(set(sector_stocks) & set(clean_popularity_stocks))
                    print(f"调试：交集结果数: {len(leader_stocks)}")

                for code in leader_stocks:
                    name = self.intel['人气面'].loc[self.intel['人气面']['代码'] == code, '名称'].values[0]
                    candidates.append({
                        "代码": code,
                        "名称": name,
                        "赛道": sector_name,
                        "推荐理由": f"政策共振 + 板块走强({sector_row['涨跌幅']}%) + 人气前排"
                    })

        return pd.DataFrame(candidates)

    def scan(self):
        candidates = []
        # 建立简单的语义联想库 (后期可接入 LLM)
        SYNONYMS = {
            "半导体": ["芯片", "光刻机", "集成电路", "自主可控"],
            "低空经济": ["飞行汽车", "无人机", "空域"],
            "人工智能": ["AI", "算力", "大模型", "英伟达"],
            "新质生产力": ["工业母机", "机器人", "高端制造"]
        }

        # 获取数据
        policy_text = " ".join(self.intel['政策面']['政策标题'].tolist())
        popularity_df = self.intel['人气面'].copy()
        popularity_df['short_code'] = popularity_df['代码'].str[-6:]

        hot_sectors = self.intel['板块面'].head(15)  # 稍微扩大范围

        print(f"🔎 正在执行加权扫描...")

        for _, sector_row in hot_sectors.iterrows():
            sector_name = sector_row['板块名称']

            # --- 维度 1: 新闻匹配分 ---
            news_score = 0
            if sector_name in policy_text:
                news_score = 50  # 字面精准匹配
            else:
                # 模糊关联匹配
                related_words = SYNONYMS.get(sector_name, [])
                if any(word in policy_text for word in related_words):
                    news_score = 30

            if news_score > 0:
                # --- 维度 2: 板块强度分 (涨幅) ---
                sector_score = float(sector_row['涨跌幅']) * 2

                # --- 维度 3: 个股共振 ---
                sector_stocks = self._get_stocks_by_sector_name(sector_name)
                # 找出在该板块中的人气股
                matched_popularity = popularity_df[popularity_df['short_code'].isin(sector_stocks)]

                for _, stock in matched_popularity.iterrows():
                    # 人气排名分：排名越靠前（index越小）分数越高
                    pop_rank_score = (20 - stock.name) if stock.name < 20 else 0

                    total_score = news_score + sector_score + pop_rank_score

                    candidates.append({
                        "代码": stock['short_code'],
                        "名称": stock['名称'],
                        "赛道": sector_name,
                        "综合热度": round(total_score, 2),
                        "推荐理由": f"政策关联({news_score}) + 板块强劲 + 人气排名({stock.name + 1})"
                    })

        # 返回按综合热度排序的结果
        df_result = pd.DataFrame(candidates)
        if not df_result.empty:
            df_result = df_result.sort_values(by="综合热度", ascending=False).drop_duplicates(subset=['代码'])
        return df_result



    def _get_stocks_by_sector_name(self, name):
        """辅助函数：根据名字反查代码列表"""
        # 从 linker 的原始 map 中查找
        ind_map = pd.read_pickle(self.linker.industry_file)
        cp_map = pd.read_pickle(self.linker.concept_file)

        if name in ind_map: return ind_map[name]
        if name in cp_map: return cp_map[name]
        return []