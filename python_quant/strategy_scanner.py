import pandas as pd


class StrategyScanner:
    def __init__(self, intel_data):
        self.intel = intel_data

    def get_candidate_stocks(self):
        """
        基于‘共振’逻辑筛选候选股
        """
        candidates = []

        # 1. 获取近期政策关键词 (简单示例：从标题中提取高频词)
        policy_titles = "".join(self.intel['政策面']['政策标题'].tolist())

        # 2. 遍历人气股，看谁属于强势板块且命中政策方向
        pop_stocks = self.intel['人气面']  # 包含代码、名称、相关板块
        hot_sectors = self.intel['板块面']['板块名称'].head(5).tolist()

        for _, row in pop_stocks.iterrows():
            stock_code = row['代码']
            stock_name = row['名称']
            sector = row.get('相关板块', '')  # 假设人气面里有板块信息

            # 筛选逻辑：属于前5热门板块，且在人气榜前10
            if any(s in sector for s in hot_sectors):
                candidates.append({
                    "code": stock_code,
                    "name": stock_name,
                    "reason": f"属于热门板块:{sector}"
                })

        return pd.DataFrame(candidates).drop_duplicates(subset=['code'])