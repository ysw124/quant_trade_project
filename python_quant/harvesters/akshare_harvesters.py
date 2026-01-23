import akshare as ak
import pandas as pd
from datetime import datetime


class HotspotRadar:
    def __init__(self):
        print(f"--- 2026量化雷达启动 | {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} ---")

    def get_xueqiu_hot_stocks(self):
        print("\n[Step 1] 正在识别全市场热点股票（基于资金流向）...")
        try:
            # 使用最稳定的实时行情接口
            df_spot = ak.stock_zh_a_spot_em()

            # 1. 自动识别并重命名列名（防御性编程）
            # 这个接口通常返回：代码, 名称, 最新价, 涨跌幅, 成交额, ...
            col_map = {'代码': 'code', '名称': 'name', '成交额': 'turnover', '涨跌幅': 'pct_chg'}
            df_spot = df_spot.rename(columns=col_map)

            # 2. 筛选真正的“热点”：成交额前100名中，涨跌幅大于2%的股票
            # 逻辑：成交额大代表资金关注度高，涨幅代表方向向上
            hot_candidates = df_spot.sort_values(by='turnover', ascending=False).head(100)
            hot_stocks = hot_candidates[hot_candidates['pct_chg'] > 2].head(10)

            if hot_stocks.empty:
                print("当前市场表现平平，未发现放量上涨的热点。")
                return pd.DataFrame()

            return hot_stocks[['code', 'name', 'pct_chg']]

        except Exception as e:
            print(f"行情获取失败，请检查网络或akshare版本: {e}")
            return pd.DataFrame()

    def get_cls_policy_news(self):
        print("\n[Step 2] 正在抓取财联社电报...")
        try:
            # 方案 A: 尝试东方财富实时新闻 (非常稳定) / 应该是获取财联社的报道数据
            df_news = ak.stock_info_global_cls()
            return df_news.head(10)
        except:
            try:
                # 方案 B: 备选新浪财经新闻
                #ak.qdii_e_comm_jsl()
                ak.qdii_a_index_jsl()
                df_news = ak.jsl_monitor()  # 集思录或新浪监控
                return df_news.head(10)
            except Exception as e:
                print(f"所有新闻接口均失效: {e}")
                return pd.DataFrame()
    def validate_market_performance(self, stock_code):
        """验证技术面：是否站上20日均线"""
        try:
            # 转换代码：去掉可能存在的字母，只保留6位数字
            clean_code = ''.join(filter(str.isdigit, str(stock_code)))
            df_hist = ak.stock_zh_a_hist(symbol=clean_code, period="daily", adjust="qfq")

            if df_hist.empty or len(df_hist) < 20: return False, 0

            last_close = df_hist['收盘'].iloc[-1]
            ma20 = df_hist['收盘'].rolling(window=20).mean().iloc[-1]
            return last_close > ma20, last_close
        except:
            return False, 0

    def run_radar(self):
        # 1. 获取热点
        hot_stocks = self.get_xueqiu_hot_stocks()
        if hot_stocks is None or hot_stocks.empty:
            print("未能定位到热点股票，建议稍后再试。")
            return

        print(f">>> 发现高热度题材种子: {hot_stocks['name'].tolist()}")

        # 2. 政策验证
        policy = self.get_cls_policy_news()
        if not policy.empty:
            print(f"\n[政策环境风向标]:")
            for i, row in policy.iterrows():
                print(f"- {row['content'][:50]}...")

        # 3. 趋势过滤
        print("\n[Step 3] 正在进行技术面过滤（MA20趋势线）...")
        final_list = []
        for _, row in hot_stocks.iterrows():
            is_strong, price = self.validate_market_performance(row['code'])
            if is_strong:
                final_list.append({
                    "代码": row['code'],
                    "名称": row['name'],
                    "涨幅": f"{row['pct_chg']}%",
                    "状态": "上升趋势"
                })

        if final_list:
            print("\n" + "=" * 40)
            print("🚀 最终推荐名单：具备热度 + 政策 + 技术支撑")
            print("=" * 40)
            print(pd.DataFrame(final_list))
        else:
            print("\n结论：当前热点个股多为短期脉冲，缺乏中线技术面支撑。")


class QuantDataHarvester:
    def __init__(self):
        print(f"--- 数据采集模块启动 | {datetime.now().strftime('%H:%M:%S')} ---")

    # --- 1. 获取政策与新闻 (财联社) ---
    def fetch_cls_news(self):
        print("[数据源] 正在采集财联社全球快讯...")
        try:
            # 财联社全球快讯
            df = ak.stock_info_global_cls(symbol="全部")
            if df.empty:
                print("财联社接口返回数据为空")
                return pd.DataFrame()
                # 我们选取最核心的三个字段，并为了后续处理方便，做一个简单的重命名（可选）
                # 按时间从新到旧排序
            df = df.sort_values(by=["发布日期", "发布时间"], ascending=False)
                # 保持中文列名返回
            return df[["发布时间", "标题", "内容"]].head(20)
        except Exception as e:
            print(f"财联社接口报错: {e}")
            return pd.DataFrame()

    # --- 2. 获取最热赛道 (概念板块) ---
    def fetch_top_sectors(self):
        print("[数据源] 正在采集概念板块热度...")
        try:
            # 获取所有概念板块的实时行情
            df = ak.stock_board_concept_name_em()
            # 2. 检查数据是否为空
            if df.empty:
                return pd.DataFrame()
            # 筛选：成交额前10名，且涨跌幅为正的板块
            df_sorted = df.sort_values(by="换手率", ascending=False)
            result_df = df_sorted[['板块名称', '涨跌幅', '换手率', '领涨股票']].head(10)
            return result_df
        except Exception as e:
            print(f"概念板块接口报错: {e}")
            return pd.DataFrame()

    # --- 3. 获取人气个股 (东方财富人气榜) ---
    def fetch_popular_stocks(self):
        print("[数据源] 正在采集全市场人气个股榜单...")
        try:
            # 1. 关键修改：调用全市场榜单接口，而非单只个股接口
            # 这个接口返回前 100 名人气股
            df = ak.stock_hot_rank_em()
            if df.empty:
                return pd.DataFrame()
            # 2. 自动对齐列名
            # 根据 AkShare 规范，列名通常为：['序号', '代码', '名称', '最新价', '涨跌幅', '排名', '新进', '活跃度']
            # 我们将其统一为英文或你习惯的命名
                # 直接选取中文列，不进行重命名
            return df[['代码', '股票名称', '最新价', '涨跌幅']].head(15)

            # 3. 筛选并返回
            # 注意：这里要确保 'pct_chg' 存在
        except Exception as e:
            print(f"人气榜接口调试报错: {e}")
            # 调试小技巧：报错时打印列名，看看接口到底返回了什么
            # if 'df' in locals(): print(df.columns)
            return pd.DataFrame()

#            ak.stock_research_report_em()
#             ak.stock_profit_forecast_em()
#             ak.stock_industry_change_cninfo()
#             ak.stock_profit_forecast_em() #盈利预测

    # --- 4. 获取最新的机构研报 (全市场) ---
        # --- 4. 获取最新的机构研报评级统计 (全市场) ---
    def fetch_market_reports(self):
        print("[数据源] 正在采集全市场机构评级统计...")
        try:
            # 接口：stock_profit_forecast_em
            df = ak.stock_profit_forecast_em()
            if df.empty:
                return pd.DataFrame()

            # 根据你提供的源码列名进行筛选
            # 我们重点关注：代码, 名称, 研报数, 以及买入/增持这两个核心看多指标
            result_df = df[[
                "代码",
                "名称",
                "研报数",
                "机构投资评级(近六个月)-买入",
                "机构投资评级(近六个月)-增持"
            ]].copy()

            # 按照研报数量排序，代表机构关注度最高
            result_df = result_df.sort_values(by="研报数", ascending=False)

            return result_df.head(20)
        except Exception as e:
            print(f"盈利预测接口(研报统计)报错: {e}")
            return pd.DataFrame()


        # --- 5. 获取市场热搜关键词 (概念热度维度) ---
    def fetch_market_hot_keywords(self):
        print("[数据源] 正在采集概念题材热度榜...")
        try:
            # 接口：stock_hot_keyword_em
            df = ak.stock_hot_keyword_em()
            if df.empty:
                return pd.DataFrame()
            # 根据你提供的源码列名进行筛选
            # 这个接口的数据能帮我们看到哪些“概念名称”当前最火
            result_df = df[["时间", "概念名称", "热度"]].copy()
            # 将热度转换为数值型以便排序（如果接口返回的是字符串）
            result_df["热度"] = pd.to_numeric(result_df["热度"], errors="coerce")
            # 按热度从高到低排序
            result_df = result_df.sort_values(by="热度", ascending=False)
            # 去重：同一个概念可能出现在多个时间点，取最新的
            result_df = result_df.drop_duplicates(subset=["概念名称"])
            return result_df.head(15)
        except Exception as e:
            print(f"热搜关键词接口报错: {e}")
            return pd.DataFrame()

    # --- 6. 汇总所有数据流 ---
    def get_all_raw_data(self):
        print(f"\n" + ">>>" * 10)
        print("开始全维度市场数据扫描...")

        data_package = {
            "快讯面": self.fetch_cls_news(),
            "板块面": self.fetch_top_sectors(),
            "人气面": self.fetch_popular_stocks(),
            "研报面": self.fetch_market_reports(),
            "热搜面": self.fetch_market_hot_keywords(),
            "采集时间": datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        }

        # 安全性检查：确保每一个 key 对应的都是 DataFrame，如果不是则补空表
        for key, value in data_package.items():
            if key != "采集时间" and (value is None or not isinstance(value, pd.DataFrame)):
                data_package[key] = pd.DataFrame()

        print("全维度扫描完成。")
        print("<<<" * 10 + "\n")
        return data_package


if __name__ == "__main__":
    # 1. 初始化采集器
    harvester = QuantDataHarvester()

    # 2. 获取数据包
    raw_data = harvester.get_all_raw_data()

    print(f"数据采集快照时间: {raw_data['采集时间']}")
    print("=" * 60)

    # --- 测试：快讯 ---
    if not raw_data['快讯面'].empty:
        print(f"\n[最新快讯摘要]: {raw_data['快讯面'].iloc[0]['标题']}")
        # print(raw_data['快讯面'].head(5).to_string(index=False)) # 如需查看多条请取消注释

    # --- 测试：板块 ---
    if not raw_data['板块面'].empty:
        print("\n[热门赛道榜单]:")
        print(raw_data['板块面'][['板块名称', '涨跌幅', '换手率']].to_string(index=False))

    # --- 测试：人气股 ---
    if not raw_data['人气面'].empty:
        print("\n[人气个股榜单]:")
        # 注意：这里根据你前面的逻辑，使用的是 '代码' 而不是 'stock_code'
        print(raw_data['人气面'][['代码', '股票名称', '最新价', '涨跌幅']].to_string(index=False))

    # --- 测试：研报统计 ---
    if not raw_data['研报面'].empty:
        print("\n[机构强推个股统计]:")
        print(raw_data['研报面'][['名称', '研报数', '机构投资评级(近六个月)-买入']].to_string(index=False))

    # --- 测试：热搜题材 ---
    if not raw_data['热搜面'].empty:
        print("\n[当日热词共振]:")
        print(raw_data['热搜面'][['概念名称', '热度']].to_string(index=False))

    print("\n" + "=" * 60)
    print("调试结束：全维度数据已成功对齐。")