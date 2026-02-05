def scan(self, agent):
    # 1. 让 AI 提取利好行业 (不再依赖死板的 SYNONYMS)
    policy_titles = self.intel['政策面']['政策标题'].tolist()
    # 这一步是点睛之笔：AI 把新闻翻译成行业
    ai_sector_scores = agent.analyze_news(policy_titles)

    candidates = []
    popularity_df = self.intel['人气面'].copy()
    popularity_df['short_code'] = popularity_df['代码'].str[-6:]

    # 2. 遍历 AI 认为有利好的行业
    for sector_name, news_score in ai_sector_scores.items():
        # 3. 映射：从映射表找个股
        sector_stocks = self._get_stocks_by_sector_name(sector_name)

        # 4. 共振：人气股交集
        matched = popularity_df[popularity_df['short_code'].isin(sector_stocks)]

        for _, stock in matched.iterrows():
            # 计算综合热度
            total_score = news_score + (20 - stock.name)  # AI分 + 人气排名分

            candidates.append({
                "代码": stock['short_code'],
                "名称": stock['名称'],
                "赛道": sector_name,
                "综合热度": total_score,
                "推荐理由": f"AI评分({news_score}) + 人气共振"
            })
    df_result = pd.DataFrame(candidates)
    if not df_result.empty:
        df_result = df_result.sort_values(by="综合热度", ascending=False)
    else:
        print("AI model do not give a recommandation")
    return df_result






下载板块分类信息
from xtquant import xtdata
xtdata.download_sector_data()
获取板块分类信息数据
from xtquant import xtdata
xtdata.get_sector_list()
返回值
list：所有板块的列表信息(包含过期板块)，可以配合板块成分股查询接口使用
获取板块成分股数据
from xtquant import xtdata
xtdata.get_stock_list_in_sector(sector_name)
参数
参数名称	数据类型	描述
sector_name	string	板块名，如'沪深300'，'中证500'、'上证50'、'我的自选'等
返回值
list：内含成份股代码，代码形式为 'stockcode.market'，如 '000002.SZ'
获取迅投行业成分股数据
from xtquant import xtdata
xtdata.get_stock_list_in_sector(sector_name)
参数
参数名称	数据类型	描述
sector_name	string	板块名，如'SW1汽车'等
返回值
list：内含成份股代码，代码形式为 'stockcode.market'，如 '000002.SZ'\





('代码', 'SZ002131') ('股票名称', '利欧股份') ('最新价', 10.18) ('涨跌幅', 2.72) ('short_code', '002131')