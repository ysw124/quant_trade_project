# main.py 预想逻辑
from market_radar import MarketRadar
#from evaluators.stock_evaluator import StockEvaluator
from utils.email_helper import send_market_report, format_to_html
import sys
from datetime import datetime
import pandas as pd
# def run_daily_analysis():
#     # 1. 扫描大盘
#     radar = MarketRadar()
#     context = radar.get_full_intelligence()
#
#     # 2. 从人气面中提取前 3 名股票代码
#     top_stocks = context["人气面"]["代码"].head(3).tolist()
#
#     # 3. 对这几只股进行深度体检
#     for code in top_stocks:
#         evaluator = StockEvaluator(code)
#         report = evaluator.deep_dive()
#         print(f"股票 {code} 体检报告已生成")


def run_daily_pipeline():
    try:
        # 1. 初始化雷达并获取全维度数据
        radar = MarketRadar()
        print(f"[{datetime.now().strftime('%H:%M:%S')}] 正在生成今日情报...")
        intelligence = radar.get_full_intelligence()

        # 2. 检查数据是否采集成功 (修正后的逻辑)
        # 逻辑：遍历字典，如果发现所有 DataFrame 都是空的，则视为采集失败
        all_empty = True
        for key, value in intelligence.items():
            if isinstance(value, pd.DataFrame):
                if not value.empty:
                    all_empty = False
                    break

        if all_empty:
            print("警告: 采集到的所有维度数据均为空，停止发送邮件。")
            return

        # 3. 转化为 HTML 格式 (此处调用之前定义的转换函数)
        html_content = format_to_html(intelligence)

        # 4. 执行邮件推送
        send_market_report(html_content)

        print(f"[{datetime.now().strftime('%H:%M:%S')}] 全流程执行完毕。")

    except Exception as e:
        print(f"程序运行过程中出现致命错误: {e}")

if __name__ == "__main__":
    #run_daily_analysis()
    run_daily_pipeline()