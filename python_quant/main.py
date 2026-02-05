# main.py 预想逻辑
from python_quant.information_layer.market_radar import MarketRadar
#from evaluators.stock_evaluator import StockEvaluator
from utils.email_helper import send_market_report, format_to_html
from logic_layer.analyst_agent import AnalystAgent
from python_quant.logic_layer.strategy_scanner import StrategyScanner
from python_quant.information_layer.industry_linker import IndustryLinker
from utils.db_helper import QuantumDB
from datetime import datetime
import pandas as pd
import json

def run_daily_pipeline():
    try:
        # 1. 数据采集层 (The Harvester)
        db = QuantumDB()
        agent = AnalystAgent()
        radar = MarketRadar()

        print(f"[{datetime.now().strftime('%H:%M:%S')}] 正在生成今日情报...")
        intelligence = radar.get_full_intelligence()

        # 2. 信息映射层 (The Interpreter)
        # 加载本地的行业/种子概念映射表
        linker = IndustryLinker()
        if not linker.load_linker():
            print("警告: 映射表加载失败，StrategyScanner 将无法正常工作。")
            # 这里可以根据需要选择是否退出，或者继续只发新闻内容

        # 3. 策略逻辑层 (The Strategist)
        # 将采集到的原始情报和映射工具传给扫描器
        # 引入AI
        scanner = StrategyScanner(intelligence, linker, agent, db)
        candidates_df = scanner.scan()

        # 将扫描出的候选股加入到 intelligence 字典中，方便后续格式化为 HTML
        intelligence['今日潜力池'] = candidates_df

        print(f"[{datetime.now().strftime('%H:%M:%S')}] 策略扫描完成，发现 {len(candidates_df)} 个共振机会。")

        #4. 数据持久化 (存入本地数据库)
        # 这一步非常重要，是为了以后复盘 AI 准不准
        db.save_analysis(
            news_list = intelligence['政策面']['政策标题'].tolist(),
            scores_map = getattr(scanner, 'last_ai_scores', {}) # 假设你在 scanner 里存了一份备份
        )

        # 5. 检查整体数据
        all_empty = True
        for key, value in intelligence.items():
            if isinstance(value, pd.DataFrame) and not value.empty:
                all_empty = False
                break

        if all_empty:
            print("警告: 采集数据均为空，停止发送。")
            return

        # 6. 格式化输出与推送 (The Messenger)
        # 注意：你需要更新 format_to_html 函数，使其支持渲染 '今日潜力池' 这个 Key
        html_content = format_to_html(intelligence)

        #发送邮件
        send_market_report(html_content)

        print(f"[{datetime.now().strftime('%H:%M:%S')}] 全流程执行完毕，邮件已发出。")

        #print(html_content)

    except Exception as e:
        import traceback
        print(f"程序运行过程中出现致命错误: {e}")
        traceback.print_exc()

def log_ai_decision(date, news, sectors):
    with open("cache/ai_decision_log.jsonl", "a", encoding="utf-8") as f:
        log_entry = {
            "date": date,
            "input_news": news,
            "ai_output_sectors": sectors
        }
        f.write(json.dumps(log_entry, ensure_ascii=False) + "\n")




#  只有新闻基本信息
# def run_daily_pipeline():
#     try:
#         # 1. 初始化雷达并获取全维度数据
#         radar = MarketRadar()
#         print(f"[{datetime.now().strftime('%H:%M:%S')}] 正在生成今日情报...")
#         intelligence = radar.get_full_intelligence()
#
#         # 2. 检查数据是否采集成功 (修正后的逻辑)
#         # 逻辑：遍历字典，如果发现所有 DataFrame 都是空的，则视为采集失败
#         all_empty = True
#         for key, value in intelligence.items():
#             if isinstance(value, pd.DataFrame):
#                 if not value.empty:
#                     all_empty = False
#                     break
#
#         if all_empty:
#             print("警告: 采集到的所有维度数据均为空，停止发送邮件。")
#             return
#
#         # 3. 转化为 HTML 格式 (此处调用之前定义的转换函数)
#         html_content = format_to_html(intelligence)
#
#         # 4. 执行邮件推送
#         send_market_report(html_content)
#
#         print(f"[{datetime.now().strftime('%H:%M:%S')}] 全流程执行完毕。")
#
#     except Exception as e:
#         print(f"程序运行过程中出现致命错误: {e}")

if __name__ == "__main__":
    #run_daily_analysis()
    run_daily_pipeline()