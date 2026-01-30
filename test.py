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