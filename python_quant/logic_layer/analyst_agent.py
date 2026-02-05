#api-key =  'sk-2ac8d0019c05436285f44a441fa0a466'

import os
from openai import OpenAI
import re
import json

class AnalystAgent:
    def __init__(self, api_key='sk-2ac8d0019c05436285f44a441fa0a466'):
        # 建议从环境变量读取，或者从 main.py 传入
        self.api_key = api_key
        self.client = OpenAI(
            api_key=self.api_key,
            base_url="https://api.deepseek.com"
        )

    def analyze_news_to_sectors(self, news_titles):
        """
        将新闻列表转化为行业利好分
        """
        if not news_titles:
            return {}

        prompt = f"""
        任务：你是资深A股策略分析师。请分析以下新闻标题，提取受利好影响最直接的'东财行业分类'名称，并给出利好评分（0-100）。

        要求：
        1. 仅输出格式：行业名称:分数
        2. 每行一个，不要任何开场白或解释。
        3. 必须匹配常见的行业词汇（如：半导体、电力设备、汽车整车、酿酒行业等）。

        新闻列表：
        {news_titles}
        """

        try:
            response = self.client.chat.completions.create(
                model="deepseek-chat",
                messages=[
                    {"role": "system", "content": "你是一个只输出结构化数据的专业金融量化助手。"},
                    {"role": "user", "content": str(prompt)},
                ],
                stream=False
            )

            raw_content = response.choices[0].message.content
            return self._parse_response(raw_content)
        except Exception as e:
            print(f"❌ DeepSeek 调用失败: {e}")
            return {}

    def batch_analyze(self, titles):
        """
        批量分析新闻标题，将其转化为结构化数据
        """
        if not titles:
            return {}

        # 1. 构建 Prompt (核心：明确 JSON 结构)
        prompt = f"""
        你是一个资深的量化策略分析师。请分析以下新闻标题对股市板块的利好程度。

        【待分析标题列表】:
        {json.dumps(titles, ensure_ascii=False)}

        【输出要求】:
        1. 必须返回标准的 JSON 格式，不要包含任何解释性文字。
        2. 结构如下：
           {{
             "新闻标题": {{
               "score": 0-100之间的整数 (利好强度),
               "sectors": {{"板块名1": 分数, "板块名2": 分数}} (相关性最高的1-3个板块)
             }}
           }}
        3. 如果新闻与股市无关，score 给 0，sectors 为空字典。
        """

        try:
            # 调用 API (以 OpenAI/DeepSeek 标准 SDK 为例)
            response = self.client.chat.completions.create(
                model="deepseek-chat",  # 或其它可用模型
                messages=[
                    {"role": "system", "content": "你是一个只输出 JSON 的数据接口。"},
                    {"role": "user", "content": prompt}
                ],
                response_format={"type": "json_object"},  # 强制返回 JSON 模式
                temperature=0.1  # 降低随机性，保证稳定性
            )

            # 2. 提取并清理结果
            content = response.choices[0].message.content

            # 简单的正则清洗，防止模型返回 ```json ... ``` 标签
            if "```" in content:
                content = re.search(r'\{.*\}', content, re.DOTALL).group()

            result_dict = json.loads(content)
            return result_dict

        except Exception as e:
            print(f"❌ AI 批量分析失败: {e}")
            # 如果失败，返回一个空结果，确保程序不崩溃
            return {t: {"score": 0, "sectors": {}} for t in titles}

    def _parse_response(self, text):
        """解析 AI 返回的 '行业:分数' 格式"""
        score_map = {}
        lines = text.strip().split('\n')
        for line in lines:
            if ':' in line:
                try:
                    sector, score = line.split(':')
                    score_map[sector.strip()] = int(score.strip())
                except:
                    continue
        return score_map
