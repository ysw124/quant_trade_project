## 数据库构建：轻量级 SQLite 方案
## 存储 AI 新闻的分析结果

import sqlite3
import json
import hashlib
from datetime import datetime

class QuantumDB:
    def __init__(self, db_path="cache/market_intelligence.db"):
        self.conn = sqlite3.connect(db_path, check_same_thread=False)
        self._create_tables()

    def _create_tables(self):
        cursor = self.conn.cursor()
        # 存储 AI 对新闻的分析结果
        cursor.execute('''
                    CREATE TABLE IF NOT EXISTS news_items (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        pub_date TEXT,       -- 新闻发布的具体时间
                        source TEXT,         -- 来源：'policy' 或 'flash'
                        title TEXT UNIQUE,    -- 标题（设为唯一，防止重复插入）
                        content_hash TEXT,    -- 内容指纹，用于深度去重
                        ai_score REAL,       -- AI给出的评分
                        sectors_json TEXT,   -- AI提取的相关板块
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    )
                ''')
        self.conn.commit()

    def _generate_hash(self, text):
        return hashlib.md5(text.encode('utf-8')).hexdigest()

    def save_news_batch(self, news_df, source_type):
        """
        批量保存新闻，自动去重
        news_df 包含: ['title', 'date', 'ai_score', 'sectors']
        """
        cursor = self.conn.cursor()
        count = 0
        for _, row in news_df.iterrows():
            content_hash = self._generate_hash(row['title'])
            try:
                # 使用 INSERT OR IGNORE 配合 UNIQUE 约束实现自动去重
                cursor.execute('''
                    INSERT OR IGNORE INTO news_items 
                    (pub_date, source, title, content_hash, ai_score, sectors_json)
                    VALUES (?, ?, ?, ?, ?, ?)
                ''', (row['date'], source_type, row['title'], content_hash,
                      row.get('ai_score', 0), json.dumps(row.get('sectors', {}))))
                if cursor.rowcount > 0:
                    count += 1
            except Exception as e:
                print(f"存储出错: {e}")
        self.conn.commit()
        return count

    def get_recent_high_value_news(self, days=7, min_score=70):
        """
        获取近期高价值新闻（核心：联合判断的基础）
        """
        cursor = self.conn.cursor()
        # 按时间排序，获取高分政策
        cursor.execute('''
            SELECT title, ai_score, sectors_json, pub_date, source 
            FROM news_items 
            WHERE (source='policy' AND ai_score >= ?) 
               OR (source='flash' AND pub_date >= date('now', ?))
            ORDER BY pub_date DESC
        ''', (min_score, f'-{days} days'))

        return cursor.fetchall()


    def get_active_intelligence(self, policy_days=14, flash_hours=24):
        """
        获取活跃利好池：
        1. 政策类：回溯 14 天，因为政策有滞后性和持续性。
        2. 快讯类：回溯 24 小时，追求时效性。
        """
        cursor = self.conn.cursor()
        query = """
            SELECT title, ai_score, sectors_json, pub_date, source,
                   (strftime('%s', 'now') - strftime('%s', created_at)) / 3600.0 as age_hours
            FROM news_items
            WHERE (source='policy' AND ai_score >= 60 AND created_at > datetime('now', ?))
               OR (source='flash' AND ai_score >= 80 AND created_at > datetime('now', ?))
            ORDER BY ai_score DESC
        """
        cursor.execute(query, (f'-{policy_days} days', f'-{flash_hours} hours'))
        return cursor.fetchall()

    def save_analysis(self, news_list, scores_map):
        """

        存储至数据库中

        """
        cursor = self.conn.cursor()
        cursor.execute(
            "INSERT INTO news_analysis (date, news_json, scores_json) VALUES (?, ?, ?)",
            (datetime.now().strftime('%Y-%m-%d'), json.dumps(news_list), json.dumps(scores_map))
        )
        self.conn.commit()

