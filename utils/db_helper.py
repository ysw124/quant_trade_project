## 数据库构建：轻量级 SQLite 方案
## 存储 AI 新闻的分析结果

import sqlite3
import json
import hashlib
from datetime import datetime
import pandas as pd

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
                    pub_date TEXT,       
                    source TEXT,         
                    title TEXT UNIQUE,   
                    link_url TEXT,       -- 新增：详情链接
                    content TEXT,        -- 新增：新闻正文或快讯内容
                    content_hash TEXT,   
                    ai_score REAL DEFAULT 0,  -- 建议给个默认值 0
                    sectors_json TEXT,   
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            ''')
        self.conn.commit()
        # 2. 策略执行记录表（果 - 为回测预留）
        cursor.execute('''
                    CREATE TABLE IF NOT EXISTS strategy_results (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        execution_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        related_news_id INTEGER,  -- 关联原始新闻
                        target_sectors TEXT,      -- 当时选中的板块
                        action_type TEXT,         -- 比如 'BUY' 或 'WATCH'
                        FOREIGN KEY (related_news_id) REFERENCES news_items(id)
                    )
                ''')


        self.conn.commit()

    def _generate_hash(self, text):
        # 强制转为字符串，防止 None 或数字导致崩溃
        safe_text = str(text or "").encode('utf-8')
        return hashlib.md5(safe_text).hexdigest()

    def save_news_batch(self, news_df, source_type):
        """
        批量保存新闻，自动去重
        news_df 包含: ['title', 'date', 'ai_score', 'sectors']
        """
        cursor = self.conn.cursor()
        count = 0
        for _, row in news_df.iterrows():
            # 这里的 get() 保证了无论叫 'title' 还是 '政策标题' 都能抓到
            title = row.get('title') or row.get('政策标题')
            date = row.get('date') or row.get('发布日期')

            # 政策面通常有链接；快讯面通常直接有‘内容’
            url = row.get('link_url') or row.get('详情链接') or ""
            content = row.get('content') or row.get('内容') or ""

            content_hash = self._generate_hash(title)
            try:
                cursor.execute('''
                            INSERT OR IGNORE INTO news_items 
                            (pub_date, source, title, link_url, content, content_hash, ai_score, sectors_json)
                            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                        ''', (
                    date,
                    source_type,
                    title,
                    url,
                    content,
                    content_hash,
                    row.get('ai_score', 0),
                    json.dumps(row.get('sectors', {}), ensure_ascii=False)
                ))
                if cursor.rowcount > 0:
                    count += 1
            except Exception as e:
                print(f"存储出错 [{title[:10]}...]: {e}")
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

    # --- 增量分析逻辑：只取没打分的 ---
    def get_pending_news(self, days, source_type):
        """
        days: 政策传 365, 快讯传 3
        source_type: 'policy' 或 'flash'
        """
        cursor = self.conn.cursor()
        # 这里的逻辑是：分数为 0 意味着是‘新来的’，需要 AI 处理
        query = """
            SELECT id, title, pub_date 
            FROM news_items 
            WHERE source = ? 
              AND ai_score = 0 
              AND pub_date >= date('now', ?)
        """
        cursor.execute(query, (source_type, f'-{days} days'))
        columns = [column[0] for column in cursor.description]
        return pd.DataFrame(cursor.fetchall(), columns=columns)

    def update_news_score(self, title, score, sectors):
        """
        将 AI 的分析结果存在在数据库里
        """
        cursor = self.conn.cursor()
        cursor.execute("""
            UPDATE news_items 
            SET ai_score = ?, sectors_json = ? 
            WHERE title = ?
        """, (score, json.dumps(sectors, ensure_ascii=False), title))
        self.conn.commit()

    def get_today_analyzed_news(self, source_type):
        """根据类型，拉取不同时间跨度的已评分新闻"""
        # 核心逻辑：政策看一年(365天)，快讯看三天(3天)
        days = 365 if source_type == 'policy' else 3

        cursor = self.conn.cursor()
        query = f"""
            SELECT pub_date, title, ai_score, sectors_json as sectors, source
            FROM news_items 
            WHERE source = ? AND ai_score > 0
              AND pub_date >= date('now', '-{days} days')
            ORDER BY pub_date DESC
        """
        cursor.execute(query, (source_type,))
        df = pd.DataFrame(cursor.fetchall(), columns=['日期', '政策标题', 'ai_score', 'sectors', '来源'])

        # 将 sectors 从字符串还原为 Python 对象
        if not df.empty:
            df['sectors'] = df['sectors'].apply(lambda x: json.loads(x) if x else {})
        return df

    def record_strategy_hit(self, title, sectors):
        """回测接口：记录高分出现的时刻和对应板块"""
        cursor = self.conn.cursor()
        # 找到对应新闻的ID
        cursor.execute("SELECT id FROM news_items WHERE title = ?", (title,))
        result = cursor.fetchone()
        if result:
            news_id = result[0]
            cursor.execute("""
                INSERT INTO strategy_results (related_news_id, target_sectors, action_type)
                VALUES (?, ?, ?)
            """, (news_id, json.dumps(sectors, ensure_ascii=False), 'SIGNAL'))
            self.conn.commit()