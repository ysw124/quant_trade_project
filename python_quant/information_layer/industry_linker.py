import akshare as ak
import pandas as pd
import time
from tqdm import tqdm
# 这是因为访问akshare 不要开代理
import os
import requests
import random

#访问前可能需要先访问下东方财富网站？，有时好使，有时不行

# 在类外部或初始化时定义一个强力清空代理的函数
def disable_proxy():
    """彻底禁用代理环境变量"""
    proxy_vars = ["http_proxy", "https_proxy", "all_proxy", "HTTP_PROXY", "HTTPS_PROXY", "ALL_PROXY"]
    for var in proxy_vars:
        if var in os.environ:
            del os.environ[var]

    # 这一步最关键：覆盖 requests 默认的代理读取逻辑
    requests.get.__kwdefaults__ = {'proxies': {'http': None, 'https': None}}


class IndustryLinker:
    def __init__(self, cache_dir="cache"):
        # 1. 获取当前文件 (industry_linker.py) 的绝对路径
        current_file_path = os.path.abspath(__file__)
        # 2. 获取该文件所在的目录 (information_layer)
        current_dir = os.path.dirname(current_file_path)
        # 3. 构造 cache 的完整路径 (指向 H:\quant_trade_project\python_quant\information_layer\cache)
        self.cache_dir = os.path.join(current_dir, cache_dir)

        if not os.path.exists(self.cache_dir):
            os.makedirs(self.cache_dir)

        self.industry_file = os.path.join(self.cache_dir, "industry_map.pkl")
        self.concept_file = os.path.join(self.cache_dir, "concept_map.pkl")
        self.stock_to_tags = {}
        self.days_to_update = 7

    def _safe_request(self, func, **kwargs):
        """带有随机延迟和错误处理的请求包装"""
        # 彻底清理环境变量中的代理
        for key in ['http_proxy', 'https_proxy', 'all_proxy', 'HTTP_PROXY', 'HTTPS_PROXY']:
            os.environ.pop(key, None)

        retries = 3
        while retries > 0:
            try:
                # 模拟一点随机延迟，防止被封
                time.sleep(random.uniform(0.8, 1.5))
                return func(**kwargs)
            except Exception as e:
                retries -= 1
                time.sleep(3)
        return pd.DataFrame()

    def quick_sync(self, concept_limit=50):
        """
        快速同步逻辑：只抓核心板块
        """
        print("🚀 开始快速同步核心映射关系...")

        # 1. 行业板块（数据量小，优先级高）
        try:
            ind_list = ak.stock_board_industry_name_em()
            ind_names = ind_list['板块名称'].tolist()
        except:
            print("❌ 无法获取行业列表，请检查网络")
            return

        ind_map = {}
        # 先读现有的，避免全量覆盖
        if os.path.exists(self.industry_file):
            ind_map = pd.read_pickle(self.industry_file)

        print("📥 同步核心行业...")
        for name in tqdm(ind_names[:], desc="行业进度"):
            if name in ind_map: continue  # 跳过已有的
            df = self._safe_request(ak.stock_board_industry_cons_em, symbol=name)
            if not df.empty:
                ind_map[name] = df['代码'].tolist()
                # 每 5 个板块保存一次
                pd.to_pickle(ind_map, self.industry_file)

        # 2. 概念板块（只取前 N 个最热门的）
        try:
            cp_list = ak.stock_board_concept_name_em()
            cp_names = cp_list['板块名称'].head(concept_limit).tolist()
        except:
            print("❌ 无法获取概念列表")
            cp_names = []

        cp_map = {}
        if os.path.exists(self.concept_file):
            cp_map = pd.read_pickle(self.concept_file)

        print(f"📥 同步前 {concept_limit} 个热门概念...")
        for name in tqdm(cp_names, desc="概念进度"):
            if name in cp_map: continue
            df = self._safe_request(ak.stock_board_concept_cons_em, symbol=name)
            if not df.empty:
                cp_map[name] = df['代码'].tolist()
                pd.to_pickle(cp_map, self.concept_file)

        print(f"✅ 快速同步完成！行业:{len(ind_map)}, 概念:{len(cp_map)}")

    def load_linker(self,force_update = False):
        """加载缓存到内存，构建双向索引"""
        """
                核心逻辑：
                1. 如果文件不存在 -> 更新
                2. 如果 force_update 为 True -> 更新
                3. 如果文件超过有效期 -> 更新
                4. 否则 -> 直接加载
        """
        need_update = False

        if not os.path.exists(self.industry_file) or force_update:
            need_update = True
        else:
            # 检查文件修改时间
            file_time = os.path.getmtime(self.industry_file)
            seconds_elapsed = time.time() - file_time
            if seconds_elapsed > (self.days_to_update * 86400):  # 86400秒 = 1天
                print(f"⏰ 缓存已超过 {self.days_to_update} 天，准备自动更新...")
                need_update = True

        if need_update:
            self.quick_sync()

        try:
            ind_map = pd.read_pickle(self.industry_file)
            cp_map = pd.read_pickle(self.concept_file)

            # 构建 stock_to_tags 索引
            # 这种结构让 StrategyScanner 查询个股属性时速度达到 O(1)
            all_mapping = {}

            # 处理行业
            for ind_name, codes in ind_map.items():
                for code in codes:
                    if code not in all_mapping: all_mapping[code] = {"industry": "", "concepts": []}
                    all_mapping[code]["industry"] = ind_name

            # 处理概念
            for cp_name, codes in cp_map.items():
                for code in codes:
                    if code not in all_mapping: all_mapping[code] = {"industry": "", "concepts": []}
                    all_mapping[code]["concepts"].append(cp_name)

            self.stock_to_tags = all_mapping
            print(f"✅ 映射表加载成功，共索引 {len(self.stock_to_tags)} 只个股")
            return True

        except Exception as e:
            print(f"❌ 映射表解析失败: {e}")
            return False



    def get_stock_info(self, stock_code):
        """
        接口：根据代码获取行业和概念标签
        """
        return self.stock_to_tags.get(stock_code, {"industry": "未知", "concepts": []})

    def get_stocks_by_keyword(self, keyword):
        """
        接口：根据关键词（如‘低空经济’）反查相关个股
        """
        # 这里预留了给 StrategyScanner 使用的模糊匹配逻辑
        found_codes = []
        # 加载缓存中的概念键
        cp_map = pd.read_pickle(self.concept_file)
        for cp_name, codes in cp_map.items():
            if keyword in cp_name:
                found_codes.extend(codes)
        return list(set(found_codes))

    def get_related_sectors(self, policy_keyword):
        """
        逻辑：如果政策里提到‘人工智能’，自动联想到‘AIGC’、『算力』、『大模型』
        """
        # 基础方案：子字符串包含匹配
        all_sectors = list(self.concept_map.keys())
        matched_sectors = [s for s in all_sectors if policy_keyword in s or s in policy_keyword]
        return matched_sectors


# --- 测试代码 ---
if __name__ == "__main__":
    linker = IndustryLinker()
    # 第一次运行需要执行这个，之后只需执行 load_linker
    # linker.update_local_cache()
    linker.load_linker()
    print(linker.get_stock_info("000063"))