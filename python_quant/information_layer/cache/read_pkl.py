import pandas as pd
import pprint


def view_pkl(file_path):
    try:
        data = pd.read_pickle(file_path)
        print(f"--- 文件内容: {file_path} ---")
        print(f"总板块数: {len(data)}")

        # 打印前 5 个板块及其对应的部分股票，看看格式对不对
        for i, (sector, stocks) in enumerate(data.items()):
            if i < 50:
                print(f"板块: {sector} | 股票数量: {len(stocks)} | 示例: {stocks[:3]}")
            else:
                break
    except Exception as e:
        print(f"读取失败: {e}")


# 查看行业表
view_pkl("industry_map.pkl")

# 查看概念表 (即便它现在是空的或不存在)
import os

if os.path.exists("concept_map.pkl"):
    view_pkl("concept_map.pkl")
else:
    print("\n❌ 警告: concept_map.pkl 文件不存在。")
