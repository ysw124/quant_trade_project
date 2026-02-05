import pickle


def load_linker():
    with open(r"cache\industry_map.pkl", "rb") as f:
        ind = pickle.load(f)
    with open(r"cache\concept_map.pkl", "rb") as f:
        con = pickle.load(f)
    return {**ind, **con}


def find_best_sectors(keyword, all_sectors):
    """
    模糊匹配：寻找包含关键词的所有板块
    比如：关键词是"半导体"，能匹配到"半导体行业"、"中证半导体"、"TGN半导体"
    """
    results = {}
    for sector_name, stocks in all_sectors.items():
        # 逻辑：只要关键词在板块名里，或者板块名在关键词里
        if keyword in sector_name or sector_name in keyword:
            results[sector_name] = stocks

    return results


# 测试一下
all_data = load_linker()
targets = ["半导体概念", "低空经济", "人工智能"]

for kw in targets:
    matches = find_best_sectors(kw, all_data)
    if matches:
        print(f"🔍 关键词【{kw}】成功匹配到：{list(matches.keys())}")
    else:
        # 如果还是找不到，尝试进一步拆解词汇（比如去掉“概念”二字）
        simplified_kw = kw.replace("概念", "").replace("板块", "")
        matches = find_best_sectors(simplified_kw, all_data)
        if matches:
            print(f"💡 简化关键词【{simplified_kw}】匹配到：{list(matches.keys())}")

print([k for k in all_data.keys() if "低空" in k])