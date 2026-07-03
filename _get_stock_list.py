#!/usr/bin/env python3
"""获取股票列表并缓存到文件，带重试"""
import json
import warnings
import os
import time

for k in list(os.environ.keys()):
    if 'proxy' in k.lower():
        os.environ.pop(k, None)

warnings.filterwarnings("ignore")
import akshare as ak  # noqa: E402

CACHE_FILE = "./output/fundamental_cache/_stock_list_cache.json"
MAX_RETRIES = 5

for attempt in range(MAX_RETRIES):
    try:
        print(f"尝试 {attempt+1}/{MAX_RETRIES} 获取股票列表...")
        df = ak.stock_info_a_code_name()
        stocks = list(zip(df["code"].astype(str).str.zfill(6), df["name"]))
        print(f"成功获取 {len(stocks)} 只股票")
        
        with open(CACHE_FILE, "w") as f:
            json.dump(stocks, f)
        print(f"已缓存到 {CACHE_FILE}")
        break
    except Exception as e:
        print(f"失败: {e}")
        if attempt < MAX_RETRIES - 1:
            wait = 5 * (attempt + 1)
            print(f"等待 {wait} 秒后重试...")
            time.sleep(wait)
        else:
            print("所有重试失败")


