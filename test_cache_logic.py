#!/usr/bin/env python3
"""
测试缓存检查逻辑
"""

import json
from datetime import date, datetime
from pathlib import Path

def _is_today_cache(cache_file: Path) -> bool:
    """检查文件是否为今天创建"""
    if not cache_file.exists():
        return False
    mtime_timestamp = cache_file.stat().st_mtime
    cache_date = datetime.fromtimestamp(mtime_timestamp).date()
    today = date.today()
    return cache_date == today

def _is_valid_tech_data(data: dict) -> bool:
    """检查技术数据是否有效"""
    if not data:
        return False
    # 关键字段：RSI、MA趋势、成交量比
    rsi = data.get("rsi")
    ma_trend = data.get("ma_trend")
    vol_ratio = data.get("vol_ratio")
    return rsi is not None and ma_trend is not None and vol_ratio is not None

def _is_valid_financial_data(data: dict) -> bool:
    """检查财务数据是否有效"""
    if not data:
        return False
    # 关键字段：至少需要ROE数据
    roe = data.get("roe_annual_latest")
    return roe is not None and roe != ""

# 模拟缓存文件
test_file = Path("/tmp/test_cache.json")

# 创建今天的缓存文件
today_cache = {
    "600000": {"rsi": 50.0, "ma_trend": "多头", "vol_ratio": 1.0},
    "000001": {"rsi": 60.0, "ma_trend": "多头", "vol_ratio": 1.5},
    "000002": {"rsi": 40.0, "ma_trend": "空头", "vol_ratio": 0.8},
}

with open(test_file, "w") as f:
    json.dump(today_cache, f)

print(f"1. 检查文件是否为今天创建: {_is_today_cache(test_file)}")

# 测试数据有效性
print("\n2. 测试数据有效性:")
for stock, data in today_cache.items():
    print(f"  {stock}: {_is_valid_tech_data(data)}")

# 测试部分数据缺失的情况
partial_cache = {
    "600000": {"rsi": 50.0, "ma_trend": "多头", "vol_ratio": 1.0},
    "000001": {"rsi": None, "ma_trend": "多头", "vol_ratio": 1.5},  # 无效数据
    "000003": {"rsi": 70.0, "ma_trend": "多头", "vol_ratio": 2.0},  # 不在需求列表
}

print("\n3. 测试断点续传逻辑:")
stock_codes = ["600000", "000001", "000002", "000004"]  # 0002有缓存，0004无缓存

# 检查哪些股票有有效缓存
valid_cached = []
missing_cached = []

for stock in stock_codes:
    if stock in today_cache:
        data = today_cache[stock]
        if _is_valid_tech_data(data):
            valid_cached.append(stock)
        else:
            missing_cached.append(stock)
    else:
        missing_cached.append(stock)

print(f"  需求股票: {stock_codes}")
print(f"  有效缓存: {valid_cached}")
print(f"  需要API获取: {missing_cached}")

# 清理
test_file.unlink(missing_ok=True)

print("\n✅ 缓存检查逻辑测试完成")