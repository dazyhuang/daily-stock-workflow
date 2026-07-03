#!/usr/bin/env python3
"""数据获取测试 — 验证辩论所需的各项数据能否获取"""
import sys
import json
import requests
import ast
from pathlib import Path
import os

if os.environ.get("RUN_EXTERNAL_DATA_TESTS") != "1":
    print("skipped: set RUN_EXTERNAL_DATA_TESTS=1 to probe live data providers")
    raise SystemExit(0)

BASE_DIR = Path(__file__).parent
API_URL = "https://mkapi2.dfcfs.com/finskillshub"
API_KEY = "test"

def test_positions():
    print("=== 1. 获取持仓 ===")
    headers = {"x-api-key": API_KEY, "Content-Type": "application/json"}
    payload = {"type": "positions", "params": {}}
    try:
        r = requests.post(API_URL, json=payload, headers=headers, timeout=15)
        print(f"HTTP {r.status_code}")
        data = r.json()
        print(f"返回结构 keys: {list(data.keys())}")
        if "data" in data:
            print(f"data keys: {list(data['data'].keys())}")
            positions = data["data"].get("positions", [])
            print(f"持仓数量: {len(positions)}")
            if positions:
                print(f"示例: {json.dumps(positions[0], ensure_ascii=False)[:300]}")
        return data
    except Exception as e:
        print(f"失败: {e}")
    return None

def test_latest_price(code="000001"):
    print(f"\n=== 2. 获取 {code} 最新价 ===")
    import subprocess
    try:
        cmd = [
            sys.executable,
            str(BASE_DIR.parent / "skills/mx-data/mx_data.py"),
            f"{code} 最新价",
        ]
        r = subprocess.run(cmd, capture_output=True, text=True, timeout=20)
        print(f"stdout: {r.stdout[:200]}")
        print(f"stderr: {r.stderr[:200] if r.stderr else ''}")
        return r.stdout
    except Exception as e:
        print(f"失败: {e}")
    return None

def test_prev_close(code="000001"):
    print(f"\n=== 3. 获取 {code} 昨收（近2日收盘价） ===")
    import subprocess
    try:
        cmd = [
            sys.executable,
            str(BASE_DIR.parent / "skills/mx-data/mx_data.py"),
            f"{code} 近2日收盘价",
        ]
        r = subprocess.run(cmd, capture_output=True, text=True, timeout=20)
        out = r.stdout.strip()
        print(f"stdout: {out[:500]}")
        try:
            data = ast.literal_eval(out)
            print(f"解析成功，K线条数: {len(data)}")
            if len(data) >= 2:
                print(f"今日: {data[-1]}")
                print(f"昨收: {data[-2]}")
        except Exception as e:
            print(f"解析失败: {e}")
    except Exception as e:
        print(f"失败: {e}")

def test_kline_5d(code="000001"):
    print(f"\n=== 4. 获取 {code} 近5日收盘价 ===")
    import subprocess
    try:
        cmd = [
            sys.executable,
            str(BASE_DIR.parent / "skills/mx-data/mx_data.py"),
            f"{code} 近5日收盘价",
        ]
        r = subprocess.run(cmd, capture_output=True, text=True, timeout=20)
        out = r.stdout.strip()
        print(f"stdout: {out[:500]}")
        try:
            data = ast.literal_eval(out)
            print(f"解析成功，K线条数: {len(data)}")
            for k in data:
                print(f"  {k}")
        except Exception as e:
            print(f"解析失败: {e}")
    except Exception as e:
        print(f"失败: {e}")

if __name__ == "__main__":
    test_positions()
    test_latest_price("000001")
    test_latest_price("300456")
    test_prev_close("000001")
    test_kline_5d("000001")
