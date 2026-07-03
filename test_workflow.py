#!/usr/bin/env python3
"""每日选股工作流轻量烟测。

不跑真实全流程，不推送，不触发外部选股；只验证当前 workflow Phase 2
规则兜底接口能接收候选股并产出 top_picks。
"""

import os
import sys
from pathlib import Path
from types import SimpleNamespace

sys.path.insert(0, str(Path(__file__).resolve().parent))

import workflow


def main() -> None:
    os.environ["FORCE_RUN"] = "1"

    candidates = [
        {
            "stock": "000001",
            "name": "平安银行",
            "pool": "资金异动",
            "reason": "烟测候选：低估值且资金改善",
            "source_pools": ["资金异动"],
            "pool_score": 82,
            "pool_rank": 1,
        },
        {
            "stock": "600519",
            "name": "贵州茅台",
            "pool": "热点龙头",
            "reason": "烟测候选：行业龙头",
            "source_pools": ["热点龙头"],
            "pool_score": 78,
            "pool_rank": 1,
        },
    ]
    gen = SimpleNamespace(candidates=candidates)
    phase1 = [
        {"name": "新闻分析师", "status": "success", "findings": "烟测新闻：市场中性。"},
        {"name": "技术分析师", "status": "success", "findings": "烟测技术：候选股趋势稳定。"},
    ]

    original_fetch = workflow._fetch_stock_tech
    try:
        workflow._fetch_stock_tech = lambda stock: {
            "rsi": 55,
            "ma_trend": "多头排列",
            "vol_ratio": 1.2,
            "momentum": 0.08,
        }
        result = workflow.route_a_phase2(phase1, gen=gen)
    finally:
        workflow._fetch_stock_tech = original_fetch

    top_picks = result.get("top_picks") or []
    assert result.get("phase") == "route_a", result
    assert len(result.get("candidates") or []) == len(candidates), result
    assert top_picks, result
    assert all("stock" in item and "total_score" in item for item in top_picks), top_picks

    print("选股工作流烟测通过")
    print(f"top_picks={[(x.get('stock'), x.get('action'), x.get('total_score')) for x in top_picks]}")


if __name__ == "__main__":
    main()
