#!/usr/bin/env python3
import json
import tempfile
from pathlib import Path

import candidate_edge_rules as mod
from candidate_edge_rules import discover_edge_rules, evaluate_candidate_edge


def test_evaluate_candidate_edge_weighting_and_penalty():
    rule = {
        "id": "edge_001",
        "description": "信号=WATCH & 超大单净流>5",
        "conditions": [
            {"field": "signal", "op": "eq", "value": "WATCH"},
            {"field": "super_net_flow", "op": "gt", "value": 5},
        ],
        "edge_bonus": 8.0,
        "metrics": {"sample_count": 30, "d5_avg_return_pct": 12.0, "d5_win_rate_pct": 80.0},
    }
    candidate = {
        "signal": "WATCH",
        "money_flow": {"super_net_flow": 6.2},
        "kline_summary": {"close_position_20d": 99},
        "rsi": 86,
        "needs_intraday_confirmation": True,
        "entry_condition": "盘中确认",
    }
    result = evaluate_candidate_edge(candidate, {"weekly": {"rules": [rule]}, "monthly": {"rules": [rule]}})
    assert result["score"] == 8.0
    assert result["match_count"] == 2
    assert result["chase_risk_penalty"] > 0
    # WATCH 本身不是风险门控；只有命中负向保护规则才 watch_only。
    assert result["watch_only"] is False


def test_discover_edge_rules_from_synthetic_pool():
    rows = []
    for i in range(30):
        rows.append({
            "report_date": f"202606{(i % 10) + 1:02d}",
            "stock": f"30{i:04d}",
            "signal": "WATCH",
            "money_flow": {"super_net_flow": 6.0, "main_net_flow": 6.0},
            "d5_return_pct": 12.0,
        })
    for i in range(40):
        rows.append({
            "report_date": f"202606{(i % 10) + 1:02d}",
            "stock": f"60{i:04d}",
            "signal": "BUY",
            "money_flow": {"super_net_flow": -1.0, "main_net_flow": -1.0},
            "d5_return_pct": -3.0,
        })
    result = discover_edge_rules(rows, min_samples=10, min_days=5, max_rules=5)
    assert result["baseline"]["n"] == 70
    assert result["rules"], result
    assert any("超大单" in rule["description"] or "主力净流" in rule["description"] for rule in result["rules"])
    assert all("信号=" not in rule["description"] and "做多分" not in rule["description"] for rule in result["rules"])


def test_stale_price_cache_is_refreshed():
    with tempfile.TemporaryDirectory() as td:
        original_dir = mod.EDGE_RULE_DIR
        try:
            mod.EDGE_RULE_DIR = Path(td)
            cache = {"daily_ohlc": {"000001": [
                {"date": "20260601", "open": 10, "high": 11, "low": 9, "close": 10},
            ]}}
            (Path(td) / "daily_ohlc_cache.json").write_text(json.dumps(cache), encoding="utf-8")
            bars = [
                {"date": f"202606{day:02d}", "open": 10, "high": 11, "low": 9, "close": 10 + day / 10}
                for day in range(1, 11)
            ]
            rows, summary = mod._augment_with_prices(
                [{"stock": "000001", "report_date": "20260602"}],
                fetcher=lambda *_args, **_kwargs: bars,
                pause_sec=0,
            )
            assert summary["refreshed_stocks"] == 1
            assert summary["stale_cache_stocks"] == 1
            assert rows and rows[0]["d5_return_pct"] is not None
        finally:
            mod.EDGE_RULE_DIR = original_dir


if __name__ == "__main__":
    test_evaluate_candidate_edge_weighting_and_penalty()
    test_discover_edge_rules_from_synthetic_pool()
    test_stale_price_cache_is_refreshed()
    print("candidate_edge_rules tests passed")
