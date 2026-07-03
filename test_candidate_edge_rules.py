#!/usr/bin/env python3
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
        "reason": "等待回调确认",
    }
    result = evaluate_candidate_edge(candidate, {"weekly": {"rules": [rule]}, "monthly": {"rules": [rule]}})
    assert result["score"] == 8.0
    assert result["match_count"] == 2
    assert result["chase_risk_penalty"] > 0
    assert result["watch_only"] is True


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
    assert any("WATCH" in rule["description"] or "超大单" in rule["description"] for rule in result["rules"])


if __name__ == "__main__":
    test_evaluate_candidate_edge_weighting_and_penalty()
    test_discover_edge_rules_from_synthetic_pool()
    print("candidate_edge_rules tests passed")
