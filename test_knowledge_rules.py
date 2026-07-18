#!/usr/bin/env python3
from stock_selection_debate.knowledge_rules import evaluate_knowledge_rules, attach_knowledge_rules


def _packet(**overrides):
    p = {
        "kline_summary": {
            "trend_pct_5d": 4.2,
            "trend_pct_10d": 8.0,
            "trend_pct_20d": 12.0,
            "vol_5avg_vs_20avg": 1.35,
            "close_position_20d": 78.0,
            "ma_system": "多头排列",
            "vol_trend": "逐日递增",
            "vol_signal": "放量",
        },
        "indicators": {"rsi_14": 61.0, "macd_signal": "金叉", "macd_breadth": "扩张"},
        "money_flow": {"main_net_flow": 1.2, "super_net_flow": 0.8, "ddx_5": 2.1, "ddy_10": 1.6},
        "kline_raw": [
            {"open": 10, "high": 10.8, "low": 9.9, "close": 10.7, "volume": 1000},
        ],
    }
    for key, value in overrides.items():
        if isinstance(value, dict) and isinstance(p.get(key), dict):
            p[key].update(value)
        else:
            p[key] = value
    return p


def test_positive_rules():
    result = evaluate_knowledge_rules(_packet())
    assert result["score_adjustment"] > 0, result
    ids = {h["rule_id"] for h in result["hits"]}
    assert "VPA_VOLUME_PRICE_CONFIRM" in ids
    assert "TREND_MA_BULLISH" in ids


def test_watch_only_risk():
    p = _packet(
        kline_summary={"trend_pct_5d": 0.2, "trend_pct_20d": 25.0, "vol_5avg_vs_20avg": 1.7, "close_position_20d": 93.0},
        indicators={"rsi_14": 78.0, "macd_signal": "死叉"},
        money_flow={"main_net_flow": 1.0, "super_net_flow": 0.5, "ddx_5": -1.2, "ddy_10": -0.8},
    )
    result = evaluate_knowledge_rules(p)
    assert result["score_adjustment"] < 0, result
    assert result["watch_only"] is True, result


def test_attach_fields():
    p = attach_knowledge_rules(_packet())
    assert p["knowledge_rule_hits"]
    assert "knowledge_rule_summary" in p


if __name__ == "__main__":
    test_positive_rules()
    test_watch_only_risk()
    test_attach_fields()
    print("knowledge_rules tests passed")
