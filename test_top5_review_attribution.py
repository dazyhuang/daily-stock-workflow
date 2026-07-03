#!/usr/bin/env python3
"""Local tests for top5_review_attribution.py."""

import json
import tempfile
from pathlib import Path

import top5_review_attribution as mod


def _write(path: Path, data):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")


def _daily_rows(start: float, moves):
    days = ["20260601", "20260602", "20260603", "20260604", "20260605", "20260608", "20260609", "20260610", "20260611", "20260612", "20260615"]
    rows = []
    price = start
    for day, move in zip(days, moves):
        open_price = price
        close = round(open_price * (1 + move / 100.0), 2)
        high = round(max(open_price, close) * 1.03, 2)
        low = round(min(open_price, close) * 0.97, 2)
        rows.append({"date": day, "open": open_price, "high": high, "low": low, "close": close})
        price = close
    return rows


def test_build_review_with_core_attributions():
    with tempfile.TemporaryDirectory() as td:
        base = Path(td)
        out = base / "output"
        report = {
            "phase2": {
                "top_picks": [
                    {
                        "stock": "000001",
                        "name": "强势未买",
                        "signal": "WATCH",
                        "confidence": 66,
                        "ranking_score": 68,
                        "pool": "趋势型",
                        "money_flow": {"main_net_flow": 0.6, "super_net_flow": 0.4, "ddx_5": 1, "ddy_10": 1},
                    },
                    {
                        "stock": "000002",
                        "name": "追高回落",
                        "signal": "BUY",
                        "confidence": 74,
                        "quant_base_score": 76,
                        "pool": "强势型",
                        "money_flow": {"main_net_flow": 0.8, "super_net_flow": 0.5, "ddx_5": 1, "ddy_10": 1},
                    },
                    {
                        "stock": "000003",
                        "name": "LLM扣分走强",
                        "signal": "WATCH",
                        "confidence": 58,
                        "quant_base_score": 78,
                        "llm_risk_adjustment": -12,
                        "pool": "成长型",
                        "money_flow": {"main_net_flow": -0.3, "super_net_flow": -0.3, "ddx_5": -1, "ddy_10": -1},
                    },
                    {
                        "stock": "000004",
                        "name": "量化高分走弱",
                        "signal": "BUY",
                        "confidence": 80,
                        "quant_base_score": 82,
                        "pool": "资金异动",
                        "money_flow": {"main_net_flow": 0.9, "super_net_flow": 0.5, "ddx_5": 2, "ddy_10": 2},
                    },
                ]
            }
        }
        _write(out / "daily_report_20260601.json", report)
        _write(out / "intraday_buy_timing_20260601.json", {
            "date": "2026-06-01",
            "stocks": {
                "000001": {
                    "status": "open",
                    "last_decision": {"action": "WAIT", "reason": "未触发", "technical_trigger": None},
                    "decision_count": 20,
                },
                "000002": {
                    "status": "filled",
                    "filled_at": "2026-06-01T10:00:00",
                    "filled_price": 10.5,
                    "filled_quantity": 1000,
                    "last_decision": {"action": "BUY_NOW", "technical_trigger": "MA120_CROSS_UP"},
                    "submitted_order_count": 1,
                },
                "000003": {
                    "status": "open",
                    "last_decision": {"action": "WAIT", "reason": "风险偏高", "technical_trigger": None},
                },
                "000004": {
                    "status": "open",
                    "last_decision": {"action": "WAIT", "reason": "观察", "technical_trigger": None},
                },
            },
        })
        _write(out / "trades.json", {
            "records": [
                {
                    "stock": "000002",
                    "name": "追高回落",
                    "buy_date": "2026-06-01",
                    "buy_price": 10.5,
                    "quantity": 1000,
                    "remaining_quantity": 1000,
                    "source": "intraday_buy_timing",
                    "sells": [],
                }
            ]
        })

        price_map = {
            "000001": _daily_rows(10, [1, 2, 3, 2, 1, 1, 1, 1, 1, 1, 1]),
            "000002": _daily_rows(10, [-4, -2, -1, 0, 1, 1, 1, 1, 1, 1, 1]),
            "000003": _daily_rows(10, [1, 3, 3, 2, 2, 1, 1, 1, 1, 1, 1]),
            "000004": _daily_rows(10, [-2, -3, -2, -1, -1, 0, 0, 0, 0, 0, 0]),
            "000300": _daily_rows(10, [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]),
        }

        def fake_daily(code, count=80):
            return price_map.get(code, [])

        review = mod.build_review(
            base_dir=base,
            days=10,
            daily_fetcher=fake_daily,
            minute_fetcher=lambda code, day: None,
            enable_money_flow_backfill=False,
        )
        items = {item["stock"]: item for item in review["items"]}
        assert "GOOD_BUT_NOT_BOUGHT" in items["000001"]["attribution_labels"], items["000001"]
        assert "ENTRY_TOO_LATE" in items["000002"]["attribution_labels"], items["000002"]
        assert "MODEL_OVER_RISK" in items["000003"]["attribution_labels"], items["000003"]
        assert "MONEY_FLOW_MISLEAD" in items["000003"]["attribution_labels"], items["000003"]
        assert "QUANT_OVER_SCORE" in items["000004"]["attribution_labels"], items["000004"]
        assert review["summary"]["top5_count"] == 4


def test_llm_deep_analysis_is_attached_without_network():
    with tempfile.TemporaryDirectory() as td:
        base = Path(td)
        out = base / "output"
        _write(out / "daily_report_20260601.json", {
            "phase2": {
                "top_picks": [
                    {
                        "stock": "000001",
                        "name": "样本股",
                        "signal": "WATCH",
                        "confidence": 66,
                        "money_flow": {"main_net_flow": None, "super_net_flow": None, "ddx_5": None, "ddy_10": None},
                    }
                ]
            }
        })
        original = mod.run_llm_deep_analysis
        try:
            mod.run_llm_deep_analysis = lambda review, **kwargs: {
                "status": "ok",
                "model": "GPT-5.5",
                "overall_conclusion": "样本偏少，优先补齐行情数据后再判断。",
                "root_causes": ["行情数据缺失", "样本不足"],
                "priority_fixes": ["修复XQShare行情源"],
                "confidence": 80,
            }
            review = mod.build_review(
                base_dir=base,
                days=10,
                include_llm=True,
                daily_fetcher=lambda code, count=80: [],
                minute_fetcher=lambda code, day: None,
                enable_money_flow_backfill=False,
            )
            assert review["llm_deep_analysis"]["model"] == "GPT-5.5"
            text = mod.format_feishu_text(review)
            assert "LLM深度解读" in text
            assert "样本偏少" in text
        finally:
            mod.run_llm_deep_analysis = original


def test_llm_deep_analysis_uses_minimax_structured_fallback():
    calls = []

    def fake_structured(prompt, model, *, fallback_used):
        calls.append(model)
        if "gpt-5.5" in model:
            return {"status": "failed", "model": "GPT-5.5", "error": "empty"}
        return {
            "status": "ok",
            "model": "MiniMax-M3",
            "fallback_used": fallback_used,
            "overall_conclusion": "MiniMax结构化兜底成功。",
            "root_causes": ["主模型空结果"],
            "selection_diagnosis": "样本可用。",
            "execution_diagnosis": "执行链路待优化。",
            "scoring_diagnosis": "评分需要继续校准。",
            "market_context_diagnosis": "市场环境中性。",
            "priority_fixes": ["保留结构化兜底"],
            "watchlist_notes": ["样本股"],
            "risk_warnings": ["避免过拟合"],
            "confidence": 80,
        }

    original_structured = mod._call_structured_deep_analysis
    original_sleep = mod.time.sleep
    original_retries = mod.DEFAULT_LLM_RETRIES
    try:
        mod._call_structured_deep_analysis = fake_structured
        mod.time.sleep = lambda *_args, **_kwargs: None
        mod.DEFAULT_LLM_RETRIES = 2
        result = mod.run_llm_deep_analysis({"summary": {}, "items": []})
        assert result["status"] == "ok"
        assert result["model"] == "MiniMax-M3"
        assert result["fallback_used"] is True
        assert calls == ["openai/gpt-5.5", "openai/gpt-5.5", "minimax-portal/MiniMax-M3"]
    finally:
        mod._call_structured_deep_analysis = original_structured
        mod.time.sleep = original_sleep
        mod.DEFAULT_LLM_RETRIES = original_retries


if __name__ == "__main__":
    test_build_review_with_core_attributions()
    test_llm_deep_analysis_is_attached_without_network()
    test_llm_deep_analysis_uses_minimax_structured_fallback()
    print("top5_review_attribution tests passed")
