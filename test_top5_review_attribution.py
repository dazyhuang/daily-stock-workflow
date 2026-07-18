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
        assert review["intraday_buy_review"]["status"] == "ok"
        assert review["intraday_buy_review"]["summary"]["observed_stock_count"] == 4
        assert "盘中买入复盘" in mod.format_feishu_text(review)


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
        original_min_samples = mod.MIN_LLM_MATURE_D5_SAMPLES
        try:
            mod.MIN_LLM_MATURE_D5_SAMPLES = 0
            mod.run_llm_deep_analysis = lambda review, **kwargs: {
                "status": "ok",
                "model": "GPT-5.6 Sol",
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
            assert review["llm_deep_analysis"]["model"] == "GPT-5.6 Sol"
            text = mod.format_feishu_text(review)
            assert "LLM深度解读" in text
            assert "样本偏少" in text
        finally:
            mod.run_llm_deep_analysis = original
            mod.MIN_LLM_MATURE_D5_SAMPLES = original_min_samples


def test_incomplete_returns_and_embedded_backtest_are_not_counted():
    with tempfile.TemporaryDirectory() as td:
        base = Path(td)
        out = base / "output"
        _write(out / "daily_report_20260601.json", {
            "phase2": {"top_picks": [{
                "stock": "000001",
                "name": "样本",
                "money_flow": {"main_net_flow": 1, "super_net_flow": 1, "ddx_5": 1, "ddy_10": 1},
                "strategy_backtest": {"return_pct": 10.0, "entry_price": 10, "exit_price": 11},
            }]}
        })
        review = mod.build_review(
            base_dir=base,
            daily_fetcher=lambda *_args, **_kwargs: [],
            minute_fetcher=lambda *_args, **_kwargs: None,
            enable_money_flow_backfill=False,
        )
        item = review["items"][0]
        assert item["future_returns_pct"]["d5"] is None
        assert item["future_return_complete"]["d5"] is False
        assert item["reference_source"] == "unavailable"
        assert "DATA_QUALITY_ISSUE" in item["attribution_labels"]
        assert review["summary"]["returns"]["d5"]["mature_sample_count"] == 0


def test_trade_review_aggregates_same_day_lots():
    records = [
        {"stock": "000001", "buy_date": "2026-06-01", "buy_price": 10, "quantity": 100, "remaining_quantity": 100, "sells": []},
        {"stock": "000001", "buy_date": "2026-06-01", "buy_price": 12, "quantity": 200, "remaining_quantity": 100,
         "sells": [{"price": 13, "quantity": 100}]},
    ]
    result = mod._extract_trade_info(records, "000001", "20260601", 14)
    assert result["actual_lot_count"] == 2
    assert result["actual_quantity"] == 300
    assert result["actual_remaining_quantity"] == 200
    assert result["actual_buy_price"] == 11.3333
    assert result["actual_return_pct_to_latest"] == 20.59


def test_partial_money_flow_is_reported():
    flow = {"main_net_flow": 1.0, "super_net_flow": 2.0, "ddx_5": None, "ddy_10": None}
    assert mod._money_flow_missing_keys(flow) == ["ddx_5", "ddy_10"]
    assert mod._money_flow_is_complete(flow) is False


def test_selection_memory_quarantines_unverified_returns():
    import selection_memory

    with tempfile.TemporaryDirectory() as td:
        path = Path(td) / "selection_memory.jsonl"
        path.write_text(json.dumps({
            "report_date": "20260601",
            "stock": "000001",
            "return_d5_pct": 10.0,
            "alpha_d5_pct": 8.0,
        }, ensure_ascii=False) + "\n", encoding="utf-8")
        review = {
            "generated_at": "2026-07-11T08:00:00",
            "items": [{
                "date": "2026-06-01",
                "stock": "000001",
                "future_returns_pct": {"d5": None},
                "future_return_complete": {"d5": False},
                "future_return_dates": {"d5": None},
            }],
        }
        assert selection_memory.update_selection_memory_from_top5_review(review, path=path) == 1
        saved = json.loads(path.read_text(encoding="utf-8").strip())
        assert saved["return_d5_pct"] is None
        assert saved["alpha_d5_pct"] is None
        assert saved["return_d5_complete"] is False


def test_forward_memory_backfill_reaches_beyond_display_window():
    with tempfile.TemporaryDirectory() as td:
        base = Path(td)
        out = base / "output"
        report_days = [
            "20260601", "20260602", "20260603", "20260604", "20260605", "20260608",
            "20260609", "20260610", "20260611", "20260612", "20260615", "20260616",
        ]
        for day in report_days:
            picks = [{
                "stock": "000001",
                "name": "旧窗口样本",
                "signal": "BUY",
                "money_flow": {"main_net_flow": 1, "super_net_flow": 1, "ddx_5": 1, "ddy_10": 1},
            }] if day == "20260601" else []
            _write(out / f"daily_report_{day}.json", {"phase2": {"top_picks": picks}})
        memory_path = out / "selection_memory.jsonl"
        memory_path.write_text(json.dumps({
            "report_date": "20260601",
            "stock": "000001",
            "top5_rank": 1,
            "return_d10_complete": False,
        }, ensure_ascii=False) + "\n", encoding="utf-8")

        price_map = {
            "000001": _daily_rows(10, [0, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1]),
            "000300": _daily_rows(10, [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]),
        }
        backfill = mod.build_forward_memory_backfill(
            base_dir=base,
            days=30,
            daily_fetcher=lambda code, count=80: price_map.get(code, []),
            minute_fetcher=lambda _code, _day: None,
        )

        assert backfill["summary"]["report_days_scanned"] == 12
        assert backfill["summary"]["completed_records"] == 1
        assert backfill["items"][0]["future_return_complete"]["d10"] is True
        display_review = {
            "generated_at": "2026-06-16T08:00:00",
            "items": [],
            "intraday_buy_review": {},
            "forward_memory_backfill": dict(backfill["summary"]),
        }
        mod.save_review(
            display_review,
            output_dir=out,
            memory_backfill_review=backfill,
        )
        saved = json.loads(memory_path.read_text(encoding="utf-8").strip())
        assert saved["return_d10_complete"] is True
        assert saved["return_d10_pct"] is not None
        assert display_review["selection_memory"]["forward_records_updated"] == 1
        assert display_review["forward_memory_backfill"]["records_updated"] == 1


def test_daily_forward_summary_falls_back_to_mature_d5():
    import workflow

    old_output = workflow.OUTPUT_DIR
    try:
        with tempfile.TemporaryDirectory() as td:
            workflow.OUTPUT_DIR = Path(td)
            rows = [
                {
                    "report_date": f"2026060{idx}",
                    "stock": f"00000{idx}",
                    "top5_rank": idx,
                    "return_d5_pct": value,
                    "return_d5_complete": True,
                    "alpha_d5_pct": value - 0.5,
                    "return_d10_pct": None,
                    "return_d10_complete": False,
                }
                for idx, value in ((1, 3.0), (2, -1.0))
            ]
            (workflow.OUTPUT_DIR / "selection_memory.jsonl").write_text(
                "".join(json.dumps(row, ensure_ascii=False) + "\n" for row in rows),
                encoding="utf-8",
            )
            result = workflow.run_backtest_strategy([{"stock": "999999"}])

        assert result["status"] == "historical_forward_validation"
        assert result["horizon"] == "d5"
        assert result["requested_horizon"] == "d10"
        assert result["fallback_used"] is True
        assert result["sample_count"] == 2
        assert result["avg_return_pct"] == 1.0
        assert "暂用D5" in result["summary"]
    finally:
        workflow.OUTPUT_DIR = old_output


def test_llm_deep_analysis_uses_minimax_structured_fallback():
    calls = []

    def fake_structured(prompt, model, *, fallback_used):
        calls.append(model)
        if "gpt-5.6-sol" in model:
            return {"status": "failed", "model": "GPT-5.6 Sol", "error": "empty"}
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
        assert calls == ["openai/gpt-5.6-sol", "openai/gpt-5.6-sol", "minimax-portal/MiniMax-M3"]
    finally:
        mod._call_structured_deep_analysis = original_structured
        mod.time.sleep = original_sleep
        mod.DEFAULT_LLM_RETRIES = original_retries


def test_llm_primary_model_uses_max_reasoning():
    from stock_selection_debate import providers

    captured = {}

    def fake_call_structured(*args, **kwargs):
        captured.update(kwargs)
        return None

    original = providers.call_structured
    try:
        providers.call_structured = fake_call_structured
        result = mod._call_structured_deep_analysis(
            "test prompt",
            mod.DEFAULT_LLM_MODEL,
            fallback_used=False,
        )
        assert result["status"] == "failed"
        assert captured["model"] == "openai/gpt-5.6-sol"
        assert captured["reasoning_effort"] == "max"
        assert captured["allow_fallback"] is False
    finally:
        providers.call_structured = original


if __name__ == "__main__":
    test_build_review_with_core_attributions()
    test_llm_deep_analysis_is_attached_without_network()
    test_incomplete_returns_and_embedded_backtest_are_not_counted()
    test_trade_review_aggregates_same_day_lots()
    test_partial_money_flow_is_reported()
    test_selection_memory_quarantines_unverified_returns()
    test_forward_memory_backfill_reaches_beyond_display_window()
    test_daily_forward_summary_falls_back_to_mature_d5()
    test_llm_deep_analysis_uses_minimax_structured_fallback()
    test_llm_primary_model_uses_max_reasoning()
    print("top5_review_attribution tests passed")
