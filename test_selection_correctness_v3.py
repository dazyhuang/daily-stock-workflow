#!/usr/bin/env python3
"""Offline correctness checks for the 2026-07-10 selection hardening."""

from __future__ import annotations

import json
import hashlib
import tempfile
from datetime import date
from pathlib import Path


def test_macd_is_canonical() -> None:
    from stock_selection_debate.technical_indicators import compute_macd

    prices = [20 - i * 0.2 for i in range(30)] + [14 + i * 0.8 for i in range(20)]
    result = compute_macd(prices)
    assert abs(result["dif"]) > 0.01
    assert abs(result["dea"]) > 0.01
    assert abs(result["hist"]) > 0.001
    assert result["state"] in {"多头", "空头", "零轴附近"}
    assert result["cross_event"] in {"金叉", "死叉", "无", "", None}


def test_evidence_values_and_field_level_partial() -> None:
    from stock_selection_debate.debate_engine import _role_evidence_errors, _validate_pm_evidence

    packet = {
        "data_contract": {
            "kline": {"status": "ok"},
            "money_flow": {
                "status": "partial",
                "field_status": {"main_net_flow": "ok", "super_net_flow": "missing"},
            },
        },
        "kline_summary": {"ma_system": "震荡"},
        "indicators": {"rsi_14": 51.2, "macd_state": "空头", "macd_cross_event": ""},
        "money_flow": {"main_net_flow": 1.2, "super_net_flow": None},
    }
    passed = _validate_pm_evidence(
        packet,
        "主力净流入为1.2亿元，短线有资金支持。",
        [{"field": "money_flow.main_net_flow", "value": "1.2", "claim": "主力净流入"}],
    )
    assert passed["status"] == "pass", passed

    wrong = _validate_pm_evidence(
        packet,
        "主力净流入较强。",
        [{"field": "money_flow.main_net_flow", "value": "9.9", "claim": "主力净流入"}],
    )
    assert wrong["status"] == "fail"
    assert any("证据值不一致" in x for x in wrong["errors"])

    missing = _validate_pm_evidence(packet, "资金流无法验证。", [])
    assert missing["status"] == "fail"
    assert "PM未返回evidence_refs" in missing["errors"]

    contradictions = _role_evidence_errors(packet, "MACD金叉且均线多头排列，RSI为80。")
    assert len(contradictions) == 3, contradictions


def test_money_flow_semantics_and_provenance() -> None:
    from stock_selection_debate.data_fetcher import _kline_has_min_bars, _merge_money_flow

    merged = _merge_money_flow(
        {"main_net_flow": 1.5, "source": "mx-data", "as_of": "20260709"},
        {"main_net_flow_5d": 3.2, "source": "eastmoney", "as_of": "20260709"},
    )
    assert merged["main_net_flow"] == 1.5
    assert merged["main_net_flow_5d"] == 3.2
    assert merged["ddx_5"] is None
    assert merged["field_sources"]["main_net_flow"] == "mx-data"
    assert merged["field_sources"]["main_net_flow_5d"] == "eastmoney"
    assert not _kline_has_min_bars([{"close": i} for i in range(59)])
    assert _kline_has_min_bars([{"close": i} for i in range(60)])


def test_pool_merge_and_position_independence() -> None:
    from llm_scorer import _merge_candidate_sources
    from workflow import _select_display_top5

    merged = _merge_candidate_sources([
        {
            "stock": "000001", "name": "A", "pool": "低优先级强池", "priority": 9,
            "pool_score": 90, "pool_rank": 1, "pool_scored_candidates": 100,
        },
        {
            "stock": "000001", "name": "A", "pool": "高优先级弱池", "priority": 1,
            "pool_score": 40, "pool_rank": 40, "pool_scored_candidates": 40,
        },
    ])[0]
    assert merged["pool"] == "低优先级强池", merged
    assert merged["pool_score"] > 70

    ranked = [
        {"stock": "000001", "signal": "WATCH", "buy_score": 90, "position_ratio": "0%"},
        {"stock": "000002", "signal": "BUY", "buy_score": 80, "position_ratio": "25%"},
    ]
    picks = _select_display_top5(ranked, target=2)
    assert [x["stock"] for x in picks] == ["000001", "000002"]


def test_causal_phase3_and_compact_report() -> None:
    import workflow

    selection = workflow.run_backtest_selection([
        {"stock": "000001", "name": "A", "signal": "WATCH"}
    ])
    assert selection["status"] == "pending_forward_validation"
    assert selection["stocks"][0]["status"] == "pending_forward_validation"

    large_candidate = {
        "stock": "000001", "name": "A", "signal": "WATCH", "buy_score": 80,
        "confidence": 65, "kline_raw": [{"close": i} for i in range(120)],
        "bull_history": "x" * 100000, "bear_history": "y" * 100000,
        "money_flow": {"main_net_flow": 1.0},
    }
    report = {
        "date": "2026-07-10", "status": "success", "artifacts": {"candidates_jsonl": "/tmp/c.jsonl"},
        "phase1": [{"name": "新闻", "status": "success", "findings": "z" * 10000}],
        "phase2": {
            "ranked_candidates": [large_candidate], "top_picks": [large_candidate],
            "buy_list": [large_candidate], "watch_list": [large_candidate], "avoid_list": [],
        },
    }
    compact = workflow._compact_daily_report(report)
    encoded = json.dumps(compact, ensure_ascii=False)
    assert "kline_raw" not in compact["phase2"]["ranked_candidates"][0]
    assert "bull_history" not in compact["phase2"]["ranked_candidates"][0]
    assert len(encoded) < 20000


def test_node_resume_and_stable_report_contract() -> None:
    from stock_selection_debate import debate_engine
    import run_daily_stock_workflow_stable as stable

    calls = []

    def node_fn(state):
        calls.append("ran")
        return {**state, "bull_history": "fresh", "count": 1}

    base = {"stock_code": "000001", "stock_name": "A", "count": 0}
    key = debate_engine._node_checkpoint_key("invest", "bull_researcher", base)
    wrapped = debate_engine._checkpointed_node("invest", "bull_researcher", node_fn)
    restored = wrapped({**base, "_node_resume": {key: {"bull_history": "cached", "count": 1}}})
    assert restored["bull_history"] == "cached"
    assert not calls

    with tempfile.TemporaryDirectory() as td:
        root = Path(td)
        artifacts = []
        for name in ("candidates.jsonl", "trace.json", "summary.json"):
            path = root / name
            path.write_text("{}", encoding="utf-8")
            artifacts.append(path)
        report_path = root / "report.json"
        report_data = {
            "date": stable._today(),
            "status": "success",
            "phase2": {"top_picks": [{"stock": str(i)} for i in range(5)]},
            "artifacts": {
                "candidates_jsonl": str(artifacts[0]),
                "trace_json": str(artifacts[1]),
                "summary_json": str(artifacts[2]),
            },
        }
        report_path.write_text(json.dumps(report_data), encoding="utf-8")
        marker = root / "push.json"
        original_marker_path = stable._push_marker_path
        try:
            stable._push_marker_path = lambda: marker
            payload = json.dumps(report_data, ensure_ascii=False, sort_keys=True, default=str)
            marker.write_text(json.dumps({
                "date": date.today().isoformat(),
                "status": "success",
                "top_picks_count": 5,
                "report_digest": hashlib.sha256(payload.encode("utf-8")).hexdigest(),
            }), encoding="utf-8")
            assert stable._report_is_complete(report_path)
            data = json.loads(report_path.read_text(encoding="utf-8"))
            data["status"] = "failed"
            report_path.write_text(json.dumps(data), encoding="utf-8")
            assert not stable._report_is_complete(report_path)
        finally:
            stable._push_marker_path = original_marker_path


def main() -> None:
    test_macd_is_canonical()
    test_evidence_values_and_field_level_partial()
    test_money_flow_semantics_and_provenance()
    test_pool_merge_and_position_independence()
    test_causal_phase3_and_compact_report()
    test_node_resume_and_stable_report_contract()
    print("selection correctness v3 tests passed")


if __name__ == "__main__":
    main()
