#!/usr/bin/env python3
"""Focused offline regressions for the 2026-07-10 selection correctness pass."""

from __future__ import annotations

import json
import hashlib
import tempfile
from datetime import date, timedelta
from pathlib import Path


def test_macd_contract():
    from stock_selection_debate.technical_indicators import compute_macd, legacy_macd_signal

    result = compute_macd([float(i) for i in range(1, 121)])
    assert result["dif"] != result["dea"]
    assert result["hist"] != 0
    assert result["state"] == "多头"
    assert legacy_macd_signal(result) in {"多头区", "金叉"}
    if result["cross_event"] != "金叉":
        assert legacy_macd_signal(result) == "多头区"


def test_evidence_values_and_role_claims():
    from stock_selection_debate.debate_engine import _role_evidence_errors, _validate_pm_evidence

    packet = {
        "indicators": {
            "rsi_14": 55,
            "macd_state": "多头",
            "macd_cross_event": "无",
        },
        "kline_summary": {"ma_system": "多头排列"},
        "data_contract": {"kline": {"status": "ok"}},
    }
    wrong = _validate_pm_evidence(
        packet,
        "RSI处于中性区",
        [{"field": "indicators.rsi_14", "value": "80", "claim": "RSI中性"}],
        [],
        [],
    )
    assert wrong["status"] == "fail"
    assert any("证据值不一致" in x for x in wrong["errors"])
    role_errors = _role_evidence_errors(packet, "RSI为80且MACD金叉")
    assert any("RSI" in x for x in role_errors)
    assert any("MACD" in x for x in role_errors)


def test_current_day_backtest_is_causal():
    import workflow

    selection = workflow.run_backtest_selection([
        {"stock": "000001", "name": "测试", "buy_score": 88, "position_ratio": "40%"}
    ])
    assert selection["status"] == "pending_forward_validation"
    assert selection["trades"] == []

    old_output = workflow.OUTPUT_DIR
    try:
        with tempfile.TemporaryDirectory() as td:
            workflow.OUTPUT_DIR = Path(td)
            prior = (date.today() - timedelta(days=10)).strftime("%Y%m%d")
            memory = {
                "report_date": prior,
                "stock": "000001",
                "top5_rank": 1,
                "return_d10_pct": 6.5,
                "return_d10_complete": True,
                "alpha_d10_pct": 4.0,
            }
            (workflow.OUTPUT_DIR / "selection_memory.jsonl").write_text(
                json.dumps(memory, ensure_ascii=False) + "\n",
                encoding="utf-8",
            )
            strategy = workflow.run_backtest_strategy([{"stock": "999999", "buy_score": 99}])
            assert strategy["status"] == "historical_forward_validation"
            assert strategy["sample_count"] == 1
            assert strategy["avg_return_pct"] == 6.5
    finally:
        workflow.OUTPUT_DIR = old_output


def test_money_flow_semantics_and_contract_date():
    from stock_selection_debate.data_fetcher import (
        _merge_money_flow,
        _money_flow_has_gap,
        _sanitize_legacy_cached_money_flow,
    )
    from stock_selection_debate.data_router import normalize_contract_item

    merged = _merge_money_flow(
        {"main_net_flow": 1.2, "main_net_flow_5d": 4.5, "source": "eastmoney", "as_of": "20260709"},
        {"super_net_flow": 0.4, "ddx_5": 0.12, "source": "mx-data", "as_of": "20260709"},
    )
    assert merged["main_net_flow_5d"] == 4.5
    assert merged["ddx_5"] == 0.12
    assert merged["field_sources"]["main_net_flow_5d"] == "eastmoney"
    assert merged["field_sources"]["ddx_5"] == "mx-data"
    assert not _money_flow_has_gap(merged)

    unknown_date = normalize_contract_item("money_flow", {"source": "mx-data", "status": "ok"})
    assert unknown_date["as_of"] == ""
    assert unknown_date["status"] == "partial"
    assert "DATE_UNKNOWN" in unknown_date["quality_flags"]
    legacy = _sanitize_legacy_cached_money_flow({
        "main_net_flow": 1.0,
        "super_net_flow": 0.2,
        "ddx_5": 8.0,
        "ddy_10": 12.0,
        "source": "cache_hot",
    })
    assert legacy["ddx_5"] is None and legacy["ddy_10"] is None
    assert "MONEY_FLOW_SEMANTICS_LEGACY" in legacy["quality_flags"]


def test_edge_rule_schema_isolates_legacy_money_semantics():
    from candidate_edge_rules import (
        EDGE_RULE_SCHEMA_VERSION,
        MONEY_FLOW_SEMANTICS_VERSION,
        load_latest_edge_rule_payloads,
    )

    with tempfile.TemporaryDirectory() as td:
        root = Path(td)
        rule_dir = root / "edge_rules"
        rule_dir.mkdir()
        legacy = {"schema_version": 1, "rules": [{"id": "old", "conditions": [{"field": "ddx_5", "op": "gt", "value": 1}]}]}
        (rule_dir / "weekly_edge_rules_latest.json").write_text(json.dumps(legacy), encoding="utf-8")
        assert load_latest_edge_rule_payloads(root) == {}
        current = {
            "schema_version": EDGE_RULE_SCHEMA_VERSION,
            "money_flow_semantics_version": MONEY_FLOW_SEMANTICS_VERSION,
            "data_as_of": date.today().strftime("%Y%m%d"),
            "rules": [{"id": "new", "conditions": [{"field": "main_net_flow", "op": "gt", "value": 0}]}],
        }
        (rule_dir / "weekly_edge_rules_latest.json").write_text(json.dumps(current), encoding="utf-8")
        assert load_latest_edge_rule_payloads(root)["weekly"]["rules"][0]["id"] == "new"


def test_kline_threshold_and_pool_relative_merge():
    from llm_scorer import _merge_candidate_sources
    from stock_selection_debate.data_fetcher import _kline_has_min_bars

    assert not _kline_has_min_bars([{"close": 1}] * 59)
    assert _kline_has_min_bars([{"close": 1}] * 60)
    merged = _merge_candidate_sources([
        {
            "stock": "000001", "name": "测试", "pool": "低分池", "screen_id": "low",
            "priority": 1, "pool_score": 20, "pool_rank": 20, "pool_scored_candidates": 20,
        },
        {
            "stock": "000001", "name": "测试", "pool": "高分池", "screen_id": "high",
            "priority": 99, "pool_score": 90, "pool_rank": 1, "pool_scored_candidates": 20,
        },
    ])[0]
    assert merged["pool"] == "高分池"
    assert merged["screen_id"] == "high"
    assert merged["pool_score"] > 70


def test_position_does_not_gate_top5():
    from stock_selection_debate.run_debate_phase import debate_phase_to_phase2_format

    result = debate_phase_to_phase2_format({
        "ranked_candidates": [
            {"stock": "000001", "signal": "WATCH", "buy_score": 80, "confidence": 70, "position_ratio": "0%"},
            {"stock": "000002", "signal": "BUY", "buy_score": 75, "confidence": 70, "position_ratio": "20%"},
        ]
    })
    assert [x["stock"] for x in result["top_picks"]][:2] == ["000001", "000002"]


def test_forward_attachment_clears_legacy_per_stock_backtest():
    import workflow

    phase2 = {
        "top_picks": [{
            "stock": "000001",
            "selection_backtest": {"status": "done", "return_pct": 10},
            "strategy_backtest": {"status": "backtested", "return_pct": 8},
        }]
    }
    selection = {
        "stocks": [{"stock": "000001", "status": "pending_forward_validation", "horizons": [1, 3, 5, 10]}]
    }
    workflow._attach_top_pick_backtests(phase2, selection, {"trades": []})
    pick = phase2["top_picks"][0]
    assert pick["selection_backtest"]["status"] == "pending_forward_validation"
    assert "strategy_backtest" not in pick


def test_node_checkpoint_resume():
    import stock_selection_debate.debate_engine as engine

    calls = []
    saved = {}

    def node(state):
        calls.append("called")
        out = dict(state)
        out["bull_history"] = "done"
        out["count"] = 1
        return out

    engine._NODE_CHECKPOINT_LOCAL.callback = lambda _code, key, snapshot: saved.setdefault(key, snapshot)
    wrapped = engine._checkpointed_node("invest", "bull_researcher", node)
    initial = {"stock_code": "000001", "stock_name": "测试", "count": 0, "_node_resume": {}}
    first = wrapped(initial)
    key = next(iter(saved))
    second = wrapped({**initial, "_node_resume": {key: saved[key]}})
    engine._NODE_CHECKPOINT_LOCAL.callback = None
    assert first["bull_history"] == "done"
    assert second["bull_history"] == "done"
    assert calls == ["called"]


def test_two_round_graph_and_full_node_resume():
    import stock_selection_debate.debate_engine as engine

    names = (
        "bull_researcher_node", "bear_researcher_node", "tech_analyst_node", "research_manager_node",
        "aggressive_analyst_node", "conservative_analyst_node", "neutral_analyst_node", "portfolio_manager_node",
    )
    originals = {name: getattr(engine, name) for name in names}
    calls = {name: 0 for name in names}

    def update(state, **values):
        out = dict(state)
        out.update(values)
        return out

    def bull(state):
        calls["bull_researcher_node"] += 1
        count = int(state.get("count", 0)) + 1
        return update(state, count=count, current_response="【多方分析师】mock", bull_history=f"bull{count}", history=f"h{count}")

    def bear(state):
        calls["bear_researcher_node"] += 1
        count = int(state.get("count", 0)) + 1
        return update(state, count=count, current_response="【空方分析师】mock", bear_history=f"bear{count}", history=f"h{count}")

    def tech(state):
        calls["tech_analyst_node"] += 1
        return update(state, tech_analyst_verdict="mock-tech", tech_signals_summary="mock-tech")

    def judge(state):
        calls["research_manager_node"] += 1
        return update(state, research_plan="mock-plan", current_response="mock-plan")

    def risk_node(name, speaker):
        def fn(state):
            calls[name] += 1
            count = int(state.get("count", 0)) + 1
            return update(state, count=count, latest_speaker=speaker, history=f"risk{count}")
        return fn

    def pm(state):
        calls["portfolio_manager_node"] += 1
        return update(
            state,
            signal="WATCH",
            buy_score=66,
            confidence=70,
            position_ratio=0.0,
            reason="mock",
            decision_source="mock",
            final_decision="mock",
            evidence_validation={"status": "pass"},
        )

    setattr(engine, "bull_researcher_node", bull)
    setattr(engine, "bear_researcher_node", bear)
    setattr(engine, "tech_analyst_node", tech)
    setattr(engine, "research_manager_node", judge)
    setattr(engine, "aggressive_analyst_node", risk_node("aggressive_analyst_node", "Aggressive"))
    setattr(engine, "conservative_analyst_node", risk_node("conservative_analyst_node", "Conservative"))
    setattr(engine, "neutral_analyst_node", risk_node("neutral_analyst_node", "Neutral"))
    setattr(engine, "portfolio_manager_node", pm)
    packet = {"stock_code": "000001", "name": "测试", "data_quality_flags": []}
    saved = {}
    try:
        first = engine.StockDebateEngine(model="mock", max_debate_rounds=2).run(
            [packet],
            max_parallel=1,
            node_checkpoint_cb=lambda _code, key, snapshot: saved.setdefault(key, snapshot),
        )
        assert first[0]["debate_rounds"] == 2
        assert calls["bull_researcher_node"] == 2
        assert calls["bear_researcher_node"] == 2
        assert calls["research_manager_node"] == 3
        assert calls["tech_analyst_node"] == 1
        assert len(saved) == 12

        for name in names:
            setattr(engine, name, lambda _state, _name=name: (_ for _ in ()).throw(AssertionError(f"node reran: {_name}")))
        resumed = engine.StockDebateEngine(model="mock", max_debate_rounds=2).run(
            [packet],
            max_parallel=1,
            resume_node_states={"000001": saved},
        )
        assert resumed[0]["signal"] == "WATCH"
    finally:
        for name, fn in originals.items():
            setattr(engine, name, fn)


def test_workflow_layered_shortlist_is_exactly_top15():
    import workflow

    packets = [
        {"stock_code": f"{i:06d}", "pool_score": float(i)}
        for i in range(20)
    ]
    two_round, one_round = workflow._split_layered_debate_packets(packets, list(reversed(packets)), 15)
    assert len(two_round) == 15
    assert len(one_round) == 5
    assert {x["stock_code"] for x in two_round} == {f"{i:06d}" for i in range(5, 20)}


def test_compact_report_and_stable_completion():
    import run_daily_stock_workflow_stable as stable
    import workflow

    report = {
        "date": date.today().isoformat(),
        "status": "success",
        "phase1": [{"name": "技术", "status": "success", "findings": "x" * 5000}],
        "phase2": {
            "ranked_candidates": [
                {
                    "stock": f"00000{i}", "name": "测试", "signal": "WATCH", "buy_score": 70 - i,
                    "confidence": 65, "position_ratio": "0%", "bull_history": "b" * 10000,
                    "bear_history": "s" * 10000,
                }
                for i in range(5)
            ],
            "top_picks": [
                {"stock": f"00000{i}", "name": "测试", "signal": "WATCH", "buy_score": 70 - i, "confidence": 65}
                for i in range(5)
            ],
        },
        "artifacts": {},
    }
    compact = workflow._compact_daily_report(report)
    encoded = json.dumps(compact, ensure_ascii=False)
    assert "bull_history" not in encoded
    assert len(encoded) < 20000

    with tempfile.TemporaryDirectory() as td:
        root = Path(td)
        artifacts = {}
        for name in ("candidates_jsonl", "trace_json", "summary_json"):
            p = root / f"{name}.json"
            p.write_text("{}", encoding="utf-8")
            artifacts[name] = str(p)
        compact["artifacts"] = artifacts
        path = root / f"daily_report_{stable._today()}.json"
        path.write_text(json.dumps(compact, ensure_ascii=False), encoding="utf-8")
        marker = root / "daily_report_push.json"
        original_marker_path = stable._push_marker_path
        try:
            stable._push_marker_path = lambda: marker
            payload = json.dumps(compact, ensure_ascii=False, sort_keys=True, default=str)
            marker.write_text(json.dumps({
                "date": date.today().isoformat(),
                "status": "success",
                "top_picks_count": 5,
                "report_digest": hashlib.sha256(payload.encode("utf-8")).hexdigest(),
            }), encoding="utf-8")
            assert stable._report_is_complete(path)
            compact["status"] = "failed"
            path.write_text(json.dumps(compact, ensure_ascii=False), encoding="utf-8")
            assert not stable._report_is_complete(path)
        finally:
            stable._push_marker_path = original_marker_path


def main():
    tests = [name for name in globals() if name.startswith("test_")]
    for name in sorted(tests):
        globals()[name]()
        print(f"PASS {name}")
    print(f"selection correctness tests passed: {len(tests)}")


if __name__ == "__main__":
    main()
