#!/usr/bin/env python3
"""Offline regressions for the 2026-07-17 three-batch morning-report pass."""

from __future__ import annotations

import json
import tempfile
from datetime import date, timedelta
from pathlib import Path


def test_llm_adjustment_is_residual_and_watch_never_adds():
    from stock_selection_debate.run_debate_phase import _llm_risk_adjustment

    candidate = {"llm_signal": "WATCH", "llm_buy_score": 95, "llm_confidence": 99}
    first, detail = _llm_risk_adjustment(candidate, 50, {"veto": False}, 95, 95, 95)
    second, _ = _llm_risk_adjustment(candidate, 50, {"veto": False}, 5, 5, 5)
    assert first == 0 and second == 0
    assert detail["mode"] == "independent_llm_residual"
    assert detail["duplicate_quant_inputs_ignored"] == ["tech", "pool", "money_flow", "next_day_buyability"]


def test_knowledge_rules_ignore_duplicate_positive_and_keep_risk():
    from stock_selection_debate.run_debate_phase import _knowledge_rule_adjustment

    duplicate = {
        "knowledge_rule_hits": [{"rule_id": "dup", "effect": 6, "evidence_fields": ["indicators.rsi_14"]}],
        "knowledge_rule_score_adjustment": 6,
    }
    independent = {
        "knowledge_rule_hits": [{"rule_id": "ind", "effect": 7, "evidence_fields": ["verified_event.catalyst"]}],
    }
    risk = {
        "knowledge_rule_hits": [{"rule_id": "risk", "effect": -12, "evidence_fields": ["financial.debt_ratio"]}],
    }
    assert _knowledge_rule_adjustment(duplicate, {})[0] == 0
    assert _knowledge_rule_adjustment(independent, {})[0] == 2
    assert _knowledge_rule_adjustment(risk, {})[0] == -8


def test_consistency_repair_demotes_blocked_buy_and_rebuilds_gate():
    from stock_selection_debate.run_debate_phase import validate_and_repair_phase2_consistency

    row = {
        "stock": "000001",
        "signal": "BUY",
        "final_signal": "BUY",
        "action": "BUY",
        "pm_signal": "BUY",
        "pm_reason": "机会尚可",
        "reason": "机会尚可",
        "signal_blockers": ["核心数据不足"],
        "execution_gate": "DIRECT_BUY_ALLOWED",
        "allow_direct_buy": True,
    }
    top_picks = []
    for index in range(5):
        pick = dict(row)
        pick["stock"] = f"00000{index + 1}"
        top_picks.append(pick)
    phase2 = {"ranked_candidates": [row], "top_picks": top_picks}
    result = validate_and_repair_phase2_consistency(phase2)
    repaired = phase2["ranked_candidates"][0]
    assert result["publishable"] is True
    assert repaired["signal"] == "WATCH"
    assert repaired["execution_gate"] == "INTRADAY_CONFIRMATION_REQUIRED"
    assert repaired["allow_direct_buy"] is False
    assert "BUY" in repaired["final_reason"] and "WATCH" in repaired["final_reason"]


def test_dynamic_chase_limit_is_hard_cap():
    import stock_selection_debate.run_debate_phase as mod

    original = mod._EDGE_RULE_PAYLOAD_CACHE
    try:
        mod._EDGE_RULE_PAYLOAD_CACHE = {
            "weekly": {"chase_policy": {"status": "active", "high_chase_limit": 0, "reason": "测试保护"}}
        }
        rows = []
        for i in range(4):
            rows.append({
                "stock": f"30000{i}", "name": f"追高{i}", "pool": "涨停强势池",
                "signal": "BUY", "buy_score": 95 - i, "confidence": 70, "reason": "追高",
            })
        for i in range(6):
            rows.append({
                "stock": f"60000{i}", "name": f"低吸{i}", "pool": "低吸池",
                "signal": "WATCH", "buy_score": 80 - i, "confidence": 65, "reason": "低吸",
            })
        phase2 = mod.debate_phase_to_phase2_format({"ranked_candidates": rows})
        assert len(phase2["top_picks"]) == 5
        assert all("涨停" not in str(row.get("pool")) for row in phase2["top_picks"])
        assert phase2["top5_policy"]["selected_high_chase_count"] == 0
    finally:
        mod._EDGE_RULE_PAYLOAD_CACHE = original


def test_data_quality_separates_core_aux_and_freshness():
    from stock_selection_debate.run_debate_phase import _summarize_data_quality

    rows = [{
        "stock": "000001",
        "name": "测试",
        "money_flow": {"source": "mx-data", "main_net_flow": 1.2, "super_net_flow": 0.2},
        "data_contract": {
            "kline": {"status": "ok", "source": "xqshare", "as_of": date.today().strftime("%Y%m%d")},
            "money_flow": {
                "status": "partial", "source": "mx-data", "as_of": date.today().strftime("%Y%m%d"),
                "field_status": {"main_net_flow": "ok", "super_net_flow": "ok", "ddx_5": "missing", "ddy_10": "missing"},
            },
            "financial": {"status": "partial", "source": "xqshare", "quality_flags": ["DATE_UNKNOWN"]},
            "sector": {"status": "ok", "source": "sector_cache", "is_stale": True},
            "news": {"status": "checked_fresh_no_recent_items", "source": "mx-search", "content_is_old": True},
        },
    }]
    summary = _summarize_data_quality(rows)
    assert summary["core_affected_count"] == 0
    assert summary["aux_affected_count"] == 1
    assert summary["freshness_affected_count"] == 1
    assert summary["flag_counts"]["MONEY_FLOW_DDX_MISSING"] == 1
    assert summary["flag_counts"]["MONEY_FLOW_DDY_MISSING"] == 1
    assert summary["flag_counts"]["FINANCIAL_DATE_UNKNOWN"] == 1
    assert summary["flag_counts"]["SECTOR_STALE"] == 1
    assert summary["flag_counts"]["NEWS_NO_RECENT_ITEMS"] == 1


def test_ddx_ddy_alias_and_row_oriented_parser():
    from stock_selection_debate.data_fetcher import _parse_ddx_ddy_tables

    keyed = [{"rows": [{"5日DDX": "0.123", "DDY10": "-0.456"}]}]
    assert _parse_ddx_ddy_tables(keyed)[:2] == (0.123, -0.456)
    row_oriented = [{"rows": [
        {"指标": "5日DDX", "数值": "0.88"},
        {"指标": "10日DDY", "数值": "-0.12"},
    ]}]
    assert _parse_ddx_ddy_tables(row_oriented)[:2] == (0.88, -0.12)
    daily_only = [{"rows": [{"当日DDX": "0.66", "当日DDY": "-0.31"}]}]
    assert _parse_ddx_ddy_tables(daily_only)[:2] == (None, None)


def test_sector_cache_freshness_and_news_dual_dates():
    import stock_selection_debate.data_fetcher as fetcher
    from stock_selection_debate.data_router import normalize_contract_item

    with tempfile.TemporaryDirectory() as td:
        original = fetcher._sector_cache_path
        cache_path = Path(td) / "sector_cache.json"
        try:
            fetcher._sector_cache_path = lambda: cache_path
            cache_path.write_text(json.dumps({
                "000001": {"sector": "电子", "updated": date.today().strftime("%Y%m%d")},
                "000002": {"sector": "旧行业", "updated": (date.today() - timedelta(days=45)).strftime("%Y%m%d")},
            }), encoding="utf-8")
            assert fetcher._get_fresh_cached_sector("000001")[0] == "电子"
            assert fetcher._get_fresh_cached_sector("000002")[0] == ""
        finally:
            fetcher._sector_cache_path = original

    contract = normalize_contract_item("news", {
        "source": "mx-search",
        "status": "ok",
        "checked_at": date.today().strftime("%Y%m%d"),
        "content_as_of": (date.today() - timedelta(days=10)).strftime("%Y%m%d"),
    })
    assert contract["is_stale"] is False
    assert contract["content_is_old"] is True
    assert contract["status"] == "checked_fresh_no_recent_items"


def test_phase1_financial_fetch_preserves_real_report_period():
    import urllib.request
    import llm_scorer

    class Response:
        def __init__(self, payload):
            self.payload = payload

        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return False

        def read(self):
            return json.dumps(self.payload).encode("utf-8")

    original = urllib.request.urlopen
    try:
        def fake_urlopen(request, timeout=0):
            url = getattr(request, "full_url", str(request))
            if "financial_data" in url:
                return Response({
                    "success": True,
                    "data": {"000001.SZ": {"PERSHAREINDEX": {
                        "columns": ["m_timetag", "equity_roe", "inc_revenue_rate", "inc_net_profit_rate", "gear_ratio", "s_fa_eps_basic", "s_fa_bps"],
                        "rows": [
                            ["20251231", 10, 8, 9, 40, 1, 5],
                            ["20260331", 2.5, 6, 7, 41, 0.3, 5.2],
                        ],
                    }}},
                })
            return Response({"success": True, "data": {"close": {
                "20260716": {"000001.SZ": 10.0},
            }}})

        urllib.request.urlopen = fake_urlopen
        result = llm_scorer._fetch_financial_via_xtquant("000001")
    finally:
        urllib.request.urlopen = original
    assert result["_as_of"] == "20260331"
    assert result["_annual_as_of"] == "20251231"
    assert result["_source"] == "xqshare"


def test_model_summary_counts_stock_coverage_not_calls():
    import workflow

    rows = [
        {"stock": "000001", "decision_models": {"bull": "volcengine-plan/ark-code-latest", "bear": "volcengine-plan/ark-code-latest", "pm": "openai/gpt-5.6-sol"}},
        {"stock": "000002", "decision_models": {"bull": "volcengine-plan/ark-code-latest", "bear": "minimax-portal/MiniMax-M3", "pm": "minimax-portal/MiniMax-M3"}},
    ]
    summary = workflow._build_model_execution_summary(rows, {"reused_stocks": ["000001"]})
    assert summary["analysis_stock_coverage"]["火山 Coding Plan"] == 2
    assert summary["pm_stock_coverage"]["GPT-5.6 Sol"] == 1
    assert summary["pm_stock_coverage"]["MiniMax M3"] == 1
    assert summary["checkpoint_reused_count"] == 1
    assert "×" not in summary["model"]


def test_card_render_is_snapshot_only_and_top5_sections_are_complete():
    import workflow

    captured = {}
    original_push = workflow.feishu_push_card
    original_summary = workflow._summarize_news_one_line
    original_loader = workflow._load_mx_data_class
    try:
        workflow.feishu_push_card = lambda card, _webhook: captured.setdefault("card", card) or True
        workflow._summarize_news_one_line = lambda *_a, **_k: (_ for _ in ()).throw(AssertionError("render called LLM"))
        workflow._load_mx_data_class = lambda: (_ for _ in ()).throw(AssertionError("render called mx-data"))
        pick = {
            "stock": "000001", "name": "测试股份", "signal": "WATCH", "buy_score": 75.3,
            "confidence": 62, "global_rank": 7, "pm_signal": "BUY", "pm_reason": "原始理由",
            "final_reason": "量化保护后等待盘中确认，理由必须完整显示到句末。", "reason": "量化保护后等待盘中确认，理由必须完整显示到句末。",
            "top5_selection_reason": "动态追高上限与行业分散后入选", "execution_gate": "INTRADAY_CONFIRMATION_REQUIRED",
            "entry_condition": "1分钟放量突破并确认承接", "decision_models": {"pm": "openai/gpt-5.6-sol"},
        }
        phase2 = {
            "phase": "route_b_complete", "ranked_candidates": [pick], "top_picks": [pick],
            "report_render_snapshot": {
                "prices": {"000001": {"close": 10, "prev_close": 9.8, "pct": 2.04}},
                "market": {"sh_pct": 0.5, "hs300_pct": 0.2, "total_turnover": "1.2万亿"},
                "news_sentiment_summary": "快照新闻研判", "sector_rotation": {"强势板块": ["电子"], "弱势板块": ["煤炭"]},
                "render_network_allowed": False,
            },
            "data_quality_summary": {"candidate_count": 1, "core_complete_count": 1, "money_flow_core_complete_count": 1, "money_flow_aux_complete_count": 1},
            "top5_policy": {"high_chase_limit": 1},
        }
        phase1 = [{"name": "新闻分析师", "findings": "1. 今日有效新闻标题"}]
        assert workflow._send_daily_report_card(phase1, phase2, {}, {}, "mock", exec_stats={"total": 1, "model": "基金经理 GPT-5.6 Sol 1/1只"})
    finally:
        workflow.feishu_push_card = original_push
        workflow._summarize_news_one_line = original_summary
        workflow._load_mx_data_class = original_loader
    text = "\n".join(
        item.get("text", {}).get("content", "")
        for item in captured["card"]["elements"]
        if item.get("tag") == "div"
    )
    assert "Top1  000001 测试股份" in text
    assert "最终信号: **WATCH**" in text
    assert "全局排名: **#7/1**" in text
    assert "量化保护后等待盘中确认，理由必须完整显示到句末。" in text
    assert "快照新闻研判" in text


def test_compact_report_stays_within_budget_and_keeps_artifact_pointer():
    import workflow

    rows = []
    for i in range(80):
        rows.append({
            "stock": f"60{i:04d}", "name": "测试", "signal": "WATCH", "buy_score": 60,
            "confidence": 60, "reason": "理由" * 2000, "debate_history": "辩论" * 5000,
            "quant_score_detail": {"tech_detail": "细节" * 5000},
        })
    report = {
        "date": "2026-07-17",
        "artifacts": {"candidates_jsonl": "/tmp/daily_candidates_20260717.jsonl"},
        "phase1": [{"name": "新闻分析师", "findings": "新闻" * 10000}],
        "phase2": {"ranked_candidates": rows, "top_picks": rows[:5]},
    }
    compact = workflow._compact_daily_report(report)
    assert workflow._report_json_size(compact) <= 300 * 1024
    assert compact["phase2"]["candidate_details_artifact"].endswith("daily_candidates_20260717.jsonl")


def test_negative_rules_and_dynamic_chase_policy_have_sample_guards():
    from candidate_edge_rules import build_chase_policy, discover_negative_rules

    rows = []
    for day in range(10):
        report_date = f"202606{day + 1:02d}"
        for i in range(5):
            rows.append({"report_date": report_date, "stock": f"3{day:02d}{i:03d}", "pool": "涨停强势池", "d5_return_pct": -8.0})
            rows.append({"report_date": report_date, "stock": f"6{day:02d}{i:03d}", "pool": "低吸池", "d5_return_pct": 5.0})
    rules = discover_negative_rules(rows, min_samples=20, min_days=5, max_rules=5)
    assert rules
    assert all((rule.get("metrics") or {}).get("sample_count", 0) >= 20 for rule in rules)
    assert all((rule.get("metrics") or {}).get("trade_days", 0) >= 5 for rule in rules)
    assert all((rule.get("anti_overfit") or {}).get("bayesian_shrinkage") is True for rule in rules)
    chase = build_chase_policy(rows, {"avg": -1.5, "win_rate": 50})
    assert chase["status"] == "active"
    assert chase["high_chase_limit"] in {0, 1, 2}


if __name__ == "__main__":
    for name, value in sorted(globals().copy().items()):
        if name.startswith("test_") and callable(value):
            value()
    print("three-batch morning-report tests passed")
