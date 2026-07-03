#!/usr/bin/env python3
"""Audit tests for stock-selection debate workflow edge cases."""

import builtins
import importlib
import io
import json
import os
import py_compile
import sys
import tempfile
from datetime import time as dt_time
from pathlib import Path

WF = Path(__file__).resolve().parent
sys.path.insert(0, str(WF))

results = []


def record(name, ok, detail=""):
    results.append({"name": name, "ok": bool(ok), "detail": detail})
    status = "PASS" if ok else "FAIL"
    print(f"[{status}] {name}: {detail}")


def compile_check():
    files = [
        WF / "workflow.py",
        WF / "llm_scorer.py",
        WF / "stock_selection_debate" / "providers.py",
        WF / "stock_selection_debate" / "debate_engine.py",
        WF / "stock_selection_debate" / "run_debate_phase.py",
        WF / "execute_debate_result.py",
        WF / "intraday_executor.py",
        WF / "position_debate" / "run_position_debate.py",
        WF / "position_debate" / "nodes.py",
        WF / "position_debate" / "graph.py",
    ]
    bad = []
    for file_path in files:
        try:
            py_compile.compile(str(file_path), doraise=True)
        except Exception as exc:
            bad.append(f"{file_path.name}: {exc}")
    record("Python 编译检查", not bad, "; ".join(bad) if bad else f"{len(files)} 个核心文件可编译")


def json_parser_checks():
    providers = importlib.import_module("stock_selection_debate.providers")

    fenced = '前缀\n```json\n{"signal":"BUY","confidence":72,"position_ratio":0.25,"reason":"趋势确认"}\n```\n后缀'
    parsed = providers.extract_json_object(
        fenced,
        required_keys={"signal", "confidence", "position_ratio", "reason"},
    )
    record("JSON fenced 块解析", parsed and parsed["signal"] == "BUY", str(parsed))

    nested = '{"outer":{"x":1}}\n真正结果 {"signal":"AVOID","confidence":80,"position_ratio":0,"reason":"风险过大"}'
    parsed_nested = providers.extract_json_object(
        nested,
        required_keys={"signal", "confidence", "position_ratio", "reason"},
    )
    record("JSON 多对象择优解析", parsed_nested and parsed_nested["signal"] == "AVOID", str(parsed_nested))

    eq_fragment = "signal=BUY confidence=73 position_ratio=25% reason=趋势确认但注意回撤"
    parsed_eq = providers.extract_json_object(
        eq_fragment,
        required_keys={"signal", "confidence", "position_ratio", "reason"},
    )
    record("等号碎片 JSON 解析", parsed_eq is not None, f"parsed={parsed_eq!r}")

    colon_percent = 'signal: BUY confidence: 73 position_ratio: 25% reason: "趋势确认但注意回撤"'
    parsed_pct = providers.extract_json_object(
        colon_percent,
        required_keys={"signal", "confidence", "position_ratio", "reason"},
    )
    ratio_ok = bool(parsed_pct and 0 <= float(parsed_pct["position_ratio"]) <= 1)
    record("百分比仓位归一化", ratio_ok, f"parsed={parsed_pct!r}")


def model_routing_checks():
    providers = importlib.import_module("stock_selection_debate.providers")
    debate_engine = importlib.import_module("stock_selection_debate.debate_engine")

    calls = []
    original = debate_engine._call_llm_with_fallback

    def fake_call(**kwargs):
        calls.append(kwargs)
        return "ok"

    try:
        debate_engine._call_llm_with_fallback = fake_call
        debate_engine._call_role("sys", "prompt", model="minimax-portal/MiniMax-M3", timeout=12)
        passed_model = calls[-1].get("model")
        record("_call_role 透传模型参数", passed_model == "minimax-portal/MiniMax-M3", f"实际传参={calls[-1]}")
        role_budget_ok = (
            calls[-1].get("thinking_budget") == providers.THINKING_BUDGET_VOLCAN
            and calls[-1].get("fallback_thinking_budget") == providers.THINKING_BUDGET_MINIMAX
            and providers.THINKING_BUDGET_MINIMAX > 0
        )
        record("辩论角色 thinking 设置", role_budget_ok, f"实际传参={calls[-1]}")
    finally:
        debate_engine._call_llm_with_fallback = original

    old_map = providers._PROVIDER_MAP
    old_open = builtins.open
    try:
        providers._PROVIDER_MAP = {"volcengine-plan": {"apiKey": ""}}

        def fake_open(path, *args, **kwargs):
            if str(path).endswith("auth-profiles.json"):
                return io.StringIO(json.dumps({
                    "profiles": {
                        "minimax-portal:default": {"key": "m" * 32}
                    }
                }))
            return old_open(path, *args, **kwargs)

        builtins.open = fake_open
        key = providers._get_api_key("volcengine-plan")
        record("火山 provider 不误取 MiniMax key", key != "m" * 32, f"返回key前缀={key[:6] if key else ''}")
    finally:
        providers._PROVIDER_MAP = old_map
        builtins.open = old_open

    record("火山 thinking 预算为 high 档", providers.THINKING_BUDGET_VOLCAN == 16000 and providers.THINKING_BUDGET_MINIMAX == 8000, f"volc={providers.THINKING_BUDGET_VOLCAN} minimax={providers.THINKING_BUDGET_MINIMAX}")


def generation_config_checks():
    providers = importlib.import_module("stock_selection_debate.providers")
    debate_engine = importlib.import_module("stock_selection_debate.debate_engine")

    prompt = debate_engine.PORTFOLIO_MANAGER_PROMPT
    no_fence = "```" not in prompt
    record("PM prompt 不含 markdown JSON fence", no_fence, f"fence_count={prompt.count('```')}")
    record(
        "PM MiniMax 兜底后不再继续切火山",
        debate_engine.PORTFOLIO_MANAGER_SECONDARY_MODEL == "minimax-portal/MiniMax-M3"
        and debate_engine.PORTFOLIO_MANAGER_SECONDARY_FALLBACK_MODEL == "",
        f"secondary={debate_engine.PORTFOLIO_MANAGER_SECONDARY_MODEL} fallback={debate_engine.PORTFOLIO_MANAGER_SECONDARY_FALLBACK_MODEL!r}",
    )
    record(
        "PM MiniMax 兜底优先 structured JSON 且保留 thinking预算",
        debate_engine._is_minimax_model(debate_engine.PORTFOLIO_MANAGER_SECONDARY_MODEL)
        and debate_engine.PORTFOLIO_MANAGER_MINIMAX_BUDGET > 0,
        f"secondary={debate_engine.PORTFOLIO_MANAGER_SECONDARY_MODEL} budget={debate_engine.PORTFOLIO_MANAGER_MINIMAX_BUDGET}",
    )

    captured = []

    class FakeResp:
        def __enter__(self):
            return self

        def __exit__(self, *args):
            return False

        def read(self):
            payload = {
                "choices": [{
                    "message": {
                        "content": '{"signal":"WATCH","confidence":50,"position_ratio":0.15,"reason":"ok"}'
                    }
                }],
                "content": [{
                    "type": "text",
                    "text": '{"signal":"WATCH","confidence":50,"position_ratio":0.15,"reason":"ok"}'
                }],
            }
            return json.dumps(payload).encode()

    def fake_urlopen(req, timeout=None):
        captured.append(json.loads(req.data.decode()))
        return FakeResp()

    old_urlopen = providers.urllib.request.urlopen
    old_get_key = providers._get_api_key
    try:
        providers.urllib.request.urlopen = fake_urlopen
        providers._get_api_key = lambda provider: "x" * 32
        providers._call_structured_volcengine("prompt", providers.PortfolioManagerOutput, 1, 15000, 1500)
        providers._call_structured_minimax("prompt", providers.PortfolioManagerOutput, 1, 15000, 1500)
    finally:
        providers.urllib.request.urlopen = old_urlopen
        providers._get_api_key = old_get_key

    volc, minimax = captured
    record("火山 structured 使用 response_format", volc.get("response_format") == {"type": "json_object"}, str(volc.get("response_format")))
    record("structured 低温输出", volc.get("temperature") == 0 and minimax.get("temperature") == 0, f"volc={volc.get('temperature')} minimax={minimax.get('temperature')}")
    record("structured 禁用 thinking 参数", "thinking" not in volc and "thinking" not in minimax, f"volc={volc.get('thinking')} minimax={minimax.get('thinking')}")


def portfolio_output_checks():
    providers = importlib.import_module("stock_selection_debate.providers")
    debate_engine = importlib.import_module("stock_selection_debate.debate_engine")
    run_debate_phase = importlib.import_module("stock_selection_debate.run_debate_phase")

    original_structured = providers.call_structured
    original_llm = providers.call_llm
    try:
        providers.call_structured = lambda *a, **k: providers.PortfolioManagerOutput(
            signal="BUY",
            confidence=76,
            position_ratio=0.25,
            reason="结构化输出测试通过",
        )
        providers.call_llm = lambda *a, **k: ""
        state = {
            "stock_name": "测试股份",
            "research_plan": "偏多",
            "history": "风险讨论",
            "signal": "WATCH",
            "confidence": 50,
        }
        out = debate_engine.portfolio_manager_node(state)
        has_ratio = "position_ratio" in out
        record("PortfolioManager 结果保留 position_ratio 字段", has_ratio, f"输出keys={sorted(out.keys())}")
        record("PortfolioManager final_decision 带仓位文本", "position_ratio=25%" in out.get("final_decision", ""), out.get("final_decision", ""))
    finally:
        providers.call_structured = original_structured
        providers.call_llm = original_llm

    debate_result = {
        "ranked_candidates": [{
            "stock": "000001",
            "name": "平安银行",
            "signal": "BUY",
            "confidence": 76,
            "final_decision": "[Structured] signal=BUY confidence=76 position_ratio=25% reason=测试",
        }]
    }
    phase2 = run_debate_phase.debate_phase_to_phase2_format(debate_result)
    top = phase2["top_picks"][0]
    record("phase2 top_picks 保留结构化仓位", top.get("position_ratio") == "25%", f"top_picks.position_ratio={top.get('position_ratio')!r}")

    original_openai_structured = providers._call_structured_openai_responses
    try:
        openai_calls = []

        def fake_openai_structured(*args, **kwargs):
            openai_calls.append(kwargs.get("prompt") or (args[1] if len(args) > 1 else ""))
            if len(openai_calls) <= 3:
                return None
            return {
                "signal": "WATCH",
                "buy_score": 63,
                "confidence": 63,
                "position_ratio": 0.15,
                "reason": "GPT-5.5空结构化结果修复成功。",
            }

        providers._call_structured_openai_responses = fake_openai_structured
        debate_engine._pm_gpt55_broken = False
        debate_engine._secondary_broken = False
        structured, source = debate_engine._call_portfolio_manager_structured(
            "原始基金经理事实材料",
            debate_engine._call_structured,
            providers.PortfolioManagerOutput,
            "测试股份",
        )
        repair_ok = (
            source == "Structured:GPT-5.5Repair"
            and structured.signal == "WATCH"
            and len(openai_calls) == 4
            and "没有返回可解析 JSON" in openai_calls[-1]
        )
        record("PM GPT-5.5空结构化结果可自修复一次", repair_ok, f"source={source} calls={len(openai_calls)}")
    finally:
        providers._call_structured_openai_responses = original_openai_structured
        debate_engine._pm_gpt55_broken = False
        debate_engine._secondary_broken = False

    original_structured = providers.call_structured
    original_llm = providers.call_llm
    try:
        def repair_call(*args, **kwargs):
            return providers.PortfolioManagerOutput(
                signal="WATCH",
                confidence=66,
                position_ratio=0.2,
                reason="纯文本裁决修复成功",
            )

        text_calls = []

        def text_call(*args, **kwargs):
            text_calls.append((args, kwargs))
            return "最终信号: WATCH\n做多分: 66\n置信度: 66\n新开仓仓位上限: 20%\n核心理由: 纯文本裁决解析成功"

        providers.call_structured = repair_call
        providers.call_llm = text_call
        state = {
            "stock_name": "测试股份",
            "research_plan": "偏中性",
            "history": "风险讨论",
            "signal": "WATCH",
            "confidence": 50,
        }
        out = debate_engine.portfolio_manager_node(state)
        structured_ok = (
            out.get("decision_source") == "Structured:minimax-portal/MiniMax-M3"
            and out.get("position_ratio") == 0.2
            and not text_calls
        )
        record("PM MiniMax structured重试成功后不落文本", structured_ok, f"source={out.get('decision_source')} ratio={out.get('position_ratio')} text_calls={len(text_calls)}")
    finally:
        providers.call_structured = original_structured
        providers.call_llm = original_llm

    original_structured = providers.call_structured
    original_llm = providers.call_llm
    try:
        text_calls = []

        def text_call(*args, **kwargs):
            text_calls.append((args, kwargs))
            return "最终信号: WATCH\n做多分: 66\n置信度: 66\n新开仓仓位上限: 20%\n核心理由: 纯文本裁决解析成功"

        providers.call_structured = lambda *a, **k: None
        providers.call_llm = text_call
        state = {
            "stock_name": "测试股份",
            "research_plan": "偏中性",
            "history": "风险讨论",
            "signal": "WATCH",
            "confidence": 50,
        }
        out = debate_engine.portfolio_manager_node(state)
        thinking_budget = text_calls[-1][1].get("thinking_budget") if text_calls else None
        text_ok = (
            out.get("decision_source") == "MiniMaxThinkingText"
            and out.get("position_ratio") == 0.2
            and thinking_budget == debate_engine.PORTFOLIO_MANAGER_MINIMAX_BUDGET
        )
        record("PM structured失败后MiniMax thinking文本仍可解析", text_ok, f"source={out.get('decision_source')} ratio={out.get('position_ratio')} thinking_budget={thinking_budget}")
    finally:
        providers.call_structured = original_structured
        providers.call_llm = original_llm

    original_structured = providers.call_structured
    original_llm = providers.call_llm
    try:
        providers.call_structured = lambda *a, **k: None
        providers.call_llm = lambda *a, **k: "这只股票风险较大，建议谨慎观察。"
        state = {
            "stock_name": "测试股份",
            "research_plan": "偏空",
            "history": "风险讨论",
            "signal": "WATCH",
            "confidence": 50,
        }
        out = debate_engine.portfolio_manager_node(state)
        ok = out.get("decision_source") == "TextOnly" and out.get("position_ratio") == 0.0 and out.get("confidence") == 0
        record("修复失败转 TextOnly 且仓位为0", ok, f"source={out.get('decision_source')} conf={out.get('confidence')} ratio={out.get('position_ratio')}")
    finally:
        providers.call_structured = original_structured
        providers.call_llm = original_llm


def intraday_parse_checks():
    intraday = importlib.import_module("intraday_executor")
    signal = {
        "signal": "BUY",
        "final_decision": "[Structured] signal=BUY confidence=76 position_ratio=25% reason=测试",
    }
    pct = intraday._get_position_pct(signal)
    record("盘中执行解析 position_ratio", abs(pct - 0.25) < 1e-9, f"pct={pct}")

    no_ratio = {"signal": "BUY", "final_decision": "[Structured] signal=BUY confidence=76 reason=测试"}
    pct2 = intraday._get_position_pct(no_ratio)
    record("盘中执行缺仓位时不买入", pct2 == 0.0, f"pct={pct2}")

    report = {
        "phase2": {
            "top_picks": [
                {"stock": "000001", "name": "早报一", "signal": "BUY", "total_score": 70, "position_ratio": "20%"},
                {"stock": "000001", "name": "早报一重复", "signal": "BUY", "total_score": 69, "position_ratio": "20%"},
                {"stock": "000003", "name": "K线缺失", "signal": "BUY", "total_score": 90, "position_ratio": "20%", "data_quality_flags": ["KLINE_MISSING"]},
                {"stock": "000002", "name": "早报二", "action": "WATCH", "total_score": 82, "position_ratio": "25%"},
            ],
            "ranked_candidates": [
                {"stock": "999999", "name": "高分误入", "signal": "BUY", "confidence": 99, "position_ratio": "20%"},
            ],
        }
    }
    selected = intraday._select_intraday_buy_signals(report)
    ok = (
        [s.get("stock") for s in selected] == ["000001", "000003", "000002"]
        and selected[0].get("confidence") == 70
        and selected[2].get("signal") == "WATCH"
        and selected[2].get("action") == "WATCH"
    )
    record("盘中买入优先读取早报top_picks", ok, str(selected))

    record("盘中买入top_picks去重且不因早报K线缺口踢出", [s.get("stock") for s in selected] == ["000001", "000003", "000002"], str(selected))

    ranked_only_report = {
        "phase2": {
            "ranked_candidates": [
                {"stock": "999999", "name": "非Top5候选", "signal": "BUY", "confidence": 99, "position_ratio": "20%"},
            ]
        }
    }
    timing_pool = intraday._select_intraday_timing_pool(ranked_only_report, 5)
    legacy_pool = intraday._select_intraday_buy_signals(ranked_only_report, 5)
    record(
        "分时买入实盘池必须有早报Top5不回退ranked_candidates",
        timing_pool == [] and [s.get("stock") for s in legacy_pool] == ["999999"],
        f"timing={timing_pool} legacy={legacy_pool}",
    )

    flexible_report = {
        "phase2": {
            "top_picks": [
                {"stock": "000010", "name": "小写信号", "signal": "buy", "confidence": "82分"},
                {"stock": "000011", "name": "中文冒号", "final_decision": "**最终信号**：WATCH\n**置信度**：68分"},
                {"stock": "000012", "name": "英文结构", "final_decision": "[Structured] signal=BUY confidence=76 position_ratio=20% reason=测试"},
                {"stock": "000013", "name": "规避", "final_decision": "最终信号：AVOID\n置信度：90"},
            ]
        }
    }
    flexible_selected = intraday._select_intraday_buy_signals(flexible_report)
    flexible_ok = (
        [s.get("stock") for s in flexible_selected] == ["000010", "000011", "000012"]
        and flexible_selected[0].get("signal") == "BUY"
        and flexible_selected[0].get("confidence") == 82
        and intraday._confidence_value(flexible_selected[0]) == 82
        and flexible_selected[1].get("signal") == "WATCH"
        and flexible_selected[1].get("confidence") == 68
        and intraday._confidence_value(flexible_selected[1]) == 68
        and flexible_selected[2].get("signal") == "BUY"
        and flexible_selected[2].get("confidence") == 76
        and intraday._confidence_value(flexible_selected[2]) == 76
    )
    record("盘中买入解析早报大小写中文冒号和文本置信值", flexible_ok, str(flexible_selected))

    order_map = intraday._build_buy_order_map([
        {"stock": "000001", "name": "一", "order_price": 10.0, "trade_price": 10.0, "quantity": 100, "order_quantity": 100, "order_time": "2026-05-19T09:31:01"},
        {"stock": "000001", "name": "一", "order_price": 10.1, "trade_price": 10.2, "quantity": 200, "order_quantity": 200, "order_time": "2026-05-19T09:31:02"},
    ])
    merged = order_map.get("000001", {})
    ok_map = (
        merged.get("quantity") == 300
        and merged.get("order_quantity") == 300
        and merged.get("duplicate_order_count") == 2
        and abs(float(merged.get("trade_price")) - 10.1333) < 0.001
    )
    record("盘中成交回查同股多笔汇总不覆盖", ok_map, str(merged))


def candidate_source_checks():
    llm_scorer = importlib.import_module("llm_scorer")
    run_debate_phase = importlib.import_module("stock_selection_debate.run_debate_phase")
    intraday = importlib.import_module("intraday_executor")

    startup = next(c for c in llm_scorer.XUANGU_SCREEN_CONFIGS if c["pool"] == "准备启动")
    capital = next(c for c in llm_scorer.XUANGU_SCREEN_CONFIGS if c["pool"] == "资金异动")
    safe_name = llm_scorer._xuangu_safe_filename(startup["query"])
    record(
        "mx-xuangu CSV 文件名映射保留筛选来源",
        " " not in safe_name and "准备启动" not in safe_name and safe_name.startswith("A股_5日均线") and "20日均线" in safe_name,
        safe_name,
    )
    record(
        "准备启动和资金异动策略配置已对调",
        startup.get("strategy_type") == "capital_absorption_dip"
        and capital.get("strategy_type") == "startup_dip"
        and "5日均线" in startup.get("query", "")
        and "10日主力资金净流入" in startup.get("query", "")
        and "成交量放量" in startup.get("query", ""),
        f"startup={startup}; capital={capital}",
    )

    merged = llm_scorer._merge_candidate_sources([
        {
            "stock": "000001",
            "name": "测试股",
            "pool": startup["pool"],
            "screen_id": startup["screen_id"],
            "strategy_type": startup["strategy_type"],
            "entry_bias": startup["entry_bias"],
            "query": startup["query"],
            "reason": "[准备启动]资金净流入",
            "priority": startup["priority"],
        },
        {
            "stock": "000001",
            "name": "测试股",
            "pool": capital["pool"],
            "screen_id": capital["screen_id"],
            "strategy_type": capital["strategy_type"],
            "entry_bias": capital["entry_bias"],
            "query": capital["query"],
            "reason": "[资金异动]下跌吸筹",
            "priority": capital["priority"],
        },
        {
            "stock": "000002",
            "name": "测试二",
            "pool": "突破新高",
            "strategy_type": "momentum_breakout",
            "entry_bias": "趋势确认后可小幅追随",
            "reason": "[突破新高]放量",
        },
        {
            "stock": "920178",
            "name": "北交测试",
            "pool": "热点龙头",
            "strategy_type": "sector_leader",
            "entry_bias": "强势板块",
            "reason": "[热点龙头]北交所",
        },
    ])
    first = next(c for c in merged if c["stock"] == "000001")
    merge_ok = (
        len(merged) == 2
        and first.get("source_pools") == ["准备启动", "资金异动"]
        and set(first.get("strategy_types", [])) == {"startup_dip", "capital_absorption_dip"}
        and "低吸" in "；".join(first.get("entry_biases", []))
    )
    record("第一阶段同股候选合并并保留全部来源", merge_ok, str(first))

    sig = llm_scorer._screening_signature()
    changed = llm_scorer._screening_signature([{**startup, "query": startup["query"] + " 测试变更"}])
    record("筛选条件签名可识别条件变更", sig != changed, f"sig={sig} changed={changed}")

    parse_ok = (
        llm_scorer._parse_cn_number("3.20亿") == 320000000
        and llm_scorer._parse_cn_number("4800万|2026-05-18") == 48000000
        and llm_scorer._parse_cn_number("--") is None
    )
    record("mx-xuangu 数值单位可解析", parse_ok, "3.20亿/4800万/--")

    ranked_rows, rank_stats = llm_scorer._rank_xuangu_rows_for_pool([
        {
            "代码": "000010", "名称": "强势一", "最新价(元)": "10.00", "涨跌幅(%)": "3.5",
            "证券类型": "A股", "主力净额(元)": "3.20亿", "量比": "2.10",
            "成交量环比增长率": "88", "成交额(元)": "8.00亿", "换手率(%)": "6.2",
            "市盈率(动)(倍)": "24", "市净率(倍)": "2.3", "总市值(元)": "180.00亿",
        },
        {
            "代码": "000011", "名称": "偏弱二", "最新价(元)": "8.00", "涨跌幅(%)": "9.8",
            "证券类型": "A股", "主力净额(元)": "100.00万", "量比": "0.40",
            "成交量环比增长率": "2", "成交额(元)": "1200.00万", "换手率(%)": "0.2",
            "市盈率(动)(倍)": "160", "市净率(倍)": "18", "总市值(元)": "20.00亿",
        },
        {
            "代码": "000012", "名称": "ST测试", "最新价(元)": "5.00", "涨跌幅(%)": "4.0",
            "证券类型": "A股", "ST股票": "是", "主力净额(元)": "9.00亿", "量比": "3.0",
            "成交额(元)": "10.00亿", "换手率(%)": "5.0",
        },
        {
            "代码": "920178", "名称": "北交测试", "最新价(元)": "20.00", "涨跌幅(%)": "6.0",
            "证券类型": "A股", "主力净额(元)": "20.00亿", "量比": "3.0",
            "成交额(元)": "20.00亿", "换手率(%)": "8.0",
        },
        {
            "代码": "000013", "名称": "中等三", "最新价(元)": "12.00", "涨跌幅(%)": "2.0",
            "证券类型": "A股", "主力净额(元)": "8000.00万", "量比": "1.3",
            "成交量环比增长率": "20", "成交额(元)": "2.00亿", "换手率(%)": "3.2",
            "市盈率(动)(倍)": "35", "市净率(倍)": "3.1", "总市值(元)": "80.00亿",
        },
    ], startup, top_n=2)
    rank_ok = (
        [r.get("代码") for r in ranked_rows] == ["000010", "000013"]
        and rank_stats.get("filtered") == 2
        and ranked_rows[0].get("_pool_rank") == 1
        and ranked_rows[0].get("_pool_score", 0) > ranked_rows[1].get("_pool_score", 0)
    )
    record("每个选股池先过滤北交所/ST再本地评分排序", rank_ok, f"stats={rank_stats} rows={ranked_rows}")

    phase2 = run_debate_phase.debate_phase_to_phase2_format({
        "ranked_candidates": [{
            "stock": "000001",
            "name": "测试股",
            "signal": "WATCH",
            "confidence": 80,
            "position_ratio": "20%",
            "pool": first.get("pool"),
            "source_pools": first.get("source_pools"),
            "strategy_type": first.get("strategy_type"),
            "strategy_types": first.get("strategy_types"),
            "entry_bias": first.get("entry_bias"),
            "entry_biases": first.get("entry_biases"),
            "pool_score": 88.8,
            "pool_rank": 1,
            "source_score_records": [{"pool": "准备启动", "score": 88.8, "rank": 1}],
        }]
    })
    top = phase2["top_picks"][0]
    source_keep_ok = (
        top.get("source_pools") == ["准备启动", "资金异动"]
        and "低吸" in top.get("entry_bias", "")
        and top.get("pool_score") == 88.8
        and top.get("pool_rank") == 1
    )
    record("phase2 top_picks 保留筛选来源、池内分和低吸偏好", source_keep_ok, str(top))

    prompt = intraday._build_buy_timing_prompt(
        top,
        {"price": 10.0, "open": 9.8, "high": 10.2, "low": 9.7, "prev_close": 9.9},
        technical_snapshot={"above_ma": ["ma5", "ma10", "ma20"], "crossed_up_ma": ["ma120"]},
    )
    prompt_ok = (
        "one_minute_technical" in prompt
        and "只用当天技术面" in prompt
        and "基本面" in prompt
        and "startup_dip" not in prompt
        and "等待放量上攻确认或回踩企稳" not in prompt
    )
    record("盘中买入提示词只使用当天技术面且不读早报建议", prompt_ok, prompt[:500])


def intraday_monitor_checks():
    intraday = importlib.import_module("intraday_executor")

    param_ok = (
        abs(intraday._normalize_pct_param(1000, 0.1) - 0.1) < 1e-9
        and abs(intraday._normalize_pct_param(-300, -0.03) + 0.03) < 1e-9
        and abs(intraday._normalize_pct_param(0.2, 0.1) - 0.2) < 1e-9
    )
    record("盘中卖出参数百分数自动归一化", param_ok, f"tp1={intraday._normalize_pct_param(1000, 0.1)}")

    stop_pct = intraday._calc_trailing_stop_pct(10.0, 10.8, None, peak_price=12.0)
    record("移动止损按最高价保护利润", abs(stop_pct - 0.10) < 1e-9, f"stop_pct={stop_pct}")
    stop_loss_pct = intraday._calc_trailing_stop_pct(10.0, 9.8, None)
    record("移动止损低收益区间使用配置止损线-3%", abs(stop_loss_pct - intraday.STOP_LOSS_PCT) < 1e-9, f"stop_pct={stop_loss_pct}")
    original_xq_quote = intraday.get_xq_realtime_quote
    try:
        intraday.get_xq_realtime_quote = lambda _stock: {"high": 12.0}
        realtime_peak = intraday._get_intraday_peak_price("000001", 10.8)
    finally:
        intraday.get_xq_realtime_quote = original_xq_quote
    record("移动止损优先读取当天实时最高价", abs(realtime_peak - 12.0) < 1e-9, f"peak={realtime_peak}")
    original_hist = intraday._get_historical_prices
    try:
        intraday._get_historical_prices = lambda _stock, days=30: [
            ["2026-05-18", 10.0, 10.5, 11.0, 9.9, 1000],
            ["2026-05-19", 10.5, 12.0, 13.0, 10.4, 1000],
            ["2026-05-20", 12.0, 11.0, 11.2, 10.9, 1000],
        ]
        post_buy_peak = intraday._get_post_buy_peak_price(
            "000001",
            {"buy_records": [{"date": "2026-05-18", "price": 10.0, "quantity": 1000, "remaining": 1000}]},
            11.0,
        )
    finally:
        intraday._get_historical_prices = original_hist
    record("移动止损回补买入后日K最高价", abs(post_buy_peak - 13.0) < 1e-9, f"peak={post_buy_peak}")

    qty_tp3 = intraday._calculate_sell_quantity_by_reason({"records": []}, "000001", 1000, "止盈第3档")
    record("无交易记录时止盈3档卖剩余", qty_tp3 == 1000, f"qty={qty_tp3}")
    qty_tp2_original = intraday._calculate_sell_quantity_by_reason(
        {
            "records": [{
                "stock": "000002",
                "quantity": 4200,
                "remaining_quantity": 3000,
                "sells": [{"quantity": 1200, "reason": "止盈第1档"}],
                "buy_records": [{"price": 10.0, "quantity": 4200, "remaining": 3000}],
            }]
        },
        "000002",
        3000,
        "止盈第2档",
    )
    record("止盈2档按原始仓位20%卖出", qty_tp2_original == 800, f"qty={qty_tp2_original}")
    tiers = intraday._executed_take_profit_tiers({
        "sells": [
            {"reason": "止盈第1档(19.9% > 10%)卖30%仓位 [确认执行]"},
            {"reason": "ATR止损"},
            {"reason": "止盈第2档（22.0% >= 20%）卖20%仓位 [确认执行]"},
        ]
    })
    record("历史卖出记录可识别已执行止盈档位", tiers == {1, 2}, str(tiers))

    rec = {
        "stock": "000001",
        "buy_price": 10.0,
        "quantity": 1000,
        "remaining_quantity": 300,
        "sells": [],
        "buy_records": [{"price": 10.0, "quantity": 300, "remaining": 300}],
    }
    changed = intraday._reconcile_trade_record_to_position(rec, 1000, 10.0)
    record("卖出前同步真实可卖持仓", changed and intraday._tracked_remaining_quantity(rec) == 1000, str(rec))

    def make_pos(stock, price=9.5, cost=10.0, avail=1000, total=1000):
        return {
            "stockCode": stock,
            "stockName": stock,
            "totalQuantity": total,
            "availQuantity": avail,
            "price": int(price * 100),
            "costPrice": int(cost * 1000),
            "priceDec": 2,
            "costPriceDec": 3,
        }

    def make_trade(stock, sells=None, remaining=1000):
        return {
            "records": [{
                "stock": stock,
                "name": stock,
                "buy_price": 10.0,
                "quantity": 1000,
                "remaining_quantity": remaining,
                "sells": sells or [],
                "buy_records": [{"date": "2026-05-18", "price": 10.0, "quantity": 1000, "remaining": remaining}],
            }]
        }

    def run_monitor_case(stock, sell_result, today_sells, price=9.5, cost=10.0, avail=1000, total=1000, trades_override=None, post_buy_peak=None):
        saved = {}
        pushes = []
        sell_calls = []
        trades = trades_override or make_trade(stock)
        original = {
            "api_key": intraday.API_KEY,
            "get_current_positions": intraday.get_current_positions,
            "_load_trades": intraday._load_trades,
            "_save_trades": intraday._save_trades,
            "_get_atr_and_ma20": intraday._get_atr_and_ma20,
            "_get_intraday_peak_price": intraday._get_intraday_peak_price,
            "_get_post_buy_peak_price": intraday._get_post_buy_peak_price,
            "sell_stock": intraday.sell_stock,
            "get_today_orders": intraday.get_today_orders,
            "feishu_push": intraday.feishu_push,
            "sleep": intraday.time.sleep,
            "allow_before_start": os.environ.get("INTRADAY_SELL_ALLOW_BEFORE_START"),
            "force_run": os.environ.get("INTRADAY_SELL_FORCE_RUN"),
        }
        execute_debate_result = importlib.import_module("execute_debate_result")
        original_is_trading_day = execute_debate_result.is_trading_day
        try:
            os.environ["INTRADAY_SELL_ALLOW_BEFORE_START"] = "1"
            os.environ["INTRADAY_SELL_FORCE_RUN"] = "1"
            intraday.API_KEY = "TEST"
            execute_debate_result.is_trading_day = lambda: True
            intraday.get_current_positions = lambda: [make_pos(stock, price=price, cost=cost, avail=avail, total=total)]
            intraday._load_trades = lambda: json.loads(json.dumps(trades))
            intraday._save_trades = lambda data: saved.setdefault("data", json.loads(json.dumps(data)))
            intraday._get_atr_and_ma20 = lambda _stock: (None, None)
            intraday._get_intraday_peak_price = lambda _stock, current_price: current_price
            intraday._get_post_buy_peak_price = lambda _stock, _record, current_price: post_buy_peak or current_price
            def fake_sell_stock(*args, **kwargs):
                sell_calls.append({"args": args, "kwargs": kwargs})
                return sell_result
            intraday.sell_stock = fake_sell_stock
            intraday.get_today_orders = lambda force=False: {"buys": [], "sells": today_sells, "_ok": True}
            intraday.feishu_push = lambda msg, webhook=None: pushes.append(msg)
            intraday.time.sleep = lambda *_args, **_kwargs: None
            intraday.run_monitor_mode()
        finally:
            intraday.API_KEY = original["api_key"]
            intraday.get_current_positions = original["get_current_positions"]
            intraday._load_trades = original["_load_trades"]
            intraday._save_trades = original["_save_trades"]
            intraday._get_atr_and_ma20 = original["_get_atr_and_ma20"]
            intraday._get_intraday_peak_price = original["_get_intraday_peak_price"]
            intraday._get_post_buy_peak_price = original["_get_post_buy_peak_price"]
            intraday.sell_stock = original["sell_stock"]
            intraday.get_today_orders = original["get_today_orders"]
            intraday.feishu_push = original["feishu_push"]
            intraday.time.sleep = original["sleep"]
            if original["allow_before_start"] is None:
                os.environ.pop("INTRADAY_SELL_ALLOW_BEFORE_START", None)
            else:
                os.environ["INTRADAY_SELL_ALLOW_BEFORE_START"] = original["allow_before_start"]
            if original["force_run"] is None:
                os.environ.pop("INTRADAY_SELL_FORCE_RUN", None)
            else:
                os.environ["INTRADAY_SELL_FORCE_RUN"] = original["force_run"]
            execute_debate_result.is_trading_day = original_is_trading_day
        return saved.get("data", {}), pushes, sell_calls

    saved_error, pushes_error, _ = run_monitor_case(
        "200001",
        {"status": "error", "stock": "200001", "error": "rejected"},
        [],
    )
    sells_error = saved_error.get("records", [{}])[0].get("sells", []) if saved_error else []
    record("卖出委托失败不写入交易记录", sells_error == [] and "未写入交易记录" in "\n".join(pushes_error), str(saved_error))

    saved_unconfirmed, pushes_unconfirmed, _ = run_monitor_case(
        "200002",
        {"status": "submitted", "stock": "200002", "quantity": 1000},
        [],
    )
    sells_unconfirmed = saved_unconfirmed.get("records", [{}])[0].get("sells", []) if saved_unconfirmed else []
    record("卖出未确认成交不写入交易记录", sells_unconfirmed == [] and "未在今日成交回报中确认" in "\n".join(pushes_unconfirmed), str(saved_unconfirmed))

    saved_confirmed, _, _ = run_monitor_case(
        "200003",
        {"status": "submitted", "stock": "200003", "quantity": 1000},
        [
            {"stock": "200003", "trade_price": 9.5, "quantity": 300, "order_quantity": 300},
            {"stock": "200003", "trade_price": 9.4, "quantity": 200, "order_quantity": 200},
        ],
    )
    rec_confirmed = saved_confirmed.get("records", [{}])[0]
    confirmed_qty = sum(s.get("quantity", 0) for s in rec_confirmed.get("sells", []))
    record("卖出成交回查同股多笔汇总不覆盖", confirmed_qty == 500 and rec_confirmed.get("remaining_quantity") == 500, str(rec_confirmed))

    already_tp1_trades = make_trade(
        "200004",
        sells=[{"date": "2026-05-19", "price": 11.9, "quantity": 300, "reason": "止盈第1档(19.0% > 10%)卖30%仓位 [确认执行]"}],
        remaining=700,
    )
    already_tp1_trades["records"][0]["highest_price"] = 11.9
    already_tp1_trades["records"][0]["highest_pnl_pct"] = 19.0
    saved_repeat_tp1, pushes_repeat_tp1, _ = run_monitor_case(
        "200004",
        {"status": "submitted", "stock": "200004", "quantity": 200},
        [],
        price=11.5,
        cost=10.0,
        avail=700,
        total=700,
        trades_override=already_tp1_trades,
    )
    repeat_tp1_avoided = not saved_repeat_tp1 and "触发卖出: 0只" in "\n".join(pushes_repeat_tp1)
    record("已执行一档止盈后未到二档不重复卖一档", repeat_tp1_avoided, f"saved={saved_repeat_tp1} pushes={pushes_repeat_tp1}")

    saved_peak_stop, pushes_peak_stop, sell_calls_peak_stop = run_monitor_case(
        "200007",
        {"status": "submitted", "stock": "200007", "quantity": 1000},
        [],
        price=11.0,
        cost=10.0,
        avail=1000,
        total=1000,
        post_buy_peak=13.0,
    )
    peak_stop_triggered = (
        len(sell_calls_peak_stop) == 1
        and "止损" in str(sell_calls_peak_stop[0]["args"])
        and "10.0% <= 25.0%" in str(sell_calls_peak_stop[0]["args"])
        and "未在今日成交回报中确认" in "\n".join(pushes_peak_stop)
    )
    record("买入后曾涨30%回落到10%触发移动止损", peak_stop_triggered, f"calls={sell_calls_peak_stop} saved={saved_peak_stop}")

    saved_pending, pushes_pending, sell_calls_pending = run_monitor_case(
        "200005",
        {"status": "submitted", "stock": "200005", "quantity": 1000},
        [{"stock": "200005", "trade_price": 0, "quantity": 0, "order_quantity": 1000, "status": "submitted", "order_id": "S1"}],
    )
    pending_sells = saved_pending.get("records", [{}])[0].get("sells", []) if saved_pending else []
    pending_skipped = not sell_calls_pending and pending_sells == [] and "已有未成交卖出委托" in "\n".join(pushes_pending)
    record("已有未成交卖单时不重复提交卖出", pending_skipped, f"calls={sell_calls_pending} saved={saved_pending}")

    original_sell_deps = {
        "mx_api_post": intraday.mx_api_post,
        "get_realtime_quote": intraday.get_realtime_quote,
        "feishu_push": intraday.feishu_push,
        "dry_run": intraday.os.environ.get("DRY_RUN"),
    }
    sell_pushes = []
    try:
        intraday.os.environ.pop("DRY_RUN", None)
        intraday.get_realtime_quote = lambda _stock: None
        intraday.mx_api_post = lambda *_args, **_kwargs: {"code": 500, "msg": "rejected"}
        intraday.feishu_push = lambda msg, webhook=None: sell_pushes.append(msg)
        sell_failure = intraday.sell_stock("200006", "200006", 10.0, 100, "测试失败")
    finally:
        intraday.mx_api_post = original_sell_deps["mx_api_post"]
        intraday.get_realtime_quote = original_sell_deps["get_realtime_quote"]
        intraday.feishu_push = original_sell_deps["feishu_push"]
        if original_sell_deps["dry_run"] is None:
            intraday.os.environ.pop("DRY_RUN", None)
        else:
            intraday.os.environ["DRY_RUN"] = original_sell_deps["dry_run"]
    failure_push_ok = sell_failure.get("status") == "error" and "卖出委托已提交" not in "\n".join(sell_pushes)
    record("卖出委托失败不提前推送已提交", failure_push_ok, f"pushes={sell_pushes}")


def intraday_buy_timing_checks():
    intraday = importlib.import_module("intraday_executor")
    from datetime import date, datetime, timedelta

    quote = intraday._normalize_realtime_quote(
        "000001",
        {
            "lastPrice": 10.0,
            "lastClose": 9.8,
            "open": 9.9,
            "high": 10.2,
            "low": 9.7,
            "bidPrice": [9.99],
            "askPrice": [10.0],
            "volume": 123456,
        },
        "xq_full_tick",
    )
    quote_ok = (
        quote
        and quote["price"] == 10.0
        and abs(quote["change_pct"] - 2.04) < 0.01
        and quote["bid1"] == 9.99
        and quote["ask1"] == 10.0
    )
    record("XQShare实时行情字段归一化", quote_ok, str(quote))
    follow_price = intraday._buy_order_price_from_decision(
        quote,
        {"price_mode": "FOLLOW", "limit_price": None, "max_premium_pct": 0.8},
    )
    custom_price = intraday._buy_order_price_from_decision(
        quote,
        {"price_mode": "CUSTOM", "limit_price": 10.03, "max_premium_pct": 0.3},
    )
    high_clamp = intraday._buy_order_price_from_decision(
        quote,
        {"price_mode": "CUSTOM", "limit_price": 11.0, "max_premium_pct": 1.5},
    )
    dip_price = intraday._buy_order_price_from_decision(
        quote,
        {"price_mode": "DIP", "limit_price": None, "max_premium_pct": -1.0},
    )
    price_ok = (
        follow_price.get("order_price") == 10.15
        and custom_price.get("order_price") == 10.15
        and high_clamp.get("order_price") == 10.15
        and dip_price.get("order_price") == 10.15
    )
    record("分时买入统一按最新价×1.015且不超过涨停报价", price_ok, f"follow={follow_price} custom={custom_price} clamp={high_clamp} dip={dip_price}")
    ordinary_limit_fallback = intraday._buy_order_price_from_decision(
        {"price": 10.95, "prev_close": 10.0},
        {"price_mode": "FOLLOW"},
        stock="000001",
    )
    star_limit_fallback = intraday._buy_order_price_from_decision(
        {"price": 11.9, "prev_close": 10.0},
        {"price_mode": "FOLLOW"},
        stock="688001",
    )
    bse_limit_fallback = intraday._buy_order_price_from_decision(
        {"price": 12.9, "prev_close": 10.0},
        {"price_mode": "FOLLOW"},
        stock="920001",
    )
    explicit_limit_fallback = intraday._buy_order_price_from_decision(
        {"price": 12.9, "prev_close": 10.0, "limit_up": 12.88},
        {"price_mode": "FOLLOW"},
        stock="920001",
    )
    fallback_limit_ok = (
        ordinary_limit_fallback.get("order_price") == 11.0
        and star_limit_fallback.get("order_price") == 12.0
        and bse_limit_fallback.get("order_price") == 13.0
        and explicit_limit_fallback.get("order_price") == 12.88
    )
    record(
        "分时买入缺少涨停价时按板块涨停幅度封顶报价",
        fallback_limit_ok,
        f"a={ordinary_limit_fallback} star={star_limit_fallback} bse={bse_limit_fallback} explicit={explicit_limit_fallback}",
    )
    limit_quote = dict(quote)
    limit_quote.update({"price": 10.78, "limit_up": 10.78, "is_limit_up": True})
    limit_order = intraday._calc_timing_buy_order(
        {"stock": "000001", "signal": "BUY", "confidence": 80},
        limit_quote,
        100000,
        100000,
        {"price_mode": "FOLLOW", "confidence": 80},
    )
    limit_order_ok = limit_order.get("ok") and limit_order.get("order_price") == 10.78
    record("分时买入订单层涨停报价按涨停价封顶不直接拦截", limit_order_ok, str(limit_order))
    lazy_cash_order = intraday._calc_timing_buy_order(
        {"stock": "000001", "signal": "BUY", "confidence": 80},
        quote,
        None,
        100000,
        {"price_mode": "FOLLOW", "confidence": 80},
    )
    lazy_cash_order_ok = lazy_cash_order.get("ok") and lazy_cash_order.get("quantity", 0) > 0
    record("分时买入首笔下单可用当前资金懒加载初始资金", lazy_cash_order_ok, str(lazy_cash_order))
    star_order = intraday._calc_timing_buy_order(
        {"stock": "688001", "signal": "BUY", "confidence": 80},
        {"price": 10.0, "limit_up": 12.0},
        11700,
        11700,
        {"price_mode": "FOLLOW", "confidence": 80},
    )
    record("分时买入科创板200股起且超过部分可1股递增", star_order.get("ok") and star_order.get("quantity") == 230, str(star_order))
    bse_order = intraday._calc_timing_buy_order(
        {"stock": "920001", "signal": "BUY", "confidence": 80},
        {"price": 10.0, "limit_up": 12.0},
        7650,
        7650,
        {"price_mode": "FOLLOW", "confidence": 80},
    )
    record("分时买入北交所100股起且超过部分可1股递增", bse_order.get("ok") and bse_order.get("quantity") == 150, str(bse_order))
    bse_limit_ok = (
        abs(intraday._stock_limit_pct("920001", "测试") - 0.30) < 1e-9
        and abs(intraday._normalize_realtime_quote("920001", {"lastPrice": 12.5, "lastClose": 10.0}, "test")["limit_up"] - 13.0) < 1e-9
    )
    record("分时买入北交所920涨停价兜底按30%计算", bse_limit_ok, str(intraday._normalize_realtime_quote("920001", {"lastPrice": 12.5, "lastClose": 10.0}, "test")))

    old_xq_get = intraday._xq_http_get
    try:
        intraday._xq_http_get = lambda endpoint, params=None, timeout=6: {
            "success": True,
            "data": {
                "000001.SZ": {
                    "lastPrice": 11.0,
                    "lastClose": 10.0,
                    "open": 10.2,
                    "high": 11.1,
                    "low": 10.1,
                }
            },
        }
        xq_quote = intraday.get_xq_realtime_quote("000001")
        record("分时买入优先读取XQShare full_tick", xq_quote and xq_quote["source"] == "xq_full_tick" and xq_quote["price"] == 11.0, str(xq_quote))
    finally:
        intraday._xq_http_get = old_xq_get

    pending = {"stock": "000001", "order_quantity": 1000, "quantity": 0, "status": "已报", "order_id": "OID1"}
    filled = {"stock": "000001", "order_quantity": 1000, "quantity": 1000, "status": "已成", "order_id": "OID1"}
    cancelled = {"stock": "000001", "order_quantity": 1000, "quantity": 0, "status": "已撤", "order_id": "OID1"}
    pending_ok = intraday._is_pending_order(pending) and not intraday._is_pending_order(filled) and not intraday._is_pending_order(cancelled)
    record("未成交买单识别为挂单", pending_ok, f"pending={intraday._is_pending_order(pending)}")
    now_for_order = datetime.now().replace(microsecond=0)
    order_time_text = now_for_order.strftime("%Y-%m-%d %H:%M:%S")
    order_time_compact = now_for_order.strftime("%Y%m%d%H%M%S")
    order_time_ok = (
        intraday._parse_order_time({"time": int(now_for_order.timestamp() * 1000)}).replace(microsecond=0) == now_for_order
        and intraday._parse_order_time({"orderTime": order_time_text}) == now_for_order
        and intraday._parse_order_time({"tradeTime": order_time_compact}) == now_for_order
        and intraday._parse_order_time({"orderTime": ""}) is None
    )
    record("今日委托时间兼容多字段格式", order_time_ok, "time/orderTime/tradeTime")
    cancel_no_id = intraday.cancel_buy_order(None, "000001")
    record("缺少委托号时撤单fail-closed", cancel_no_id.get("status") == "error", str(cancel_no_id))

    dry_run_originals = {
        "dry_run": intraday.os.environ.get("DRY_RUN"),
        "mx_api_post": intraday.mx_api_post,
        "feishu_push": intraday.feishu_push,
    }
    try:
        buy_api_calls = []
        buy_pushes = []
        intraday.os.environ["DRY_RUN"] = "0"
        intraday.mx_api_post = lambda endpoint, payload: buy_api_calls.append((endpoint, payload)) or {
            "code": 200,
            "data": {"orderId": "DRYRUN-ZERO-REAL"},
        }
        intraday.feishu_push = lambda msg, webhook=None: buy_pushes.append(msg)
        dry_zero_result = intraday.buy_stock("000001", "平安银行", 10.0, 100, "DRY_RUN=0测试")
        intraday.os.environ["DRY_RUN"] = "true"
        dry_true_result = intraday.buy_stock("000001", "平安银行", 10.0, 100, "DRY_RUN=true测试")
        dry_flag_ok = (
            dry_zero_result.get("status") == "submitted"
            and len(buy_api_calls) == 1
            and dry_true_result.get("status") == "dry_run"
        )
        record("DRY_RUN只有显式真值才模拟下单", dry_flag_ok, f"zero={dry_zero_result} true={dry_true_result} calls={buy_api_calls}")
    finally:
        intraday.mx_api_post = dry_run_originals["mx_api_post"]
        intraday.feishu_push = dry_run_originals["feishu_push"]
        if dry_run_originals["dry_run"] is None:
            intraday.os.environ.pop("DRY_RUN", None)
        else:
            intraday.os.environ["DRY_RUN"] = dry_run_originals["dry_run"]

    original_api_key = intraday.API_KEY
    original_mx_post = intraday.mx_api_post
    original_orders_cache = dict(intraday._TODAY_ORDERS_CACHE)
    try:
        intraday.API_KEY = "test"
        intraday._TODAY_ORDERS_CACHE["ts"] = 0
        today_order_time = datetime.now().replace(microsecond=0).strftime("%Y-%m-%d %H:%M:%S")
        intraday.mx_api_post = lambda endpoint, payload: {
            "code": 200,
            "data": {"orders": [{
                "secCode": "000001",
                "secName": "平安银行",
                "orderTime": today_order_time,
                "price": 1015,
                "priceDec": 2,
                "tradePrice": 0,
                "tradePriceDec": 2,
                "tradeCount": 0,
                "count": 1000,
                "status": "已报",
                "drt": "1",
                "orderId": "OID-TIME",
            }]},
        }
        today_orders = intraday.get_today_orders(force=True)
        order_time_api_ok = (
            len(today_orders.get("buys", [])) == 1
            and today_orders["buys"][0].get("order_id") == "OID-TIME"
            and today_orders["buys"][0].get("order_time")
        )
        record("今日委托回查兼容orderTime字段", order_time_api_ok, str(today_orders))
        intraday._TODAY_ORDERS_CACHE["ts"] = 0
        intraday.mx_api_post = lambda endpoint, payload: {}
        empty_orders = intraday.get_today_orders(force=True)
        intraday._TODAY_ORDERS_CACHE["ts"] = 0
        intraday.mx_api_post = lambda endpoint, payload: {"code": 200, "data": {}}
        missing_orders = intraday.get_today_orders(force=True)
        orders_fail_closed = empty_orders.get("_ok") is False and missing_orders.get("_ok") is False
        record("今日委托空响应或缺orders字段时fail-closed", orders_fail_closed, f"empty={empty_orders} missing={missing_orders}")
    finally:
        intraday.API_KEY = original_api_key
        intraday.mx_api_post = original_mx_post
        intraday._TODAY_ORDERS_CACHE.clear()
        intraday._TODAY_ORDERS_CACHE.update(original_orders_cache)

    action_ok = (
        intraday._sanitize_timing_action("BUY_NOW", has_pending=True) == "KEEP_ORDER"
        and intraday._sanitize_timing_action("CANCEL_REBUY", has_pending=False) == "WAIT"
        and intraday._sanitize_timing_action("SKIP_TODAY", has_pending=False, now=datetime(2026, 5, 19, 14, 56)) == "WAIT"
        and intraday._sanitize_timing_action("SKIP_TODAY", has_pending=False, now=datetime(2026, 5, 19, 14, 57)) == "SKIP_TODAY"
    )
    record("分时买入同股挂单不重复且14:57前不永久跳过", action_ok, "BUY_NOW+pending=>KEEP_ORDER, 14:56 SKIP=>WAIT")

    schedule_ok = (
        intraday._next_buy_timing_check(datetime(2026, 5, 19, 9, 31, 30)).strftime("%H:%M") == "09:32"
        and intraday._next_buy_timing_check(datetime(2026, 5, 19, 11, 31, 0)).strftime("%H:%M") == "13:00"
        and intraday._next_buy_timing_check(datetime(2026, 5, 19, 14, 45, 0)).strftime("%H:%M") == "14:46"
        and intraday._next_buy_timing_check(datetime(2026, 5, 19, 14, 56, 0)).strftime("%H:%M") == "14:57"
        and intraday._next_buy_timing_check(datetime(2026, 5, 19, 14, 57, 0)) is None
    )
    record("分时买入调度覆盖每分钟检查、午休和14:57截止", schedule_ok, "09:31->09:32, 11:31->13:00, 14:56->14:57")

    old_wait_env = intraday.os.environ.pop("INTRADAY_BUY_REPORT_WAIT_SECONDS", None)
    old_wait_override_env = intraday.os.environ.pop("INTRADAY_BUY_REPORT_WAIT_SECONDS_OVERRIDE", None)
    try:
        wait_default = intraday._buy_timing_report_wait_seconds(datetime(2026, 5, 19, 9, 25))
        intraday.os.environ["INTRADAY_BUY_REPORT_WAIT_SECONDS"] = "1800"
        wait_stale_env = intraday._buy_timing_report_wait_seconds(datetime(2026, 5, 19, 9, 25))
        intraday.os.environ["INTRADAY_BUY_REPORT_WAIT_SECONDS_OVERRIDE"] = "1"
        wait_override = intraday._buy_timing_report_wait_seconds(datetime(2026, 5, 19, 9, 25))
        report_wait_ok = wait_default > 1800 and wait_stale_env == wait_default and wait_override == 1800
        record(
            "分时买入默认等待早报直到14:57且不受残留短等待变量影响",
            report_wait_ok,
            f"default={wait_default} stale_env={wait_stale_env} override={wait_override}",
        )
    finally:
        if old_wait_env is not None:
            intraday.os.environ["INTRADAY_BUY_REPORT_WAIT_SECONDS"] = old_wait_env
        else:
            intraday.os.environ.pop("INTRADAY_BUY_REPORT_WAIT_SECONDS", None)
        if old_wait_override_env is not None:
            intraday.os.environ["INTRADAY_BUY_REPORT_WAIT_SECONDS_OVERRIDE"] = old_wait_override_env
        else:
            intraday.os.environ.pop("INTRADAY_BUY_REPORT_WAIT_SECONDS_OVERRIDE", None)

    llm_interval_ok = (
        intraday._buy_timing_llm_interval_minutes(datetime(2026, 5, 19, 9, 59)) == 3
        and intraday._buy_timing_llm_interval_minutes(datetime(2026, 5, 19, 10, 0)) == 10
        and intraday._is_buy_timing_llm_due({}, datetime(2026, 5, 19, 9, 31))
        and not intraday._is_buy_timing_llm_due({"last_llm_check_at": "2026-05-19T09:31:00"}, datetime(2026, 5, 19, 9, 33, 59))
        and intraday._is_buy_timing_llm_due({"last_llm_check_at": "2026-05-19T09:31:00"}, datetime(2026, 5, 19, 9, 34))
        and not intraday._is_buy_timing_llm_due({"last_llm_check_at": "2026-05-19T10:00:00"}, datetime(2026, 5, 19, 10, 9, 59))
        and intraday._is_buy_timing_llm_due({"last_llm_check_at": "2026-05-19T10:00:00"}, datetime(2026, 5, 19, 10, 10))
    )
    record("分时买入LLM判断按10点前3分钟/10点后10分钟降频", llm_interval_ok, "09:31->09:34, 10:00->10:10")

    stop_waiting_top5_ok = (
        not intraday._should_stop_buy_timing_loop(False, True, False)
        and intraday._should_stop_buy_timing_loop(False, True, True)
        and intraday._should_stop_buy_timing_loop(True, False, False)
    )
    record("分时买入顺延股完成后仍等待今日Top5加入", stop_waiting_top5_ok, "carryover_done_without_top5=>continue")

    old_realtime_thread_env = intraday.os.environ.pop("INTRADAY_BUY_ENABLE_REALTIME_THREAD", None)
    try:
        realtime_default_ok = not intraday._buy_timing_realtime_thread_enabled()
        intraday.os.environ["INTRADAY_BUY_ENABLE_REALTIME_THREAD"] = "1"
        realtime_env_ignored_ok = not intraday._buy_timing_realtime_thread_enabled()
        record("分时买入旧实时线程永久关闭避免双轮询", realtime_default_ok and realtime_env_ignored_ok, f"default={realtime_default_ok} env_ignored={realtime_env_ignored_ok}")
        intraday_source = Path(intraday.__file__).read_text(encoding="utf-8")
        realtime_thread_removed_ok = (
            "threading.Thread" not in intraday_source
            and "兼容线程已停用" in intraday_source
            and "兼容线程已启用" not in intraday_source
            and "兼容线程异常" not in intraday_source
            and "快轮询启动" not in intraday_source
            and "REALTIME_POLL_INTERVAL" not in intraday_source
            and "旧实时硬触发快轮询入口已禁用" in intraday_source
        )
        record("分时买入不再保留兼容线程启动分支", realtime_thread_removed_ok, "")
    finally:
        if old_realtime_thread_env is not None:
            intraday.os.environ["INTRADAY_BUY_ENABLE_REALTIME_THREAD"] = old_realtime_thread_env
        else:
            intraday.os.environ.pop("INTRADAY_BUY_ENABLE_REALTIME_THREAD", None)

    old_index_env = intraday.os.environ.pop("INTRADAY_BUY_INCLUDE_INDEX_DATA", None)
    try:
        index_default_ok = not intraday._buy_timing_index_data_enabled()
        intraday.os.environ["INTRADAY_BUY_INCLUDE_INDEX_DATA"] = "1"
        index_opt_in_ok = intraday._buy_timing_index_data_enabled()
        record("分时买入指数数据默认关闭避免额外QMT轮询", index_default_ok and index_opt_in_ok, f"default={index_default_ok} opt_in={index_opt_in_ok}")
    finally:
        if old_index_env is not None:
            intraday.os.environ["INTRADAY_BUY_INCLUDE_INDEX_DATA"] = old_index_env
        else:
            intraday.os.environ.pop("INTRADAY_BUY_INCLUDE_INDEX_DATA", None)

    health = importlib.import_module("check_intraday_buy_health")
    original_health_base = health.BASE_DIR
    try:
        with tempfile.TemporaryDirectory(prefix="buy-health-env-") as tmpdir:
            tmp_base = Path(tmpdir)
            (tmp_base / ".env").write_text(
                "\n".join([
                    "DRY_RUN=true",
                    "ALLOW_BUY_OUTSIDE_WINDOW=yes",
                    "INTRADAY_BUY_TIMING_ONCE=1",
                    "INTRADAY_BUY_ENABLE_REALTIME_THREAD=1",
                    "INTRADAY_BUY_TIMING_CUTOFF=14:50",
                    "INTRADAY_BUY_INCLUDE_INDEX_DATA=1",
                    "INTRADAY_BUY_INCLUDE_BOARD_DATA=on",
                ]),
                encoding="utf-8",
            )
            health.BASE_DIR = tmp_base
            env_values = health._load_env_file_values()
            dangerous_detected = (
                health._env_value_enabled(env_values.get("DRY_RUN", ("", ""))[0])
                and health._env_value_enabled(env_values.get("ALLOW_BUY_OUTSIDE_WINDOW", ("", ""))[0])
                and env_values.get("INTRADAY_BUY_TIMING_ONCE") == ("1", ".env")
                and env_values.get("INTRADAY_BUY_ENABLE_REALTIME_THREAD") == ("1", ".env")
                and env_values.get("INTRADAY_BUY_TIMING_CUTOFF") == ("14:50", ".env")
                and env_values.get("INTRADAY_BUY_INCLUDE_INDEX_DATA") == ("1", ".env")
                and health._env_value_enabled(env_values.get("INTRADAY_BUY_INCLUDE_BOARD_DATA", ("", ""))[0])
            )
            record("分时买入健康检查可识别危险.env开关", dangerous_detected, str(env_values))
        with tempfile.TemporaryDirectory(prefix="buy-health-model-env-") as tmpdir:
            tmp_base = Path(tmpdir)
            health.BASE_DIR = tmp_base
            default_models = health._effective_intraday_buy_models(health._load_env_file_values())
            (tmp_base / ".env").write_text(
                "\n".join([
                    "INTRADAY_LLM_MODEL=volcengine-plan/ark-code-latest",
                    "INTRADAY_LLM_FALLBACK_MODEL=minimax-portal/MiniMax-M3",
                ]),
                encoding="utf-8",
            )
            inherited_models = health._effective_intraday_buy_models(health._load_env_file_values())
            (tmp_base / ".env").write_text(
                "\n".join([
                    "INTRADAY_LLM_MODEL=volcengine-plan/ark-code-latest",
                    "INTRADAY_BUY_TIMING_LLM_MODEL=minimax-portal/MiniMax-M3",
                    "INTRADAY_BUY_TIMING_LLM_FALLBACK_MODEL=openai-codex/gpt-5.5",
                ]),
                encoding="utf-8",
            )
            explicit_models = health._effective_intraday_buy_models(health._load_env_file_values())
            model_env_ok = (
                default_models["primary"] == ("minimax-portal/MiniMax-M3", "default")
                and default_models["fallback"] == ("openai-codex/gpt-5.5", "default")
                and inherited_models["primary"] == ("volcengine-plan/ark-code-latest", ".env")
                and inherited_models["fallback"] == ("minimax-portal/MiniMax-M3", ".env")
                and explicit_models["primary"] == ("minimax-portal/MiniMax-M3", ".env")
                and explicit_models["fallback"] == ("openai-codex/gpt-5.5", ".env")
            )
            record(
                "分时买入健康检查按有效模型配置识别MiniMax主用和GPT5.5兜底",
                model_env_ok,
                f"default={default_models} inherited={inherited_models} explicit={explicit_models}",
            )
    finally:
        health.BASE_DIR = original_health_base

    test_signal = {"stock": "000001", "name": "平安银行", "signal": "BUY", "confidence": 80, "position_ratio": "20%"}
    opening_decision = intraday._technical_buy_timing_decision(
        test_signal,
        {"price": 10.2, "open": 10.0, "high": 10.25, "low": 9.98},
        [{"time": datetime(2026, 5, 19, 9, 31), "open": 10.0, "high": 10.25, "low": 9.98, "close": 10.2}],
        {"decisions": []},
        datetime(2026, 5, 19, 9, 31, 20),
    )
    opening_retry_entry = {
        "status": "open",
        "decision_count": 1,
        "last_decision_at": "2026-05-19T09:31:05",
    }
    opening_retry_decision = intraday._technical_buy_timing_decision(
        test_signal,
        {"price": 10.2, "open": 10.0, "high": 10.25, "low": 9.98},
        [{"time": datetime(2026, 5, 19, 9, 32), "open": 10.0, "high": 10.25, "low": 9.98, "close": 10.2}],
        opening_retry_entry,
        datetime(2026, 5, 19, 9, 32, 0),
    )
    opening_retry_consumed = intraday._technical_buy_timing_decision(
        test_signal,
        {"price": 10.2, "open": 10.0, "high": 10.25, "low": 9.98},
        [{"time": datetime(2026, 5, 19, 9, 33), "open": 10.0, "high": 10.25, "low": 9.98, "close": 10.2}],
        opening_retry_entry,
        datetime(2026, 5, 19, 9, 33, 0),
    )
    opening_incomplete_entry = {"status": "open"}
    opening_incomplete_decision = intraday._technical_buy_timing_decision(
        test_signal,
        {"price": 10.2, "high": 10.25, "low": 9.98},
        [],
        opening_incomplete_entry,
        datetime(2026, 5, 19, 9, 31, 30),
    )
    ma120_bars = [
        {"time": datetime.combine(date.today(), datetime.min.time()).replace(hour=9, minute=30), "open": 10.0, "high": 10.0, "low": 10.0, "close": 10.0}
        for _ in range(119)
    ] + [
        {"time": datetime.combine(date.today(), datetime.min.time()).replace(hour=11, minute=29), "open": 9.9, "high": 9.95, "low": 9.85, "close": 9.9},
        {"time": datetime.combine(date.today(), datetime.min.time()).replace(hour=13, minute=0), "open": 9.95, "high": 10.25, "low": 9.95, "close": 10.2},
    ]
    ma120_decision = intraday._technical_buy_timing_decision(
        test_signal,
        {"price": 10.2, "open": 10.0, "high": 10.25, "low": 9.85},
        ma120_bars,
        {"decisions": [{"action": "WAIT"}]},
        datetime(2026, 5, 19, 13, 0, 30),
    )
    technical_trigger_ok = (
        opening_decision and opening_decision.get("technical_trigger") == "OPENING_STRONG"
        and opening_retry_decision and opening_retry_decision.get("technical_trigger") == "OPENING_STRONG"
        and opening_retry_entry.get("opening_chase_evaluated_at") == "2026-05-19T09:32:00"
        and opening_retry_consumed is None
        and opening_incomplete_decision is None
        and "opening_chase_evaluated_at" not in opening_incomplete_entry
        and ma120_decision and ma120_decision.get("technical_trigger") == "MA120_CROSS_UP"
        and ma120_decision.get("force_llm_review")
    )
    record("分时买入开盘强势直买且技术触发LLM判断", technical_trigger_ok, f"open={opening_decision} technical={ma120_decision}")
    false_cross = intraday._ma120_cross_buy_decision({
        "latest": 10.2,
        "prev_bar_close": 10.1,
        "ma120_1m": 10.0,
        "crossed_up_ma": [],
        "ma": {"ma5": 10.25, "ma10": 10.12, "ma20": 10.18, "ma60": 10.05, "ma120": 10.0},
        "above_ma": ["ma10", "ma60", "ma120"],
    })
    record("分时买入MA120必须本分钟明确上穿才触发", false_cross is None, str(false_cross))
    multi_ma_without_120 = intraday._ma120_cross_buy_decision({
        "latest": 10.3,
        "prev_bar_close": 10.0,
        "ma120_1m": 10.6,
        "crossed_up_ma": ["ma5", "ma10", "ma20", "ma60"],
        "ma": {"ma5": 10.05, "ma10": 10.0, "ma20": 9.95, "ma60": 9.9, "ma120": 10.6},
        "above_ma": ["ma5", "ma10", "ma20", "ma60"],
    })
    record("分时买入MA120触发仍要求明确上穿", multi_ma_without_120 is None, str(multi_ma_without_120))
    moving_ma_true_cross = intraday._ma120_cross_buy_decision({
        "latest": 10.3,
        "prev_bar_close": 10.1,
        "ma120_1m": 10.0,
        "crossed_up_ma": ["ma120"],
        "ma": {"ma5": 10.05, "ma10": 10.0, "ma20": 9.95, "ma60": 9.9, "ma120": 10.0},
        "above_ma": ["ma5", "ma10", "ma20", "ma60", "ma120"],
    })
    moving_ma_cross_ok = (
        moving_ma_true_cross
        and moving_ma_true_cross.get("technical_trigger") == "MA120_CROSS_UP"
        and moving_ma_true_cross.get("force_llm_review")
    )
    record("分时买入MA120上穿以crossed_up_ma为准不被当前均线二次误杀", moving_ma_cross_ok, str(moving_ma_true_cross))
    continuation_snapshot = {
        "latest": 10.35,
        "day_open": 10.0,
        "prev_bar_close": 10.2,
        "high_retreat_pct": -0.4,
        "above_ma": ["ma5", "ma10", "ma20"],
        "crossed_up_ma": ["ma5", "ma10"],
        "macd_hist": 0.02,
        "kdj_k": 61,
        "kdj_d": 55,
        "rsi14": 62,
        "bb_upper": 10.9,
        "vwap": 10.18,
        "vwap_distance_pct": 1.67,
        "change_pct": 3.5,
        "ma": {"ma5": 10.2, "ma20": 10.1},
    }
    continuation_decision = intraday._opening_continuation_buy_decision(
        continuation_snapshot,
        datetime(2026, 5, 19, 9, 35),
        {},
    )
    pullback_decision = intraday._pullback_resume_buy_decision(
        {**continuation_snapshot, "latest": 10.6, "prev_bar_close": 10.05, "crossed_up_ma": ["ma5"], "vwap": 10.1, "vwap_distance_pct": 1.0},
        datetime(2026, 5, 19, 10, 15),
        {},
    )
    anti_chase_block = intraday._opening_continuation_buy_decision(
        {**continuation_snapshot, "rsi14": 88},
        datetime(2026, 5, 19, 9, 35),
        {},
    )
    new_trigger_ok = (
        continuation_decision and continuation_decision.get("technical_trigger") == "OPENING_STRENGTH_CONTINUATION"
        and continuation_decision.get("force_llm_review")
        and pullback_decision and pullback_decision.get("technical_trigger") == "PULLBACK_RESUME"
        and pullback_decision.get("force_llm_review")
        and anti_chase_block and anti_chase_block.get("action") == "WAIT"
        and anti_chase_block.get("anti_chase_reason")
    )
    record("分时买入新增早盘强势延续/回踩再上攻并反追高", new_trigger_ok, f"cont={continuation_decision} pullback={pullback_decision} block={anti_chase_block}")
    short_ma_bars = [
        {"time": datetime.combine(date.today(), datetime.min.time()).replace(hour=9, minute=31), "open": 9.8, "high": 9.9, "low": 9.7, "close": 9.8},
        {"time": datetime.combine(date.today(), datetime.min.time()).replace(hour=9, minute=32), "open": 9.8, "high": 10.2, "low": 9.8, "close": 10.2},
    ]
    short_ma_snapshot = intraday._intraday_technical_snapshot("000001", {"price": 10.2, "open": 9.8, "high": 10.2, "low": 9.7}, short_ma_bars)
    last_day = date.today() - timedelta(days=1)
    prev_tail = [
        {
            "time": datetime.combine(last_day, datetime.min.time()).replace(hour=13, minute=1) + timedelta(minutes=i),
            "open": 10.0,
            "high": 10.0,
            "low": 10.0,
            "close": 10.0,
        }
        for i in range(118)
    ]
    full_ma_snapshot = intraday._intraday_technical_snapshot("000001", {"price": 10.2, "open": 9.8, "high": 10.2, "low": 9.7}, prev_tail + short_ma_bars)
    strict_ma_ok = (
        short_ma_snapshot.get("ma120") is None
        and "ma120" not in (short_ma_snapshot.get("ma") or {})
        and full_ma_snapshot.get("ma120") is not None
        and abs(full_ma_snapshot.get("ma120") - ((118 * 10.0 + 9.8 + 10.2) / 120)) < 1e-9
    )
    record("分时买入MA120严格使用120根1分钟收盘价", strict_ma_ok, f"short={short_ma_snapshot.get('ma120')} full={full_ma_snapshot.get('ma120')}")
    intraday_source = (WF / "intraday_executor.py").read_text(encoding="utf-8")
    kline_count_limited = "count=60000" not in intraday_source and "INTRADAY_BUY_1M_BAR_COUNT" in intraday_source
    record("分时买入1分钟K线请求数量受限避免拖慢轮询", kline_count_limited, "no count=60000")
    text_decision = intraday._parse_buy_timing_text_response(
        "盘口回踩承接良好，可以买。\n"
        "action：buy_now\n"
        "price_mode: follow\n"
        "max_premium_pct：0.5\n"
        "confidence: 78\n"
        "reason：1分钟MA120上穿后承接稳定\n"
    )
    text_parse_ok = (
        text_decision
        and text_decision.get("action") == "BUY_NOW"
        and text_decision.get("price_mode") == "FOLLOW"
        and text_decision.get("limit_price") is None
        and text_decision.get("confidence") == 78
    )
    record("分时买入文本兜底解析兼容中文冒号和缺省报价", text_parse_ok, str(text_decision))
    json_text_decision = intraday._parse_buy_timing_text_response(
        "```json\n"
        "{\"action\":\"BUY_NOW\",\"price_mode\":\"FOLLOW\",\"limit_price\":null,"
        "\"max_premium_pct\":0.5,\"confidence\":81,\"reason\":\"MA120上穿后承接稳定\"}"
        "\n```"
    )
    json_text_parse_ok = (
        json_text_decision
        and json_text_decision.get("action") == "BUY_NOW"
        and json_text_decision.get("price_mode") == "FOLLOW"
        and json_text_decision.get("limit_price") is None
        and json_text_decision.get("confidence") == 81
    )
    record("分时买入文本兜底解析兼容JSON代码块", json_text_parse_ok, str(json_text_decision))
    cn_text_decision = intraday._parse_buy_timing_text_response(
        "技术面转强，允许买入。\n"
        "动作：买入\n"
        "报价模式：跟随\n"
        "限价：无\n"
        "最大溢价：0.5\n"
        "置信度：79\n"
        "理由：MA120上穿后量价承接正常\n"
    )
    cn_text_parse_ok = (
        cn_text_decision
        and cn_text_decision.get("action") == "BUY_NOW"
        and cn_text_decision.get("price_mode") == "FOLLOW"
        and cn_text_decision.get("limit_price") is None
        and cn_text_decision.get("confidence") == 79
    )
    record("分时买入文本兜底解析兼容中文字段和值", cn_text_parse_ok, str(cn_text_decision))
    cn_json_decision = intraday._parse_buy_timing_text_response(
        '{"动作":"撤单重报","报价模式":"跟随","限价":null,"最大溢价":0.5,"置信度":82,"理由":"原挂单未成交且走势继续上攻"}'
    )
    cn_json_parse_ok = (
        cn_json_decision
        and cn_json_decision.get("action") == "CANCEL_REBUY"
        and cn_json_decision.get("price_mode") == "FOLLOW"
        and cn_json_decision.get("confidence") == 82
    )
    record("分时买入文本兜底解析兼容中文JSON字段", cn_json_parse_ok, str(cn_json_decision))

    original_output_dir = intraday.OUTPUT_DIR
    try:
        with tempfile.TemporaryDirectory() as tmpdir:
            intraday.OUTPUT_DIR = Path(tmpdir)
            prev_state = {
                "date": "2026-05-19",
                "selected_stocks": ["000001", "000002", "000006", "000007", "000008", "000004"],
                "selected_signals": [
                    {"stock": "000001", "name": "未成交一", "signal": "BUY", "confidence": 82, "position_ratio": "20%"},
                    {"stock": "000002", "name": "已成交", "signal": "BUY", "confidence": 80, "position_ratio": "20%"},
                    {"stock": "000006", "name": "当日放弃但未成交", "signal": "WATCH", "confidence": 68, "position_ratio": "15%"},
                    {"stock": "000007", "name": "截止撤单未买入", "signal": "BUY", "confidence": 76, "position_ratio": "20%"},
                    {"stock": "000008", "name": "仍挂单未买入", "signal": "WATCH", "confidence": 65, "position_ratio": "15%"},
                    {"stock": "000004", "name": "已顺延一次", "signal": "WATCH", "confidence": 70, "position_ratio": "15%"},
                ],
                "carryover_stocks": ["000004"],
                "stocks": {
                    "000001": {"status": "open"},
                    "000002": {"status": "filled", "filled_quantity": 1000},
                    "000006": {"status": "skip_today"},
                    "000007": {"status": "cancelled"},
                    "000008": {"status": "pending"},
                    "000004": {"status": "open"},
                },
            }
            (intraday.OUTPUT_DIR / "intraday_buy_timing_20260519.json").write_text(json.dumps(prev_state, ensure_ascii=False), encoding="utf-8")
            prev_report = {
                "phase2": {
                    "top_picks": [
                        {"stock": "000001", "name": "未成交一", "signal": "BUY", "confidence": 82, "position_ratio": "20%"},
                        {"stock": "000002", "name": "已成交", "signal": "BUY", "confidence": 80, "position_ratio": "20%"},
                        {"stock": "000006", "name": "当日放弃但未成交", "signal": "WATCH", "confidence": 68, "position_ratio": "15%"},
                        {"stock": "000007", "name": "截止撤单未买入", "signal": "BUY", "confidence": 76, "position_ratio": "20%"},
                        {"stock": "000008", "name": "仍挂单未买入", "signal": "WATCH", "confidence": 65, "position_ratio": "15%"},
                        {"stock": "000004", "name": "已顺延一次", "signal": "WATCH", "confidence": 70, "position_ratio": "15%"},
                    ]
                }
            }
            today_pool = [{"stock": "000005", "name": "今日Top", "signal": "BUY", "confidence": 90, "position_ratio": "20%"}]
            carryovers = intraday._carryover_intraday_timing_signals(today_pool, today=datetime(2026, 5, 20).date())
            merged = intraday._merge_intraday_timing_pool(today_pool, carryovers)
            carry_ok = (
                [s["stock"] for s in carryovers] == ["000001", "000006", "000007", "000008"]
                and carryovers[0]["signal"] == "BUY"
                and carryovers[0]["confidence"] == 82
                and carryovers[1]["signal"] == "WATCH"
                and carryovers[1]["confidence"] == 68
                and carryovers[2]["signal"] == "BUY"
                and carryovers[2]["confidence"] == 76
                and carryovers[3]["signal"] == "WATCH"
                and carryovers[3]["confidence"] == 65
                and all(s.get("carryover_from") == "2026-05-19" for s in carryovers)
                and [s["stock"] for s in merged] == ["000005", "000001", "000006", "000007", "000008"]
            )
            duplicate_pool = [{"stock": "000001", "name": "今日重复", "signal": "WATCH", "confidence": 66, "position_ratio": "15%"}]
            duplicate_carry = intraday._carryover_intraday_timing_signals(duplicate_pool, today=datetime(2026, 5, 20).date())
            duplicate_ok = "000001" not in [s["stock"] for s in duplicate_carry]
            (intraday.OUTPUT_DIR / "intraday_buy_timing_20260519.json").unlink()
            stale_state = dict(prev_state)
            stale_state["date"] = "2026-05-18"
            (intraday.OUTPUT_DIR / "intraday_buy_timing_20260518.json").write_text(json.dumps(stale_state, ensure_ascii=False), encoding="utf-8")
            (intraday.OUTPUT_DIR / "daily_report_20260518.json").write_text(json.dumps(prev_report, ensure_ascii=False), encoding="utf-8")
            stale_carry = intraday._carryover_intraday_timing_signals(today_pool, today=datetime(2026, 5, 20).date())
            stale_ok = stale_carry == []
            compact_keeps_snapshot = bool(intraday._compact_buy_timing_state(prev_state).get("selected_signals"))
            (intraday.OUTPUT_DIR / "intraday_buy_timing_20260518.json").unlink()
            legacy_prev_state = dict(prev_state)
            legacy_prev_state.pop("selected_signals", None)
            legacy_prev_state["date"] = "2026-05-19"
            (intraday.OUTPUT_DIR / "intraday_buy_timing_20260519.json").write_text(json.dumps(legacy_prev_state, ensure_ascii=False), encoding="utf-8")
            (intraday.OUTPUT_DIR / "daily_report_20260519.json").write_text(json.dumps(prev_report, ensure_ascii=False), encoding="utf-8")
            legacy_carry = intraday._carryover_intraday_timing_signals(today_pool, today=datetime(2026, 5, 20).date())
            legacy_ok = [s["stock"] for s in legacy_carry] == ["000001", "000006", "000007", "000008"]
            record("上一交易日未买入票只顺延一次且状态快照保留BUY/WATCH置信值", carry_ok and duplicate_ok and stale_ok and compact_keeps_snapshot and legacy_ok, f"carry={carryovers} duplicate={duplicate_carry} stale={stale_carry} legacy={legacy_carry}")

            legacy_state_path = intraday.OUTPUT_DIR / "intraday_buy_timing_20260517.json"
            legacy_state_path.write_text(json.dumps({"selected_stocks": [], "stocks": {}}, ensure_ascii=False), encoding="utf-8")
            legacy_state = intraday._load_buy_timing_state(legacy_state_path)
            record("旧分时买入状态缺date时按文件名补交易日", legacy_state.get("date") == "2026-05-17", str(legacy_state))
    finally:
        intraday.OUTPUT_DIR = original_output_dir

    signal = {"stock": "000001", "name": "平安银行", "signal": "BUY", "confidence": 80, "position_ratio": "20%"}
    original = {
        "get_today_orders": intraday.get_today_orders,
        "get_intraday_buy_quote": intraday.get_intraday_buy_quote,
        "get_intraday_1m_bars": intraday.get_intraday_1m_bars,
        "_technical_buy_timing_decision": intraday._technical_buy_timing_decision,
        "call_llm_buy_timing_decision": intraday.call_llm_buy_timing_decision,
        "buy_stock": intraday.buy_stock,
        "cancel_buy_order": intraday.cancel_buy_order,
        "_get_available_cash": intraday._get_available_cash,
        "feishu_push": intraday.feishu_push,
        "sleep": intraday.time.sleep,
    }
    try:
        intraday.feishu_push = lambda *args, **kwargs: None
        buy_calls = []
        intraday.get_today_orders = lambda force=False: {"buys": [pending], "sells": []}
        intraday.get_intraday_buy_quote = lambda _stock: quote
        intraday.get_intraday_1m_bars = lambda _stock, *args, **kwargs: []
        intraday.call_llm_buy_timing_decision = lambda *a, **k: {
            "action": "BUY_NOW",
            "price_mode": "FOLLOW",
            "limit_price": None,
            "max_premium_pct": 0.8,
            "confidence": 80,
            "reason": "已有挂单时不应重复",
        }
        intraday.buy_stock = lambda *args, **kwargs: buy_calls.append(args) or {"status": "submitted", "order_id": "NEW"}
        intraday.cancel_buy_order = lambda *args, **kwargs: {"status": "submitted"}
        intraday.time.sleep = lambda *_args, **_kwargs: None
        state = {"stocks": {}, "rounds": []}
        intraday._run_buy_timing_round([signal], {"000001": "平安银行"}, state, 100000, 100000, datetime(2026, 5, 19, 10, 0))
        no_duplicate = len(buy_calls) == 0 and state["stocks"]["000001"]["status"] == "pending"
        record("已有挂单时BUY_NOW不会重复提交", no_duplicate, str(state["stocks"]["000001"]))

        old_cancelled = {
            "stock": "000001",
            "order_quantity": 1000,
            "quantity": 0,
            "status": "已撤",
            "order_id": "OID-OLD",
            "order_time": "2026-05-19T09:40:00",
        }
        later_pending = {
            "stock": "000001",
            "order_quantity": 1000,
            "quantity": 0,
            "status": "已报",
            "order_id": "OID-PENDING",
            "order_time": "2026-05-19T10:02:00",
        }
        buy_calls = []
        intraday.get_today_orders = lambda force=False: {"buys": [old_cancelled, later_pending], "sells": [], "_ok": True}
        state_multi_orders = {"stocks": {}, "rounds": []}
        intraday._run_buy_timing_round([signal], {"000001": "平安银行"}, state_multi_orders, 100000, 100000, datetime(2026, 5, 19, 10, 3))
        multi_order_no_duplicate = (
            len(buy_calls) == 0
            and state_multi_orders["stocks"]["000001"]["status"] == "pending"
            and state_multi_orders["stocks"]["000001"]["pending_order"]["order_id"] == "OID-PENDING"
        )
        record("同股多委托优先识别未成交挂单避免重复买入", multi_order_no_duplicate, str(state_multi_orders["stocks"]["000001"]))

        buy_calls = []
        intraday.get_today_orders = lambda force=False: {"buys": [], "sells": [], "_ok": True}
        intraday.get_intraday_buy_quote = lambda _stock: quote
        intraday.get_intraday_1m_bars = lambda _stock, *args, **kwargs: []
        intraday._get_available_cash = lambda: 100000
        intraday.call_llm_buy_timing_decision = lambda *a, **k: {
            "action": "BUY_NOW",
            "price_mode": "FOLLOW",
            "limit_price": None,
            "max_premium_pct": 0.8,
            "confidence": 80,
            "reason": "本地挂单已消失后重新买入",
        }
        intraday.buy_stock = lambda *args, **kwargs: buy_calls.append(args) or {"status": "submitted", "order_id": "RETRY"}
        state_missing_pending = {"stocks": {"000001": {"status": "pending", "pending_order": pending, "last_order": pending, "submitted_order_count": 1}}, "rounds": []}
        intraday._run_buy_timing_round([signal], {"000001": "平安银行"}, state_missing_pending, 100000, 100000, datetime(2026, 5, 19, 10, 10))
        cleared_missing_pending = (
            len(buy_calls) == 1
            and state_missing_pending["stocks"]["000001"]["status"] == "pending"
            and state_missing_pending["stocks"]["000001"]["pending_order"]["order_id"] == "RETRY"
            and state_missing_pending["stocks"]["000001"].get("last_order_status") == "not_found_in_order_snapshot"
        )
        record("订单快照成功但本地挂单消失时清理pending并允许重评", cleared_missing_pending, f"calls={buy_calls} state={state_missing_pending['stocks']['000001']}")

        buy_calls = []
        recent_pending = dict(pending)
        recent_pending["time"] = "2026-05-19T10:09:00"
        state_recent_missing = {"stocks": {"000001": {"status": "pending", "pending_order": recent_pending, "last_order": recent_pending, "submitted_order_count": 1}}, "rounds": []}
        intraday._run_buy_timing_round([signal], {"000001": "平安银行"}, state_recent_missing, 100000, 100000, datetime(2026, 5, 19, 10, 10))
        recent_missing_grace_ok = (
            len(buy_calls) == 0
            and state_recent_missing["stocks"]["000001"]["status"] == "pending"
            and state_recent_missing["stocks"]["000001"]["pending_order"]["order_id"] == "OID1"
            and state_recent_missing["stocks"]["000001"].get("last_order_status") == "missing_in_order_snapshot_grace"
        )
        record("刚提交挂单短暂未出现在订单快照时继续视为pending", recent_missing_grace_ok, f"calls={buy_calls} state={state_recent_missing['stocks']['000001']}")

        order_calls = []
        intraday.get_today_orders = lambda force=False: order_calls.append(force) or {"buys": [], "sells": []}
        llm_calls = []
        buy_calls = []
        intraday.call_llm_buy_timing_decision = lambda *a, **k: llm_calls.append(a) or {
            "action": "BUY_NOW",
            "price_mode": "FOLLOW",
            "limit_price": None,
            "max_premium_pct": 0.8,
            "confidence": 80,
            "reason": "LLM到间隔后买入",
        }
        state_llm = {"stocks": {"000001": {"last_llm_check_at": "2026-05-19T10:00:00"}}, "rounds": []}
        intraday._run_buy_timing_round([signal], {"000001": "平安银行"}, state_llm, 100000, 100000, datetime(2026, 5, 19, 10, 5))
        skipped_ok = len(llm_calls) == 0 and len(order_calls) == 0 and state_llm["stocks"]["000001"]["last_decision"].get("llm_skipped")
        intraday._run_buy_timing_round([signal], {"000001": "平安银行"}, state_llm, 100000, 100000, datetime(2026, 5, 19, 10, 10))
        due_ok = len(llm_calls) == 1 and len(order_calls) == 1 and state_llm["stocks"]["000001"].get("last_llm_check_at") == "2026-05-19T10:10:00"
        record("未到LLM间隔时每分钟只做技术触发检查且不查委托", skipped_ok and due_ok, f"llm_calls={len(llm_calls)} order_calls={len(order_calls)} state={state_llm['stocks']['000001']}")

        llm_calls = []
        buy_calls = []
        intraday.get_intraday_buy_quote = lambda _stock: {"price": 10.2, "open": 10.0, "high": 10.25, "low": 9.85, "source": "mock"}
        intraday.get_intraday_1m_bars = lambda _stock, *args, **kwargs: ma120_bars
        intraday.call_llm_buy_timing_decision = lambda *a, **k: llm_calls.append(a) or {
            "action": "BUY_NOW",
            "price_mode": "FOLLOW",
            "confidence": 80,
            "reason": "MA120上穿后LLM确认买入",
        }
        state_technical = {"stocks": {"000001": {"last_llm_check_at": "2026-05-19T13:00:00", "decisions": [{"action": "WAIT"}]}}, "rounds": []}
        intraday._run_buy_timing_round([signal], {"000001": "平安银行"}, state_technical, 100000, 100000, datetime(2026, 5, 19, 13, 1))
        hard_trigger_llm_review = (
            len(llm_calls) == 1
            and len(buy_calls) == 1
            and state_technical["stocks"]["000001"]["last_decision"].get("technical_trigger") == "MA120_CROSS_UP"
        )
        record("技术触发每分钟检查且触发LLM重评", hard_trigger_llm_review, f"calls={len(llm_calls)} buys={buy_calls}")

        llm_calls = []
        buy_calls = []
        intraday.get_today_orders = lambda force=False: {"buys": [], "sells": [], "_ok": True}
        intraday.get_intraday_buy_quote = lambda _stock: quote
        intraday.get_intraday_1m_bars = lambda _stock, *args, **kwargs: []
        intraday._technical_buy_timing_decision = lambda *a, **k: {
            "action": "LLM_REVIEW",
            "technical_trigger": "FORCED_TEST_REVIEW",
            "force_llm_review": True,
            "reason": "测试强制进入LLM复核",
        }
        intraday.call_llm_buy_timing_decision = lambda *a, **k: llm_calls.append(a) or {
            "action": "BUY_NOW",
            "price_mode": "FOLLOW",
            "confidence": 80,
            "reason": "技术触发后确认买入",
        }
        intraday.buy_stock = lambda *args, **kwargs: buy_calls.append(args) or {"status": "submitted", "order_id": "MULTI"}
        state_multi_trigger = {"stocks": {"000001": {"last_llm_check_at": "2026-05-19T13:01:00"}}, "rounds": []}
        intraday._run_buy_timing_round([signal], {"000001": "平安银行"}, state_multi_trigger, 100000, 100000, datetime(2026, 5, 19, 13, 2))
        multi_trigger_preserved = (
            len(llm_calls) == 1
            and len(buy_calls) == 1
            and state_multi_trigger["stocks"]["000001"]["last_decision"].get("technical_trigger") == "FORCED_TEST_REVIEW"
        )
        record("技术触发交给LLM后保留原始触发类型", multi_trigger_preserved, f"state={state_multi_trigger['stocks']['000001']}")

        intraday.get_intraday_buy_quote = lambda _stock: quote
        intraday.get_intraday_1m_bars = lambda _stock, *args, **kwargs: []
        intraday.get_today_orders = lambda force=False: {"buys": [pending], "sells": []}
        cancel_calls = []
        buy_calls = []
        intraday.call_llm_buy_timing_decision = lambda *a, **k: {
            "action": "CANCEL_REBUY",
            "price_mode": "CUSTOM",
            "limit_price": 10.07,
            "max_premium_pct": 0.7,
            "confidence": 82,
            "reason": "回踩企稳后重报",
        }
        intraday.cancel_buy_order = lambda *args, **kwargs: cancel_calls.append(args) or {"status": "submitted"}
        intraday.buy_stock = lambda *args, **kwargs: buy_calls.append(args) or {"status": "submitted", "order_id": "NEW2"}
        state2 = {"stocks": {"000001": {"status": "pending", "pending_order": pending}}, "rounds": []}
        intraday._run_buy_timing_round([signal], {"000001": "平安银行"}, state2, 100000, 100000, datetime(2026, 5, 19, 10, 10))
        rebuy_ok = (
            len(cancel_calls) == 1
            and len(buy_calls) == 1
            and buy_calls[0][2] == 10.15
            and state2["stocks"]["000001"]["reprice_count"] == 1
            and state2["stocks"]["000001"]["pending_order"]["price_mode"] == "UNIFIED_1_015"
        )
        record("LLM可撤单后按统一报价重报", rebuy_ok, f"cancel={cancel_calls} buy={buy_calls} state={state2}")

        buy_calls = []
        intraday.buy_stock = lambda *args, **kwargs: buy_calls.append(args) or {"status": "submitted", "order_id": "FAST"}
        fast_entry = {}
        returned_cash = intraday._execute_buy_timing_action(
            "000001",
            "平安银行",
            signal,
            quote,
            {"action": "BUY_NOW", "price_mode": "FOLLOW", "confidence": 80, "reason": "兼容线程测试"},
            fast_entry,
            100000,
            100000,
            datetime(2026, 5, 19, 10, 10),
        )
        gem_quote = {"price": 48.85, "open": 53.01, "high": 54.05, "low": 48.76, "prev_close": 55.20, "change_pct": -11.5, "limit_down": 44.16, "limit_up": 66.24, "source": "mock"}
        gem_signal = {"stock": "301132", "name": "满坤科技", "signal": "WATCH", "confidence": 70, "position_ratio": "10%"}
        gem_prompt = intraday._build_buy_timing_prompt(gem_signal, gem_quote, now=datetime(2026, 6, 29, 10, 42))
        gem_entry = {}
        buy_calls = []
        intraday.buy_stock = lambda *args, **kwargs: buy_calls.append(args) or {"status": "submitted", "order_id": "GEM-NOT-LIMIT-DOWN"}
        gem_cash = intraday._execute_buy_timing_action(
            "301132",
            "满坤科技",
            gem_signal,
            gem_quote,
            {"action": "BUY_NOW", "price_mode": "FOLLOW", "confidence": 80, "reason": "创业板未跌停测试"},
            gem_entry,
            100000,
            100000,
            datetime(2026, 6, 29, 10, 42),
        )
        gem_not_limit_down_ok = (
            not intraday._is_buy_quote_limit_down(gem_quote)
            and '"is_limit_down": false' in gem_prompt
            and "change_pct <= -9.5%" not in gem_prompt
            and "不要用固定-9.5%判断创业板/科创板跌停" in gem_prompt
            and gem_cash == 100000
            and len(buy_calls) == 1
            and gem_entry.get("status") == "pending"
        )
        record("创业板跌超9.5%但未到20%跌停时不拦截买入", gem_not_limit_down_ok, f"prompt_has_limit={'is_limit_down' in gem_prompt} entry={gem_entry} calls={buy_calls}")

        no_120m_api = not hasattr(intraday, "get_120m_bars")
        record("分时买入技术判断不再保留120m K线入口", no_120m_api, f"has_get_120m_bars={hasattr(intraday, 'get_120m_bars')}")

        cutoff_cancel_calls = []
        intraday.get_today_orders = lambda force=False: {"buys": [], "sells": [], "_ok": False}
        intraday.cancel_buy_order = (
            lambda *args, **kwargs: cutoff_cancel_calls.append(args) or {"status": "submitted"}
        )
        cutoff_state = {
            "stocks": {
                "000001": {
                    "status": "pending",
                    "pending_order": {
                        "stock": "000001",
                        "order_id": "LOCAL-PENDING",
                        "quantity": 1000,
                        "status": "已报",
                    },
                },
                "000002": {
                    "status": "pending",
                    "pending_order": {
                        "stock": "000002",
                        "order_id": "OTHER-PENDING",
                        "quantity": 1000,
                        "status": "已报",
                    },
                },
            }
        }
        cancelled = intraday._cancel_timing_pending_orders(cutoff_state, "截止测试", {"000001"})
        cutoff_fallback_ok = (
            len(cutoff_cancel_calls) == 1
            and cutoff_cancel_calls[0][0] == "LOCAL-PENDING"
            and cutoff_cancel_calls[0][1] == "000001"
            and cutoff_state["stocks"]["000001"]["status"] == "open"
            and cutoff_state["stocks"]["000002"]["status"] == "pending"
            and len(cancelled) == 1
        )
        record("分时买入截止撤单接口失败时用本地pending_order兜底", cutoff_fallback_ok, f"calls={cutoff_cancel_calls} state={cutoff_state}")
    finally:
        intraday.get_today_orders = original["get_today_orders"]
        intraday.get_intraday_buy_quote = original["get_intraday_buy_quote"]
        intraday.get_intraday_1m_bars = original["get_intraday_1m_bars"]
        intraday._technical_buy_timing_decision = original["_technical_buy_timing_decision"]
        intraday.call_llm_buy_timing_decision = original["call_llm_buy_timing_decision"]
        intraday.buy_stock = original["buy_stock"]
        intraday.cancel_buy_order = original["cancel_buy_order"]
        intraday._get_available_cash = original["_get_available_cash"]
        intraday.feishu_push = original["feishu_push"]
        intraday.time.sleep = original["sleep"]

    original_call_structured = None
    import stock_selection_debate.providers as providers
    original_call_structured = providers.call_structured
    original_text_call = intraday._call_thinking_minimax
    original_model = intraday.INTRADAY_BUY_TIMING_LLM_MODEL
    try:
        providers.call_structured = lambda *args, **kwargs: None
        intraday._call_thinking_minimax = lambda *args, **kwargs: (
            "技术触发后承接尚可。\n"
            "action: BUY_NOW\n"
            "price_mode: FOLLOW\n"
            "limit_price: null\n"
            "max_premium_pct: 0.5\n"
            "confidence: 76\n"
            "reason: 1分钟MA120上穿后未明显转弱"
        )
        intraday.INTRADAY_BUY_TIMING_LLM_MODEL = "minimax-portal/MiniMax-M3"
        fallback_decision = intraday.call_llm_buy_timing_decision(signal, quote, None, datetime(2026, 5, 19, 10, 10), {})
        record("分时买入结构化失败后可用MiniMax文本兜底", fallback_decision.get("action") == "BUY_NOW", str(fallback_decision))
        providers.call_structured = lambda *args, **kwargs: (_ for _ in ()).throw(RuntimeError("structured boom"))
        fallback_after_exception = intraday.call_llm_buy_timing_decision(signal, quote, None, datetime(2026, 5, 19, 10, 11), {})
        record(
            "分时买入结构化异常后仍可用MiniMax文本兜底",
            fallback_after_exception.get("action") == "BUY_NOW",
            str(fallback_after_exception),
        )
    finally:
        providers.call_structured = original_call_structured
        intraday._call_thinking_minimax = original_text_call
        intraday.INTRADAY_BUY_TIMING_LLM_MODEL = original_model

    original_load_trades = intraday._load_trades
    original_save_trades = intraday._save_trades
    try:
        trades_box = {"records": []}
        intraday._load_trades = lambda: trades_box
        intraday._save_trades = lambda data: trades_box.update(data)
        first_saved = intraday._append_confirmed_buy_trade(
            signal,
            {"stock": "000001", "trade_price": 10.08, "quantity": 1000, "order_id": "BATCH-A"},
            "平安银行",
            decision={"confidence": 80, "reason": "第一笔"},
        )
        duplicate_saved = intraday._append_confirmed_buy_trade(
            signal,
            {"stock": "000001", "trade_price": 10.08, "quantity": 1000, "order_id": "BATCH-A"},
            "平安银行",
            decision={"confidence": 80, "reason": "第一笔重复回查"},
        )
        second_saved = intraday._append_confirmed_buy_trade(
            signal,
            {"stock": "000001", "trade_price": 10.28, "quantity": 500, "order_id": "BATCH-B"},
            "平安银行",
            decision={"confidence": 82, "reason": "第二笔"},
        )
        third_saved = intraday._append_confirmed_buy_trade(
            signal,
            {"stock": "000001", "trade_price": 10.38, "quantity": 300, "order_time": "2026-05-19T10:30:00"},
            "平安银行",
            decision={"confidence": 83, "reason": "第三笔无委托号"},
        )
        fourth_saved = intraday._append_confirmed_buy_trade(
            signal,
            {"stock": "000001", "trade_price": 10.48, "quantity": 200, "order_time": "2026-05-19T10:40:00"},
            "平安银行",
            decision={"confidence": 84, "reason": "第四笔无委托号"},
        )
        records = trades_box.get("records", [])
        multi_lot_ok = (
            first_saved
            and not duplicate_saved
            and second_saved
            and third_saved
            and fourth_saved
            and len(records) == 4
            and [rec.get("order_id") for rec in records[:2]] == ["BATCH-A", "BATCH-B"]
            and [rec.get("buy_price") for rec in records] == [10.08, 10.28, 10.38, 10.48]
            and records[0]["buy_records"][0].get("order_id") == "BATCH-A"
            and records[1]["buy_records"][0].get("order_id") == "BATCH-B"
            and records[2].get("trade_key") != records[3].get("trade_key")
            and records[2]["buy_records"][0].get("trade_key") == records[2].get("trade_key")
        )
        record("分时买入同股同日不同成交批次分别保留原始买入价", multi_lot_ok, str(records))
    finally:
        intraday._load_trades = original_load_trades
        intraday._save_trades = original_save_trades

    saved_records = []
    original_get_today_orders = intraday.get_today_orders
    original_append = intraday._append_confirmed_buy_trade
    original_reconcile = intraday.reconcile_trades_file_with_account
    try:
        intraday.get_today_orders = lambda force=False: {
            "buys": [{"stock": "000001", "trade_price": 10.08, "quantity": 1000, "order_quantity": 1000, "order_time": "2026-05-19T10:11:00"}],
            "sells": [],
        }
        intraday._append_confirmed_buy_trade = lambda sig, api_order, name, **kwargs: saved_records.append((sig, api_order, name, kwargs)) or True
        intraday.reconcile_trades_file_with_account = lambda *args, **kwargs: {"fixed": [], "is_consistent": True}
        state3 = {"stocks": {"000001": {"submitted_orders": [{"order_id": "NEW2"}]}}}
        fills = intraday._refresh_buy_timing_fills(state3, {"000001": signal}, {"000001": "平安银行"})
        fill_ok = state3["stocks"]["000001"]["status"] == "filled" and len(saved_records) == 1 and fills[0]["quantity"] == 1000
        record("分时买入只在成交回查确认后写交易记录", fill_ok, f"state={state3} fills={fills}")
        old_filled_state = {
            "stocks": {
                "000001": {
                    "status": "filled",
                    "filled_quantity": 1000,
                    "last_order": {"order_id": "OLD-FILL", "quantity": 1000},
                }
            }
        }
        compacted_old_filled = intraday._compact_buy_timing_state(old_filled_state)
        old_filled_entry = compacted_old_filled["stocks"]["000001"]
        record(
            "分时买入老filled状态可补齐委托号和trade_key",
            old_filled_entry.get("order_id") == "OLD-FILL"
            and old_filled_entry.get("trade_key") == "order:OLD-FILL",
            str(old_filled_entry),
        )
    finally:
        intraday.get_today_orders = original_get_today_orders
        intraday._append_confirmed_buy_trade = original_append
        intraday.reconcile_trades_file_with_account = original_reconcile

    saved_records = []
    try:
        intraday.get_today_orders = lambda force=False: {
            "buys": [{"stock": "000001", "trade_price": 10.08, "quantity": 500, "order_quantity": 1000, "status": "已报", "order_time": "2026-05-19T10:12:00"}],
            "sells": [],
        }
        intraday._append_confirmed_buy_trade = lambda sig, api_order, name, **kwargs: saved_records.append((sig, api_order, name, kwargs)) or True
        state4 = {"stocks": {"000001": {"submitted_orders": [{"order_id": "NEW3"}]}}}
        fills_partial = intraday._refresh_buy_timing_fills(state4, {"000001": signal}, {"000001": "平安银行"})
        partial_ok = (
            state4["stocks"]["000001"]["status"] == "pending"
            and state4["stocks"]["000001"]["partial_filled_quantity"] == 500
            and not saved_records
            and fills_partial == []
        )
        record("部分成交但仍挂单时暂不写交易记录", partial_ok, f"state={state4} fills={fills_partial}")
    finally:
        intraday.get_today_orders = original_get_today_orders
        intraday._append_confirmed_buy_trade = original_append

    saved_records = []
    try:
        intraday.get_today_orders = lambda force=False: {
            "buys": [{"stock": "000001", "trade_price": 10.08, "quantity": 500, "order_quantity": 1000, "status": "已撤", "order_time": "2026-05-19T14:57:00"}],
            "sells": [],
        }
        intraday._append_confirmed_buy_trade = lambda sig, api_order, name, **kwargs: saved_records.append((sig, api_order, name, kwargs)) or True
        state_partial_cancelled = {"stocks": {"000001": {"submitted_orders": [{"order_id": "NEW4"}]}}}
        fills_partial_cancelled = intraday._refresh_buy_timing_fills(state_partial_cancelled, {"000001": signal}, {"000001": "平安银行"})
        partial_cancelled_ok = (
            state_partial_cancelled["stocks"]["000001"]["status"] == "filled"
            and state_partial_cancelled["stocks"]["000001"]["filled_quantity"] == 500
            and len(saved_records) == 1
            and fills_partial_cancelled[0]["quantity"] == 500
        )
        record("部分成交后撤单按实际成交数量写入买入记录", partial_cancelled_ok, f"state={state_partial_cancelled} fills={fills_partial_cancelled}")
    finally:
        intraday.get_today_orders = original_get_today_orders
        intraday._append_confirmed_buy_trade = original_append

    original_get_today_orders = intraday.get_today_orders
    original_cancel_buy = intraday.cancel_buy_order
    try:
        cancelled = []
        intraday.get_today_orders = lambda force=False: {
            "buys": [
                {"stock": "000001", "order_id": "A", "order_quantity": 100, "quantity": 0, "status": "已报"},
                {"stock": "000002", "order_id": "B", "order_quantity": 100, "quantity": 0, "status": "已报"},
            ],
            "sells": [],
            "_ok": True,
        }
        intraday.cancel_buy_order = lambda order_id, stock="", reason="": cancelled.append((stock, order_id)) or {"status": "submitted"}
        intraday._cancel_timing_pending_orders({"stocks": {}}, "测试过滤", {"000001"})
        record("截止撤单只处理本流程Top5股票", cancelled == [("000001", "A")], str(cancelled))
    finally:
        intraday.get_today_orders = original_get_today_orders
        intraday.cancel_buy_order = original_cancel_buy

    command_check_ok = (
        intraday._command_is_buy_timing("python3 intraday_executor.py --mode=buy-timing")
        and intraday._command_is_buy_timing("python3 intraday_executor.py --mode buy")
        and not intraday._command_is_buy_timing("python3 intraday_executor.py --mode=buy-legacy")
        and not intraday._command_is_buy_timing("python3 intraday_executor.py --mode=monitor")
    )
    record("分时买入进程识别不误判buy-legacy", command_check_ok, "")

    stale_state = intraday._compact_buy_timing_state({
        "date": "2026-05-19",
        "started_at": "2026-05-19T09:31:00",
        "finished_at": "2026-05-19T14:57:00",
        "stocks": {"000001": {"status": "pending"}},
    })
    stale_state["started_at"] = "2026-05-19T10:00:00"
    stale_state.pop("finished_at", None)
    record(
        "分时买入新启动会清理旧finished_at",
        stale_state.get("started_at") == "2026-05-19T10:00:00" and "finished_at" not in stale_state,
        str(stale_state),
    )

    original_output_dir = intraday.OUTPUT_DIR
    original_pid_check = intraday._pid_looks_like_buy_timing
    try:
        with tempfile.TemporaryDirectory(prefix="buy-timing-lock-") as tmpdir:
            tmp_dir = Path(tmpdir)
            intraday.OUTPUT_DIR = tmp_dir
            (tmp_dir / "buy_timing.pid").write_text("999999", encoding="utf-8")
            intraday._pid_looks_like_buy_timing = lambda _pid: False
            intraday._cleanup_stale_buy_timing_pid()
            stale_pid_cleaned = not (tmp_dir / "buy_timing.pid").exists()
            first_lock = intraday._acquire_buy_timing_process_lock()
            intraday._pid_looks_like_buy_timing = lambda _pid: True
            second_lock = intraday._acquire_buy_timing_process_lock()
            lock_ok = (
                stale_pid_cleaned
                and bool(first_lock)
                and second_lock is None
                and (tmp_dir / "buy_timing.pid").exists()
                and bool(list(tmp_dir.glob("intraday_buy_timing_*.lockdir")))
            )
            intraday._release_buy_timing_process_lock(first_lock)
            released_ok = not (tmp_dir / "buy_timing.pid").exists() and not list(tmp_dir.glob("intraday_buy_timing_*.lockdir"))
        record("分时买入进程单例锁阻止重复启动", lock_ok and released_ok, f"lock_ok={lock_ok} released_ok={released_ok}")
    finally:
        intraday.OUTPUT_DIR = original_output_dir
        intraday._pid_looks_like_buy_timing = original_pid_check

    original_acquire_lock = intraday._acquire_buy_timing_process_lock
    original_feishu_push = intraday.feishu_push
    original_launch_earliest = intraday._buy_timing_launch_earliest
    execute_debate_result = importlib.import_module("execute_debate_result")
    original_is_trading_day = execute_debate_result.is_trading_day
    old_push_non_trading = intraday.os.environ.get("INTRADAY_PUSH_NON_TRADING_SKIP")
    try:
        lock_attempts = []
        pushes = []
        execute_debate_result.is_trading_day = lambda: False
        intraday._acquire_buy_timing_process_lock = lambda: lock_attempts.append(True) or {}
        intraday.feishu_push = lambda msg, webhook=None: pushes.append(msg)
        intraday.os.environ.pop("INTRADAY_PUSH_NON_TRADING_SKIP", None)
        non_trading_exit_code = intraday.run_buy_timing_mode()
        record("分时买入非交易日默认静默跳过且不拿锁", non_trading_exit_code == 0 and not lock_attempts and not pushes, f"code={non_trading_exit_code} locks={lock_attempts} pushes={pushes}")

        lock_attempts = []
        execute_debate_result.is_trading_day = lambda: True
        intraday._buy_timing_launch_earliest = lambda: dt_time(23, 59)
        intraday._acquire_buy_timing_process_lock = lambda: lock_attempts.append(True) or {}
        too_early_exit_code = intraday.run_buy_timing_mode()
        record("分时买入过早启动直接退出且不拿锁", too_early_exit_code == 0 and not lock_attempts, f"code={too_early_exit_code} locks={lock_attempts}")
        intraday._buy_timing_launch_earliest = original_launch_earliest

        pushes = []
        execute_debate_result.is_trading_day = lambda: True
        intraday._acquire_buy_timing_process_lock = lambda: None
        old_allow_outside = intraday.os.environ.get("ALLOW_BUY_OUTSIDE_WINDOW")
        intraday.os.environ["ALLOW_BUY_OUTSIDE_WINDOW"] = "1"
        duplicate_exit_code = intraday.run_buy_timing_mode()
        record("分时买入重复启动返回非零避免外层误判成功", duplicate_exit_code == 75 and pushes, f"code={duplicate_exit_code} pushes={pushes}")
    finally:
        intraday._acquire_buy_timing_process_lock = original_acquire_lock
        intraday.feishu_push = original_feishu_push
        intraday._buy_timing_launch_earliest = original_launch_earliest
        execute_debate_result.is_trading_day = original_is_trading_day
        if old_push_non_trading is None:
            intraday.os.environ.pop("INTRADAY_PUSH_NON_TRADING_SKIP", None)
        else:
            intraday.os.environ["INTRADAY_PUSH_NON_TRADING_SKIP"] = old_push_non_trading
        if 'old_allow_outside' in locals():
            if old_allow_outside is None:
                intraday.os.environ.pop("ALLOW_BUY_OUTSIDE_WINDOW", None)
            else:
                intraday.os.environ["ALLOW_BUY_OUTSIDE_WINDOW"] = old_allow_outside

    startup_originals = {
        "OUTPUT_DIR": intraday.OUTPUT_DIR,
        "API_KEY": intraday.API_KEY,
        "_load_daily_report_with_top_picks": intraday._load_daily_report_with_top_picks,
        "_load_ready_daily_report_for_timing": intraday._load_ready_daily_report_for_timing,
        "_carryover_intraday_timing_signals": intraday._carryover_intraday_timing_signals,
        "_get_available_cash": intraday._get_available_cash,
        "_run_buy_timing_round": intraday._run_buy_timing_round,
        "_refresh_buy_timing_fills": intraday._refresh_buy_timing_fills,
        "_cancel_timing_pending_orders": intraday._cancel_timing_pending_orders,
        "feishu_push": intraday.feishu_push,
        "is_trading_day": execute_debate_result.is_trading_day,
        "allow_env": intraday.os.environ.get("ALLOW_BUY_OUTSIDE_WINDOW"),
        "once_env": intraday.os.environ.get("INTRADAY_BUY_TIMING_ONCE"),
    }
    try:
        execute_debate_result.is_trading_day = lambda: True
        with tempfile.TemporaryDirectory(prefix="buy-timing-start-") as tmpdir:
            intraday.OUTPUT_DIR = Path(tmpdir)
            intraday.API_KEY = "dummy"
            cash_calls = []
            round_calls = []
            intraday._load_ready_daily_report_for_timing = lambda *_args, **_kwargs: {
                "phase2": {"top_picks": [signal], "ranked_candidates": [signal]}
            }
            intraday._carryover_intraday_timing_signals = lambda *_args, **_kwargs: []
            intraday._get_available_cash = lambda: cash_calls.append(True) or 100000
            intraday._run_buy_timing_round = lambda signals_arg, name_map_arg, state_arg, initial_cash_arg, available_cash_arg, now_arg=None: round_calls.append((signals_arg, initial_cash_arg)) or available_cash_arg
            intraday._refresh_buy_timing_fills = lambda *_args, **_kwargs: []
            intraday._cancel_timing_pending_orders = lambda *_args, **_kwargs: []
            intraday.feishu_push = lambda *_args, **_kwargs: None
            intraday.os.environ["ALLOW_BUY_OUTSIDE_WINDOW"] = "1"
            intraday.os.environ["INTRADAY_BUY_TIMING_ONCE"] = "1"
            intraday._run_buy_timing_mode_unlocked()
            startup_no_cash = not cash_calls and round_calls and round_calls[0][1] is None
            record("分时买入启动不预查mx-moni资金", startup_no_cash, f"cash_calls={cash_calls} round_calls={round_calls}")

        with tempfile.TemporaryDirectory(prefix="buy-timing-carry-start-") as tmpdir:
            intraday.OUTPUT_DIR = Path(tmpdir)
            intraday.API_KEY = "dummy"
            ready_wait_calls = []
            round_calls = []
            carry_signal = {"stock": "000009", "name": "昨日顺延", "signal": "WATCH", "confidence": 68}
            intraday._load_daily_report_with_top_picks = lambda *_args, **_kwargs: None
            intraday._load_ready_daily_report_for_timing = lambda *_args, **_kwargs: ready_wait_calls.append(True) or None
            intraday._carryover_intraday_timing_signals = lambda *_args, **_kwargs: [carry_signal]
            intraday._run_buy_timing_round = lambda signals_arg, name_map_arg, state_arg, initial_cash_arg, available_cash_arg, now_arg=None: round_calls.append((signals_arg, list(state_arg.get("selected_stocks") or []))) or available_cash_arg
            intraday._refresh_buy_timing_fills = lambda *_args, **_kwargs: []
            intraday._cancel_timing_pending_orders = lambda *_args, **_kwargs: []
            intraday.feishu_push = lambda *_args, **_kwargs: None
            intraday.os.environ["ALLOW_BUY_OUTSIDE_WINDOW"] = "1"
            intraday.os.environ["INTRADAY_BUY_TIMING_ONCE"] = "1"
            intraday._run_buy_timing_mode_unlocked()
            carry_start_ok = (
                not ready_wait_calls
                and round_calls
                and [s.get("stock") for s in round_calls[0][0]] == ["000009"]
                and round_calls[0][1] == ["000009"]
            )
            record("早报Top5未就绪时昨日未成交顺延票先进入观察池", carry_start_ok, f"ready_wait={ready_wait_calls} rounds={round_calls}")
    finally:
        intraday.OUTPUT_DIR = startup_originals["OUTPUT_DIR"]
        intraday.API_KEY = startup_originals["API_KEY"]
        intraday._load_daily_report_with_top_picks = startup_originals["_load_daily_report_with_top_picks"]
        intraday._load_ready_daily_report_for_timing = startup_originals["_load_ready_daily_report_for_timing"]
        intraday._carryover_intraday_timing_signals = startup_originals["_carryover_intraday_timing_signals"]
        intraday._get_available_cash = startup_originals["_get_available_cash"]
        intraday._run_buy_timing_round = startup_originals["_run_buy_timing_round"]
        intraday._refresh_buy_timing_fills = startup_originals["_refresh_buy_timing_fills"]
        intraday._cancel_timing_pending_orders = startup_originals["_cancel_timing_pending_orders"]
        intraday.feishu_push = startup_originals["feishu_push"]
        execute_debate_result.is_trading_day = startup_originals["is_trading_day"]
        if startup_originals["allow_env"] is None:
            intraday.os.environ.pop("ALLOW_BUY_OUTSIDE_WINDOW", None)
        else:
            intraday.os.environ["ALLOW_BUY_OUTSIDE_WINDOW"] = startup_originals["allow_env"]
        if startup_originals["once_env"] is None:
            intraday.os.environ.pop("INTRADAY_BUY_TIMING_ONCE", None)
        else:
            intraday.os.environ["INTRADAY_BUY_TIMING_ONCE"] = startup_originals["once_env"]


def strategy_backtest_signal_checks():
    from backtest.strategy import parse_phase2_signals
    workflow = importlib.import_module("workflow")

    top5_payload = {
        "ranked_candidates": [
            {"stock": "600909", "signal": "BUY", "total_score": 70, "position_ratio": "20%", "simulate_buy": True},
            {"stock": "603986", "signal": "WATCH", "total_score": 82, "position_ratio": "25%", "simulate_buy": True},
            {"stock": "002371", "action": "WATCH", "total_score": 75, "position_ratio": "25%", "simulate_buy": True},
            {"stock": "600522", "final_decision": "[Structured] signal=WATCH confidence=72 position_ratio=25% reason=测试", "simulate_buy": True},
            {"stock": "002915", "final_decision": "**最终信号**: WATCH\n**置信度**: 72", "simulate_buy": True},
        ]
    }
    parsed = parse_phase2_signals(top5_payload)
    ok = (
        len(parsed) == 5
        and parsed["600909"]["action"] == "BUY"
        and parsed["603986"]["action"] == "WATCH"
        and parsed["603986"]["confidence"] == 82
        and parsed["002371"]["position_ratio"] == "25%"
        and all(sig.get("simulate_buy") for sig in parsed.values())
    )
    record("策略模拟解析Top5并保留WATCH买入模拟", ok, str(parsed))

    confidence_ok = (
        workflow._confidence_value({"total_score": 82}) == 82
        and workflow._confidence_value({"final_score": 76}) == 76
        and workflow._confidence_value({"confidence": 68}) == 68
    )
    record("策略回测置信度读取total_score/final_score兜底", confidence_ok, "total_score=82 final_score=76 confidence=68")


def feishu_daily_card_checks():
    workflow = importlib.import_module("workflow")

    phase2 = {
        "phase": "route_b_complete",
        "generated_at": "2026-05-18T09:30:00",
        "ranked_candidates": [
            {
                "stock": "000001",
                "name": "平安银行",
                "signal": "WATCH",
                "confidence": 80,
                "position_ratio": "0%",
                "reason": "等待放量确认",
                "final_decision": "[Structured] signal=WATCH confidence=80 position_ratio=0% reason=等待放量确认",
                "decision_source": "Structured",
            },
            {
                "stock": "000002",
                "name": "万科A",
                "signal": "WATCH",
                "confidence": 70,
                "position_ratio": 0.2,
                "reason": "\"回踩可观察",
                "final_decision": "[Structured] signal=WATCH confidence=70 position_ratio=20% reason=回踩可观察",
                "decision_source": "Structured",
                "data_quality_flags": ["KLINE_SHORT"],
            },
            {
                "stock": "000003",
                "name": "测试三",
                "signal": "AVOID",
                "confidence": 95,
                "position_ratio": "0%",
                "reason": "风险过高",
                "decision_source": "Structured",
            },
            {
                "stock": "000004",
                "name": "测试四",
                "signal": "WATCH",
                "confidence": 66,
                "position_ratio": "15%",
                "reason": "空方论点具有前瞻性优势，当前风险收益比不适合重仓介入；但极致资金流数据形成短期惯性，中性分析师的右侧确认策略具备可执行性。综合多空判断，15%仓位上限可在严格止损纪律下参与短期投机机会，但主方向应保持观望。",
                "final_decision": "[Structured] signal=WATCH confidence=66 position_ratio=15% reason=空方论点具有前瞻性优势，当前风险收益比不适合重仓介入；但极致资金流数据形成短期惯性，中性分析师的右侧确认策略具备可执行性。综合多空判断，15%仓位上限可在严格止损纪律下参与短期投机机会，但主方向应保持观望。",
                "decision_source": "Structured",
            },
        ],
        "data_quality_summary": {
            "affected_count": 1,
            "flag_counts": {"KLINE_SHORT": 1},
            "core_flag_counts": {"KLINE_SHORT": 1},
            "aux_flag_counts": {},
            "affected": [{"stock": "000002", "name": "万科A", "flags": ["KLINE_SHORT"]}],
        },
    }

    captured = {}
    original_push = workflow.feishu_push_card
    original_skills_dir = workflow.SKILLS_DIR
    original_disable_live_market = os.environ.get("OPENCLAW_DISABLE_LIVE_MARKET")
    phase1 = [{
        "name": "新闻分析师",
        "status": "success",
        "findings": (
            "有色金属、大消费、医药等板块领跌；"
            "电子、计算机等科技板块逆势走强，佰维存储等个股表现亮眼。"
        ),
    }]
    try:
        workflow.SKILLS_DIR = WF / "__missing_skills__"
        os.environ["OPENCLAW_DISABLE_LIVE_MARKET"] = "1"
        workflow.feishu_push_card = lambda card, webhook: captured.setdefault("card", card)
        workflow._send_daily_report_card(
            phase1,
            phase2,
            {},
            {},
            "mock-webhook",
            data_quality={"summary": phase2["data_quality_summary"]},
            exec_stats={"total": 3, "model": "mock-model", "decision_sources": {"Structured": 3}},
        )
    finally:
        workflow.feishu_push_card = original_push
        workflow.SKILLS_DIR = original_skills_dir
        if original_disable_live_market is None:
            os.environ.pop("OPENCLAW_DISABLE_LIVE_MARKET", None)
        else:
            os.environ["OPENCLAW_DISABLE_LIVE_MARKET"] = original_disable_live_market

    card = captured.get("card", {})
    content = "\n".join(
        elem.get("text", {}).get("content", "")
        for elem in card.get("elements", [])
        if elem.get("tag") == "div"
    )
    record("飞书早报过滤0仓位WATCH和K线偏短后展示观察Top", "今日无BUY，展示观察Top1" in content and "000001" not in content and "000002" not in content and "000004" in content, content[:300])
    record("飞书早报不把缺失指数伪装为0涨跌", "指数涨跌: 暂无数据" in content and "上证 +0.00%" not in content, content[:300])
    record("飞书早报从新闻提取强弱板块", "强势板块: 电子、计算机" in content and "弱势板块: 有色金属、大消费、医药" in content, content[:500])
    record("飞书早报拆分关键数据缺口", "关键缺口1/4: K线偏短1" in content, content[-300:])
    record("飞书早报完整展示长理由", "但主方向应保持观望" in content and "短期投机机会，但主方向" in content, content)


def data_quality_summary_checks():
    run_debate_phase = importlib.import_module("stock_selection_debate.run_debate_phase")
    workflow = importlib.import_module("workflow")

    summary = run_debate_phase._summarize_data_quality([
        {"stock": "000001", "name": "一号", "data_quality_flags": ["KLINE_SHORT", "SECTOR_MISSING"]},
        {"stock": "000001", "name": "一号", "data_quality_flags": ["KLINE_SHORT", "MONEY_FLOW_PARTIAL"]},
        {"stock": "000002", "name": "二号", "data_quality_flags": ["MONEY_FLOW_MISSING"]},
        {"stock": "000003", "name": "三号", "data_quality_flags": []},
    ])
    ok = (
        summary.get("affected_count") == 2
        and summary.get("flag_counts", {}).get("KLINE_SHORT") == 1
        and summary.get("flag_counts", {}).get("MONEY_FLOW_PARTIAL") == 1
        and summary.get("core_flag_counts", {}).get("MONEY_FLOW_MISSING") == 1
        and summary.get("aux_flag_counts", {}).get("SECTOR_MISSING") == 1
    )
    record("数据质量统计按股票去重并拆分核心/辅助", ok, str(summary))

    top = workflow._select_display_top5([
        {"stock": "000001", "signal": "BUY", "confidence": 99, "position_ratio": "25%", "data_quality_flags": ["KLINE_SHORT"]},
        {"stock": "000002", "signal": "BUY", "confidence": 80, "position_ratio": "20%"},
        {"stock": "000003", "signal": "WATCH", "confidence": 90, "position_ratio": "25%", "data_quality_flags": ["KLINE_MISSING"]},
        {"stock": "000004", "signal": "WATCH", "confidence": 70, "position_ratio": "15%"},
    ], target=2)
    record("早报Top5过滤K线缺失/偏短", [s.get("stock") for s in top] == ["000002", "000004"], str(top))

    phase2 = run_debate_phase.debate_phase_to_phase2_format({
        "ranked_candidates": [
            {"stock": "000001", "signal": "BUY", "confidence": 99, "position_ratio": "20%", "data_quality_flags": ["KLINE_MISSING"]},
            {"stock": "000002", "signal": "BUY", "confidence": 80, "position_ratio": "20%"},
            {"stock": "000003", "signal": "WATCH", "confidence": 95, "position_ratio": "0%"},
            {"stock": "000004", "signal": "WATCH", "confidence": 70, "position_ratio": "15%"},
            {"stock": "000005", "signal": "AVOID", "confidence": 100, "position_ratio": "0%"},
        ]
    })
    record("phase2 top_picks 只保留可买BUY和正仓位WATCH", [s.get("stock") for s in phase2.get("top_picks", [])] == ["000002", "000004"], str(phase2.get("top_picks", [])))


def market_news_format_checks():
    workflow = importlib.import_module("workflow")

    findings = "\n".join([
        "1. 【一】第一条完整内容。",
        "   📍来源:来源A 2026-05-18 09:00:00",
        "2. 【二】第二条完整内容。",
        "   📍来源:来源B 2026-05-18 10:00:00",
        "3. 【三】第三条完整内容。",
        "   📍来源:来源C 2026-05-18 11:00:00",
        "4. 【四】第四条完整内容，不能被截断。",
        "   📍来源:来源D 2026-05-18 12:00:00",
        "5. 【五】第五条不应展示。",
        "   📍来源:来源E 2026-05-18 13:00:00",
    ])
    formatted = workflow._format_market_news(findings, max_items=4)
    ok = (
        "4. 【四】第四条完整内容，不能被截断。 | 来源:来源D" in formatted
        and "5. 【五】" not in formatted
    )
    record("市场要闻按完整条目展示前4条", ok, formatted)


def main():
    compile_check()
    for name, check in [
        ("JSON 解析测试执行", json_parser_checks),
        ("模型路由测试执行", model_routing_checks),
        ("生成侧配置测试执行", generation_config_checks),
        ("组合输出测试执行", portfolio_output_checks),
        ("盘中解析测试执行", intraday_parse_checks),
        ("候选来源测试执行", candidate_source_checks),
        ("盘中卖出监控测试执行", intraday_monitor_checks),
        ("分时买入测试执行", intraday_buy_timing_checks),
        ("策略模拟信号测试执行", strategy_backtest_signal_checks),
        ("飞书早报测试执行", feishu_daily_card_checks),
        ("数据质量测试执行", data_quality_summary_checks),
        ("市场要闻格式测试执行", market_news_format_checks),
    ]:
        try:
            check()
        except Exception as exc:
            record(name, False, repr(exc))

    failed = [result for result in results if not result["ok"]]
    print("\nSUMMARY")
    print(json.dumps({"total": len(results), "failed": len(failed), "failures": failed}, ensure_ascii=False, indent=2))
    sys.exit(1 if failed else 0)


if __name__ == "__main__":
    main()
