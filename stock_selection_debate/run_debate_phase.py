"""
选股辩论阶段入口
================
替代 Phase 2 的 LLM 打分

用法：
    from stock_selection_debate.run_debate_phase import run_debate_phase
    result = run_debate_phase(candidates, output_dir, model="volcengine-plan/ark-code-latest")
"""

import sys
import json
import logging
import time
import re
import os
from pathlib import Path
from datetime import date, timedelta
from typing import Dict, List, Any

logger = logging.getLogger("daily_stock_workflow.debate.run")

from scoring_overlay import (
    SCORING_VERSION,
    PROMPT_VERSION,
    EDGE_RULE_VERSION,
    TOP5_RULE_VERSION,
)


def _format_position_ratio(value: Any, decision_text: str = "") -> str:
    """Return a stable percent string from decimal, percent, or final_decision text."""
    raw = value
    if raw in (None, ""):
        m = re.search(r'position_ratio\s*[=：:]\s*([0-9.]+)\s*(%)?', decision_text or "", re.IGNORECASE)
        if m:
            raw = m.group(1)
            has_percent = bool(m.group(2)) or float(raw) > 1
        else:
            return "0%"
    else:
        has_percent = isinstance(raw, str) and "%" in raw

    try:
        ratio = float(str(raw).strip().rstrip("%"))
    except (TypeError, ValueError):
        return "0%"
    pct = ratio if has_percent or ratio > 1 else ratio * 100
    return f"{round(max(0.0, min(100.0, pct))):.0f}%"


def _extract_reason(decision_text: str) -> str:
    m = re.search(r'reason\s*[=：:]\s*([^\n]+)', decision_text or "", re.IGNORECASE)
    return m.group(1).strip().strip(",，}") if m else ""


def _numeric_value(value: Any, default: float = 0.0) -> float:
    try:
        if value not in (None, ""):
            return float(value)
    except (TypeError, ValueError):
        pass
    return default


def _pool_rank_value(candidate: Dict) -> int:
    try:
        rank = candidate.get("pool_rank")
        if rank not in (None, ""):
            return int(rank)
    except (TypeError, ValueError):
        pass
    return 999999


def _buy_score_value(candidate: Dict) -> float:
    for key in ("buy_score", "long_score", "ranking_score", "final_score", "quant_base_score"):
        value = candidate.get(key)
        if value not in (None, ""):
            return max(0.0, min(100.0, _numeric_value(value, 0.0)))
    signal = str(candidate.get("signal", "WATCH")).upper()
    confidence = _numeric_value(candidate.get("confidence"), 50.0)
    if signal == "BUY":
        return max(70.0, confidence)
    if signal == "WATCH":
        return min(max(55.0, confidence), 69.0)
    return min(confidence, 54.0)


def _clamp_score(value: Any, low: float = 0.0, high: float = 100.0, default: float = 50.0) -> float:
    try:
        score = float(value)
    except (TypeError, ValueError):
        score = default
    return max(low, min(high, score))


def _signal_from_score(score: float) -> str:
    score = _clamp_score(score)
    if score >= 70:
        return "BUY"
    if score >= 55:
        return "WATCH"
    return "AVOID"


_EDGE_RULE_PAYLOAD_CACHE = None


def _evaluate_historical_edge_overlay(candidate: Dict[str, Any]) -> Dict[str, Any]:
    """Return historical edge bonus and chase-risk penalty for one candidate."""
    global _EDGE_RULE_PAYLOAD_CACHE
    try:
        from candidate_edge_rules import evaluate_candidate_edge, load_latest_edge_rule_payloads
        output_dir = Path(__file__).resolve().parents[1] / "output"
        if _EDGE_RULE_PAYLOAD_CACHE is None:
            _EDGE_RULE_PAYLOAD_CACHE = load_latest_edge_rule_payloads(output_dir)
        return evaluate_candidate_edge(candidate, _EDGE_RULE_PAYLOAD_CACHE, output_dir=output_dir)
    except Exception as exc:
        logger.debug("historical edge overlay unavailable: %s", exc)
        return {
            "score": 0.0,
            "matches": [],
            "match_count": 0,
            "negative_score": 0.0,
            "negative_matches": [],
            "chase_risk_penalty": 0.0,
            "penalty_reasons": [],
            "watch_only": False,
            "payload_modes": [],
            "error": str(exc)[:200],
        }




def _apply_knowledge_rules_to_packet(packet: Dict[str, Any], candidate: Dict[str, Any] | None = None) -> Dict[str, Any]:
    """Attach local short-term knowledge-rule hits to the debate packet."""
    try:
        from .knowledge_rules import attach_knowledge_rules
    except Exception:
        try:
            from stock_selection_debate.knowledge_rules import attach_knowledge_rules
        except Exception as exc:
            logger.debug("knowledge rules unavailable: %s", exc)
            return packet
    try:
        return attach_knowledge_rules(packet, candidate)
    except Exception as exc:
        logger.warning("知识规则命中失败 %s: %s", packet.get("stock_code") or packet.get("stock"), exc)
        return packet


def _refresh_packet_contracts(packet: Dict[str, Any]) -> Dict[str, Any]:
    """Refresh data-router metadata and verified market snapshot after packet overrides."""
    try:
        from .data_router import attach_data_router_metadata
    except Exception:
        try:
            from stock_selection_debate.data_router import attach_data_router_metadata
        except Exception as exc:
            logger.debug("data router unavailable: %s", exc)
            attach_data_router_metadata = None
    if attach_data_router_metadata is not None:
        try:
            packet = attach_data_router_metadata(packet)
        except Exception as exc:
            logger.warning("数据路由合同刷新失败 %s: %s", packet.get("stock_code") or packet.get("stock"), exc)
    try:
        from .market_snapshot import attach_verified_market_snapshot
    except Exception:
        try:
            from stock_selection_debate.market_snapshot import attach_verified_market_snapshot
        except Exception as exc:
            logger.debug("market snapshot unavailable: %s", exc)
            attach_verified_market_snapshot = None
    if attach_verified_market_snapshot is not None:
        try:
            packet = attach_verified_market_snapshot(packet)
        except Exception as exc:
            logger.warning("行情事实快照刷新失败 %s: %s", packet.get("stock_code") or packet.get("stock"), exc)
    return packet


def _first_knowledge_blocker(candidate: Dict[str, Any]) -> str:
    for item in candidate.get("knowledge_rule_hits") or []:
        try:
            effect = float(item.get("effect") or 0)
        except (TypeError, ValueError):
            effect = 0.0
        if effect < 0 and item.get("watch_only"):
            return str(item.get("claim") or item.get("rule_id") or "知识规则要求盘中确认")[:160]
    return "知识规则要求盘中确认"


def _bool_value(value: Any, default: bool | None = None) -> bool | None:
    if isinstance(value, bool):
        return value
    if value in (None, ""):
        return default
    text = str(value).strip().lower()
    if text in {"true", "1", "yes", "y", "是", "允许"}:
        return True
    if text in {"false", "0", "no", "n", "否", "不允许"}:
        return False
    return default


def _execution_gate_fields(
    candidate: Dict[str, Any],
    final_signal: str,
    signal_blockers: List[str] | None = None,
) -> Dict[str, Any]:
    """Build execution fields from the final signal only.

    PM-provided gate fields are evidence for the PM decision, not authoritative
    after quantitative/risk post-processing changes the final signal.
    """
    final_signal = str(final_signal or "WATCH").upper()
    blockers = [str(x).strip() for x in (signal_blockers or []) if str(x).strip()]
    allow = final_signal == "BUY" and not blockers
    needs = final_signal == "WATCH"
    entry_condition = str(candidate.get("entry_condition") or "").strip()
    if allow:
        entry_condition = entry_condition or "开盘强势或盘中强势确认后执行"
        block_reason = ""
    elif needs:
        entry_condition = entry_condition or "盘中放量突破或回踩承接确认"
        block_reason = "；".join(blockers) or "最终信号为WATCH，需盘中确认"
    else:
        entry_condition = "不执行新开仓"
        block_reason = "；".join(blockers) or f"最终信号为{final_signal}"
    return {
        "allow_direct_buy": bool(allow),
        "needs_intraday_confirmation": bool(needs),
        "entry_condition": entry_condition[:160],
        "block_buy_reason": block_reason[:160],
    }


def _data_quality_profile(candidate: Dict[str, Any], dq_penalty: float, dq_hits: List[str]) -> Dict[str, Any]:
    contract = candidate.get("data_contract") or {}
    score = 100.0
    missing_core = []
    stale_items = []
    for key in ("kline", "money_flow", "financial", "sector"):
        item = contract.get(key) if isinstance(contract, dict) else {}
        if not isinstance(item, dict):
            continue
        status = str(item.get("status") or "unknown")
        if status in {"missing", "failed", "error"}:
            score -= 18 if key in {"kline", "money_flow"} else 8
            missing_core.append(key)
        elif status in {"partial", "unknown"}:
            score -= 8 if key in {"kline", "money_flow"} else 4
        if item.get("is_stale") is True:
            score -= 6
            stale_items.append(key)
    score -= min(20.0, dq_penalty * 0.6)
    score = round(max(0.0, min(100.0, score)), 2)
    core_blockers = set(dq_hits or []).intersection({"KLINE_MISSING", "KLINE_SHORT", "MODEL_FAILED"})
    tradable_data_ok = score >= 60 and not core_blockers and "kline" not in missing_core
    return {
        "data_quality_score": score,
        "tradable_data_ok": tradable_data_ok,
        "missing_core_data": sorted(set(missing_core)),
        "stale_data_items": stale_items,
    }


def _data_freshness_profile(candidate: Dict[str, Any]) -> Dict[str, Any]:
    contract = candidate.get("data_contract") or {}
    summary: Dict[str, Any] = {}
    if not isinstance(contract, dict):
        return summary
    for key, item in contract.items():
        if not isinstance(item, dict):
            continue
        summary[key] = {
            "source": item.get("source") or "unknown",
            "as_of": item.get("as_of") or item.get("timestamp") or item.get("date") or "",
            "age_minutes": item.get("age_minutes"),
            "is_stale": bool(item.get("is_stale", False)),
            "status": item.get("status") or "unknown",
        }
    return summary


def _model_score_summary(candidates: List[Dict[str, Any]]) -> Dict[str, Any]:
    grouped: Dict[str, Dict[str, Any]] = {}
    for c in candidates:
        model = ((c.get("decision_models") or {}).get("pm") or c.get("decision_source") or "unknown")
        item = grouped.setdefault(str(model), {"count": 0, "buy_score_sum": 0.0, "confidence_sum": 0.0, "signals": {}})
        item["count"] += 1
        item["buy_score_sum"] += _buy_score_value(c)
        item["confidence_sum"] += _numeric_value(c.get("confidence"), 0.0)
        sig = str(c.get("signal") or "unknown")
        item["signals"][sig] = item["signals"].get(sig, 0) + 1
    for item in grouped.values():
        count = max(1, item["count"])
        item["avg_buy_score"] = round(item.pop("buy_score_sum") / count, 2)
        item["avg_confidence"] = round(item.pop("confidence_sum") / count, 2)
    return grouped

def _copy_candidate_metadata_to_packet(packet: Dict[str, Any], candidate: Dict[str, Any]) -> None:
    """Keep screening metadata available through the debate engine and final report."""
    for key in (
        "pool", "source", "source_pools", "source_queries", "source_reasons",
        "screen_id", "screen_ids", "strategy_type", "strategy_types",
        "entry_bias", "entry_biases", "screening_reason", "pool_score",
        "pool_rank", "pool_score_detail", "pool_total_candidates",
        "pool_scored_candidates", "source_score_records",
    ):
        value = candidate.get(key)
        if value not in (None, "", [], {}):
            packet[key] = value




def _candidate_pool_names(candidate: Dict[str, Any]) -> List[str]:
    pools: List[str] = []
    for key in ("screen_ids", "source_pools"):
        value = candidate.get(key) or []
        if isinstance(value, (list, tuple, set)):
            pools.extend(str(x) for x in value if x)
    if not pools:
        for key in ("screen_id", "pool"):
            value = candidate.get(key)
            if value:
                pools.append(str(value))
    deduped: List[str] = []
    seen = set()
    for pool in pools:
        norm = pool.strip()
        if norm and norm not in seen:
            deduped.append(norm)
            seen.add(norm)
    return deduped


def _pool_dynamic_adjustment(candidate: Dict[str, Any]) -> tuple[float, Dict[str, Any]]:
    """Small Top5-review feedback adjustment by pool; never overrides current evidence."""
    pools = _candidate_pool_names(candidate)
    if not pools:
        return 0.0, {"pools": [], "samples": 0, "reason": "无池来源"}

    path = Path(__file__).resolve().parents[1] / "output" / "selection_memory.jsonl"
    pool_stats: Dict[str, Dict[str, Any]] = {
        pool: {"samples": 0, "days": set(), "wins": 0, "returns": [], "bad_labels": 0, "missed": 0}
        for pool in pools
    }
    if path.exists():
        try:
            lines = path.read_text(encoding="utf-8").splitlines()[-500:]
        except Exception:
            lines = []
        cutoff = date.today() - timedelta(days=20)
        for line in lines:
            try:
                item = json.loads(line)
            except Exception:
                continue
            report_day = str(item.get("report_date") or "")[:8]
            if len(report_day) == 8:
                try:
                    if date(int(report_day[:4]), int(report_day[4:6]), int(report_day[6:8])) < cutoff:
                        continue
                except Exception:
                    pass
            item_pools = _candidate_pool_names(item)
            matched = [pool for pool in pools if pool in item_pools]
            if not matched:
                continue
            ret = None
            for horizon in ("d3", "d5", "d1"):
                if item.get(f"return_{horizon}_complete") is True:
                    ret = item.get(f"return_{horizon}_pct")
                    if ret is not None:
                        break
            try:
                ret_f = float(ret)
            except (TypeError, ValueError):
                continue
            labels = set(item.get("attribution_labels") or [])
            for pool in matched:
                stat = pool_stats[pool]
                stat["samples"] += 1
                if report_day:
                    stat["days"].add(report_day)
                stat["returns"].append(ret_f)
                if ret_f > 0:
                    stat["wins"] += 1
                if labels.intersection({"SELECTION_WEAK", "QUANT_OVER_SCORE", "MONEY_FLOW_MISLEAD", "ENTRY_TOO_LATE"}):
                    stat["bad_labels"] += 1
                if labels.intersection({"GOOD_BUT_NOT_BOUGHT", "ENTRY_MISSED"}):
                    stat["missed"] += 1

    adjustments: List[float] = []
    reasons: List[str] = []
    for pool in pools:
        base_adj = 0.0
        stat = pool_stats.get(pool) or {}
        samples = int(stat.get("samples") or 0)
        trade_days = len(stat.get("days") or set())
        if samples >= 20 and trade_days >= 5:
            avg_ret = sum(stat["returns"]) / samples
            win_rate = stat["wins"] / samples
            bad_rate = stat["bad_labels"] / samples
            if avg_ret >= 3 and win_rate >= 0.55:
                base_adj += min(4.0, avg_ret * 0.4)
                reasons.append(f"{pool}:复盘均值{avg_ret:+.1f}%胜率{win_rate:.0%}")
            elif avg_ret <= 0 or bad_rate >= 0.45:
                base_adj -= 4.0
                reasons.append(f"{pool}:复盘偏弱/误导率高")
        elif samples:
            reasons.append(f"{pool}:样本{samples}/交易日{trade_days}，未达到启用门槛")
        adjustments.append(base_adj)

    if not adjustments:
        return 0.0, {"pools": pools, "samples": 0, "reason": "无可用复盘样本"}
    adjustment = round(max(-8.0, min(6.0, sum(adjustments) / len(adjustments))), 2)
    sample_count = sum(int((pool_stats.get(pool) or {}).get("samples") or 0) for pool in pools)
    return adjustment, {
        "pools": pools,
        "samples": sample_count,
        "adjustment": adjustment,
        "reasons": reasons[:5],
    }


def _score_money_flow(money_flow: Dict[str, Any]) -> tuple[float, Dict[str, Any]]:
    """Score capital-flow support on a stable 0-100 scale."""
    mf = money_flow if isinstance(money_flow, dict) else {}
    score = 50.0
    detail: Dict[str, Any] = {"source": mf.get("source", "none")}

    main = _numeric_value(mf.get("main_net_flow"), None)
    super_flow = _numeric_value(mf.get("super_net_flow"), None)
    ddx_5 = _numeric_value(mf.get("ddx_5"), None)
    ddy_10 = _numeric_value(mf.get("ddy_10"), None)
    flow_5 = _numeric_value(mf.get("main_net_flow_5d"), None)
    flow_10 = _numeric_value(mf.get("main_net_flow_10d"), None)

    if main is None:
        score -= 8
    else:
        detail["main_net_flow"] = round(main, 4)
        score += max(-24.0, min(24.0, main * 8.0))

    if super_flow is not None:
        detail["super_net_flow"] = round(super_flow, 4)
        score += max(-10.0, min(10.0, super_flow * 8.0))
    if ddx_5 is not None:
        detail["ddx_5"] = round(ddx_5, 4)
        score += max(-8.0, min(8.0, ddx_5 * 2.5))
    if ddy_10 is not None:
        detail["ddy_10"] = round(ddy_10, 4)
        score += max(-8.0, min(8.0, ddy_10 * 1.8))
    if ddx_5 is None and flow_5 is not None:
        detail["main_net_flow_5d"] = round(flow_5, 4)
        score += max(-5.0, min(5.0, flow_5 * 0.8))
    if ddy_10 is None and flow_10 is not None:
        detail["main_net_flow_10d"] = round(flow_10, 4)
        score += max(-4.0, min(4.0, flow_10 * 0.5))

    return round(_clamp_score(score), 2), detail


def _score_next_day_buyability(packet: Dict[str, Any]) -> tuple[float, Dict[str, Any]]:
    """Estimate whether the next trading day is still buyable, not merely historically strong."""
    ksum = packet.get("kline_summary", {}) if isinstance(packet, dict) else {}
    ind = packet.get("indicators", {}) if isinstance(packet, dict) else {}
    score = 50.0
    detail: Dict[str, Any] = {}

    ma_system = str(ksum.get("ma_system") or "")
    detail["ma_system"] = ma_system
    if "多头" in ma_system:
        score += 14
    elif "空头" in ma_system:
        score -= 14

    close_position = _numeric_value(ksum.get("close_position_20d"), None)
    if close_position is not None:
        detail["close_position_20d"] = round(close_position, 2)
        if 45 <= close_position <= 88:
            score += 12
        elif 88 < close_position <= 96:
            score += 4
        elif close_position > 96:
            score -= 10
        elif close_position < 25:
            score -= 8

    rsi = _numeric_value(ind.get("rsi_14") or ind.get("rsi"), None)
    if rsi is not None:
        detail["rsi_14"] = round(rsi, 2)
        if 42 <= rsi <= 68:
            score += 12
        elif 68 < rsi <= 75:
            score += 3
        elif rsi > 80:
            score -= 18
        elif rsi < 35:
            score -= 10

    macd_signal = str(ind.get("macd_signal") or "")
    macd_state = str(ind.get("macd_state") or "")
    macd_cross = str(ind.get("macd_cross_event") or "")
    detail["macd_signal"] = macd_signal
    detail["macd_state"] = macd_state
    detail["macd_cross_event"] = macd_cross
    if macd_cross == "金叉":
        score += 8
    elif macd_cross == "死叉":
        score -= 10
    elif macd_state == "多头" or macd_signal == "多头区":
        score += 4
    elif macd_state == "空头" or macd_signal == "空头区":
        score -= 5

    volume_momentum = str(ind.get("volume_momentum") or "")
    detail["volume_momentum"] = volume_momentum
    if any(word in volume_momentum for word in ("放大", "上升", "增强")):
        score += 6
    elif any(word in volume_momentum for word in ("缩", "下降", "走弱")):
        score -= 5

    return round(_clamp_score(score), 2), detail


def _score_backtest_proxy(candidate: Dict[str, Any]) -> tuple[float, Dict[str, Any]]:
    """Current-day Top5 cannot have a causal post-selection backtest score."""
    return 0.0, {"source": "disabled_current_day_lookahead_guard", "active": False}


def _data_quality_penalty(flags: List[str]) -> tuple[float, List[str]]:
    core = {"KLINE_MISSING", "KLINE_SHORT", "FINANCIAL_MISSING", "MODEL_FAILED"}
    aux = {
        "MONEY_FLOW_MISSING", "MONEY_FLOW_FETCH_FAILED", "MONEY_FLOW_PARTIAL",
        "MONEY_FLOW_SEMANTICS_LEGACY", "SECTOR_MISSING",
    }
    penalty = 0.0
    hits = []
    for flag in flags or []:
        if flag in core:
            penalty += 8.0
            hits.append(flag)
        elif flag in aux:
            penalty += 3.0
            hits.append(flag)
    return min(24.0, penalty), hits


def _llm_risk_adjustment(
    candidate: Dict[str, Any],
    quant_base: float,
    tech_result: Dict[str, Any],
    pool_score: float = 50.0,
    money_score: float = 50.0,
    buyability_score: float = 50.0,
) -> tuple[float, Dict[str, Any]]:
    """Use only the LLM residual relative to frozen quantitative evidence.

    Technical, pool, money-flow and buyability scores already participate in
    ``quant_base``.  Reading them again here double-counted the same evidence.
    Confidence scales reliability; it never turns a confident WATCH into a
    bullish contribution.
    """
    llm_signal = str(candidate.get("llm_signal") or candidate.get("signal") or "WATCH").upper()
    llm_buy_score = _clamp_score(candidate.get("llm_buy_score", candidate.get("buy_score")), default=quant_base)
    llm_conf = _clamp_score(candidate.get("llm_confidence", candidate.get("confidence")), default=50)
    residual = llm_buy_score - quant_base
    confidence_scale = 0.65 + min(1.0, llm_conf / 100.0) * 0.35
    adjustment = residual * 0.18 * confidence_scale
    reasons: List[str] = [f"LLM相对量化残差{residual:+.1f}"]

    if llm_signal == "BUY":
        adjustment = max(-6.0, min(4.0, adjustment))
        reasons.append("PM确认BUY")
    elif llm_signal == "WATCH":
        adjustment = max(-8.0, min(0.0, adjustment))
        reasons.append("PM为WATCH，不产生正向加分")
    elif llm_signal == "AVOID":
        adjustment = min(-10.0, adjustment)
        reasons.append("PM明确AVOID")
    elif llm_signal in {"MODEL_FAILED", "PENDING_RETRY"}:
        adjustment = -25.0
        reasons.append("模型失败或待重试")

    if tech_result.get("veto"):
        adjustment = min(adjustment, -20.0)
        reasons.append("技术硬否决")

    return round(max(-25.0, min(4.0, adjustment)), 2), {
        "llm_signal": llm_signal,
        "llm_buy_score": round(llm_buy_score, 2),
        "llm_confidence": round(llm_conf, 2),
        "quant_base_score": round(quant_base, 2),
        "residual": round(residual, 2),
        "confidence_scale": round(confidence_scale, 3),
        "mode": "independent_llm_residual",
        "duplicate_quant_inputs_ignored": ["tech", "pool", "money_flow", "next_day_buyability"],
        "reasons": reasons,
    }


def _knowledge_rule_adjustment(candidate: Dict[str, Any], packet: Dict[str, Any]) -> tuple[float, Dict[str, Any]]:
    """Keep knowledge rules explanatory; only independent positives can add score."""
    raw = _clamp_score(
        candidate.get("knowledge_rule_score_adjustment", packet.get("knowledge_rule_score_adjustment")),
        -20.0,
        20.0,
        0.0,
    )
    hits = candidate.get("knowledge_rule_hits") or packet.get("knowledge_rule_hits") or []
    duplicate_prefixes = ("kline_summary.", "indicators.", "money_flow.", "kline_raw")
    independent_positive = 0.0
    risk_total = 0.0
    ignored_positive: List[str] = []
    accepted_positive: List[str] = []
    for hit in hits if isinstance(hits, list) else []:
        effect = _numeric_value(hit.get("effect"), 0.0)
        fields = [str(x) for x in (hit.get("evidence_fields") or [])]
        if effect < 0:
            risk_total += effect
        elif effect > 0:
            if fields and all(any(field.startswith(prefix) for prefix in duplicate_prefixes) for field in fields):
                ignored_positive.append(str(hit.get("rule_id") or "unknown"))
            else:
                independent_positive += effect
                accepted_positive.append(str(hit.get("rule_id") or "unknown"))
    # Backward compatibility for packets without detailed hits: never let a
    # positive aggregate exceed the small independent-evidence allowance.
    if not hits and raw > 0:
        independent_positive = raw
    if not hits and raw < 0:
        risk_total = raw
    effective = max(-8.0, min(0.0, risk_total)) + min(2.0, max(0.0, independent_positive))
    return round(max(-8.0, min(2.0, effective)), 2), {
        "raw_adjustment": round(raw, 2),
        "risk_adjustment": round(max(-8.0, min(0.0, risk_total)), 2),
        "independent_positive_adjustment": round(min(2.0, max(0.0, independent_positive)), 2),
        "ignored_duplicate_positive_rules": ignored_positive[:8],
        "accepted_positive_rules": accepted_positive[:8],
        "mode": "explain_gate_independent_positive_cap_2",
    }


def _historical_reliability_profile(candidate: Dict[str, Any]) -> Dict[str, Any]:
    """Calibrate confidence with mature, same-bucket outcomes from selection memory."""
    memory_path = Path(__file__).resolve().parents[1] / "output" / "selection_memory.jsonl"
    if not memory_path.exists():
        return {"active": False, "reason": "selection_memory_missing", "samples": 0}
    signal = str(candidate.get("pm_signal") or candidate.get("llm_signal") or candidate.get("signal") or "WATCH").upper()
    llm_conf = _clamp_score(candidate.get("pm_confidence", candidate.get("llm_confidence", candidate.get("confidence"))), default=50)
    bucket = "high" if llm_conf >= 70 else "low"
    values: List[float] = []
    days = set()
    try:
        lines = memory_path.read_text(encoding="utf-8").splitlines()[-1200:]
    except Exception:
        lines = []
    cutoff = date.today() - timedelta(days=45)
    for line in lines:
        try:
            item = json.loads(line)
        except Exception:
            continue
        item_signal = str(item.get("pm_signal") or item.get("llm_signal") or item.get("signal") or "WATCH").upper()
        item_conf = _clamp_score(item.get("llm_confidence", item.get("confidence")), default=50)
        if item_signal != signal or ("high" if item_conf >= 70 else "low") != bucket:
            continue
        report_day = str(item.get("report_date") or "")[:8]
        if len(report_day) == 8:
            try:
                observed = date(int(report_day[:4]), int(report_day[4:6]), int(report_day[6:8]))
                if observed < cutoff:
                    continue
            except Exception:
                pass
        value = item.get("return_d5_pct")
        if item.get("return_d5_complete") is not True:
            continue
        try:
            values.append(float(value))
            if report_day:
                days.add(report_day)
        except (TypeError, ValueError):
            continue
    if len(values) < 20 or len(days) < 5:
        return {
            "active": False,
            "reason": "insufficient_mature_samples",
            "samples": len(values),
            "trade_days": len(days),
            "signal": signal,
            "bucket": bucket,
        }
    wins = sum(v > 0 for v in values)
    # Beta(2,2) prior avoids extreme confidence from short histories.
    empirical_win_rate = (wins + 2.0) / (len(values) + 4.0) * 100.0
    return {
        "active": True,
        "samples": len(values),
        "trade_days": len(days),
        "signal": signal,
        "bucket": bucket,
        "empirical_win_rate": round(empirical_win_rate, 2),
        "avg_return_pct": round(sum(values) / len(values), 2),
        "blend_weight": 0.25,
    }


def _final_reliability_confidence(
    candidate: Dict[str, Any],
    *,
    llm_confidence: float,
    final_score: float,
    quant_base: float,
    tech_score: float,
    money_score: float,
    buyability_score: float,
    data_quality_penalty: float,
    data_quality_hits: List[str],
) -> tuple[int, Dict[str, Any]]:
    """Reliability confidence: keep LLM confidence, then apply small evidence-quality tweaks."""
    conf = _clamp_score(llm_confidence, 0, 100, 50)
    adjustments = []

    signal = str(candidate.get("signal") or candidate.get("llm_signal") or "WATCH").upper()
    llm_buy_score = _clamp_score(candidate.get("llm_buy_score", candidate.get("buy_score")), default=final_score)

    if "MODEL_FAILED" in (candidate.get("data_quality_flags") or []):
        conf -= 25
        adjustments.append("模型失败-25")
    if data_quality_penalty:
        penalty = min(12.0, data_quality_penalty * 0.5)
        conf -= penalty
        adjustments.append(f"数据质量-{penalty:.1f}")
    quality_flags = set(candidate.get("data_quality_flags") or [])
    aux_money_missing = len(quality_flags.intersection({"MONEY_FLOW_DDX_MISSING", "MONEY_FLOW_DDY_MISSING"}))
    if aux_money_missing:
        penalty = float(aux_money_missing)
        conf -= penalty
        adjustments.append(f"DDX/DDY完整度-{penalty:.0f}")
    stale_contracts = 0
    unknown_dates = 0
    for contract in (candidate.get("data_contract") or {}).values():
        if not isinstance(contract, dict):
            continue
        if contract.get("is_stale") is True:
            stale_contracts += 1
        if "DATE_UNKNOWN" in (contract.get("quality_flags") or []):
            unknown_dates += 1
    if stale_contracts:
        penalty = min(6.0, stale_contracts * 2.0)
        conf -= penalty
        adjustments.append(f"数据过期-{penalty:.0f}")
    if unknown_dates:
        penalty = min(3.0, float(unknown_dates))
        conf -= penalty
        adjustments.append(f"数据日期未知-{penalty:.0f}")

    evidence_scores = [tech_score, money_score, buyability_score]
    strong_evidence = sum(1 for score in evidence_scores if score >= 65)
    weak_evidence = sum(1 for score in evidence_scores if score < 40)
    if strong_evidence >= 2:
        conf += 3
        adjustments.append("技术/资金/可买性共振+3")
    elif weak_evidence >= 2:
        conf -= 4
        adjustments.append("关键证据偏弱-4")

    if abs(final_score - quant_base) <= 6 and abs(final_score - llm_buy_score) <= 12:
        conf += 2
        adjustments.append("量化与LLM接近+2")
    elif abs(final_score - llm_buy_score) >= 22:
        conf -= 4
        adjustments.append("量化与LLM分歧-4")

    if signal == "AVOID" and final_score >= 55:
        conf -= 5
        adjustments.append("信号与机会分分歧-5")
    elif signal in {"BUY", "WATCH"} and final_score < 45:
        conf -= 5
        adjustments.append("信号与机会分分歧-5")

    validation = candidate.get("evidence_validation") or {}
    validation_status = str(validation.get("status") or "").lower()
    if validation_status == "pass":
        conf += 1
        adjustments.append("证据校验通过+1")
    elif validation_status in {"fail", "failed", "error"}:
        conf -= 8
        adjustments.append("证据校验失败-8")
    unsupported_count = len(candidate.get("unsupported_claims") or [])
    if unsupported_count:
        penalty = min(6.0, unsupported_count * 2.0)
        conf -= penalty
        adjustments.append(f"不受支持主张-{penalty:.0f}")
    missing_used_count = len(candidate.get("missing_data_used") or [])
    if missing_used_count:
        penalty = min(4.0, missing_used_count * 1.5)
        conf -= penalty
        adjustments.append(f"使用缺失数据-{penalty:.1f}")

    pm_model = str((candidate.get("decision_models") or {}).get("pm") or candidate.get("decision_source") or "")
    pm_model_lower = pm_model.lower()
    if "minimax" in pm_model_lower:
        conf -= 3
        adjustments.append("基金经理备用模型-3")
    elif any(word in pm_model_lower for word in ("textonly", "text-only", "thinkingtext")):
        conf -= 6
        adjustments.append("文本修复裁决-6")
    elif "repair" in pm_model_lower:
        conf -= 1
        adjustments.append("结构化修复-1")

    historical = _historical_reliability_profile(candidate)
    if historical.get("active"):
        empirical = float(historical.get("empirical_win_rate") or 50.0)
        weight = float(historical.get("blend_weight") or 0.25)
        before = conf
        conf = conf * (1.0 - weight) + empirical * weight
        adjustments.append(f"历史命中率校准{before:.1f}->{conf:.1f}")

    final_conf = int(round(max(0, min(100, conf))))
    return final_conf, {
        "base_llm_confidence": round(llm_confidence, 2),
        "final_confidence": final_conf,
        "adjustments": adjustments,
        "data_quality_hits": data_quality_hits,
        "evidence_validation_status": validation_status or "unknown",
        "unsupported_claim_count": unsupported_count,
        "missing_data_used_count": missing_used_count,
        "pm_model": pm_model,
        "historical_calibration": historical,
        "evidence_scores": {
            "tech": round(tech_score, 2),
            "money_flow": round(money_score, 2),
            "next_day_buyability": round(buyability_score, 2),
        },
    }


def _apply_quant_confidence_overlay(candidate: Dict[str, Any], packet: Dict[str, Any], original: Dict[str, Any]) -> Dict[str, Any]:
    """Final buy_score = quantitative opportunity score; confidence = reliability of the judgement."""
    from .tech_scoring_engine import compute_tech_score

    tech_result = compute_tech_score(packet or {})
    tech_score = _clamp_score(tech_result.get("total_score"), default=50)
    pool_score = _clamp_score(original.get("pool_score", candidate.get("pool_score")), default=50)
    money_score, money_detail = _score_money_flow(candidate.get("money_flow") or packet.get("money_flow") or {})
    buyability_score, buyability_detail = _score_next_day_buyability(packet or {})
    backtest_score, backtest_detail = _score_backtest_proxy(candidate)
    pool_dynamic_adjust, pool_dynamic_detail = _pool_dynamic_adjustment(original or candidate)
    dq_penalty, dq_hits = _data_quality_penalty(candidate.get("data_quality_flags") or [])
    data_quality_profile = _data_quality_profile(candidate, dq_penalty, dq_hits)
    data_freshness = _data_freshness_profile(candidate)

    quant_base = (
        tech_score * 0.35
        + pool_score * 0.20
        + money_score * 0.20
        + buyability_score * 0.25
        + pool_dynamic_adjust
        - dq_penalty
    )
    quant_base = round(_clamp_score(quant_base), 2)

    candidate["pm_signal"] = str(candidate.get("signal", "WATCH") or "WATCH").upper()
    candidate["pm_score"] = _clamp_score(candidate.get("buy_score", _buy_score_value(candidate)), default=50)
    candidate["pm_confidence"] = _clamp_score(candidate.get("confidence"), default=50)
    candidate["pm_reason"] = str(
        candidate.get("reason")
        or _extract_reason(candidate.get("final_decision", ""))
        or "基金经理未提供结构化理由"
    ).strip()[:800]
    # Compatibility aliases consumed by existing reports and review files.
    candidate["llm_signal"] = candidate["pm_signal"]
    candidate["llm_buy_score"] = candidate["pm_score"]
    candidate["llm_confidence"] = candidate["pm_confidence"]
    risk_adjust, risk_detail = _llm_risk_adjustment(
        candidate,
        quant_base,
        tech_result,
        pool_score=pool_score,
        money_score=money_score,
        buyability_score=buyability_score,
    )
    knowledge_adjust, knowledge_adjust_detail = _knowledge_rule_adjustment(candidate, packet)
    knowledge_hits = candidate.get("knowledge_rule_hits") or packet.get("knowledge_rule_hits") or []
    knowledge_watch_only = bool(candidate.get("knowledge_rule_watch_only", packet.get("knowledge_rule_watch_only", False)))
    knowledge_hard_blocker = bool(candidate.get("knowledge_rule_hard_blocker", packet.get("knowledge_rule_hard_blocker", False)))
    pre_edge_score = round(_clamp_score(quant_base + risk_adjust + knowledge_adjust, 0, 95, quant_base), 2)
    candidate["pre_edge_score"] = pre_edge_score
    edge_detail = _evaluate_historical_edge_overlay(candidate)
    historical_edge_score = _clamp_score(edge_detail.get("score"), 0, 8, 0)
    historical_weakness_penalty = _clamp_score(edge_detail.get("negative_score"), 0, 10, 0)
    review_advantage_score = historical_edge_score
    chase_risk_penalty = _clamp_score(edge_detail.get("chase_risk_penalty"), 0, 12, 0)
    final_score = round(
        _clamp_score(
            pre_edge_score + historical_edge_score - historical_weakness_penalty - chase_risk_penalty,
            0,
            95,
            pre_edge_score,
        ),
        2,
    )
    raw_signal_by_score = _signal_from_score(final_score)
    final_signal = raw_signal_by_score
    signal_blockers: List[str] = []
    if str(candidate.get("llm_signal", "")).upper() == "AVOID" and final_signal == "BUY":
        final_signal = "WATCH"
        signal_blockers.append("LLM原始信号AVOID，禁止直接升BUY")
    if edge_detail.get("watch_only") and final_signal == "BUY":
        final_signal = "WATCH"
        signal_blockers.append(edge_detail.get("penalty_reasons", ["结构化门控需盘中确认"])[0])
    if knowledge_hard_blocker and final_signal == "BUY":
        final_signal = "WATCH"
        signal_blockers.append(_first_knowledge_blocker(candidate))
    if knowledge_watch_only and final_signal == "BUY":
        final_signal = "WATCH"
        signal_blockers.append(_first_knowledge_blocker(candidate))
    if not data_quality_profile.get("tradable_data_ok") and final_signal == "BUY":
        final_signal = "WATCH"
        signal_blockers.append("核心数据质量不足，禁止直接BUY")
    if "MODEL_FAILED" in (candidate.get("data_quality_flags") or []):
        final_signal = "MODEL_FAILED"
        signal_blockers.append("模型失败")
    if final_signal == "BUY":
        execution_gate = "DIRECT_BUY_ALLOWED"
    elif final_signal == "WATCH":
        execution_gate = "INTRADAY_CONFIRMATION_REQUIRED"
    else:
        execution_gate = "NO_BUY"
    gate_fields = _execution_gate_fields(candidate, final_signal, signal_blockers)
    candidate.update(gate_fields)
    reliability_confidence, confidence_detail = _final_reliability_confidence(
        candidate,
        llm_confidence=_clamp_score(candidate.get("llm_confidence"), default=50),
        final_score=final_score,
        quant_base=quant_base,
        tech_score=tech_score,
        money_score=money_score,
        buyability_score=buyability_score,
        data_quality_penalty=dq_penalty,
        data_quality_hits=dq_hits,
    )

    candidate["quant_base_score"] = quant_base
    candidate["pool_dynamic_adjustment"] = pool_dynamic_adjust
    candidate["pool_dynamic_detail"] = pool_dynamic_detail
    candidate["llm_risk_adjustment"] = risk_adjust
    candidate["llm_opportunity_adjustment"] = risk_adjust
    candidate["historical_edge_score"] = historical_edge_score
    candidate["historical_edge_matches"] = edge_detail.get("matches", [])
    candidate["historical_edge_detail"] = edge_detail
    candidate["historical_weakness_penalty"] = historical_weakness_penalty
    candidate["historical_weakness_matches"] = edge_detail.get("negative_matches", [])
    candidate["review_advantage_score"] = review_advantage_score
    candidate["review_advantage_matches"] = edge_detail.get("matches", [])
    candidate["review_advantage_detail"] = edge_detail
    candidate["knowledge_rule_score_adjustment"] = round(knowledge_adjust, 2)
    candidate["knowledge_rule_adjustment_detail"] = knowledge_adjust_detail
    candidate["knowledge_rule_hits"] = knowledge_hits[:8] if isinstance(knowledge_hits, list) else []
    candidate["knowledge_rule_summary"] = candidate.get("knowledge_rule_summary") or packet.get("knowledge_rule_summary", "")
    candidate["knowledge_rule_watch_only"] = knowledge_watch_only
    candidate["knowledge_rule_hard_blocker"] = knowledge_hard_blocker
    candidate["knowledge_rule_version"] = candidate.get("knowledge_rule_version") or packet.get("knowledge_rule_version", "")
    candidate["chase_risk_penalty"] = chase_risk_penalty
    candidate["base_opportunity_score"] = pre_edge_score
    candidate["raw_signal_by_score"] = raw_signal_by_score
    candidate["final_signal"] = final_signal
    candidate["execution_gate"] = execution_gate
    candidate["signal_blockers"] = signal_blockers
    candidate["data_quality_score"] = data_quality_profile.get("data_quality_score")
    candidate["tradable_data_ok"] = data_quality_profile.get("tradable_data_ok")
    candidate["missing_core_data"] = data_quality_profile.get("missing_core_data", [])
    candidate["data_freshness"] = data_freshness
    candidate["scoring_version"] = SCORING_VERSION
    candidate["prompt_version"] = PROMPT_VERSION
    candidate["edge_rule_version"] = EDGE_RULE_VERSION
    candidate["top5_rule_version"] = TOP5_RULE_VERSION
    candidate["top5_sort_score"] = final_score
    candidate["ranking_score"] = final_score
    candidate["final_score"] = final_score
    candidate["buy_score"] = final_score
    candidate["confidence"] = reliability_confidence
    candidate["signal"] = final_signal
    candidate["action"] = final_signal
    transition_reasons: List[str] = []
    if final_signal != candidate.get("pm_signal"):
        transition_reasons.append(
            f"系统按量化机会分{final_score:.1f}将PM的{candidate.get('pm_signal')}调整为{final_signal}"
        )
    if signal_blockers:
        transition_reasons.append("门控: " + "；".join(signal_blockers[:3]))
    if historical_weakness_penalty:
        transition_reasons.append(f"历史弱组合扣{historical_weakness_penalty:.1f}分")
    final_reason = candidate.get("pm_reason") or ""
    if transition_reasons:
        final_reason = (final_reason + " | " + "；".join(transition_reasons)).strip(" |")
    candidate["final_reason"] = final_reason[:1000]
    candidate["reason"] = candidate["final_reason"]
    candidate["position_ratio"] = candidate.get("position_ratio", 0.0)
    candidate["quant_score_detail"] = {
        "tech_score": tech_score,
        "pool_score": pool_score,
        "money_flow_score": money_score,
        "next_day_buyability_score": buyability_score,
        "backtest_score": backtest_score,
        "data_quality_penalty": dq_penalty,
        "data_quality_hits": dq_hits,
        "weights": {
            "tech": 0.35,
            "pool": 0.20,
            "money_flow": 0.20,
            "next_day_buyability": 0.25,
            "backtest": 0.0,
            "pool_dynamic": "additive_small_feedback",
            "historical_edge": "additive_cap_8_distinct_evidence_families",
            "historical_weakness": "subtractive_cap_10_out_of_sample_rules",
            "review_advantage": "alias_of_historical_edge_from_weekly_monthly_review",
            "chase_risk": "subtractive_cap_12",
            "knowledge_rules": "duplicate_positive_ignored_independent_positive_cap_2_risk_cap_8",
        },
        "pre_edge_score": pre_edge_score,
        "historical_edge_score": historical_edge_score,
        "historical_edge_matches": edge_detail.get("matches", []),
        "historical_edge_detail": edge_detail,
        "historical_weakness_penalty": historical_weakness_penalty,
        "historical_weakness_matches": edge_detail.get("negative_matches", []),
        "review_advantage_score": review_advantage_score,
        "review_advantage_matches": edge_detail.get("matches", []),
        "review_advantage_detail": edge_detail,
        "knowledge_rule_score_adjustment": round(knowledge_adjust, 2),
        "knowledge_rule_adjustment_detail": knowledge_adjust_detail,
        "knowledge_rule_hits": candidate.get("knowledge_rule_hits", []),
        "knowledge_rule_summary": candidate.get("knowledge_rule_summary", ""),
        "chase_risk_penalty": chase_risk_penalty,
        "raw_signal_by_score": raw_signal_by_score,
        "final_signal": final_signal,
        "execution_gate": execution_gate,
        "signal_blockers": signal_blockers,
        "data_quality_profile": data_quality_profile,
        "data_freshness": data_freshness,
        "top5_sort_score": candidate.get("top5_sort_score"),
        "versions": {
            "scoring_version": SCORING_VERSION,
            "prompt_version": PROMPT_VERSION,
            "edge_rule_version": EDGE_RULE_VERSION,
            "top5_rule_version": TOP5_RULE_VERSION,
            "market_snapshot_version": packet.get("market_snapshot_version", ""),
            "data_router_version": packet.get("data_router_version", ""),
        },
        "pool_dynamic_adjustment": pool_dynamic_adjust,
        "pool_dynamic_detail": pool_dynamic_detail,
        "money_flow_detail": money_detail,
        "next_day_buyability_detail": buyability_detail,
        "backtest_detail": backtest_detail,
        "tech_detail": tech_result.get("breakdown", {}),
        "tech_veto": tech_result.get("veto", False),
        "tech_veto_reasons": tech_result.get("veto_reasons", []),
        "llm_risk_detail": risk_detail,
        "llm_opportunity_detail": risk_detail,
        "confidence_detail": confidence_detail,
    }
    candidate["confidence_method"] = "llm_confidence_with_evidence_quality_adjustment"
    return candidate


def _repair_candidate_consistency(candidate: Dict[str, Any]) -> tuple[int, List[str]]:
    """Repair deterministic signal/gate fields and return unrecoverable errors."""
    repaired = 0
    errors: List[str] = []
    final_signal = str(candidate.get("final_signal") or candidate.get("signal") or "WATCH").upper()
    if final_signal not in {"BUY", "WATCH", "AVOID", "MODEL_FAILED", "PENDING_RETRY"}:
        errors.append(f"invalid_signal:{final_signal}")
        return repaired, errors
    blockers = [str(x) for x in (candidate.get("signal_blockers") or []) if str(x).strip()]
    if final_signal == "BUY" and blockers:
        final_signal = "WATCH"
        candidate["final_signal"] = final_signal
        repaired += 1
    if candidate.get("signal") != final_signal:
        candidate["signal"] = final_signal
        repaired += 1
    if candidate.get("action") != final_signal:
        candidate["action"] = final_signal
        repaired += 1
    if candidate.get("final_signal") != final_signal:
        candidate["final_signal"] = final_signal
        repaired += 1

    expected_gate = _execution_gate_fields(candidate, final_signal, blockers)
    expected_execution = (
        "DIRECT_BUY_ALLOWED" if final_signal == "BUY" and not blockers
        else "INTRADAY_CONFIRMATION_REQUIRED" if final_signal == "WATCH"
        else "NO_BUY"
    )
    if candidate.get("execution_gate") != expected_execution:
        candidate["execution_gate"] = expected_execution
        repaired += 1
    for key, value in expected_gate.items():
        if candidate.get(key) != value:
            candidate[key] = value
            repaired += 1

    pm_signal = str(candidate.get("pm_signal") or candidate.get("llm_signal") or final_signal).upper()
    candidate.setdefault("pm_signal", pm_signal)
    candidate.setdefault("pm_score", candidate.get("llm_buy_score", candidate.get("buy_score")))
    candidate.setdefault("pm_confidence", candidate.get("llm_confidence", candidate.get("confidence")))
    pm_reason = str(candidate.get("pm_reason") or candidate.get("reason") or "").strip()
    candidate.setdefault("pm_reason", pm_reason)
    final_reason = str(candidate.get("final_reason") or candidate.get("reason") or pm_reason).strip()
    if pm_signal != final_signal and "调整为" not in final_reason:
        final_reason = (
            f"{pm_reason} | 系统按最终机会分将PM的{pm_signal}调整为{final_signal}"
        ).strip(" |")
        repaired += 1
    if not final_reason:
        errors.append("missing_final_reason")
    else:
        candidate["final_reason"] = final_reason[:1000]
        candidate["reason"] = candidate["final_reason"]
    return repaired, errors


def validate_and_repair_phase2_consistency(phase2: Dict[str, Any]) -> Dict[str, Any]:
    """Validate publish-facing Phase2 rows; deterministic conflicts are repaired."""
    repaired = 0
    errors: List[Dict[str, Any]] = []
    checked = 0
    for collection in ("ranked_candidates", "top_picks"):
        for candidate in phase2.get(collection) or []:
            if not isinstance(candidate, dict):
                errors.append({"collection": collection, "stock": "", "errors": ["row_not_object"]})
                continue
            checked += 1
            count, row_errors = _repair_candidate_consistency(candidate)
            repaired += count
            if row_errors:
                errors.append({
                    "collection": collection,
                    "stock": candidate.get("stock", ""),
                    "errors": row_errors,
                })
    seen = set()
    top_picks = phase2.get("top_picks") or []
    for candidate in top_picks:
        stock = str(candidate.get("stock") or "")
        if stock and stock in seen:
            errors.append({"collection": "top_picks", "stock": stock, "errors": ["duplicate_stock"]})
        seen.add(stock)
        if set(candidate.get("data_quality_flags") or []).intersection({"KLINE_MISSING", "KLINE_SHORT"}):
            errors.append({"collection": "top_picks", "stock": stock, "errors": ["non_buyable_kline"]})
    if len(top_picks) != 5:
        errors.append({
            "collection": "top_picks",
            "stock": "",
            "errors": [f"expected_5_picks_got_{len(top_picks)}"],
        })
    summary = {
        "checked_rows": checked,
        "repaired_fields": repaired,
        "error_count": len(errors),
        "errors": errors[:20],
        "publishable": not errors and bool(phase2.get("top_picks")),
        "version": "2026-07-17.signal-gate-v1",
    }
    phase2["consistency_check"] = summary
    return summary


def _summarize_data_quality(candidates: List[Dict]) -> Dict[str, Any]:
    counts: Dict[str, int] = {}
    affected_by_stock: Dict[str, Dict[str, Any]] = {}
    money_flow_source_counts: Dict[str, int] = {}
    data_contract_counts: Dict[str, Dict[str, int]] = {}

    def add_flag(candidate: Dict[str, Any], flag: str) -> None:
        if not flag:
            return
        stock = str(candidate.get("stock") or "")
        item = affected_by_stock.setdefault(stock, {
            "stock": stock,
            "name": candidate.get("name", ""),
            "flags": [],
        })
        if flag not in item["flags"]:
            item["flags"].append(flag)

    for c in candidates:
        src = (c.get("money_flow") or {}).get("source")
        if src:
            money_flow_source_counts[src] = money_flow_source_counts.get(src, 0) + 1
        for category, item in (c.get("data_contract") or {}).items():
            if not isinstance(item, dict):
                continue
            status = str(item.get("status") or "unknown")
            data_contract_counts.setdefault(category, {})
            data_contract_counts[category][status] = data_contract_counts[category].get(status, 0) + 1
            category_upper = str(category).upper()
            if item.get("is_stale") is True:
                add_flag(c, f"{category_upper}_STALE")
            for quality_flag in item.get("quality_flags") or []:
                if quality_flag == "DATE_UNKNOWN":
                    add_flag(c, f"{category_upper}_DATE_UNKNOWN")
                else:
                    add_flag(c, str(quality_flag))
            if category == "money_flow":
                field_status = item.get("field_status") or {}
                if field_status.get("ddx_5") == "missing":
                    add_flag(c, "MONEY_FLOW_DDX_MISSING")
                if field_status.get("ddy_10") == "missing":
                    add_flag(c, "MONEY_FLOW_DDY_MISSING")
                if field_status.get("main_net_flow") == "missing":
                    add_flag(c, "MONEY_FLOW_MISSING")
                elif field_status.get("super_net_flow") == "missing":
                    add_flag(c, "MONEY_FLOW_PARTIAL")
            if category == "news" and (
                item.get("content_is_old") is True
                or status in {"no_recent_items", "checked_fresh_no_recent_items"}
            ):
                add_flag(c, "NEWS_NO_RECENT_ITEMS")
        for flag in sorted(set(c.get("data_quality_flags") or [])):
            add_flag(c, flag)

    for item in affected_by_stock.values():
        for flag in item["flags"]:
            counts[flag] = counts.get(flag, 0) + 1

    affected = list(affected_by_stock.values())
    core_flags = {
        "KLINE_MISSING", "KLINE_SHORT", "MONEY_FLOW_MISSING",
        "MONEY_FLOW_FETCH_FAILED", "TECH_ANALYSIS_MISSING", "MODEL_FAILED",
    }
    aux_flags = {
        "FINANCIAL_MISSING", "FINANCIAL_DATE_UNKNOWN", "MONEY_FLOW_PARTIAL",
        "MONEY_FLOW_DDX_MISSING", "MONEY_FLOW_DDY_MISSING",
        "MONEY_FLOW_SEMANTICS_LEGACY", "SECTOR_MISSING", "NEWS_NO_RECENT_ITEMS",
    }
    freshness_flags = {flag for flag in counts if flag.endswith("_STALE")}
    core_affected = {
        item["stock"] for item in affected
        if set(item.get("flags") or []).intersection(core_flags)
    }
    aux_affected = {
        item["stock"] for item in affected
        if set(item.get("flags") or []).intersection(aux_flags)
    }
    freshness_affected = {
        item["stock"] for item in affected
        if set(item.get("flags") or []).intersection(freshness_flags)
    }
    return {
        "candidate_count": len(candidates),
        "affected_count": len(affected),
        "core_affected_count": len(core_affected),
        "aux_affected_count": len(aux_affected),
        "freshness_affected_count": len(freshness_affected),
        "core_complete_count": max(0, len(candidates) - len(core_affected)),
        "flag_counts": counts,
        "core_flag_counts": {k: v for k, v in counts.items() if k in core_flags},
        "aux_flag_counts": {k: v for k, v in counts.items() if k in aux_flags},
        "freshness_flag_counts": {k: v for k, v in counts.items() if k in freshness_flags},
        "money_flow_core_complete_count": max(
            0,
            len(candidates) - len({
                item["stock"] for item in affected
                if set(item.get("flags") or []).intersection({"MONEY_FLOW_MISSING", "MONEY_FLOW_FETCH_FAILED", "MONEY_FLOW_PARTIAL"})
            }),
        ),
        "money_flow_aux_complete_count": max(
            0,
            len(candidates) - len({
                item["stock"] for item in affected
                if set(item.get("flags") or []).intersection({"MONEY_FLOW_DDX_MISSING", "MONEY_FLOW_DDY_MISSING"})
            }),
        ),
        "money_flow_source_counts": money_flow_source_counts,
        "data_contract_counts": data_contract_counts,
        "affected": affected,
    }


def _to_yi_from_pool_flow(value: Any) -> float:
    """将池内 main_flow_value 统一转换为亿元。"""
    try:
        v = float(value)
    except (TypeError, ValueError):
        return 0.0
    if abs(v) > 10000:
        return round(v / 100000000, 4)
    return round(v, 4)


def _apply_pool_money_flow_seed(packet: Dict[str, Any], candidate: Dict[str, Any]) -> None:
    """
    当实时资金流接口不稳时，用候选池已有 main_flow_value 兜底 main_net_flow。
    仅在 main_net_flow 缺失时启用，避免覆盖更高优先级实时源。
    """
    if not isinstance(packet, dict) or not isinstance(candidate, dict):
        return
    money_flow = packet.get("money_flow", {})
    if not isinstance(money_flow, dict):
        return
    if money_flow.get("main_net_flow") is not None:
        return
    detail = candidate.get("pool_score_detail", {}) or {}
    seed_val = detail.get("main_flow_value")
    seed_yi = _to_yi_from_pool_flow(seed_val)
    if seed_yi == 0.0:
        return
    money_flow["main_net_flow"] = seed_yi
    src = str(money_flow.get("source", "") or "")
    money_flow["source"] = (src + "+pool_seed").strip("+") if src else "pool_seed"
    money_flow.setdefault("field_sources", {})["main_net_flow"] = "pool_seed"
    money_flow.setdefault("field_as_of", {})["main_net_flow"] = ""
    money_flow.setdefault("units", {})["main_net_flow"] = "CNY_100M"
    packet["money_flow"] = money_flow

    money_contract = packet.setdefault("data_contract", {}).setdefault("money_flow", {})
    money_contract["source"] = money_flow["source"]
    money_contract["status"] = "partial" if money_flow.get("super_net_flow") is None else "ok"
    money_contract.setdefault("field_status", {})["main_net_flow"] = "ok"
    money_contract.setdefault("field_sources", {})["main_net_flow"] = "pool_seed"
    money_contract.setdefault("field_as_of", {})["main_net_flow"] = ""
    money_contract.setdefault("units", {})["main_net_flow"] = "CNY_100M"

    flags = list(packet.get("data_quality_flags", []) or [])
    flags = [f for f in flags if f != "MONEY_FLOW_FETCH_FAILED"]
    if "MONEY_FLOW_MISSING" in flags:
        flags = [f for f in flags if f != "MONEY_FLOW_MISSING"]
        if money_flow.get("super_net_flow") is None:
            if "MONEY_FLOW_PARTIAL" not in flags:
                flags.append("MONEY_FLOW_PARTIAL")
    packet["data_quality_flags"] = list(dict.fromkeys(flags))

# ── 依赖检查 ───────────────────────────────────────────

def check_deps():
    """检查依赖"""
    errors = []
    # xqshare
    try:
        kb_dir = Path(__file__).parent.parent.parent / "knowledge-base"
        sys.path.insert(0, str(kb_dir / "xqshare"))
        import client
    except Exception as e:
        errors.append(f"xqshare: {e}")

    # mx-data
    try:
        skills_dir = Path(__file__).parent.parent.parent / "skills"
        import mx_data  # noqa
    except Exception as e:
        errors.append(f"mx-data: {e}")

    if errors:
        raise RuntimeError(f"依赖检查失败: {'; '.join(errors)}")
    return True


# ── 主函数 ────────────────────────────────────────────

def run_debate_phase(
    candidates: List[Dict],
    output_dir: Path,
    model: str = "volcengine-plan/ark-code-latest",
    resume: bool = False,
) -> Dict[str, Any]:
    """
    执行选股辩论阶段

    Args:
        candidates: 候选股列表，格式 [{stock: "002463", name: "沪电股份", reason: "..."}, ...]
        output_dir: workflow output 目录（用于读写缓存）
        model: LLM 模型

    Returns:
        {
            "method": "stock_selection_debate",
            "ranked_candidates": [...],
            "buy_list": [...],
            "watch_list": [...],
            "avoid_list": [...],
            "debate_record": {...},
            "portfolio_suggestion": "...",
        }
    """
    from .debate_engine import StockDebateEngine, run_debate
    from .data_fetcher import (
        build_debate_packet,
        load_phase1_cache,
        _prefetch_debate_data,
        get_cached_debate_klines,
        get_kline_via_xqshare,
        get_kline_via_akshare,
        _fetch_kline_via_http,
        get_kline_via_mx_data,
        get_kline_via_tencent,
    )

    def get_kline_with_fallback(stock: str, days: int = 120) -> list:
        """
        兜底获取K线，按优先级尝试各数据源：
        1. XQShare 本地完整日K
        2. QMT HTTP API
        3. mx-data
        4. akshare
        5. 腾讯行情API
        """
        import logging
        logger = logging.getLogger("daily_stock_workflow.debate.run")

        def retry_source(name: str, fn, retries: int = 3):
            last_err = None
            for attempt in range(retries):
                try:
                    return fn()
                except Exception as e:
                    last_err = e
                    if attempt < retries - 1:
                        logger.warning(f"K线获取失败 [{stock}] via {name} 第{attempt + 1}次，重试: {e}")
                        time.sleep(1.5 * (attempt + 1))
                    else:
                        raise last_err

        sources = [
            ("XQShare", lambda: get_kline_via_xqshare(stock, days)),
            ("QMT HTTP", lambda: _fetch_kline_via_http(stock, days)),
            ("mx-data", lambda: get_kline_via_mx_data(stock, days)),
            ("akshare", lambda: get_kline_via_akshare(stock, days)),
            ("Tencent", lambda: get_kline_via_tencent(stock, days)),
        ]

        for name, fn in sources:
            try:
                result = retry_source(name, fn, retries=2 if name == "mx-data" else 3)
                if result and len(result) >= 60:
                    logger.info(f"K线获取成功 [{stock}] via {name}: {len(result)}条")
                    return result
                else:
                    logger.warning(f"K线获取返回数据不足 [{stock}] via {name}: {len(result) if result else 0}条")
            except Exception as e:
                logger.warning(f"K线获取失败 [{stock}] via {name}: {e}")

        logger.error(f"K线获取全部失败 [{stock}]")
        return []

    start = time.time()
    logger.info(f"选股辩论开始，候选股 {len(candidates)} 只")
    if candidates:
        try:
            _prefetch_debate_data(candidates)
        except Exception as e:
            logger.warning(f"辩论前预获取失败（将继续逐票拉取）: {e}")

    # Step A: 加载 Phase 1 财务缓存
    phase1_cache = load_phase1_cache(output_dir)
    logger.info(f"Phase1 缓存加载完成: {len(phase1_cache)} 只股票")

    # ── Step A2: 断点续跑 checkpoint ─────────────────────
    import json
    date_str = date.today().strftime("%Y%m%d")
    cp_file = output_dir / f"debate_checkpoint_{date_str}.json"

    def load_checkpoint():
        if cp_file.exists():
            try:
                with open(cp_file) as f:
                    return json.load(f)
            except:
                pass
        return {"completed": [], "failed": [], "results": {}}

    def save_checkpoint(cp):
        tmp = cp_file.with_name(cp_file.name + ".tmp")
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(cp, f, ensure_ascii=False)
            f.flush()
        tmp.replace(cp_file)

    def checkpoint_cb(stock_code, result):
        cp = load_checkpoint()
        if result:
            cp["completed"].append(stock_code)
            cp["results"][stock_code] = result
        else:
            cp["failed"].append(stock_code)
        save_checkpoint(cp)
        logger.info(f"[Checkpoint] {stock_code} → 完成({len(cp['completed'])}) | 失败({len(cp['failed'])})")

    checkpoint = load_checkpoint()
    done_codes = set(checkpoint["completed"])
    failed_codes = set(checkpoint["failed"])
    logger.info(f"Checkpoint 加载: {len(done_codes)} 已完成, {len(failed_codes)} 失败, {len(done_codes) + len(failed_codes)} 总记录")

    # 恢复已完成的 result（从 checkpoint）
    saved_results = checkpoint.get("results", {})

    # Step B: xqshare 获取 K 线
    debate_packets = []
    for i, c in enumerate(candidates):
        stock = c.get("stock", "")
        name = c.get("name", "")

        logger.info(f"K线获取 [{i+1}/{len(candidates)}]: {stock}")
        kline = get_cached_debate_klines(stock) or get_kline_with_fallback(stock, days=120)

        if kline:
            packet = build_debate_packet(stock, name, phase1_cache, kline)
            _copy_candidate_metadata_to_packet(packet, c)
            _apply_pool_money_flow_seed(packet, c)
            _refresh_packet_contracts(packet)
            _apply_knowledge_rules_to_packet(packet, c)
            # ★ 回填到 candidates 列表，避免 data_quality_summary 误报 + gen_report 缺字段
            c["_financial"] = packet.get("financial")
            c["money_flow"] = packet.get("money_flow")
            c["_data_quality_flags"] = packet.get("data_quality_flags", [])
            debate_packets.append(packet)
        else:
            # K线获取失败时，用缓存数据+空K线
            logger.warning(f"K线获取失败，使用空数据: {stock}")
            packet = build_debate_packet(stock, name, phase1_cache, [])
            _copy_candidate_metadata_to_packet(packet, c)
            _apply_pool_money_flow_seed(packet, c)
            _refresh_packet_contracts(packet)
            _apply_knowledge_rules_to_packet(packet, c)
            # ★ 同上：失败路径也回填，避免漏数据
            c["_financial"] = packet.get("financial")
            c["money_flow"] = packet.get("money_flow")
            c["_data_quality_flags"] = packet.get("data_quality_flags", [])
            debate_packets.append(packet)

    logger.info(f"辩论数据包准备完成: {len(debate_packets)} 只")
    candidate_map = {str(c.get("stock", "")).zfill(6): c for c in candidates if c.get("stock")}

    # Step C: 分层执行辩论
    # 过滤掉已完成和确定失败的股票，只辩论未处理的
    pending_packets = [p for p in debate_packets
                       if p.get("stock_code") not in done_codes
                       and p.get("stock_code") not in failed_codes]
    logger.info(f"待辩论: {len(pending_packets)} 只（跳过 {len(done_codes)} 已完成, {len(failed_codes)} 失败）")

    if pending_packets:
        safe_parallel = max(1, min(3, int(os.getenv("DEBATE_MAX_PARALLEL", "3"))))
        shortlist = {
            str(p.get("stock_code") or "")
            for p in sorted(debate_packets, key=lambda x: -float(x.get("pool_score") or 0))[:15]
        }
        pending_results = []
        for rounds, packets in (
            (2, [p for p in pending_packets if str(p.get("stock_code") or "") in shortlist]),
            (1, [p for p in pending_packets if str(p.get("stock_code") or "") not in shortlist]),
        ):
            if packets:
                debate = StockDebateEngine(model=model, max_debate_rounds=rounds)
                pending_results.extend(debate.run(
                    packets,
                    market_context="",
                    checkpoint_cb=checkpoint_cb,
                    max_parallel=safe_parallel,
                ))
    else:
        pending_results = []

    # 合并：已保存的结果 + 新辩论的结果
    results = []
    for code, r in saved_results.items():
        results.append(r)
    results.extend(pending_results)

    # 构建 packet 映射（用于注入 pe/rsi 到结果）
    packet_map = {p.get("stock_code"): p for p in debate_packets}

    # 转换为本函数兼容格式（兼容 debate_phase_to_phase2_format）
    ranked = []
    for r in results:
        stock = r.get("stock_code", "")
        packet = packet_map.get(stock, {})
        fin = packet.get("financial", {})
        indicators = packet.get("indicators", {})
        pe_ttm = fin.get("pe_ttm") or fin.get("pe")
        rsi_val = indicators.get("rsi")
        sector = r.get("sector") or packet.get("sector", "")
        flags = list(dict.fromkeys(r.get("data_quality_flags", packet.get("data_quality_flags", [])) or []))
        if sector and "SECTOR_MISSING" in flags:
            flags = [f for f in flags if f != "SECTOR_MISSING"]

        c = {
            "stock": stock,
            "name": r.get("stock_name", ""),
            "sector": sector,
            "pool": r.get("pool", "") or candidate_map.get(str(stock).zfill(6), {}).get("pool", ""),
            "source": candidate_map.get(str(stock).zfill(6), {}).get("source", ""),
            "source_pools": r.get("source_pools", []) or candidate_map.get(str(stock).zfill(6), {}).get("source_pools", []),
            "source_queries": r.get("source_queries", []) or candidate_map.get(str(stock).zfill(6), {}).get("source_queries", []),
            "source_reasons": r.get("source_reasons", []) or candidate_map.get(str(stock).zfill(6), {}).get("source_reasons", []),
            "screen_id": r.get("screen_id", "") or candidate_map.get(str(stock).zfill(6), {}).get("screen_id", ""),
            "screen_ids": r.get("screen_ids", []) or candidate_map.get(str(stock).zfill(6), {}).get("screen_ids", []),
            "strategy_type": r.get("strategy_type", "") or candidate_map.get(str(stock).zfill(6), {}).get("strategy_type", ""),
            "strategy_types": r.get("strategy_types", []) or candidate_map.get(str(stock).zfill(6), {}).get("strategy_types", []),
            "entry_bias": r.get("entry_bias", "") or candidate_map.get(str(stock).zfill(6), {}).get("entry_bias", ""),
            "entry_biases": r.get("entry_biases", []) or candidate_map.get(str(stock).zfill(6), {}).get("entry_biases", []),
            "screening_reason": r.get("screening_reason", "") or candidate_map.get(str(stock).zfill(6), {}).get("screening_reason", ""),
            "pool_score": r.get("pool_score", candidate_map.get(str(stock).zfill(6), {}).get("pool_score")),
            "pool_rank": r.get("pool_rank", candidate_map.get(str(stock).zfill(6), {}).get("pool_rank")),
            "pool_score_detail": r.get("pool_score_detail", {}) or candidate_map.get(str(stock).zfill(6), {}).get("pool_score_detail", {}),
            "pool_total_candidates": r.get("pool_total_candidates", candidate_map.get(str(stock).zfill(6), {}).get("pool_total_candidates")),
            "pool_scored_candidates": r.get("pool_scored_candidates", candidate_map.get(str(stock).zfill(6), {}).get("pool_scored_candidates")),
            "source_score_records": r.get("source_score_records", []) or candidate_map.get(str(stock).zfill(6), {}).get("source_score_records", []),
            "money_flow": packet.get("money_flow", {}),
            "data_quality_flags": flags,
            "verified_market_snapshot": packet.get("verified_market_snapshot", {}),
            "market_snapshot_version": packet.get("market_snapshot_version", ""),
            "data_router_version": packet.get("data_router_version", ""),
            "data_router_summary": packet.get("data_router_summary", {}),
            "data_contract": packet.get("data_contract", {}),
            "knowledge_rule_hits": packet.get("knowledge_rule_hits", []),
            "knowledge_rule_score_adjustment": packet.get("knowledge_rule_score_adjustment", 0),
            "knowledge_rule_summary": packet.get("knowledge_rule_summary", ""),
            "knowledge_rule_watch_only": packet.get("knowledge_rule_watch_only", False),
            "knowledge_rule_hard_blocker": packet.get("knowledge_rule_hard_blocker", False),
            "knowledge_rule_version": packet.get("knowledge_rule_version", ""),
            "kline_summary": packet.get("kline_summary", {}),
            "indicators": packet.get("indicators", {}),
            "kline_raw": packet.get("kline_raw", []),
            "kline_count": len([x for x in (packet.get("kline_raw") or []) if isinstance(x, dict) and x]),
            "signal": r.get("signal", "WATCH"),
            "confidence": r.get("confidence", 50),
            "buy_score": r.get("buy_score", _buy_score_value(r)),
            "final_decision": r.get("final_decision", ""),
            "position_ratio": _format_position_ratio(r.get("position_ratio"), r.get("final_decision", "")),
            "allow_direct_buy": r.get("allow_direct_buy"),
            "needs_intraday_confirmation": r.get("needs_intraday_confirmation"),
            "entry_condition": r.get("entry_condition", ""),
            "block_buy_reason": r.get("block_buy_reason", ""),
            "reason": r.get("reason", "") or _extract_reason(r.get("final_decision", "")),
            "decision_source": r.get("decision_source", ""),
            "raw_final_decision": r.get("raw_final_decision", ""),
            "evidence_refs": r.get("evidence_refs", []),
            "missing_data_used": r.get("missing_data_used", []),
            "unsupported_claims": r.get("unsupported_claims", []),
            "evidence_validation": r.get("evidence_validation", {}),
            "research_plan": r.get("research_plan", ""),
            "debate_history": r.get("debate_history", ""),
            "bull_history": r.get("bull_history", ""),
            "bear_history": r.get("bear_history", ""),
            # 注入 PE/RSI（供早报数据质量统计）
            "pe": pe_ttm,
            "rsi": rsi_val,
        }
        # 断点复用历史结果时再做一次池内主力资金兜底，避免旧缺失直接透传到输出。
        original_candidate = candidate_map.get(str(stock).zfill(6), {})
        _apply_pool_money_flow_seed(c, original_candidate)
        _apply_quant_confidence_overlay(c, packet, original_candidate)
        ranked.append(c)

    buy_list = [r for r in ranked if r.get("signal") == "BUY"]
    watch_list = [r for r in ranked if r.get("signal") == "WATCH"]
    avoid_list = [r for r in ranked if r.get("signal") == "AVOID"]

    result = {
        "ranked_candidates": ranked,
        "buy_list": buy_list,
        "watch_list": watch_list,
        "avoid_list": avoid_list,
    }

    elapsed = time.time() - start
    logger.info(f"选股辩论完成，耗时: {elapsed:.1f}s")

    # 标记方法
    result["method"] = "stock_selection_debate"
    result["phase"] = "route_b_complete"
    result["elapsed_seconds"] = round(elapsed, 1)

    return result


def debate_phase_to_phase2_format(debate_result: Dict) -> Dict:
    """
    将辩论结果转换为 workflow 的 phase2 格式
    兼容 daily_report JSON 结构
    """
    ranked = debate_result.get("ranked_candidates", [])
    for candidate in ranked:
        if isinstance(candidate, dict):
            _repair_candidate_consistency(candidate)

    # 二次排序：BUY/WATCH 合并按最终做多吸引力排序；WATCH 是等待确认，不是弱票。
    # AVOID/MODEL_FAILED 不参与 Top5，只作为风险列表保留。
    def _signal_rank(candidate: Dict) -> int:
        sig = str(candidate.get("signal", "WATCH")).upper()
        if sig in {"BUY", "WATCH"}:
            return 0
        return 1

    ranked_sorted = sorted(
        ranked,
        key=lambda c: (
            _signal_rank(c),
            -_numeric_value(c.get("top5_sort_score"), _buy_score_value(c)),
            -_buy_score_value(c),
            -_numeric_value(c.get("ranking_score"), 0.0),
            -_numeric_value(c.get("confidence"), 0.0),
            -_numeric_value(c.get("pool_score"), 0.0),
            _pool_rank_value(c),
        )
    )
    for index, candidate in enumerate(ranked_sorted, 1):
        candidate["global_rank"] = index

    def _has_buyable_kline(candidate: Dict) -> bool:
        flags = set(candidate.get("data_quality_flags") or [])
        return not flags.intersection({"KLINE_MISSING", "KLINE_SHORT"})

    buyable_source = [
        c for c in ranked_sorted
        if str(c.get("signal", "WATCH")).upper() in {"BUY", "WATCH"}
        and _has_buyable_kline(c)
    ]

    top_source = []
    seen_stocks = set()

    def _append_top_candidate(
        candidate: Dict,
        sector_penalty: float = 0.0,
        selection_reason: str = "按最终做多分与数据门控入选",
    ) -> bool:
        stock = candidate.get("stock") or f"{candidate.get('name', '')}:{id(candidate)}"
        if stock in seen_stocks:
            return False
        if sector_penalty:
            candidate["sector_crowding_penalty"] = sector_penalty
            candidate["top5_effective_score"] = round(
                _numeric_value(candidate.get("top5_sort_score"), _buy_score_value(candidate)) - sector_penalty,
                2,
            )
        candidate["top5_selection_reason"] = selection_reason
        seen_stocks.add(stock)
        top_source.append(candidate)
        return True

    sector_counts: Dict[str, int] = {}
    deferred_same_sector = []
    deferred_high_chase = []
    high_chase_count = 0
    try:
        from candidate_edge_rules import resolve_dynamic_chase_limit
        chase_policy = resolve_dynamic_chase_limit(
            payloads=_EDGE_RULE_PAYLOAD_CACHE,
            output_dir=Path(__file__).resolve().parents[1] / "output",
        )
    except Exception as exc:
        chase_policy = {
            "status": "default",
            "high_chase_limit": 2,
            "reason": f"动态追高策略读取失败，使用默认上限2只: {type(exc).__name__}",
        }
    high_chase_limit = max(0, min(2, int(chase_policy.get("high_chase_limit", 2))))
    def _is_high_chase(candidate: Dict) -> bool:
        text = " ".join(
            str(x or "")
            for x in (
                candidate.get("pool"),
                candidate.get("screen_id"),
                " ".join(candidate.get("source_pools") or []),
                " ".join(candidate.get("screen_ids") or []),
            )
        )
        return any(key in text for key in ("首板", "涨停", "突破新高", "limit_follow", "momentum_breakout"))

    for c in buyable_source:
        if len(top_source) >= 5:
            break
        if _is_high_chase(c) and high_chase_count >= high_chase_limit:
            deferred_high_chase.append(c)
            continue
        sector = str(c.get("sector") or "")
        if sector and sector_counts.get(sector, 0) >= 2:
            deferred_same_sector.append(c)
            continue
        if _append_top_candidate(c):
            if _is_high_chase(c):
                high_chase_count += 1
            if sector:
                sector_counts[sector] = sector_counts.get(sector, 0) + 1

    for c in deferred_same_sector:
        if len(top_source) >= 5:
            break
        if _is_high_chase(c) and high_chase_count >= high_chase_limit:
            deferred_high_chase.append(c)
            continue
        if _append_top_candidate(c, sector_penalty=3.0, selection_reason="行业分散规则后补位"):
            if _is_high_chase(c):
                high_chase_count += 1

    # 转换为 workflow 期望的 top_picks 格式
    top_picks = []
    for c in top_source:
        top_picks.append({
            "stock": c.get("stock", ""),
            "name": c.get("name", ""),
            "sector": c.get("sector", ""),
            "pool": c.get("pool", ""),
            "source": c.get("source", ""),
            "source_pools": c.get("source_pools", []),
            "source_queries": c.get("source_queries", []),
            "source_reasons": c.get("source_reasons", []),
            "screen_id": c.get("screen_id", ""),
            "screen_ids": c.get("screen_ids", []),
            "strategy_type": c.get("strategy_type", ""),
            "strategy_types": c.get("strategy_types", []),
            "entry_bias": c.get("entry_bias", ""),
            "entry_biases": c.get("entry_biases", []),
            "screening_reason": c.get("screening_reason", ""),
            "pool_score": c.get("pool_score"),
            "pool_rank": c.get("pool_rank"),
            "pool_score_detail": c.get("pool_score_detail", {}),
            "pool_total_candidates": c.get("pool_total_candidates"),
            "pool_scored_candidates": c.get("pool_scored_candidates"),
            "source_score_records": c.get("source_score_records", []),
            "total_score": c.get("final_score", c.get("confidence", 50)),
            "confidence": c.get("confidence", 50),
            "buy_score": c.get("buy_score", _buy_score_value(c)),
            "final_score": c.get("final_score", c.get("confidence", 50)),
            "ranking_score": c.get("ranking_score", c.get("final_score", c.get("confidence", 50))),
            "quant_base_score": c.get("quant_base_score"),
            "llm_risk_adjustment": c.get("llm_risk_adjustment"),
            "pool_dynamic_adjustment": c.get("pool_dynamic_adjustment"),
            "pool_dynamic_detail": c.get("pool_dynamic_detail", {}),
            "historical_edge_score": c.get("historical_edge_score", 0),
            "historical_edge_matches": c.get("historical_edge_matches", []),
            "historical_edge_detail": c.get("historical_edge_detail", {}),
            "historical_weakness_penalty": c.get("historical_weakness_penalty", 0),
            "historical_weakness_matches": c.get("historical_weakness_matches", []),
            "review_advantage_score": c.get("review_advantage_score", c.get("historical_edge_score", 0)),
            "review_advantage_matches": c.get("review_advantage_matches", c.get("historical_edge_matches", [])),
            "review_advantage_detail": c.get("review_advantage_detail", c.get("historical_edge_detail", {})),
            "verified_market_snapshot": c.get("verified_market_snapshot", {}),
            "market_snapshot_version": c.get("market_snapshot_version", ""),
            "data_router_version": c.get("data_router_version", ""),
            "data_router_summary": c.get("data_router_summary", {}),
            "knowledge_rule_score_adjustment": c.get("knowledge_rule_score_adjustment", 0),
            "knowledge_rule_hits": c.get("knowledge_rule_hits", []),
            "knowledge_rule_summary": c.get("knowledge_rule_summary", ""),
            "knowledge_rule_watch_only": c.get("knowledge_rule_watch_only", False),
            "knowledge_rule_hard_blocker": c.get("knowledge_rule_hard_blocker", False),
            "knowledge_rule_version": c.get("knowledge_rule_version", ""),
            "chase_risk_penalty": c.get("chase_risk_penalty", 0),
            "raw_signal_by_score": c.get("raw_signal_by_score"),
            "final_signal": c.get("final_signal"),
            "execution_gate": c.get("execution_gate"),
            "signal_blockers": c.get("signal_blockers", []),
            "allow_direct_buy": c.get("allow_direct_buy"),
            "needs_intraday_confirmation": c.get("needs_intraday_confirmation"),
            "entry_condition": c.get("entry_condition", ""),
            "block_buy_reason": c.get("block_buy_reason", ""),
            "top5_sort_score": c.get("top5_sort_score"),
            "top5_effective_score": c.get("top5_effective_score", c.get("top5_sort_score")),
            "global_rank": c.get("global_rank"),
            "top5_selection_reason": c.get("top5_selection_reason", ""),
            "sector_crowding_penalty": c.get("sector_crowding_penalty", 0),
            "data_quality_score": c.get("data_quality_score"),
            "tradable_data_ok": c.get("tradable_data_ok"),
            "missing_core_data": c.get("missing_core_data", []),
            "data_freshness": c.get("data_freshness", {}),
            "scoring_version": c.get("scoring_version", SCORING_VERSION),
            "prompt_version": c.get("prompt_version", PROMPT_VERSION),
            "edge_rule_version": c.get("edge_rule_version", EDGE_RULE_VERSION),
            "top5_rule_version": c.get("top5_rule_version", TOP5_RULE_VERSION),
            "base_opportunity_score": c.get("base_opportunity_score"),
            "pre_edge_score": c.get("pre_edge_score"),
            "quant_score_detail": c.get("quant_score_detail", {}),
            "confidence_method": c.get("confidence_method", ""),
            "llm_signal": c.get("llm_signal"),
            "llm_buy_score": c.get("llm_buy_score"),
            "llm_confidence": c.get("llm_confidence"),
            "pm_signal": c.get("pm_signal"),
            "pm_score": c.get("pm_score"),
            "pm_confidence": c.get("pm_confidence"),
            "pm_reason": c.get("pm_reason", ""),
            "final_reason": c.get("final_reason", c.get("reason", "")),
            "signal": c.get("signal", "WATCH"),
            "action": c.get("signal", "WATCH"),  # 兼容现有字段
            "reason": c.get("reason") or c.get("verdict", "") or _extract_reason(c.get("final_decision", "")),
            "position_ratio": _format_position_ratio(c.get("position_ratio"), c.get("final_decision", "")),
            "money_flow": c.get("money_flow", {}),
            "data_contract": c.get("data_contract", {}),
            "kline_summary": c.get("kline_summary", {}),
            "indicators": c.get("indicators", {}),
            "kline_raw_count": len(c.get("kline_raw", []) or []),
            "kline_count": c.get("kline_count"),
            "decision_source": c.get("decision_source", ""),
            "decision_models": c.get("decision_models", {}),
            "pm_model": (c.get("decision_models") or {}).get("pm") or c.get("decision_source", ""),
            "raw_final_decision": c.get("raw_final_decision", ""),
            "evidence_refs": c.get("evidence_refs", []),
            "missing_data_used": c.get("missing_data_used", []),
            "unsupported_claims": c.get("unsupported_claims", []),
            "evidence_validation": c.get("evidence_validation", {}),
            "data_quality_flags": c.get("data_quality_flags", []),
            "conviction": c.get("conviction", "中"),
            "bull_argument": c.get("bull_history", ""),
            "bear_argument": c.get("bear_history", ""),
            "route": "debate",
            "scoring_method": "stock_selection_debate",
        })

    def _compact_candidate(candidate: Dict) -> Dict:
        compact = dict(candidate)
        compact.pop("kline_raw", None)
        for key in ("bull_argument", "bear_argument", "bull_history", "bear_history", "debate_history", "research_plan", "raw_final_decision"):
            value = compact.get(key)
            if isinstance(value, str) and len(value) > 1200:
                compact[key] = value[:1200] + "..."
        return compact

    phase2 = {
        "method": "stock_selection_debate",
        "phase": debate_result.get("phase", "route_b_complete"),
        "ranked_candidates": [_compact_candidate(c) for c in ranked_sorted],
        "top_picks": top_picks,
        "watch_list": debate_result.get("watch_list", []),
        "avoid_list": debate_result.get("avoid_list", []),
        "debate_record": debate_result.get("debate_record", {}),
        "data_quality_summary": _summarize_data_quality(ranked),
        "model_score_summary": _model_score_summary(ranked),
        "top5_policy": {
            "high_chase_limit": high_chase_limit,
            "chase_policy": chase_policy,
            "selected_global_ranks": [c.get("global_rank") for c in top_source],
            "selected_high_chase_count": high_chase_count,
            "underfilled_count": max(0, 5 - len(top_source)),
            "deferred_same_sector": [c.get("stock") for c in deferred_same_sector],
            "deferred_high_chase": [c.get("stock") for c in deferred_high_chase],
        },
        "version_summary": {
            "scoring_version": SCORING_VERSION,
            "prompt_version": PROMPT_VERSION,
            "edge_rule_version": EDGE_RULE_VERSION,
            "top5_rule_version": TOP5_RULE_VERSION,
            "market_snapshot_version": next((c.get("market_snapshot_version") for c in ranked_sorted if c.get("market_snapshot_version")), ""),
            "data_router_version": next((c.get("data_router_version") for c in ranked_sorted if c.get("data_router_version")), ""),
        },
        "portfolio_suggestion": debate_result.get("portfolio_suggestion", ""),
        "elapsed_seconds": debate_result.get("elapsed_seconds", 0),
    }
    validate_and_repair_phase2_consistency(phase2)
    return phase2


# ── CLI 调试入口 ─────────────────────────────────────

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s: %(message)s")

    check_deps()

    # 模拟候选股
    candidates = [
        {"stock": "002463", "name": "沪电股份", "reason": "PCB龙头"},
        {"stock": "300857", "name": "协创数据", "reason": "数据要素"},
        {"stock": "600176", "name": "中国巨石", "reason": "玻纤龙头"},
    ]

    output_dir = Path(__file__).parent.parent / "output"
    result = run_debate_phase(candidates, output_dir)

    print(f"\n辩论完成: BUY={len(result.get('buy_list', []))} "
          f"WATCH={len(result.get('watch_list', []))} "
          f"AVOID={len(result.get('avoid_list', []))}")
    print(f"耗时: {result.get('elapsed_seconds', 0)}s")
