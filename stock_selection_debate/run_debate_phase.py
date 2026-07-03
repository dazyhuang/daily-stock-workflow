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

logger = logging.getLogger("stock_selection_debate.run")


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


def _position_percent(candidate: Dict) -> float:
    text = _format_position_ratio(candidate.get("position_ratio"), candidate.get("final_decision", ""))
    return _numeric_value(str(text).rstrip("%"), 0.0)


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
            "chase_risk_penalty": 0.0,
            "penalty_reasons": [],
            "watch_only": False,
            "payload_modes": [],
            "error": str(exc)[:200],
        }


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
    for key in ("pool", "source", "strategy_type"):
        value = candidate.get(key)
        if value:
            pools.append(str(value))
    for key in ("source_pools", "strategy_types", "entry_biases"):
        value = candidate.get(key) or []
        if isinstance(value, (list, tuple, set)):
            pools.extend(str(x) for x in value if x)
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
    pool_stats: Dict[str, Dict[str, Any]] = {pool: {"samples": 0, "wins": 0, "returns": [], "bad_labels": 0, "missed": 0} for pool in pools}
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
            matched = [pool for pool in pools if any(pool in x or x in pool for x in item_pools)]
            if not matched:
                continue
            ret = item.get("return_d3_pct")
            if ret is None:
                ret = item.get("return_d5_pct")
            if ret is None:
                ret = item.get("return_d1_pct")
            try:
                ret_f = float(ret)
            except (TypeError, ValueError):
                continue
            labels = set(item.get("attribution_labels") or [])
            for pool in matched:
                stat = pool_stats[pool]
                stat["samples"] += 1
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
        if any(word in pool for word in ("突破新高", "新高突破", "突破")):
            base_adj -= 3.0
            reasons.append(f"{pool}:突破池近期降权")
        if any(word in pool for word in ("首板", "强势反包", "资金异动")):
            base_adj += 2.0
            reasons.append(f"{pool}:近期重点观察池")
        stat = pool_stats.get(pool) or {}
        samples = int(stat.get("samples") or 0)
        if samples >= 3:
            avg_ret = sum(stat["returns"]) / samples
            win_rate = stat["wins"] / samples
            bad_rate = stat["bad_labels"] / samples
            if avg_ret >= 3 and win_rate >= 0.55:
                base_adj += min(4.0, avg_ret * 0.4)
                reasons.append(f"{pool}:复盘均值{avg_ret:+.1f}%胜率{win_rate:.0%}")
            elif avg_ret <= 0 or bad_rate >= 0.45:
                base_adj -= 4.0
                reasons.append(f"{pool}:复盘偏弱/误导率高")
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
    detail["macd_signal"] = macd_signal
    if macd_signal == "金叉":
        score += 8
    elif macd_signal == "死叉":
        score -= 10

    volume_momentum = str(ind.get("volume_momentum") or "")
    detail["volume_momentum"] = volume_momentum
    if any(word in volume_momentum for word in ("放大", "上升", "增强")):
        score += 6
    elif any(word in volume_momentum for word in ("缩", "下降", "走弱")):
        score -= 5

    return round(_clamp_score(score), 2), detail


def _score_backtest_proxy(candidate: Dict[str, Any]) -> tuple[float, Dict[str, Any]]:
    """Use existing attached backtest fields when present; neutral when unavailable."""
    detail: Dict[str, Any] = {}
    ret = None
    for key in ("strategy_return_pct", "selection_return_pct", "return_pct"):
        if candidate.get(key) not in (None, ""):
            ret = _numeric_value(candidate.get(key), None)
            detail["source"] = key
            break
    for field in ("strategy_backtest", "selection_backtest"):
        bt = candidate.get(field)
        if isinstance(bt, dict) and bt.get("return_pct") not in (None, ""):
            ret = _numeric_value(bt.get("return_pct"), None)
            detail["source"] = field
            break
    if ret is None:
        detail["source"] = "neutral_missing"
        return 50.0, detail
    detail["return_pct"] = round(ret, 2)
    return round(_clamp_score(50 + ret * 2.0, 20, 85, 50), 2), detail


def _data_quality_penalty(flags: List[str]) -> tuple[float, List[str]]:
    core = {"KLINE_MISSING", "KLINE_SHORT", "FINANCIAL_MISSING", "MODEL_FAILED"}
    aux = {"MONEY_FLOW_MISSING", "MONEY_FLOW_FETCH_FAILED", "MONEY_FLOW_PARTIAL", "SECTOR_MISSING"}
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
    """Treat LLM as short-term opportunity judge with hard-risk guardrails."""
    llm_signal = str(candidate.get("llm_signal") or candidate.get("signal") or "WATCH").upper()
    llm_buy_score = _clamp_score(candidate.get("llm_buy_score", candidate.get("buy_score")), default=quant_base)
    llm_conf = _clamp_score(candidate.get("llm_confidence", candidate.get("confidence")), default=50)
    text = " ".join(str(candidate.get(k, "") or "") for k in ("reason", "final_decision", "raw_final_decision"))
    adjustment = 0.0
    reasons = []

    tech_score = _clamp_score(tech_result.get("total_score"), default=50)
    strong_opportunity = False

    if tech_result.get("veto"):
        adjustment -= 24
        reasons.append("技术否决")

    if llm_signal == "BUY":
        boost = min(6.0, max(2.0, (llm_buy_score - quant_base) * 0.12 + 2.0))
        adjustment += boost
        strong_opportunity = True
        reasons.append("LLM确认短线机会")
    elif llm_signal == "WATCH":
        if llm_buy_score >= 62:
            adjustment += 3
            strong_opportunity = True
            reasons.append("LLM观察但短线机会较强")
        elif llm_buy_score >= 55:
            adjustment += 1
            reasons.append("LLM观察且有做多线索")
        else:
            adjustment -= 1
            reasons.append("LLM弱观察")
    elif llm_signal in {"AVOID", "MODEL_FAILED"}:
        if llm_signal == "MODEL_FAILED":
            adjustment -= 18
            reasons.append("LLM模型失败")
        elif llm_buy_score >= 45 and (pool_score >= 60 or money_score >= 60 or buyability_score >= 60):
            adjustment -= 7
            reasons.append("LLM回避但仍有短线线索")
        else:
            adjustment -= 14
            reasons.append("LLM明确回避")

    if llm_conf < 45:
        adjustment -= 5
        reasons.append("LLM低置信")
    elif llm_conf >= 75 and llm_signal in {"BUY", "WATCH"} and llm_buy_score >= 60:
        adjustment += 1
        reasons.append("LLM高置信机会确认")
    elif llm_conf >= 75 and llm_signal == "AVOID":
        adjustment -= 2
        reasons.append("LLM高置信回避")

    opportunity_words = {
        "涨停": 3, "首板": 3, "突破": 3, "新高": 2, "主力净流入": 3,
        "超大单净流入": 3, "资金净流入": 2, "放量": 2, "多头": 2,
        "金叉": 2, "承接": 2, "题材": 1.5, "催化": 1.5, "低估值": 1.5,
        "回踩": 1.5, "低吸": 1.5, "试错": 1.0, "强势": 2,
    }
    opportunity_hits = []
    opportunity_bonus = 0.0
    for word, pts in opportunity_words.items():
        if word in text:
            opportunity_hits.append(word)
            opportunity_bonus += pts
    if opportunity_hits:
        bonus = min(3.0, opportunity_bonus)
        adjustment += bonus
        strong_opportunity = strong_opportunity or bonus >= 4
        reasons.append("机会词:" + ",".join(opportunity_hits[:5]))

    if tech_score >= 70:
        adjustment += 3
        strong_opportunity = True
        reasons.append("技术强势")
    elif tech_score >= 60:
        adjustment += 2
        reasons.append("技术尚可")
    elif tech_score < 45:
        adjustment -= 4
        reasons.append("技术偏弱")

    if pool_score >= 70:
        adjustment += 3
        strong_opportunity = True
        reasons.append("池内强")
    elif pool_score >= 60:
        adjustment += 2
        reasons.append("池内较强")

    if money_score >= 70:
        adjustment += 4
        strong_opportunity = True
        reasons.append("资金强")
    elif money_score >= 60:
        adjustment += 2
        reasons.append("资金较强")
    elif money_score < 35:
        adjustment -= 5
        reasons.append("资金弱")

    if buyability_score >= 70:
        adjustment += 4
        strong_opportunity = True
        reasons.append("次日可买性强")
    elif buyability_score >= 60:
        adjustment += 2
        reasons.append("次日可买性尚可")
    elif buyability_score < 35:
        adjustment -= 5
        reasons.append("次日可买性弱")

    soft_risk_words = {
        "高位": 2, "超买": 3, "长上影": 3, "死叉": 3,
        "业绩下滑": 4, "高负债": 4, "减持": 4,
    }
    hard_risk_words = {
        "出货": 7, "主力流出": 7, "资金流出": 6, "放量滞涨": 7,
        "破位": 8, "空头排列": 6, "均线空头": 6, "技术面AVOID": 8,
        "技术否决": 10, "否决项": 8, "模型失败": 12,
    }
    risk_hits = []
    risk_penalty = 0.0
    for word, pts in {**soft_risk_words, **hard_risk_words}.items():
        if word in text:
            risk_hits.append(word)
            risk_penalty += pts
    if risk_hits:
        cap = 10.0 if strong_opportunity else 16.0
        adjustment -= min(cap, risk_penalty)
        reasons.append("风险词:" + ",".join(risk_hits[:5]))

    upper_bound = 8.0
    if llm_signal == "WATCH" and llm_buy_score < 70:
        upper_bound = 6.0
    elif llm_signal == "AVOID":
        upper_bound = 2.0

    return round(max(-25.0, min(upper_bound, adjustment)), 2), {
        "llm_signal": llm_signal,
        "llm_buy_score": round(llm_buy_score, 2),
        "llm_confidence": round(llm_conf, 2),
        "mode": "short_term_opportunity",
        "tech_score": round(tech_score, 2),
        "pool_score": round(pool_score, 2),
        "money_flow_score": round(money_score, 2),
        "next_day_buyability_score": round(buyability_score, 2),
        "reasons": reasons,
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

    final_conf = int(round(max(0, min(100, conf))))
    return final_conf, {
        "base_llm_confidence": round(llm_confidence, 2),
        "final_confidence": final_conf,
        "adjustments": adjustments,
        "data_quality_hits": data_quality_hits,
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

    quant_base = (
        tech_score * 0.32
        + pool_score * 0.20
        + money_score * 0.18
        + buyability_score * 0.20
        + backtest_score * 0.10
        + pool_dynamic_adjust
        - dq_penalty
    )
    quant_base = round(_clamp_score(quant_base), 2)

    candidate["llm_signal"] = candidate.get("signal", "WATCH")
    candidate["llm_buy_score"] = candidate.get("buy_score", _buy_score_value(candidate))
    candidate["llm_confidence"] = candidate.get("confidence", 50)
    risk_adjust, risk_detail = _llm_risk_adjustment(
        candidate,
        quant_base,
        tech_result,
        pool_score=pool_score,
        money_score=money_score,
        buyability_score=buyability_score,
    )
    pre_edge_score = round(_clamp_score(quant_base + risk_adjust, 0, 95, quant_base), 2)
    candidate["pre_edge_score"] = pre_edge_score
    edge_detail = _evaluate_historical_edge_overlay(candidate)
    historical_edge_score = _clamp_score(edge_detail.get("score"), 0, 15, 0)
    chase_risk_penalty = _clamp_score(edge_detail.get("chase_risk_penalty"), 0, 12, 0)
    final_score = round(_clamp_score(pre_edge_score + historical_edge_score - chase_risk_penalty, 0, 95, pre_edge_score), 2)
    final_signal = _signal_from_score(final_score)
    if str(candidate.get("llm_signal", "")).upper() == "AVOID" and final_signal == "BUY":
        final_signal = "WATCH"
    if edge_detail.get("watch_only") and final_signal == "BUY":
        final_signal = "WATCH"
    if "MODEL_FAILED" in (candidate.get("data_quality_flags") or []):
        final_signal = "MODEL_FAILED"
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
    candidate["chase_risk_penalty"] = chase_risk_penalty
    candidate["base_opportunity_score"] = pre_edge_score
    candidate["ranking_score"] = final_score
    candidate["final_score"] = final_score
    candidate["buy_score"] = final_score
    candidate["confidence"] = reliability_confidence
    candidate["signal"] = final_signal
    candidate["action"] = final_signal
    if final_signal in {"BUY", "WATCH"} and _position_percent(candidate) <= 0:
        candidate["position_ratio"] = 0.25 if final_signal == "BUY" else 0.10
    elif final_signal == "AVOID":
        candidate["position_ratio"] = 0.0
    candidate["quant_score_detail"] = {
        "tech_score": tech_score,
        "pool_score": pool_score,
        "money_flow_score": money_score,
        "next_day_buyability_score": buyability_score,
        "backtest_score": backtest_score,
        "data_quality_penalty": dq_penalty,
        "data_quality_hits": dq_hits,
        "weights": {
            "tech": 0.32,
            "pool": 0.20,
            "money_flow": 0.18,
            "next_day_buyability": 0.20,
            "backtest": 0.10,
            "pool_dynamic": "additive_small_feedback",
            "historical_edge": "additive_cap_15",
            "chase_risk": "subtractive_cap_12",
        },
        "pre_edge_score": pre_edge_score,
        "historical_edge_score": historical_edge_score,
        "historical_edge_matches": edge_detail.get("matches", []),
        "historical_edge_detail": edge_detail,
        "chase_risk_penalty": chase_risk_penalty,
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


def _summarize_data_quality(candidates: List[Dict]) -> Dict[str, Any]:
    counts: Dict[str, int] = {}
    affected_by_stock: Dict[str, Dict[str, Any]] = {}
    money_flow_source_counts: Dict[str, int] = {}
    data_contract_counts: Dict[str, Dict[str, int]] = {}
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
        flags = c.get("data_quality_flags") or []
        if not flags:
            continue
        stock = c.get("stock", "")
        if stock not in affected_by_stock:
            affected_by_stock[stock] = {
                "stock": stock,
                "name": c.get("name", ""),
                "flags": [],
            }
        for flag in sorted(set(flags)):
            if flag not in affected_by_stock[stock]["flags"]:
                affected_by_stock[stock]["flags"].append(flag)

    for item in affected_by_stock.values():
        for flag in item["flags"]:
            counts[flag] = counts.get(flag, 0) + 1

    affected = list(affected_by_stock.values())
    core_flags = {
        "KLINE_MISSING", "KLINE_SHORT", "FINANCIAL_MISSING",
        "MONEY_FLOW_MISSING", "MONEY_FLOW_FETCH_FAILED", "TECH_ANALYSIS_MISSING", "MODEL_FAILED",
    }
    aux_flags = {"MONEY_FLOW_PARTIAL", "SECTOR_MISSING"}
    return {
        "affected_count": len(affected),
        "flag_counts": counts,
        "core_flag_counts": {k: v for k, v in counts.items() if k in core_flags},
        "aux_flag_counts": {k: v for k, v in counts.items() if k in aux_flags},
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
    packet["money_flow"] = money_flow

    flags = list(packet.get("data_quality_flags", []) or [])
    flags = [f for f in flags if f != "MONEY_FLOW_FETCH_FAILED"]
    if "MONEY_FLOW_MISSING" in flags:
        flags = [f for f in flags if f != "MONEY_FLOW_MISSING"]
        if any(money_flow.get(k) is None for k in ("super_net_flow", "ddx_5", "ddy_10")):
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
        get_kline_via_akshare,
        _fetch_kline_via_http,
        get_kline_via_mx_data,
        get_kline_via_tencent,
    )

    def get_kline_with_fallback(stock: str, days: int = 120) -> list:
        """
        兜底获取K线，按优先级尝试各数据源：
        1. QMT HTTP API（本地，速度最快）
        2. akshare（东方财富网络，最常用）
        3. mx-data（备用网络源）
        4. 腾讯行情API（最后兜底）
        """
        import logging
        logger = logging.getLogger("run_debate_phase")

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
            ("QMT HTTP", lambda: _fetch_kline_via_http(stock, days)),
            ("akshare", lambda: get_kline_via_akshare(stock, days)),
            ("mx-data", lambda: get_kline_via_mx_data(stock, days)),
            ("Tencent", lambda: get_kline_via_tencent(stock, days)),
        ]

        for name, fn in sources:
            try:
                result = retry_source(name, fn, retries=2 if name == "mx-data" else 3)
                if result and len(result) >= 5:
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
        kline = get_kline_with_fallback(stock, days=120)

        if kline:
            packet = build_debate_packet(stock, name, phase1_cache, kline)
            _copy_candidate_metadata_to_packet(packet, c)
            _apply_pool_money_flow_seed(packet, c)
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
            # ★ 同上：失败路径也回填，避免漏数据
            c["_financial"] = packet.get("financial")
            c["money_flow"] = packet.get("money_flow")
            c["_data_quality_flags"] = packet.get("data_quality_flags", [])
            debate_packets.append(packet)

    logger.info(f"辩论数据包准备完成: {len(debate_packets)} 只")
    candidate_map = {str(c.get("stock", "")).zfill(6): c for c in candidates if c.get("stock")}

    # Step C: 执行辩论
    debate = StockDebateEngine(model=model, max_debate_rounds=1)
    # 过滤掉已完成和确定失败的股票，只辩论未处理的
    pending_packets = [p for p in debate_packets
                       if p.get("stock_code") not in done_codes
                       and p.get("stock_code") not in failed_codes]
    logger.info(f"待辩论: {len(pending_packets)} 只（跳过 {len(done_codes)} 已完成, {len(failed_codes)} 失败）")

    if pending_packets:
        safe_parallel = max(1, min(3, int(os.getenv("DEBATE_MAX_PARALLEL", "3"))))
        pending_results = debate.run(pending_packets, market_context="", checkpoint_cb=checkpoint_cb, max_parallel=safe_parallel)
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
            "kline_summary": packet.get("kline_summary", {}),
            "indicators": packet.get("indicators", {}),
            "kline_raw": packet.get("kline_raw", []),
            "kline_count": len([x for x in (packet.get("kline_raw") or []) if isinstance(x, dict) and x]),
            "signal": r.get("signal", "WATCH"),
            "confidence": r.get("confidence", 50),
            "buy_score": r.get("buy_score", _buy_score_value(r)),
            "final_decision": r.get("final_decision", ""),
            "position_ratio": _format_position_ratio(r.get("position_ratio"), r.get("final_decision", "")),
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
            -_buy_score_value(c),
            -_numeric_value(c.get("ranking_score"), 0.0),
            -_numeric_value(c.get("confidence"), 0.0),
            -_numeric_value(c.get("pool_score"), 0.0),
            _pool_rank_value(c),
        )
    )

    def _has_buyable_kline(candidate: Dict) -> bool:
        flags = set(candidate.get("data_quality_flags") or [])
        return not flags.intersection({"KLINE_MISSING", "KLINE_SHORT"})

    def _position_positive(candidate: Dict) -> bool:
        pct = _format_position_ratio(candidate.get("position_ratio"), candidate.get("final_decision", ""))
        try:
            return float(pct.rstrip("%")) > 0
        except (TypeError, ValueError):
            return False

    buyable_source = [
        c for c in ranked_sorted
        if str(c.get("signal", "WATCH")).upper() in {"BUY", "WATCH"}
        and _has_buyable_kline(c)
        and (_position_positive(c) or str(c.get("signal", "WATCH")).upper() == "BUY")
    ]

    top_source = []
    seen_stocks = set()

    def _append_top_candidate(candidate: Dict) -> bool:
        stock = candidate.get("stock") or f"{candidate.get('name', '')}:{id(candidate)}"
        if stock in seen_stocks:
            return False
        seen_stocks.add(stock)
        top_source.append(candidate)
        return True

    edge_source = sorted(
        [c for c in buyable_source if _numeric_value(c.get("historical_edge_score"), 0.0) > 0],
        key=lambda c: (
            -_numeric_value(c.get("historical_edge_score"), 0.0),
            -_buy_score_value(c),
            _pool_rank_value(c),
        )
    )
    for c in edge_source:
        _append_top_candidate(c)
        if len(top_source) >= 2:
            break

    sector_counts: Dict[str, int] = {}
    deferred_same_sector = []
    for c in buyable_source:
        if len(top_source) >= 5:
            break
        sector = str(c.get("sector") or "")
        if sector and sector_counts.get(sector, 0) >= 2:
            deferred_same_sector.append(c)
            continue
        if _append_top_candidate(c):
            if sector:
                sector_counts[sector] = sector_counts.get(sector, 0) + 1

    for c in deferred_same_sector:
        if len(top_source) >= 5:
            break
        _append_top_candidate(c)

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
            "chase_risk_penalty": c.get("chase_risk_penalty", 0),
            "base_opportunity_score": c.get("base_opportunity_score"),
            "pre_edge_score": c.get("pre_edge_score"),
            "quant_score_detail": c.get("quant_score_detail", {}),
            "confidence_method": c.get("confidence_method", ""),
            "llm_signal": c.get("llm_signal"),
            "llm_buy_score": c.get("llm_buy_score"),
            "llm_confidence": c.get("llm_confidence"),
            "signal": c.get("signal", "WATCH"),
            "action": c.get("signal", "WATCH"),  # 兼容现有字段
            "reason": c.get("reason") or c.get("verdict", "") or _extract_reason(c.get("final_decision", "")),
            "position_ratio": _format_position_ratio(c.get("position_ratio"), c.get("final_decision", "")),
            "money_flow": c.get("money_flow", {}),
            "data_contract": c.get("data_contract", {}),
            "kline_summary": c.get("kline_summary", {}),
            "indicators": c.get("indicators", {}),
            "kline_raw": c.get("kline_raw", []),
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

    return {
        "method": "stock_selection_debate",
        "phase": debate_result.get("phase", "route_b_complete"),
        "ranked_candidates": ranked_sorted,
        "top_picks": top_picks,
        "watch_list": debate_result.get("watch_list", []),
        "avoid_list": debate_result.get("avoid_list", []),
        "debate_record": debate_result.get("debate_record", {}),
        "data_quality_summary": _summarize_data_quality(ranked),
        "portfolio_suggestion": debate_result.get("portfolio_suggestion", ""),
        "elapsed_seconds": debate_result.get("elapsed_seconds", 0),
    }


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
