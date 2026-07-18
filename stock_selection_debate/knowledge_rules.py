"""Local short-term securities knowledge rules.

This module turns selected local knowledge-base notes into small, traceable
rule hits. It intentionally avoids injecting full books into prompts.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Tuple

KNOWLEDGE_RULE_VERSION = "2026-07-09.short-term-rules-v1"
KB_ROOT = Path(__file__).resolve().parents[2] / "knowledge-base"


@dataclass(frozen=True)
class RuleHit:
    rule_id: str
    category: str
    effect: float
    claim: str
    source: str
    evidence_fields: Tuple[str, ...]
    watch_only: bool = False
    hard_blocker: bool = False

    def as_dict(self) -> Dict[str, Any]:
        return {
            "rule_id": self.rule_id,
            "category": self.category,
            "effect": round(float(self.effect), 2),
            "claim": self.claim,
            "source": self.source,
            "evidence_fields": list(self.evidence_fields),
            "watch_only": bool(self.watch_only),
            "hard_blocker": bool(self.hard_blocker),
        }


def _num(value: Any, default: float = 0.0) -> float:
    try:
        if value not in (None, ""):
            return float(value)
    except (TypeError, ValueError):
        pass
    return default


def _bar_num(bar: Dict[str, Any], *keys: str) -> float | None:
    for key in keys:
        value = bar.get(key)
        if value not in (None, ""):
            try:
                return float(value)
            except (TypeError, ValueError):
                continue
    return None


def _recent_bars(packet: Dict[str, Any], count: int = 5) -> List[Dict[str, Any]]:
    bars = packet.get("kline_raw") or []
    if not isinstance(bars, list):
        return []
    return [x for x in bars[-count:] if isinstance(x, dict)]


def _last_candle_features(packet: Dict[str, Any]) -> Dict[str, float]:
    bars = _recent_bars(packet, 1)
    if not bars:
        return {}
    b = bars[-1]
    o = _bar_num(b, "open", "Open")
    h = _bar_num(b, "high", "High")
    l = _bar_num(b, "low", "Low")
    c = _bar_num(b, "close", "Close")
    if None in (o, h, l, c) or h <= l:
        return {}
    body = abs(c - o)
    rng = max(1e-9, h - l)
    upper = h - max(o, c)
    lower = min(o, c) - l
    return {
        "open": o,
        "high": h,
        "low": l,
        "close": c,
        "body_pct": body / rng,
        "upper_shadow_pct": upper / rng,
        "lower_shadow_pct": lower / rng,
        "close_to_high_pct": (h - c) / rng,
        "close_to_low_pct": (c - l) / rng,
        "is_up": 1.0 if c >= o else 0.0,
    }


def _has_source(path: str) -> bool:
    return (KB_ROOT / path).exists()


def _hit(rule_id: str, category: str, effect: float, claim: str, source: str, *fields: str,
         watch_only: bool = False, hard_blocker: bool = False) -> RuleHit:
    return RuleHit(
        rule_id=rule_id,
        category=category,
        effect=effect,
        claim=claim,
        source=source,
        evidence_fields=tuple(fields),
        watch_only=watch_only,
        hard_blocker=hard_blocker,
    )


def evaluate_knowledge_rules(packet: Dict[str, Any], candidate: Dict[str, Any] | None = None) -> Dict[str, Any]:
    """Evaluate short-term technical knowledge rules for one stock packet."""
    packet = packet or {}
    kls = packet.get("kline_summary") or {}
    ind = packet.get("indicators") or {}
    mf = packet.get("money_flow") or {}

    trend5 = _num(kls.get("trend_pct_5d"))
    trend10 = _num(kls.get("trend_pct_10d"))
    trend20 = _num(kls.get("trend_pct_20d"))
    vol_ratio = _num(kls.get("vol_5avg_vs_20avg"), 1.0)
    close_pos = _num(kls.get("close_position_20d"), 50.0)
    ma_system = str(kls.get("ma_system") or "")
    vol_trend = str(kls.get("vol_trend") or "")
    vol_signal = str(kls.get("vol_signal") or "")
    rsi = _num(ind.get("rsi_14"), 50.0)
    macd_signal = str(ind.get("macd_signal") or "")
    macd_state = str(ind.get("macd_state") or "")
    macd_cross = str(ind.get("macd_cross_event") or "")
    macd_breadth = str(ind.get("macd_breadth") or "")
    main_flow = _num(mf.get("main_net_flow"))
    super_flow = _num(mf.get("super_net_flow"))
    ddx5 = _num(mf.get("ddx_5"))
    ddy10 = _num(mf.get("ddy_10"))

    hits: List[RuleHit] = []

    # Volume-price analysis: confirmation and anomalies.
    if trend5 > 0 and vol_ratio >= 1.2 and main_flow > 0:
        hits.append(_hit(
            "VPA_VOLUME_PRICE_CONFIRM", "volume_price", 3.0,
            "价涨量增且主力净流入，量价一致支持短线延续",
            "volume-price-analysis/README.md",
            "kline_summary.trend_pct_5d", "kline_summary.vol_5avg_vs_20avg", "money_flow.main_net_flow",
        ))
    if vol_ratio >= 1.2 and trend5 >= 1 and ddx5 > 0 and ddy10 > 0:
        hits.append(_hit(
            "VPA_MONEY_FLOW_CONTINUITY", "volume_price", 2.5,
            "放量上涨叠加5日/10日资金延续，短线承接较好",
            "volume-price-analysis/chapter11-synthesis.md",
            "kline_summary.vol_5avg_vs_20avg", "money_flow.ddx_5", "money_flow.ddy_10",
        ))
    if trend5 > 3 and vol_ratio < 0.8:
        hits.append(_hit(
            "VPA_PRICE_UP_VOLUME_DOWN", "volume_price", -3.0,
            "价涨量缩，趋势延续需要等待成交量确认",
            "technical-analysis-murphy/chapter7-volume-oi.md",
            "kline_summary.trend_pct_5d", "kline_summary.vol_5avg_vs_20avg",
            watch_only=True,
        ))
    if vol_ratio >= 1.45 and abs(trend5) < 1.2 and (ddx5 < 0 or close_pos >= 80):
        hits.append(_hit(
            "VPA_HIGH_VOLUME_STALL", "volume_price", -5.0,
            "高位放量但价格停滞，存在派发或追高失败风险",
            "volume-price-analysis/chapter5-global-view.md",
            "kline_summary.vol_5avg_vs_20avg", "kline_summary.trend_pct_5d", "kline_summary.close_position_20d", "money_flow.ddx_5",
            watch_only=True,
        ))
    if main_flow > 0 and super_flow > 0 and (ddx5 < 0 or ddy10 < 0):
        hits.append(_hit(
            "VPA_SINGLE_DAY_FLOW_DIVERGENCE", "volume_price", -3.0,
            "单日资金流入但中短期资金指标为负，资金持续性不足",
            "volume-price-analysis/chapter4-principles.md",
            "money_flow.main_net_flow", "money_flow.super_net_flow", "money_flow.ddx_5", "money_flow.ddy_10",
            watch_only=True,
        ))

    # Trend and moving-average rules.
    if ma_system == "多头排列" and trend10 >= 3:
        hits.append(_hit(
            "TREND_MA_BULLISH", "trend", 2.0,
            "均线多头且10日动量为正，趋势背景偏多",
            "technical-analysis-murphy/chapter9-moving-averages.md",
            "kline_summary.ma_system", "kline_summary.trend_pct_10d",
        ))
    if ma_system == "多头排列" and -3 <= trend5 <= 2 and close_pos < 85:
        hits.append(_hit(
            "TREND_PULLBACK_IN_UPTREND", "trend", 1.5,
            "上升趋势内温和回踩，适合盘中观察承接",
            "candlestick-charting/practical-applications.md",
            "kline_summary.ma_system", "kline_summary.trend_pct_5d", "kline_summary.close_position_20d",
        ))
    if ma_system == "空头排列" and trend20 < 0:
        hits.append(_hit(
            "TREND_MA_BEARISH", "trend", -5.0,
            "均线空头且20日趋势为负，短线做多背景较弱",
            "technical-analysis-murphy/chapter9-moving-averages.md",
            "kline_summary.ma_system", "kline_summary.trend_pct_20d",
            watch_only=True,
        ))

    # Oscillator rules: secondary evidence only.
    if (macd_cross == "金叉" or macd_state == "多头" or macd_signal in {"金叉", "多头区"}) and 45 <= rsi <= 70:
        hits.append(_hit(
            "MURPHY_MACD_RSI_CONFIRM", "indicator", 2.0,
            "MACD金叉且RSI未过热，动量确认质量较好",
            "technical-analysis-murphy/chapter10-oscillators.md",
            "indicators.macd_signal", "indicators.rsi_14",
        ))
    if rsi >= 75 and close_pos >= 85:
        hits.append(_hit(
            "MURPHY_RSI_HIGH_POSITION_RISK", "indicator", -4.0,
            "RSI高位叠加20日价格高位，追涨需等待盘中确认",
            "technical-analysis-murphy/chapter10-oscillators.md",
            "indicators.rsi_14", "kline_summary.close_position_20d",
            watch_only=True,
        ))
    elif rsi >= 70 and close_pos >= 92 and trend20 >= 15:
        hits.append(_hit(
            "MURPHY_OVERHEAT_CHASE_RISK", "indicator", -3.0,
            "涨幅和位置偏热，继续买入需要量价重新确认",
            "technical-analysis-murphy/chapter10-oscillators.md",
            "indicators.rsi_14", "kline_summary.close_position_20d", "kline_summary.trend_pct_20d",
            watch_only=True,
        ))

    # Turtle / trend-following breakout rules.
    if close_pos >= 95 and trend20 >= 5 and vol_ratio >= 1.1:
        hits.append(_hit(
            "TURTLE_20D_BREAKOUT_CONFIRM", "breakout", 3.0,
            "接近20日高位突破且成交量确认，符合短线趋势跟随入场条件",
            "turtle-trading/core-rules.md",
            "kline_summary.close_position_20d", "kline_summary.trend_pct_20d", "kline_summary.vol_5avg_vs_20avg",
        ))
    if close_pos >= 95 and vol_ratio < 1.0:
        hits.append(_hit(
            "TURTLE_BREAKOUT_WITHOUT_VOLUME", "breakout", -3.0,
            "接近突破但量能不足，容易形成假突破或回落确认",
            "stock-trend-technical-analysis/part1-theory/chapter12-gaps.md",
            "kline_summary.close_position_20d", "kline_summary.vol_5avg_vs_20avg",
            watch_only=True,
        ))

    # Candlestick last-bar context.
    candle = _last_candle_features(packet)
    if candle:
        if candle["is_up"] and candle["body_pct"] >= 0.55 and candle["close_to_high_pct"] <= 0.25 and vol_ratio >= 1.1:
            hits.append(_hit(
                "CANDLE_STRONG_WHITE_CONFIRM", "candlestick", 2.0,
                "长阳接近高收且量能确认，短线供需偏多",
                "candlestick-charting/core-concepts.md",
                "kline_raw", "kline_summary.vol_5avg_vs_20avg",
            ))
        if candle["upper_shadow_pct"] >= 0.45 and close_pos >= 80:
            hits.append(_hit(
                "CANDLE_UPPER_SHADOW_HIGH_RISK", "candlestick", -3.0,
                "高位长上影显示上方抛压，直接追涨需谨慎",
                "candlestick-charting/practical-applications.md",
                "kline_raw", "kline_summary.close_position_20d",
                watch_only=True,
            ))
        if candle["lower_shadow_pct"] >= 0.45 and candle["close_to_high_pct"] <= 0.35 and ma_system != "空头排列":
            hits.append(_hit(
                "CANDLE_LOWER_SHADOW_SUPPORT", "candlestick", 1.5,
                "长下影后收回，说明盘中承接存在",
                "candlestick-charting/practical-applications.md",
                "kline_raw", "kline_summary.ma_system",
            ))

    # Keep only hits whose source exists; if a source file moved, preserve the hit
    # but mark it as missing so tests and reports expose the mapping issue.
    out_hits: List[Dict[str, Any]] = []
    for h in hits:
        item = h.as_dict()
        item["source_exists"] = _has_source(h.source)
        out_hits.append(item)

    raw_adjust = sum(float(h["effect"]) for h in out_hits)
    score_adjustment = max(-8.0, min(8.0, raw_adjust))
    watch_only = any(h.get("watch_only") and float(h.get("effect") or 0) < 0 for h in out_hits)
    hard_blocker = any(h.get("hard_blocker") for h in out_hits)

    positive = [h for h in out_hits if float(h.get("effect") or 0) > 0]
    negative = [h for h in out_hits if float(h.get("effect") or 0) < 0]
    summary_parts = []
    if positive:
        summary_parts.append("正向: " + "；".join(f"{h['claim']}({h['effect']:+.1f})" for h in positive[:3]))
    if negative:
        summary_parts.append("风险: " + "；".join(f"{h['claim']}({h['effect']:+.1f})" for h in negative[:3]))
    summary = " | ".join(summary_parts) if summary_parts else "未命中明确短线知识规则"

    return {
        "version": KNOWLEDGE_RULE_VERSION,
        "score_adjustment": round(score_adjustment, 2),
        "raw_score_adjustment": round(raw_adjust, 2),
        "watch_only": bool(watch_only),
        "hard_blocker": bool(hard_blocker),
        "hits": out_hits[:8],
        "summary": summary[:600],
        "source_root": str(KB_ROOT),
    }


def attach_knowledge_rules(packet: Dict[str, Any], candidate: Dict[str, Any] | None = None) -> Dict[str, Any]:
    result = evaluate_knowledge_rules(packet, candidate)
    packet["knowledge_rule_version"] = result["version"]
    packet["knowledge_rule_hits"] = result["hits"]
    packet["knowledge_rule_score_adjustment"] = result["score_adjustment"]
    packet["knowledge_rule_watch_only"] = result["watch_only"]
    packet["knowledge_rule_hard_blocker"] = result["hard_blocker"]
    packet["knowledge_rule_summary"] = result["summary"]
    return packet


def render_knowledge_rules_for_prompt(packet: Dict[str, Any], limit: int = 5) -> str:
    hits = packet.get("knowledge_rule_hits") or []
    if not hits:
        return "未命中明确短线知识规则。"
    lines = []
    for item in hits[:limit]:
        try:
            effect = float(item.get("effect") or 0)
            effect_text = f"{effect:+.1f}"
        except (TypeError, ValueError):
            effect_text = str(item.get("effect") or "")
        gate = "；需盘中确认" if item.get("watch_only") else ""
        lines.append(
            f"- {item.get('rule_id')}: {item.get('claim')} ({effect_text}{gate}; source={item.get('source')})"
        )
    return "\n".join(lines)
