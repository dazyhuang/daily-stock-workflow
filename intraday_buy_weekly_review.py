#!/usr/bin/env python3
"""Evidence-based weekly review for the intraday buy workflow."""

from __future__ import annotations

import gzip
import json
import re
from collections import Counter, defaultdict
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple


SCHEMA_VERSION = 1
ROUND_TRIP_COST_PCT = 0.25
MIN_RULE_SAMPLES = 20
MIN_RULE_WEEKS = 2
FULL_QUALITY = "FULL"
PARTIAL_QUALITY = "PARTIAL"
INVALID_QUALITY = "INVALID"


def _safe_float(value: Any) -> Optional[float]:
    try:
        if value in (None, "", "null", "None"):
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def _safe_int(value: Any) -> int:
    try:
        return int(float(value or 0))
    except (TypeError, ValueError):
        return 0


def _round(value: Optional[float], digits: int = 2) -> Optional[float]:
    return round(value, digits) if value is not None else None


def _avg(values: Iterable[Optional[float]]) -> Optional[float]:
    valid = [float(v) for v in values if v is not None]
    return round(sum(valid) / len(valid), 2) if valid else None


def _rate(part: int, total: int) -> Optional[float]:
    return round(part / total * 100.0, 2) if total else None


def _parse_dt(value: Any) -> Optional[datetime]:
    if isinstance(value, datetime):
        return value
    if not value:
        return None
    text = str(value).strip().replace("Z", "+00:00")
    try:
        parsed = datetime.fromisoformat(text)
        return parsed.replace(tzinfo=None)
    except ValueError:
        pass
    for fmt in ("%Y-%m-%d %H:%M:%S,%f", "%Y-%m-%d %H:%M:%S", "%Y%m%d%H%M%S"):
        try:
            return datetime.strptime(text, fmt)
        except ValueError:
            continue
    return None


def _normalize_day(value: Any) -> str:
    text = re.sub(r"\D", "", str(value or ""))
    if len(text) >= 8:
        return f"{text[:4]}-{text[4:6]}-{text[6:8]}"
    return str(value or "")[:10]


def _normalize_stock(value: Any) -> str:
    match = re.search(r"(\d{6})", str(value or ""))
    return match.group(1) if match else str(value or "").strip()


def _read_json(path: Path, default: Any) -> Any:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return default


def _read_jsonl(path: Path) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    if not path.exists():
        return rows
    try:
        for line in path.read_text(encoding="utf-8").splitlines():
            if not line.strip():
                continue
            value = json.loads(line)
            if isinstance(value, dict):
                rows.append(value)
    except Exception:
        return []
    unique: Dict[str, Dict[str, Any]] = {}
    for idx, row in enumerate(rows):
        key = str(row.get("event_id") or f"legacy:{idx}:{row.get('time')}:{row.get('stock')}:{row.get('event_type')}")
        unique[key] = row
    return sorted(unique.values(), key=lambda row: str(row.get("time") or ""))


def _read_market_package(path: Path) -> Dict[str, Any]:
    if not path.exists():
        return {}
    try:
        with gzip.open(path, "rt", encoding="utf-8") as handle:
            value = json.load(handle)
        return value if isinstance(value, dict) else {}
    except Exception:
        return {}


def _state_day(path: Path) -> str:
    match = re.search(r"(\d{8})", path.stem)
    return _normalize_day(match.group(1)) if match else ""


def _parse_legacy_log(path: Path, day: str) -> List[Dict[str, Any]]:
    """Best-effort history recovery. Legacy logs never count as FULL evidence."""
    if not path.exists():
        return []
    events: List[Dict[str, Any]] = []
    llm_re = re.compile(
        r"^(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})[^\n]*LLM分时买入.*?(\d{6}):\s*"
        r"(BUY_NOW|WAIT|SKIP_TODAY|KEEP_ORDER|CANCEL_WAIT|CANCEL_REBUY|CANCEL_SKIP_TODAY)"
    )
    buy_re = re.compile(
        r"^(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})[^\n]*\[REAL\] BUY (\d{6}) @([0-9.]+) x (\d+) \((.*?)\)"
    )
    try:
        lines = path.read_text(encoding="utf-8", errors="replace").splitlines()
    except Exception:
        return []
    for idx, line in enumerate(lines):
        match = llm_re.search(line)
        if match:
            events.append({
                "schema_version": 0,
                "event_id": f"legacy-llm:{day}:{idx}",
                "date": day,
                "time": match.group(1).replace(" ", "T"),
                "event_type": "LLM_DECISION",
                "stock": match.group(2),
                "decision": {"action": match.group(3), "llm_status": "ok"},
                "evidence_quality": PARTIAL_QUALITY,
            })
            continue
        match = buy_re.search(line)
        if match:
            events.append({
                "schema_version": 0,
                "event_id": f"legacy-order:{day}:{idx}",
                "date": day,
                "time": match.group(1).replace(" ", "T"),
                "event_type": "ORDER_SUBMIT",
                "stock": match.group(2),
                "order": {
                    "order_price": _safe_float(match.group(3)),
                    "quantity": _safe_int(match.group(4)),
                    "reason": match.group(5)[:300],
                    "status": "submitted",
                },
                "evidence_quality": PARTIAL_QUALITY,
            })
    return events


def _synthetic_state_events(state: Dict[str, Any], day: str) -> List[Dict[str, Any]]:
    events: List[Dict[str, Any]] = []
    for stock, entry in (state.get("stocks") or {}).items():
        if not isinstance(entry, dict):
            continue
        decision = entry.get("last_decision") if isinstance(entry.get("last_decision"), dict) else None
        if decision:
            events.append({
                "schema_version": 0,
                "event_id": f"state:{day}:{stock}:last-decision",
                "date": day,
                "time": decision.get("time") or entry.get("last_decision_at"),
                "event_type": "STATE_LAST_DECISION",
                "stock": _normalize_stock(stock),
                "decision": decision,
                "evidence_quality": PARTIAL_QUALITY,
            })
        if str(entry.get("status") or "").lower() == "filled":
            events.append({
                "schema_version": 0,
                "event_id": f"state:{day}:{stock}:fill",
                "date": day,
                "time": entry.get("filled_at"),
                "event_type": "ORDER_FILL",
                "stock": _normalize_stock(stock),
                "order": {
                    "filled_price": entry.get("filled_price"),
                    "filled_quantity": entry.get("filled_quantity"),
                    "order_id": entry.get("order_id"),
                },
                "evidence_quality": PARTIAL_QUALITY,
            })
    return events


def _load_day_evidence(
    output_dir: Path,
    day: str,
    state: Dict[str, Any],
    now: Optional[datetime] = None,
) -> Tuple[List[Dict[str, Any]], str, List[str]]:
    now = now or datetime.now()
    key = day.replace("-", "")
    journal = output_dir / f"intraday_buy_events_{key}.jsonl"
    market_path = output_dir / f"intraday_buy_market_{key}.json.gz"
    reasons: List[str] = []
    events = _read_jsonl(journal)
    market = _read_market_package(market_path)
    if not events:
        events = _parse_legacy_log(output_dir / f"intraday_buy_{key}.log", day)
        events.extend(_synthetic_state_events(state, day))
        reasons.append("缺少追加式盘中事件审计，使用旧日志和最终状态回建")
    if not market:
        reasons.append("缺少收盘分钟行情包，分钟级机会与回撤只能部分计算")
    if not state:
        reasons.append("盘中状态文件缺失")
    elif not state.get("finished_at") and (
        date.fromisoformat(day) < now.date()
        or (date.fromisoformat(day) == now.date() and now.time().hour >= 15)
    ):
        reasons.append("盘中任务没有完成时间")
    if not state:
        quality = INVALID_QUALITY
    elif journal.exists() and bool(market) and state.get("finished_at"):
        quality = FULL_QUALITY
    else:
        quality = PARTIAL_QUALITY
    for event in events:
        event.setdefault("evidence_quality", quality)
    return events, quality, reasons


def _stock_bars(market: Dict[str, Any], stock: str) -> List[Dict[str, Any]]:
    stock_data = (market.get("stocks") or {}).get(stock) or {}
    bars = []
    for row in stock_data.get("bars") or []:
        if not isinstance(row, dict) or _parse_dt(row.get("time")) is None:
            continue
        close = _safe_float(row.get("close"))
        if close is None or close <= 0:
            continue
        bars.append({
            "time": _parse_dt(row.get("time")),
            "open": _safe_float(row.get("open")) or close,
            "high": _safe_float(row.get("high")) or close,
            "low": _safe_float(row.get("low")) or close,
            "close": close,
            "volume": _safe_float(row.get("volume")) or 0.0,
        })
    return sorted(bars, key=lambda row: row["time"])


def _event_decision(event: Dict[str, Any]) -> Dict[str, Any]:
    return event.get("decision") if isinstance(event.get("decision"), dict) else {}


def _event_market(event: Dict[str, Any]) -> Dict[str, Any]:
    return event.get("market") if isinstance(event.get("market"), dict) else {}


def _event_price(event: Dict[str, Any]) -> Optional[float]:
    decision = _event_decision(event)
    market = _event_market(event)
    return _safe_float(
        market.get("price")
        or market.get("latest")
        or decision.get("quote_price")
        or (event.get("order") or {}).get("order_price")
    )


def _performance_from_entry(
    entry_price: Optional[float],
    entry_time: Optional[datetime],
    bars: List[Dict[str, Any]],
    fallback_close: Optional[float] = None,
) -> Dict[str, Optional[float]]:
    if entry_price is None or entry_price <= 0:
        return {"close_return_pct": None, "mfe_pct": None, "mae_pct": None}
    relevant = [bar for bar in bars if entry_time is None or bar["time"] >= entry_time]
    close = relevant[-1]["close"] if relevant else fallback_close
    high = max((bar["high"] for bar in relevant), default=None)
    low = min((bar["low"] for bar in relevant), default=None)
    return {
        "close_return_pct": _round((close / entry_price - 1) * 100) if close else None,
        "mfe_pct": _round((high / entry_price - 1) * 100) if high else None,
        "mae_pct": _round((low / entry_price - 1) * 100) if low else None,
    }


def _window_performance(event: Dict[str, Any], bars: List[Dict[str, Any]], minutes: int = 30) -> Dict[str, Optional[float]]:
    event_time = _parse_dt(event.get("time"))
    price = _event_price(event)
    if not event_time or not price:
        return {"return_pct": None, "mfe_pct": None, "mae_pct": None}
    end = event_time + timedelta(minutes=minutes)
    relevant = [bar for bar in bars if event_time <= bar["time"] <= end]
    if not relevant:
        return {"return_pct": None, "mfe_pct": None, "mae_pct": None}
    return {
        "return_pct": _round((relevant[-1]["close"] / price - 1) * 100),
        "mfe_pct": _round((max(bar["high"] for bar in relevant) / price - 1) * 100),
        "mae_pct": _round((min(bar["low"] for bar in relevant) / price - 1) * 100),
    }


def _decision_episodes(events: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    llm_events = []
    for event in events:
        decision = _event_decision(event)
        if event.get("event_type") != "LLM_DECISION" or decision.get("llm_skipped"):
            continue
        llm_events.append(event)
    episodes: List[Dict[str, Any]] = []
    for event in sorted(llm_events, key=lambda row: str(row.get("time") or "")):
        current_time = _parse_dt(event.get("time"))
        decision = _event_decision(event)
        if not episodes:
            episodes.append(event)
            continue
        previous = episodes[-1]
        previous_time = _parse_dt(previous.get("time"))
        previous_decision = _event_decision(previous)
        same_regime = (
            decision.get("action") == previous_decision.get("action")
            and decision.get("technical_trigger") == previous_decision.get("technical_trigger")
        )
        if current_time and previous_time and same_regime and (current_time - previous_time).total_seconds() < 600:
            continue
        episodes.append(event)
    return episodes


def _process_gap_count(events: List[Dict[str, Any]]) -> int:
    times = sorted(
        parsed
        for parsed in (_parse_dt(event.get("time")) for event in events if event.get("event_type") == "ROUND_HEARTBEAT")
        if parsed is not None
    )
    gaps = 0
    for previous, current in zip(times, times[1:]):
        if previous.time().hour < 12 and current.time().hour >= 13:
            continue
        if (current - previous).total_seconds() > 20 * 60:
            gaps += 1
    return gaps


def _first_event(events: List[Dict[str, Any]], predicate) -> Optional[Dict[str, Any]]:
    for event in sorted(events, key=lambda row: str(row.get("time") or "")):
        if predicate(event):
            return event
    return None


def _shadow_result(event: Optional[Dict[str, Any]], bars: List[Dict[str, Any]], fallback_close: Optional[float]) -> Dict[str, Any]:
    if not event:
        return {"eligible": False, "fill_status": "not_triggered", "gross_return_pct": None}
    market = _event_market(event)
    reference = _event_price(event)
    event_time = _parse_dt(event.get("time"))
    if not reference or not event_time:
        return {"eligible": True, "fill_status": "missing_price", "gross_return_pct": None}
    limit_up = _safe_float(market.get("limit_up"))
    limit_price = reference * 1.015
    if limit_up and limit_up > 0:
        limit_price = min(limit_price, limit_up)
    ask1 = _safe_float(market.get("ask1"))
    approximate_fill = max(reference, ask1 or reference)
    if approximate_fill > limit_price:
        return {"eligible": True, "fill_status": "not_filled", "limit_price": _round(limit_price), "gross_return_pct": None}
    same_minute = [bar for bar in bars if bar["time"].replace(second=0, microsecond=0) == event_time.replace(second=0, microsecond=0)]
    if limit_up and same_minute:
        bar = same_minute[-1]
        if bar["low"] >= limit_up and bar["high"] <= limit_up:
            return {
                "eligible": True,
                "fill_status": "uncertain_limit_up_queue",
                "limit_price": _round(limit_price),
                "gross_return_pct": None,
            }
    perf = _performance_from_entry(approximate_fill, event_time, bars, fallback_close)
    gross = perf.get("close_return_pct")
    return {
        "eligible": True,
        "fill_status": "approximate_filled",
        "event_time": event_time.isoformat(),
        "reference_price": _round(reference, 4),
        "fill_price": _round(approximate_fill, 4),
        "limit_price": _round(limit_price, 4),
        "gross_return_pct": gross,
        "net_return_pct": _round(gross - ROUND_TRIP_COST_PCT) if gross is not None else None,
        **perf,
    }


def _matching_selection_item(items_by_key: Dict[Tuple[str, str], Dict[str, Any]], day: str, stock: str, signal: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    direct = items_by_key.get((day, stock))
    if direct:
        return direct
    carryover_from = _normalize_day(signal.get("carryover_from"))
    return items_by_key.get((carryover_from, stock)) if carryover_from else None


def _future_returns(item: Optional[Dict[str, Any]], same_selection_day: bool) -> Dict[str, Any]:
    result: Dict[str, Any] = {}
    values = (item or {}).get("future_returns_pct") or {}
    complete = (item or {}).get("future_return_complete") or {}
    for key in ("d1", "d3", "d5"):
        is_complete = bool(same_selection_day and complete.get(key) is True)
        result[key] = values.get(key) if is_complete else None
        result[f"{key}_complete"] = is_complete
    return result


def _future_alpha(item: Optional[Dict[str, Any]], same_selection_day: bool) -> Dict[str, Optional[float]]:
    values = (item or {}).get("alpha_pct") or {}
    complete = (item or {}).get("future_return_complete") or {}
    return {
        key: values.get(key) if same_selection_day and complete.get(key) is True else None
        for key in ("d1", "d3", "d5")
    }


def _attribute(record: Dict[str, Any]) -> List[str]:
    labels: List[str] = []
    quality_reasons = record.get("data_quality_reasons") or []
    if record.get("evidence_quality") != FULL_QUALITY:
        labels.append("DATA_QUALITY_ISSUE")
    if (
        any("没有完成时间" in reason or "状态文件缺失" in reason for reason in quality_reasons)
        or record.get("fatal_error_count", 0) > 0
        or record.get("round_error_count", 0) >= 3
        or record.get("market_data_error_count", 0) >= 3
        or (record.get("evidence_quality") == FULL_QUALITY and record.get("process_gap_count", 0) > 0)
    ):
        labels.append("PROCESS_FAILURE")
    if record.get("order_error_count", 0) > 0 or record.get("cancel_error_count", 0) > 0:
        labels.append("QUOTE_OR_CANCEL_ERROR")
    if record.get("llm_failure_count", 0) > 0:
        labels.append("LLM_DECISION_FAILED")
    actual = record.get("actual_performance") or {}
    first = record.get("first_eligible_performance") or {}
    bought = bool(record.get("actual_bought"))
    trigger = str(record.get("actual_trigger") or record.get("first_eligible_trigger") or "")
    if not bought and record.get("submitted_order_count", 0) > 0 and (first.get("mfe_pct") or -999) >= 3:
        labels.append("ORDER_NOT_FILLED")
    if bought:
        delay = record.get("entry_delay_minutes")
        slippage = record.get("entry_slippage_pct")
        has_actual_performance = any(actual.get(key) is not None for key in ("close_return_pct", "mfe_pct", "mae_pct"))
        if delay is not None and slippage is not None and delay >= 10 and slippage >= 1.5:
            labels.append("ENTRY_TOO_LATE")
        if (actual.get("close_return_pct") or 0) <= -2 or (actual.get("mae_pct") or 0) <= -3:
            labels.append("OPENING_CHASE_BAD" if trigger == "OPENING_STRONG" else "BAD_BUY")
        elif trigger == "OPENING_STRONG" and has_actual_performance:
            labels.append("OPENING_CHASE_GOOD")
        elif has_actual_performance:
            labels.append("BUY_EXECUTION_GOOD")
    elif record.get("first_eligible_time"):
        if (first.get("mfe_pct") or -999) >= 3 and (first.get("close_return_pct") or -999) >= 1:
            labels.append("WAITED_TOO_LONG")
        elif (first.get("close_return_pct") is not None and first.get("close_return_pct") <= 0) or (first.get("mae_pct") or 0) <= -2:
            labels.append("CORRECT_WAIT")
    if not bought and record.get("missed_trigger_count", 0) > 0:
        labels.append("SIGNAL_MISSED")
    futures = record.get("future_returns") or {}
    alphas = record.get("future_alpha_pct") or {}
    mature_returns = [futures.get(key) for key in ("d3", "d5") if futures.get(f"{key}_complete") is True and futures.get(key) is not None]
    mature_alphas = [alphas.get(key) for key in ("d3", "d5") if alphas.get(key) is not None]
    if mature_returns and sum(mature_returns) / len(mature_returns) <= -2 and (not mature_alphas or sum(mature_alphas) / len(mature_alphas) < 0):
        labels.append("SELECTION_WEAK")
    if not labels:
        labels.append("NO_OBVIOUS_ISSUE")
    precedence = [
        "PROCESS_FAILURE", "QUOTE_OR_CANCEL_ERROR", "LLM_DECISION_FAILED", "DATA_QUALITY_ISSUE", "SIGNAL_MISSED",
        "ORDER_NOT_FILLED", "ENTRY_TOO_LATE", "WAITED_TOO_LONG", "OPENING_CHASE_BAD",
        "BAD_BUY", "CORRECT_WAIT", "OPENING_CHASE_GOOD", "BUY_EXECUTION_GOOD", "SELECTION_WEAK", "NO_OBVIOUS_ISSUE",
    ]
    return [label for label in precedence if label in labels]


def _build_stock_record(
    output_dir: Path,
    day: str,
    stock: str,
    signal: Dict[str, Any],
    entry: Dict[str, Any],
    day_events: List[Dict[str, Any]],
    market: Dict[str, Any],
    evidence_quality: str,
    evidence_reasons: List[str],
    selection_item: Optional[Dict[str, Any]],
    task_finished: bool,
) -> Dict[str, Any]:
    day_system_error_count = sum(
        event.get("event_type") in {"ROUND_ERROR", "TASK_ERROR"}
        for event in day_events
        if not _normalize_stock(event.get("stock"))
    )
    day_fatal_error_count = sum(
        event.get("event_type") == "TASK_ERROR"
        for event in day_events
        if not _normalize_stock(event.get("stock"))
    )
    day_process_gap_count = _process_gap_count(day_events)
    events = [event for event in day_events if _normalize_stock(event.get("stock")) == stock]
    bars = _stock_bars(market, stock)
    daily_close = _safe_float(((selection_item or {}).get("ohlc") or {}).get("close"))
    rule_or_llm = lambda event: bool(
        _event_decision(event).get("technical_trigger")
        and _event_decision(event).get("technical_trigger") != "PENDING_ORDER_REVIEW"
    )
    opening_event = _first_event(events, lambda event: _event_decision(event).get("technical_trigger") == "OPENING_STRONG")
    technical_event = _first_event(events, rule_or_llm)
    llm_buy_event = _first_event(
        events,
        lambda event: event.get("event_type") == "LLM_DECISION" and _event_decision(event).get("action") == "BUY_NOW",
    )
    first_eligible = opening_event or technical_event or llm_buy_event
    fill_event = _first_event(events, lambda event: event.get("event_type") == "ORDER_FILL")
    filled_price = _safe_float(entry.get("filled_price"))
    filled_time = _parse_dt(entry.get("filled_at"))
    if fill_event:
        order = fill_event.get("order") if isinstance(fill_event.get("order"), dict) else {}
        filled_price = _safe_float(order.get("filled_price") or order.get("trade_price")) or filled_price
        filled_time = _parse_dt(fill_event.get("time")) or filled_time
    actual_bought = str(entry.get("status") or "").lower() == "filled" or bool(filled_price)
    actual_perf = _performance_from_entry(filled_price, filled_time, bars, daily_close)
    first_price = _event_price(first_eligible) if first_eligible else None
    first_time = _parse_dt(first_eligible.get("time")) if first_eligible else None
    first_perf = _performance_from_entry(first_price, first_time, bars, daily_close)
    entry_delay = None
    entry_slippage = None
    if first_time and filled_time:
        entry_delay = round(max(0.0, (filled_time - first_time).total_seconds() / 60.0), 2)
    if first_price and filled_price:
        entry_slippage = round((filled_price / first_price - 1) * 100.0, 2)

    episodes = _decision_episodes(events)
    wait_outcomes = []
    for event in episodes:
        if _event_decision(event).get("action") == "WAIT":
            wait_outcomes.append(_window_performance(event, bars, 30))
    llm_events = [event for event in events if event.get("event_type") == "LLM_DECISION" and not _event_decision(event).get("llm_skipped")]
    llm_failures = [event for event in llm_events if _event_decision(event).get("llm_status") == "failed"]
    llm_latencies = [_safe_float(_event_decision(event).get("llm_latency_seconds")) for event in llm_events]
    technical_to_llm_seconds = None
    if technical_event:
        trigger_time = _parse_dt(technical_event.get("time"))
        matching_llm = _first_event(
            llm_events,
            lambda event: (
                _parse_dt(event.get("time")) is not None
                and trigger_time is not None
                and _parse_dt(event.get("time")) >= trigger_time
                and _event_decision(event).get("technical_trigger") == _event_decision(technical_event).get("technical_trigger")
            ),
        )
        if matching_llm and trigger_time:
            technical_to_llm_seconds = round((_parse_dt(matching_llm.get("time")) - trigger_time).total_seconds(), 3)
    order_events = [event for event in events if event.get("event_type") == "ORDER_SUBMIT"]
    cancel_events = [event for event in events if event.get("event_type") == "ORDER_CANCEL"]
    blocked_order_events = [event for event in events if event.get("event_type") == "ORDER_BLOCKED"]
    missed_trigger_count = 0
    actual_llm_times = [_parse_dt(event.get("time")) for event in llm_events]
    for event in events:
        decision = _event_decision(event)
        if not decision.get("technical_trigger") or not decision.get("llm_skipped"):
            continue
        event_time = _parse_dt(event.get("time"))
        if event_time and not any(ts and 0 <= (ts - event_time).total_seconds() <= 180 for ts in actual_llm_times):
            missed_trigger_count += 1

    shadow = {
        "opening_strong_direct": _shadow_result(opening_event, bars, daily_close),
        "first_technical_direct": _shadow_result(technical_event, bars, daily_close),
        "first_llm_buy": _shadow_result(llm_buy_event, bars, daily_close),
        "no_buy": {"eligible": True, "fill_status": "no_trade", "gross_return_pct": 0.0, "net_return_pct": 0.0},
    }
    actual_gross = actual_perf.get("close_return_pct")
    current_gross = actual_gross if actual_bought else 0.0
    current_net = (
        _round(actual_gross - ROUND_TRIP_COST_PCT)
        if actual_bought and actual_gross is not None
        else (0.0 if not actual_bought else None)
    )
    shadow["current_actual"] = {
        "eligible": True,
        "fill_status": "filled" if actual_bought else "no_trade",
        "gross_return_pct": current_gross,
        "net_return_pct": current_net,
    }

    same_selection_day = bool(selection_item and _normalize_day(selection_item.get("date")) == day)
    futures = _future_returns(selection_item, same_selection_day)
    first_decision = _event_decision(first_eligible) if first_eligible else {}
    record = {
        "date": day,
        "stock": stock,
        "name": signal.get("name") or (selection_item or {}).get("name") or stock,
        "pool_source": "carryover" if signal.get("carryover_from") else "daily_top5",
        "carryover_from": signal.get("carryover_from"),
        "selection_signal": signal.get("signal") or signal.get("action"),
        "selection_confidence": _safe_float(signal.get("confidence")),
        "selection_buy_score": _safe_float(signal.get("buy_score")),
        "evidence_quality": evidence_quality,
        "task_finished": bool(task_finished),
        "data_quality_reasons": list(evidence_reasons),
        "event_count": len(events),
        "bar_count": len(bars),
        "decision_count": _safe_int(entry.get("decision_count")),
        "decision_episode_count": len(episodes),
        "llm_call_count": len(llm_events),
        "llm_failure_count": len(llm_failures),
        "market_data_error_count": sum(event.get("event_type") == "MARKET_DATA_ERROR" for event in events),
        "avg_llm_latency_seconds": _avg(llm_latencies),
        "technical_to_llm_seconds": technical_to_llm_seconds,
        "wait_episode_count": sum(_event_decision(event).get("action") == "WAIT" for event in episodes),
        "wait_missed_rise_count": sum((outcome.get("mfe_pct") or -999) >= 2 for outcome in wait_outcomes),
        "wait_avoided_loss_count": sum((outcome.get("return_pct") or 999) <= -1.5 for outcome in wait_outcomes),
        "submitted_order_count": _safe_int(entry.get("submitted_order_count")) or len(order_events),
        "order_blocked_count": len(blocked_order_events),
        "order_error_count": (
            sum(bool((event.get("order") or {}).get("error")) for event in order_events)
            + sum(bool(event.get("error")) for event in blocked_order_events)
        ),
        "cancel_error_count": sum(bool((event.get("order") or {}).get("error")) for event in cancel_events),
        "round_error_count": day_system_error_count + sum(event.get("event_type") in {"ROUND_ERROR", "TASK_ERROR"} for event in events),
        "fatal_error_count": day_fatal_error_count + sum(event.get("event_type") == "TASK_ERROR" for event in events),
        "process_gap_count": day_process_gap_count,
        "missed_trigger_count": missed_trigger_count,
        "actual_bought": actual_bought,
        "filled_time": filled_time.isoformat() if filled_time else None,
        "filled_price": filled_price,
        "filled_quantity": _safe_int(entry.get("filled_quantity")),
        "actual_trigger": (entry.get("last_decision") or {}).get("technical_trigger"),
        "actual_performance": actual_perf,
        "first_eligible_time": first_time.isoformat() if first_time else None,
        "first_eligible_trigger": first_decision.get("technical_trigger"),
        "first_eligible_action": first_decision.get("action"),
        "first_eligible_price": first_price,
        "first_eligible_performance": first_perf,
        "entry_delay_minutes": entry_delay,
        "entry_slippage_pct": entry_slippage,
        "future_returns": futures,
        "future_alpha_pct": _future_alpha(selection_item, same_selection_day),
        "shadow_policies": shadow,
        "model_counts": dict(Counter(str(_event_decision(event).get("llm_model") or "unknown") for event in llm_events)),
    }
    record["attribution_labels"] = _attribute(record)
    record["primary_attribution"] = record["attribution_labels"][0]
    return record


def _policy_summary(records: List[Dict[str, Any]]) -> Dict[str, Any]:
    records = [record for record in records if record.get("evidence_quality") == FULL_QUALITY]
    policies = ("current_actual", "opening_strong_direct", "first_technical_direct", "first_llm_buy", "no_buy")
    result: Dict[str, Any] = {}
    for policy in policies:
        values = []
        eligible_count = 0
        uncertain_count = 0
        for record in records:
            data = (record.get("shadow_policies") or {}).get(policy) or {}
            if not data.get("eligible"):
                continue
            eligible_count += 1
            if str(data.get("fill_status") or "").startswith("uncertain"):
                uncertain_count += 1
                continue
            value = data.get("net_return_pct")
            if value is not None:
                values.append(float(value))
        result[policy] = {
            "eligible_count": eligible_count,
            "evaluated_count": len(values),
            "uncertain_count": uncertain_count,
            "avg_net_return_pct": _avg(values),
            "win_rate_pct": _rate(sum(value > 0 for value in values), len(values)),
        }
    return result


def _trigger_summary(records: List[Dict[str, Any]]) -> Dict[str, Any]:
    grouped: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    for record in records:
        trigger = str(record.get("first_eligible_trigger") or "NO_TRIGGER")
        grouped[trigger].append(record)
    result = {}
    for trigger, rows in sorted(grouped.items()):
        full_rows = [row for row in rows if row.get("evidence_quality") == FULL_QUALITY]
        values = [(row.get("first_eligible_performance") or {}).get("close_return_pct") for row in full_rows]
        valid = [float(value) for value in values if value is not None]
        result[trigger] = {
            "count": len(rows),
            "bought_count": sum(bool(row.get("actual_bought")) for row in rows),
            "full_quality_count": sum(row.get("evidence_quality") == FULL_QUALITY for row in rows),
            "avg_close_return_pct": _avg(valid),
            "win_rate_pct": _rate(sum(value > 0 for value in valid), len(valid)),
            "avg_mfe_pct": _avg((row.get("first_eligible_performance") or {}).get("mfe_pct") for row in full_rows),
            "avg_mae_pct": _avg((row.get("first_eligible_performance") or {}).get("mae_pct") for row in full_rows),
        }
    return result


def _future_performance_summary(records: List[Dict[str, Any]]) -> Dict[str, Any]:
    result: Dict[str, Any] = {}
    for key in ("d1", "d3", "d5"):
        values = [
            (record.get("future_returns") or {}).get(key)
            for record in records
            if (record.get("future_returns") or {}).get(f"{key}_complete") is True
        ]
        valid = [float(value) for value in values if value is not None]
        result[key] = {
            "mature_sample_count": len(valid),
            "avg_return_pct": _avg(valid),
            "win_rate_pct": _rate(sum(value > 0 for value in valid), len(valid)),
        }
    for label, bought in (("bought", True), ("not_bought", False)):
        rows = [record for record in records if bool(record.get("actual_bought")) is bought]
        result[label] = {
            key: _avg(
                (record.get("future_returns") or {}).get(key)
                for record in rows
                if (record.get("future_returns") or {}).get(f"{key}_complete") is True
            )
            for key in ("d1", "d3", "d5")
        }
    return result


def _rule_change_gate(records: List[Dict[str, Any]], policy_summary: Dict[str, Any]) -> Dict[str, Any]:
    full = [record for record in records if record.get("evidence_quality") == FULL_QUALITY]
    week_groups: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    for record in full:
        record_day = date.fromisoformat(record["date"])
        week_start = record_day - timedelta(days=record_day.weekday())
        week_groups[week_start.isoformat()].append(record)
    current_avg = (policy_summary.get("current_actual") or {}).get("avg_net_return_pct")
    best_name = None
    best_avg = current_avg
    for name in ("opening_strong_direct", "first_technical_direct", "first_llm_buy"):
        avg_value = (policy_summary.get(name) or {}).get("avg_net_return_pct")
        if avg_value is not None and (best_avg is None or avg_value > best_avg):
            best_name, best_avg = name, avg_value
    same_direction_weeks = 0
    if best_name:
        for rows in week_groups.values():
            weekly = _policy_summary(rows)
            candidate = (weekly.get(best_name) or {}).get("avg_net_return_pct")
            baseline = (weekly.get("current_actual") or {}).get("avg_net_return_pct")
            if candidate is not None and baseline is not None and candidate > baseline:
                same_direction_weeks += 1
    enough_samples = len(full) >= MIN_RULE_SAMPLES
    enough_weeks = same_direction_weeks >= MIN_RULE_WEEKS
    return {
        "automatic_change_allowed": False,
        "candidate_policy": best_name,
        "full_quality_sample_count": len(full),
        "supporting_week_count": same_direction_weeks,
        "required_samples": MIN_RULE_SAMPLES,
        "required_weeks": MIN_RULE_WEEKS,
        "eligible_for_manual_review": bool(best_name and enough_samples and enough_weeks),
        "reason": (
            "达到样本和连续周门槛，只生成候选调整建议，仍不自动修改实盘参数"
            if best_name and enough_samples and enough_weeks
            else "完整样本或连续周证据不足，保持现行实盘规则"
        ),
    }


def _recommendations(summary: Dict[str, Any], gate: Dict[str, Any]) -> List[Dict[str, str]]:
    labels = summary.get("attribution_counts") or {}
    recommendations: List[Dict[str, str]] = []
    if labels.get("PROCESS_FAILURE", 0) or labels.get("QUOTE_OR_CANCEL_ERROR", 0):
        recommendations.append({"priority": "P0", "action": "修复系统或订单链路", "basis": "存在任务、行情、报价、撤单或订单故障样本"})
    if labels.get("LLM_DECISION_FAILED", 0):
        recommendations.append({"priority": "P0", "action": "优先降低LLM调用和结构化失败", "basis": "模型失败直接阻断了盘中判断"})
    if labels.get("DATA_QUALITY_ISSUE", 0):
        recommendations.append({"priority": "P1", "action": "继续采集完整盘中审计证据", "basis": "PARTIAL样本只作线索，不用于调整实盘规则"})
    if labels.get("WAITED_TOO_LONG", 0) > labels.get("CORRECT_WAIT", 0):
        recommendations.append({"priority": "P1", "action": "复核WAIT口径是否过严", "basis": "错过上涨的等待样本多于成功避险样本"})
    if labels.get("OPENING_CHASE_BAD", 0) > labels.get("OPENING_CHASE_GOOD", 0):
        recommendations.append({"priority": "P1", "action": "复核09:31强势过滤条件", "basis": "开盘追入后的负面样本多于正面样本"})
    if gate.get("eligible_for_manual_review"):
        recommendations.append({"priority": "P1", "action": f"人工评估影子策略 {gate.get('candidate_policy')}", "basis": gate.get("reason", "")})
    if not recommendations:
        recommendations.append({"priority": "KEEP", "action": "保持现行规则并继续积累完整样本", "basis": "当前没有达到稳定调整门槛的问题"})
    return recommendations[:5]


def build_intraday_buy_weekly_review(
    *,
    output_dir: Path,
    selection_items: Optional[List[Dict[str, Any]]] = None,
    rolling_days: int = 20,
    now: Optional[datetime] = None,
) -> Dict[str, Any]:
    now = now or datetime.now()
    state_files = sorted(output_dir.glob("intraday_buy_timing_*.json"), key=_state_day)
    state_files = [path for path in state_files if _state_day(path) and _state_day(path) <= now.date().isoformat()]
    state_files = state_files[-max(1, rolling_days):]
    items_by_key = {
        (_normalize_day(item.get("date")), _normalize_stock(item.get("stock"))): item
        for item in (selection_items or [])
        if item.get("date") and item.get("stock")
    }
    records: List[Dict[str, Any]] = []
    for state_path in state_files:
        day = _state_day(state_path)
        state = _read_json(state_path, {}) or {}
        key = day.replace("-", "")
        market = _read_market_package(output_dir / f"intraday_buy_market_{key}.json.gz")
        day_events, quality, quality_reasons = _load_day_evidence(output_dir, day, state, now=now)
        selected = {
            _normalize_stock(signal.get("stock")): signal
            for signal in (state.get("selected_signals") or [])
            if isinstance(signal, dict) and signal.get("stock")
        }
        for stock in (state.get("selected_stocks") or []):
            selected.setdefault(_normalize_stock(stock), {"stock": _normalize_stock(stock)})
        for stock, entry in (state.get("stocks") or {}).items():
            selected.setdefault(_normalize_stock(stock), {"stock": _normalize_stock(stock)})
        for stock, signal in selected.items():
            entry = (state.get("stocks") or {}).get(stock) or {}
            selection_item = _matching_selection_item(items_by_key, day, stock, signal)
            records.append(_build_stock_record(
                output_dir,
                day,
                stock,
                signal,
                entry,
                day_events,
                market,
                quality,
                quality_reasons,
                selection_item,
                bool(state.get("finished_at")),
            ))

    existing_keys = {(record["date"], record["stock"]) for record in records}
    for (day, stock), selection_item in items_by_key.items():
        try:
            selection_day = date.fromisoformat(day)
        except ValueError:
            continue
        task_should_have_finished = selection_day < now.date() or (
            selection_day == now.date() and now.time().hour >= 15
        )
        if not task_should_have_finished or (day, stock) in existing_keys:
            continue
        signal = {
            "stock": stock,
            "name": selection_item.get("name") or stock,
            "signal": selection_item.get("signal"),
            "confidence": selection_item.get("confidence"),
            "buy_score": selection_item.get("buy_score"),
        }
        records.append(_build_stock_record(
            output_dir,
            day,
            stock,
            signal,
            {},
            [],
            {},
            INVALID_QUALITY,
            ["盘中任务状态文件缺失，无法确认任务启动或该股进入观察池"],
            selection_item,
            False,
        ))

    if records:
        latest_day = max(date.fromisoformat(record["date"]) for record in records)
        week_start = latest_day - timedelta(days=latest_day.weekday())
        current_week = [record for record in records if date.fromisoformat(record["date"]) >= week_start]
    else:
        latest_day = now.date()
        week_start = latest_day - timedelta(days=latest_day.weekday())
        current_week = []

    labels = Counter(label for record in current_week for label in record.get("attribution_labels", []))
    models = Counter()
    for record in current_week:
        models.update(record.get("model_counts") or {})
    gaps_by_day: Dict[str, int] = {}
    completion_by_day: Dict[str, bool] = {}
    for record in current_week:
        gaps_by_day[record["date"]] = max(gaps_by_day.get(record["date"], 0), int(record.get("process_gap_count", 0) or 0))
        completion_by_day[record["date"]] = completion_by_day.get(record["date"], True) and bool(record.get("task_finished"))
    opening_records = [record for record in current_week if record.get("first_eligible_trigger") == "OPENING_STRONG"]
    opening_bought = [record for record in opening_records if record.get("actual_bought")]
    opening_timely = [
        record for record in opening_bought
        if record.get("entry_delay_minutes") is not None and record.get("entry_delay_minutes") <= 2
    ]
    week_policy = _policy_summary(current_week)
    rolling_policy = _policy_summary(records)
    gate = _rule_change_gate(records, rolling_policy)
    full_week_records = [record for record in current_week if record.get("evidence_quality") == FULL_QUALITY]
    summary = {
        "week_start": week_start.isoformat(),
        "week_end": latest_day.isoformat(),
        "observed_stock_count": len(current_week),
        "trading_day_count": len(completion_by_day),
        "completed_task_day_count": sum(completion_by_day.values()),
        "task_availability_pct": _rate(sum(completion_by_day.values()), len(completion_by_day)),
        "full_quality_count": sum(record.get("evidence_quality") == FULL_QUALITY for record in current_week),
        "partial_quality_count": sum(record.get("evidence_quality") == PARTIAL_QUALITY for record in current_week),
        "invalid_quality_count": sum(record.get("evidence_quality") == INVALID_QUALITY for record in current_week),
        "bought_count": sum(bool(record.get("actual_bought")) for record in current_week),
        "fill_rate_pct": _rate(sum(bool(record.get("actual_bought")) for record in current_week), len(current_week)),
        "opening_strong_count": sum(record.get("first_eligible_trigger") == "OPENING_STRONG" for record in current_week),
        "opening_strong_bought_count": len(opening_bought),
        "opening_strong_capture_rate_pct": _rate(len(opening_bought), len(opening_records)),
        "opening_strong_two_minute_capture_rate_pct": _rate(len(opening_timely), len(opening_records)),
        "submitted_order_stock_count": sum(record.get("submitted_order_count", 0) > 0 for record in current_week),
        "unfilled_order_stock_count": sum(record.get("submitted_order_count", 0) > 0 and not record.get("actual_bought") for record in current_week),
        "llm_call_count": sum(record.get("llm_call_count", 0) for record in current_week),
        "llm_failure_count": sum(record.get("llm_failure_count", 0) for record in current_week),
        "market_data_error_count": sum(record.get("market_data_error_count", 0) for record in current_week),
        "llm_success_rate_pct": _rate(
            sum(record.get("llm_call_count", 0) - record.get("llm_failure_count", 0) for record in current_week),
            sum(record.get("llm_call_count", 0) for record in current_week),
        ),
        "process_gap_count": sum(gaps_by_day.values()),
        "process_gap_day_count": sum(value > 0 for value in gaps_by_day.values()),
        "wait_missed_rise_count": sum(record.get("wait_missed_rise_count", 0) for record in current_week),
        "wait_avoided_loss_count": sum(record.get("wait_avoided_loss_count", 0) for record in current_week),
        "avg_actual_close_return_pct": _avg(
            (record.get("actual_performance") or {}).get("close_return_pct")
            for record in full_week_records if record.get("actual_bought")
        ),
        "avg_entry_delay_minutes": _avg(record.get("entry_delay_minutes") for record in full_week_records),
        "avg_entry_slippage_pct": _avg(record.get("entry_slippage_pct") for record in full_week_records),
        "avg_llm_latency_seconds": _avg(record.get("avg_llm_latency_seconds") for record in full_week_records),
        "avg_technical_to_llm_seconds": _avg(record.get("technical_to_llm_seconds") for record in full_week_records),
        "attribution_counts": dict(labels.most_common()),
        "model_counts": dict(models.most_common()),
        "trigger_performance": _trigger_summary(current_week),
        "future_performance": _future_performance_summary(current_week),
        "policy_comparison": week_policy,
        "rolling_20d_policy_comparison": rolling_policy,
        "round_trip_cost_pct": ROUND_TRIP_COST_PCT,
        "rule_change_gate": gate,
    }
    opportunity_missed = sorted(
        [record for record in current_week if "WAITED_TOO_LONG" in record.get("attribution_labels", []) or "ORDER_NOT_FILLED" in record.get("attribution_labels", [])],
        key=lambda record: (record.get("first_eligible_performance") or {}).get("mfe_pct") or -999,
        reverse=True,
    )[:3]
    entry_late = sorted(
        [record for record in current_week if "ENTRY_TOO_LATE" in record.get("attribution_labels", [])],
        key=lambda record: record.get("entry_slippage_pct") or -999,
        reverse=True,
    )[:3]
    correct_wait = sorted(
        [record for record in current_week if "CORRECT_WAIT" in record.get("attribution_labels", [])],
        key=lambda record: (record.get("first_eligible_performance") or {}).get("close_return_pct") if (record.get("first_eligible_performance") or {}).get("close_return_pct") is not None else 999,
    )[:3]
    bad_buys = sorted(
        [record for record in current_week if any(label in record.get("attribution_labels", []) for label in ("OPENING_CHASE_BAD", "BAD_BUY"))],
        key=lambda record: (record.get("actual_performance") or {}).get("close_return_pct") if (record.get("actual_performance") or {}).get("close_return_pct") is not None else 999,
    )[:3]
    failures = sorted(
        [record for record in current_week if any(label in record.get("attribution_labels", []) for label in ("PROCESS_FAILURE", "QUOTE_OR_CANCEL_ERROR", "LLM_DECISION_FAILED"))],
        key=lambda record: len(record.get("attribution_labels", [])),
        reverse=True,
    )[:3]
    return {
        "schema_version": SCHEMA_VERSION,
        "generated_at": now.isoformat(),
        "status": "ok" if records else "no_data",
        "summary": summary,
        "opportunity_missed_top3": opportunity_missed,
        "correct_wait_top3": correct_wait,
        "bad_buy_top3": bad_buys,
        "entry_too_late_top3": entry_late,
        "system_or_model_failures_top3": failures,
        "recommendations": _recommendations(summary, gate),
        "records": current_week,
        "rolling_record_count": len(records),
        "historical_note": "旧日期缺少事件审计或分钟行情包时仅标记PARTIAL，不参与正式规则自动调整。",
    }


def compact_intraday_review_for_llm(review: Dict[str, Any]) -> Dict[str, Any]:
    def compact_record(record: Dict[str, Any]) -> Dict[str, Any]:
        return {
            key: record.get(key)
            for key in (
                "date", "stock", "name", "pool_source", "evidence_quality", "actual_bought",
                "actual_trigger", "first_eligible_time", "first_eligible_trigger", "entry_delay_minutes",
                "entry_slippage_pct", "actual_performance", "first_eligible_performance",
                "future_returns", "future_alpha_pct", "primary_attribution", "attribution_labels",
            )
        }
    return {
        "status": review.get("status"),
        "summary": review.get("summary") or {},
        "opportunity_missed_top3": [compact_record(row) for row in review.get("opportunity_missed_top3") or []],
        "correct_wait_top3": [compact_record(row) for row in review.get("correct_wait_top3") or []],
        "bad_buy_top3": [compact_record(row) for row in review.get("bad_buy_top3") or []],
        "entry_too_late_top3": [compact_record(row) for row in review.get("entry_too_late_top3") or []],
        "system_or_model_failures_top3": [compact_record(row) for row in review.get("system_or_model_failures_top3") or []],
        "recommendations": review.get("recommendations") or [],
        "historical_note": review.get("historical_note"),
    }


def format_intraday_review_text(review: Dict[str, Any]) -> str:
    if not review or review.get("status") == "no_data":
        return "盘中买入复盘: 暂无可用盘中状态数据"
    summary = review.get("summary") or {}
    labels = summary.get("attribution_counts") or {}
    top_labels = "、".join(f"{key}:{value}" for key, value in list(labels.items())[:3]) or "暂无明显问题"
    lines = [
        f"盘中买入复盘 {summary.get('week_start')}至{summary.get('week_end')}",
        f"观察{summary.get('observed_stock_count', 0)}只 | 买入{summary.get('bought_count', 0)}只 | 成交率{summary.get('fill_rate_pct')}%",
        f"任务可用率: {summary.get('completed_task_day_count', 0)}/{summary.get('trading_day_count', 0)}天 ({summary.get('task_availability_pct')}%)",
        f"09:31强势捕获: {summary.get('opening_strong_bought_count', 0)}/{summary.get('opening_strong_count', 0)} | 2分钟内{summary.get('opening_strong_two_minute_capture_rate_pct')}%",
        f"证据质量: 完整{summary.get('full_quality_count', 0)}只/部分{summary.get('partial_quality_count', 0)}只",
        f"LLM调用{summary.get('llm_call_count', 0)}次 | 失败{summary.get('llm_failure_count', 0)}次 | 成功率{summary.get('llm_success_rate_pct')}% | 异常空窗{summary.get('process_gap_count', 0)}段",
        f"等待效果: 错过上涨{summary.get('wait_missed_rise_count', 0)}段/避开下跌{summary.get('wait_avoided_loss_count', 0)}段",
        f"主要归因: {top_labels}",
    ]
    policy = summary.get("policy_comparison") or {}
    current = (policy.get("current_actual") or {}).get("avg_net_return_pct")
    technical = (policy.get("first_technical_direct") or {}).get("avg_net_return_pct")
    opening = (policy.get("opening_strong_direct") or {}).get("avg_net_return_pct")
    lines.append(f"影子对照净收益: 实际{current}%/开盘强势{opening}%/首个技术触发{technical}%")
    missed = review.get("opportunity_missed_top3") or []
    if missed:
        lines.append("错过机会Top3: " + "；".join(
            f"{row.get('stock')} {row.get('name')} MFE{(row.get('first_eligible_performance') or {}).get('mfe_pct')}%({row.get('evidence_quality')})"
            for row in missed
        ))
    correct_wait = review.get("correct_wait_top3") or []
    if correct_wait:
        lines.append("正确等待Top3: " + "；".join(
            f"{row.get('stock')} {row.get('name')} 触发后收盘{(row.get('first_eligible_performance') or {}).get('close_return_pct')}%"
            for row in correct_wait
        ))
    bad_buys = review.get("bad_buy_top3") or []
    if bad_buys:
        lines.append("错误买入Top3: " + "；".join(
            f"{row.get('stock')} {row.get('name')} 买后收盘{(row.get('actual_performance') or {}).get('close_return_pct')}%"
            for row in bad_buys
        ))
    entry_late = review.get("entry_too_late_top3") or []
    if entry_late:
        lines.append("买点过晚Top3: " + "；".join(
            f"{row.get('stock')} {row.get('name')} 晚{row.get('entry_delay_minutes')}分钟/滑点{row.get('entry_slippage_pct')}%"
            for row in entry_late
        ))
    failures = review.get("system_or_model_failures_top3") or []
    if failures:
        lines.append("系统或模型故障Top3: " + "；".join(
            f"{row.get('stock')} {row.get('name')} {row.get('primary_attribution')}"
            for row in failures
        ))
    recommendations = review.get("recommendations") or []
    if recommendations:
        lines.append("下周建议: " + "；".join(str(row.get("action")) for row in recommendations[:3]))
    gate = summary.get("rule_change_gate") or {}
    lines.append(f"规则调整门槛: {gate.get('reason')}")
    return "\n".join(lines)


def update_intraday_policy_memory(review: Dict[str, Any], output_dir: Path) -> int:
    if not review or review.get("status") == "no_data":
        return 0
    path = output_dir / "intraday_buy_policy_memory.jsonl"
    summary = review.get("summary") or {}
    week_end = str(summary.get("week_end") or "")
    snapshot = {
        "schema_version": SCHEMA_VERSION,
        "week_start": summary.get("week_start"),
        "week_end": week_end,
        "generated_at": review.get("generated_at"),
        "summary": {
            key: summary.get(key)
            for key in (
                "observed_stock_count", "trading_day_count", "completed_task_day_count",
                "task_availability_pct", "full_quality_count", "bought_count", "fill_rate_pct",
                "opening_strong_capture_rate_pct", "opening_strong_two_minute_capture_rate_pct",
                "llm_call_count", "llm_failure_count", "wait_missed_rise_count",
                "wait_avoided_loss_count", "process_gap_count", "avg_entry_delay_minutes",
                "avg_entry_slippage_pct", "attribution_counts", "policy_comparison",
                "rule_change_gate",
            )
        },
        "recommendations": review.get("recommendations") or [],
    }
    existing = _read_jsonl(path)
    existing = [row for row in existing if str(row.get("week_end") or "") != week_end]
    existing.append(snapshot)
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(f".{path.name}.{datetime.now().timestamp():.0f}.tmp")
    tmp.write_text("".join(json.dumps(row, ensure_ascii=False, default=str) + "\n" for row in existing), encoding="utf-8")
    tmp.replace(path)
    return 1
