#!/usr/bin/env python3
"""Discover and apply historical edge rules from the full daily candidate pool.

This module is intentionally independent from Top5 attribution.  It reviews all
ranked candidates in daily_report_YYYYMMDD.json and writes compact rule files
that the stock-selection workflow can read on the next run.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import statistics
import time
import urllib.parse
import urllib.request
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

try:
    import requests
except Exception:  # pragma: no cover
    requests = None

BASE_DIR = Path(__file__).resolve().parent
OUTPUT_DIR = BASE_DIR / "output"
EDGE_RULE_DIR = OUTPUT_DIR / "edge_rules"
HORIZONS = (1, 3, 5, 10)
XQ_HTTP_BASE = os.getenv("XQSHARE_HTTP_BASE", "http://127.0.0.1:8080").rstrip("/")
XQ_TIMEOUT = float(os.getenv("EDGE_RULE_XQ_TIMEOUT", "3"))
MAX_CONSECUTIVE_EMPTY = int(os.getenv("EDGE_RULE_MAX_CONSECUTIVE_EMPTY", "40"))
EDGE_RULE_SCHEMA_VERSION = 4
MONEY_FLOW_SEMANTICS_VERSION = "2026-07-10.field-provenance-v2"
ROUND_TRIP_COST_PCT = float(os.getenv("EDGE_RULE_ROUND_TRIP_COST_PCT", "0.25"))
EDGE_RULE_MAX_AGE_DAYS = int(os.getenv("EDGE_RULE_MAX_AGE_DAYS", "14"))


def _safe_float(value: Any, default: Optional[float] = None) -> Optional[float]:
    try:
        if value in (None, ""):
            return default
        return float(value)
    except (TypeError, ValueError):
        return default


def _safe_int(value: Any, default: int = 0) -> int:
    try:
        if value in (None, ""):
            return default
        return int(float(value))
    except (TypeError, ValueError):
        return default


def _normalize_date_key(value: Any) -> str:
    digits = re.sub(r"\D", "", str(value or ""))
    return digits[:8]


def _report_date_from_path(path: Path) -> str:
    m = re.search(r"daily_report_(\d{8})\.json$", path.name)
    return m.group(1) if m else ""


def _to_xt_code(stock: str) -> str:
    s = str(stock or "").strip().upper()
    if not s:
        return s
    if "." in s:
        return s
    if s.startswith(("6", "9")):
        return f"{s}.SH"
    if s.startswith(("0", "2", "3")):
        return f"{s}.SZ"
    if s.startswith(("4", "8")):
        return f"{s}.BJ"
    return s


def _load_env(path: Path = BASE_DIR / ".env") -> None:
    if not path.exists():
        return
    for raw in path.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip().strip('"').strip("'")
        if key and key not in os.environ:
            os.environ[key] = value


def _configure_network() -> None:
    try:
        from domestic_network import configure_domestic_direct_network
        configure_domestic_direct_network()
    except Exception:
        bridge_host = urllib.parse.urlparse(XQ_HTTP_BASE).hostname
        additions = [item for item in (bridge_host, "localhost", "127.0.0.1") if item]
        for key in ("NO_PROXY", "no_proxy"):
            current = [x.strip() for x in os.environ.get(key, "").split(",") if x.strip()]
            seen = {x.lower() for x in current}
            for item in additions:
                if item.lower() not in seen:
                    current.append(item)
                    seen.add(item.lower())
            os.environ[key] = ",".join(current)


def _xq_http_get(endpoint: str, params: Dict[str, Any], timeout: float = XQ_TIMEOUT) -> Dict[str, Any]:
    url = f"{XQ_HTTP_BASE}{endpoint}?{urllib.parse.urlencode(params)}"
    if requests is not None:
        resp = requests.get(url, timeout=(min(1.5, timeout), timeout))
        resp.raise_for_status()
        return resp.json() or {}
    req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return json.loads(resp.read().decode("utf-8")) or {}


def _parse_xq_market_data3(payload: Dict[str, Any]) -> List[Dict[str, Any]]:
    data = payload.get("data") if isinstance(payload, dict) else {}
    if not isinstance(data, dict):
        return []
    maps = {
        name: data.get(name, {})
        for name in ("open", "high", "low", "close", "volume")
        if isinstance(data.get(name, {}), dict)
    }
    dates = sorted({_normalize_date_key(day) for fmap in maps.values() for day in fmap.keys()})
    rows: List[Dict[str, Any]] = []
    for day in dates:
        if not day:
            continue

        def val(name: str) -> Optional[float]:
            fmap = maps.get(name, {})
            raw = fmap.get(day) or fmap.get(f"{day[:4]}-{day[4:6]}-{day[6:8]}") or {}
            if isinstance(raw, dict) and raw:
                return _safe_float(next(iter(raw.values())))
            return _safe_float(raw)

        close = val("close")
        if close is None or close <= 0:
            continue
        rows.append({
            "date": day,
            "open": val("open"),
            "high": val("high"),
            "low": val("low"),
            "close": close,
            "volume": val("volume"),
            "source": "xqshare",
        })
    return rows


def fetch_daily_ohlc(stock: str, count: int = 120) -> List[Dict[str, Any]]:
    try:
        payload = _xq_http_get(
            "/market_data3",
            {"stock": _to_xt_code(stock), "period": "1d", "count": count},
            timeout=XQ_TIMEOUT,
        )
        if payload.get("success") is not False:
            rows = _parse_xq_market_data3(payload)
            if rows:
                return rows
    except Exception:
        pass
    try:
        # Reuse the bounded multi-source route and its per-run circuit breakers.
        from top5_review_attribution import fetch_daily_ohlc as routed_fetch
        return routed_fetch(stock, count)
    except Exception:
        return []


def list_report_files(output_dir: Path = OUTPUT_DIR, days: Optional[int] = None, month: Optional[str] = None) -> List[Path]:
    files: List[Path] = []
    for path in output_dir.glob("daily_report_*.json"):
        if "daily_report_push_" in path.name:
            continue
        day = _report_date_from_path(path)
        if not day:
            continue
        if month and not day.startswith(month):
            continue
        files.append(path)
    files = sorted(files, key=_report_date_from_path, reverse=True)
    return files[:days] if days else files


def _read_json(path: Path) -> Dict[str, Any]:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}


def _candidate_rows_from_reports(paths: Iterable[Path]) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for path in paths:
        report_day = _report_date_from_path(path)
        if not report_day:
            continue
        report = _read_json(path)
        phase2 = report.get("phase2") or {}
        artifact_value = (
            phase2.get("candidate_details_artifact")
            or (report.get("artifacts") or {}).get("candidates_jsonl")
            or ""
        )
        ranked = []
        if artifact_value:
            artifact_path = Path(str(artifact_value)).expanduser()
            if not artifact_path.is_absolute():
                artifact_path = path.parent / artifact_path
            if artifact_path.exists():
                try:
                    ranked = [
                        json.loads(line)
                        for line in artifact_path.read_text(encoding="utf-8").splitlines()
                        if line.strip()
                    ]
                except Exception:
                    ranked = []
        if not ranked:
            ranked = phase2.get("ranked_candidates") or []
        for idx, c in enumerate(ranked, 1):
            if not isinstance(c, dict):
                continue
            stock = str(c.get("stock") or "").strip()
            if not stock:
                continue
            row = dict(c)
            row["report_date"] = report_day
            row["candidate_rank"] = idx
            rows.append(row)
    return rows


def _get_nested(obj: Dict[str, Any], dotted: str) -> Any:
    cur: Any = obj
    for part in dotted.split("."):
        if not isinstance(cur, dict):
            return None
        cur = cur.get(part)
    return cur


def _feature(candidate: Dict[str, Any], name: str) -> Any:
    q = candidate.get("quant_score_detail") or {}
    mf = candidate.get("money_flow") or {}
    k = candidate.get("kline_summary") or {}
    if name == "signal":
        return str(candidate.get("signal") or candidate.get("action") or "").upper()
    if name == "pool":
        return candidate.get("pool") or candidate.get("strategy_type") or ""
    if name == "pool_not":
        return candidate.get("pool") or candidate.get("strategy_type") or ""
    if name == "score":
        return _safe_float(candidate.get("pre_edge_score") or candidate.get("buy_score") or candidate.get("ranking_score") or candidate.get("final_score"), 0.0)
    if name == "confidence":
        return _safe_float(candidate.get("confidence"), 0.0)
    if name == "rank":
        return _safe_float(candidate.get("candidate_rank") or candidate.get("pool_rank"), 999999)
    if name == "pool_score":
        return _safe_float(candidate.get("pool_score"), 0.0)
    if name == "main_net_flow":
        return _safe_float(mf.get("main_net_flow"), None)
    if name == "super_net_flow":
        return _safe_float(mf.get("super_net_flow"), None)
    if name == "ddx_5":
        return _safe_float(mf.get("ddx_5"), None)
    if name == "ddy_10":
        return _safe_float(mf.get("ddy_10"), None)
    if name == "main_net_flow_5d":
        return _safe_float(mf.get("main_net_flow_5d"), None)
    if name == "main_net_flow_10d":
        return _safe_float(mf.get("main_net_flow_10d"), None)
    if name == "rsi":
        return _safe_float(candidate.get("rsi") or _get_nested(q, "next_day_buyability_detail.rsi_14"), None)
    if name == "close_position_20d":
        return _safe_float(k.get("close_position_20d") or _get_nested(q, "next_day_buyability_detail.close_position_20d"), None)
    if name == "ma_system":
        return k.get("ma_system") or _get_nested(q, "tech_detail.ma_system") or ""
    if name == "vol_signal":
        return k.get("vol_signal") or ""
    if name == "tech_score":
        return _safe_float(q.get("tech_score"), 0.0)
    if name == "money_flow_score":
        return _safe_float(q.get("money_flow_score"), 0.0)
    return candidate.get(name)


def _condition_matches(candidate: Dict[str, Any], condition: Dict[str, Any]) -> bool:
    field = condition.get("field")
    op = condition.get("op")
    target = condition.get("value")
    value = _feature(candidate, str(field))
    if op == "eq":
        return str(value).upper() == str(target).upper()
    if op == "neq_contains":
        return str(target) not in str(value)
    if op == "contains":
        return str(target) in str(value)
    num = _safe_float(value, None)
    tgt = _safe_float(target, None)
    if num is None or tgt is None:
        return False
    if op == "gt":
        return num > tgt
    if op == "gte":
        return num >= tgt
    if op == "lt":
        return num < tgt
    if op == "lte":
        return num <= tgt
    return False


def _rule_matches(candidate: Dict[str, Any], rule: Dict[str, Any]) -> bool:
    return all(_condition_matches(candidate, cond) for cond in rule.get("conditions") or [])


def _rule_evidence_family(rule: Dict[str, Any]) -> str:
    family_by_field = {
        "pool": "pool",
        "pool_not": "pool",
        "pool_score": "pool_strength",
        "main_net_flow": "money_flow",
        "super_net_flow": "money_flow",
        "ddx_5": "money_flow",
        "ddy_10": "money_flow",
        "main_net_flow_5d": "money_flow",
        "main_net_flow_10d": "money_flow",
        "money_flow_score": "money_flow",
        "rsi": "technical",
        "close_position_20d": "technical",
        "ma_system": "technical",
        "vol_signal": "technical",
        "tech_score": "technical",
    }
    families = {
        family_by_field.get(str(cond.get("field") or ""), str(cond.get("field") or "other"))
        for cond in (rule.get("conditions") or [])
    }
    return "+".join(sorted(families))


def _condition_text(cond: Dict[str, Any]) -> str:
    field = str(cond.get("field") or "")
    op = cond.get("op")
    value = cond.get("value")
    labels = {
        "signal": "信号", "pool": "池", "pool_not": "池非", "score": "做多分",
        "rank": "候选排名", "pool_score": "池内分", "main_net_flow": "主力净流", "super_net_flow": "超大单净流",
        "ddx_5": "DDX5", "ddy_10": "DDY10", "main_net_flow_5d": "5日主力净流累计",
        "main_net_flow_10d": "10日主力净流累计", "rsi": "RSI", "close_position_20d": "20日位置",
        "ma_system": "均线", "vol_signal": "量能", "money_flow_score": "资金分", "tech_score": "技术分",
    }
    name = labels.get(field, field)
    if op == "eq":
        return f"{name}={value}"
    if op == "contains":
        return f"{name}含{value}"
    if op == "neq_contains":
        return f"{name}不含{value}"
    signs = {"gt": ">", "gte": ">=", "lt": "<", "lte": "<="}
    return f"{name}{signs.get(op, op)}{value}"


def _predicate_catalog() -> List[Dict[str, Any]]:
    # Avoid signal/score/rank predicates here: those are outputs of the current
    # selection model and can create self-reinforcing rules.  Edge rules should
    # be based on frozen market/pool/data features available before overlay.
    return [
        {"field": "pool", "op": "contains", "value": "低吸"},
        {"field": "pool", "op": "contains", "value": "强势"},
        {"field": "pool", "op": "contains", "value": "涨停"},
        {"field": "pool", "op": "contains", "value": "首板"},
        {"field": "pool", "op": "contains", "value": "突破新高"},
        {"field": "pool_not", "op": "neq_contains", "value": "突破新高"},
        {"field": "pool_score", "op": "gte", "value": 60},
        {"field": "main_net_flow", "op": "gt", "value": 0},
        {"field": "main_net_flow", "op": "gt", "value": 5},
        {"field": "super_net_flow", "op": "gt", "value": 0},
        {"field": "super_net_flow", "op": "gt", "value": 5},
        {"field": "main_net_flow_5d", "op": "gt", "value": 0},
        {"field": "main_net_flow_10d", "op": "gt", "value": 0},
        {"field": "rsi", "op": "lte", "value": 80},
        {"field": "rsi", "op": "gte", "value": 75},
        {"field": "close_position_20d", "op": "lte", "value": 95},
        {"field": "close_position_20d", "op": "gte", "value": 95},
        {"field": "ma_system", "op": "contains", "value": "多头"},
        {"field": "vol_signal", "op": "contains", "value": "放量"},
        {"field": "money_flow_score", "op": "gte", "value": 60},
        {"field": "money_flow_score", "op": "lte", "value": 40},
        {"field": "tech_score", "op": "gte", "value": 60},
        {"field": "tech_score", "op": "lte", "value": 45},
    ]


def _non_conflicting(conds: List[Dict[str, Any]]) -> bool:
    exact = set()
    threshold_fields = set()
    for cond in conds:
        key = (cond.get("field"), cond.get("op"), str(cond.get("value")))
        if key in exact:
            return False
        exact.add(key)
        if cond.get("op") in {"gt", "gte", "lt", "lte"}:
            marker = (cond.get("field"), "threshold")
            if marker in threshold_fields:
                return False
            threshold_fields.add(marker)
    return True


def _condition_combos(predicates: List[Dict[str, Any]], max_size: int = 3) -> Iterable[List[Dict[str, Any]]]:
    n = len(predicates)
    for i in range(n):
        yield [predicates[i]]
    for i in range(n):
        for j in range(i + 1, n):
            combo = [predicates[i], predicates[j]]
            if _non_conflicting(combo):
                yield combo
    if max_size >= 3:
        for i in range(n):
            for j in range(i + 1, n):
                for k in range(j + 1, n):
                    combo = [predicates[i], predicates[j], predicates[k]]
                    if _non_conflicting(combo):
                        yield combo


def _return_stats(values: List[float]) -> Dict[str, Any]:
    if not values:
        return {"n": 0, "avg": None, "median": None, "win_rate": None, "min": None, "max": None}
    return {
        "n": len(values),
        "avg": round(sum(values) / len(values), 2),
        "median": round(statistics.median(values), 2),
        "win_rate": round(sum(1 for v in values if v > 0) / len(values) * 100, 2),
        "min": round(min(values), 2),
        "max": round(max(values), 2),
    }


def _load_price_cache(path: Path) -> Dict[str, List[Dict[str, Any]]]:
    data = _read_json(path) if path.exists() else {}
    cache = data.get("daily_ohlc") if isinstance(data, dict) else {}
    return cache if isinstance(cache, dict) else {}


def _save_price_cache(path: Path, cache: Dict[str, List[Dict[str, Any]]]) -> None:
    _write_json_atomic(path, {
        "updated_at": datetime.now().isoformat(timespec="seconds"),
        "daily_ohlc": cache,
    })


def _augment_with_prices(
    rows: List[Dict[str, Any]],
    fetcher=fetch_daily_ohlc,
    pause_sec: float = 0.01,
    cache_dir: Optional[Path] = None,
) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    cache_path = (cache_dir or EDGE_RULE_DIR) / "daily_ohlc_cache.json"
    cache: Dict[str, List[Dict[str, Any]]] = _load_price_cache(cache_path)
    priced: List[Dict[str, Any]] = []
    missing_price = 0
    incomplete_d5 = 0
    fetched = 0
    cache_hits = 0
    consecutive_empty = 0
    aborted_fetch = False
    refreshed_stocks = set()
    stale_cache_stocks = set()
    untradable_rows = 0
    latest_required = max((str(r.get("report_date") or "") for r in rows), default="")
    for row in rows:
        stock = row.get("stock")
        report_day = row.get("report_date")
        if not stock:
            missing_price += 1
            continue
        cached_bars = cache.get(stock) or []
        cached_latest = str((cached_bars[-1] if cached_bars else {}).get("date") or "")
        needs_refresh = not cached_bars or (latest_required and cached_latest < latest_required)
        if cached_bars:
            cache_hits += 1
        if needs_refresh and stock not in refreshed_stocks and not aborted_fetch:
            refreshed_stocks.add(stock)
            if cached_bars:
                stale_cache_stocks.add(stock)
            bars = fetcher(stock, 160) or []
            fetched += 1
            fetched_latest = str((bars[-1] if bars else {}).get("date") or "")
            made_progress = bool(bars) and fetched_latest > cached_latest
            if made_progress:
                consecutive_empty = 0
                merged = {str(bar.get("date") or ""): bar for bar in cached_bars if isinstance(bar, dict)}
                merged.update({str(bar.get("date") or ""): bar for bar in bars if isinstance(bar, dict)})
                cache[stock] = [merged[key] for key in sorted(merged) if key]
            else:
                consecutive_empty += 1
                if consecutive_empty >= MAX_CONSECUTIVE_EMPTY:
                    aborted_fetch = True
            if pause_sec:
                time.sleep(pause_sec)
        bars = cache.get(stock) or []
        idx = next((i for i, b in enumerate(bars) if b.get("date") == report_day), None)
        if idx is None:
            missing_price += 1
            continue
        entry = _safe_float(bars[idx].get("open") or bars[idx].get("close"), None)
        if not entry or entry <= 0:
            missing_price += 1
            continue
        day_bar = bars[idx]
        day_prices = [_safe_float(day_bar.get(key), None) for key in ("open", "high", "low", "close")]
        if all(value is not None for value in day_prices) and max(day_prices) == min(day_prices):
            untradable_rows += 1
            continue
        out = dict(row)
        out["entry_price"] = entry
        out["price_source"] = bars[idx].get("source") or "xqshare_cache" if stock in cache else "xqshare"
        for h in HORIZONS:
            if idx + h < len(bars):
                close = _safe_float(bars[idx + h].get("close"), None)
                if close and close > 0:
                    gross = (close / entry - 1) * 100
                    out[f"d{h}_return_pct"] = round(gross - ROUND_TRIP_COST_PCT, 2)
        if out.get("d5_return_pct") is None:
            incomplete_d5 += 1
            continue
        priced.append(out)
    if fetched:
        _save_price_cache(cache_path, cache)
    return priced, {
        "loaded_candidates": len(rows),
        "unique_stocks": len({r.get("stock") for r in rows if r.get("stock")}),
        "priced_rows": len(priced),
        "missing_price": missing_price,
        "no_d5": incomplete_d5,
        "cache_hits": cache_hits,
        "fetched_stocks": fetched,
        "refreshed_stocks": len(refreshed_stocks),
        "stale_cache_stocks": len(stale_cache_stocks),
        "untradable_one_price_rows": untradable_rows,
        "round_trip_cost_pct": ROUND_TRIP_COST_PCT,
        "latest_required_date": latest_required,
        "latest_bar_date": max((str((bars[-1] if bars else {}).get("date") or "") for bars in cache.values()), default=""),
        "aborted_fetch": aborted_fetch,
        "max_consecutive_empty": MAX_CONSECUTIVE_EMPTY,
    }


def _split_train_validation_test(rows: List[Dict[str, Any]]) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]], List[Dict[str, Any]]]:
    dates = sorted({str(r.get("report_date") or "") for r in rows if r.get("report_date")})
    if len(dates) < 8:
        cutoff = max(1, min(len(dates) - 1, int(len(dates) * 0.7)))
        train_dates = set(dates[:cutoff])
        valid_dates = set(dates[cutoff:])
        test_dates = set()
    else:
        train_cutoff = max(1, int(len(dates) * 0.6))
        valid_cutoff = max(train_cutoff + 1, int(len(dates) * 0.8))
        valid_cutoff = min(valid_cutoff, len(dates) - 1)
        train_dates = set(dates[:train_cutoff])
        valid_dates = set(dates[train_cutoff:valid_cutoff])
        test_dates = set(dates[valid_cutoff:])
    train = [r for r in rows if r.get("report_date") in train_dates]
    valid = [r for r in rows if r.get("report_date") in valid_dates]
    test = [r for r in rows if r.get("report_date") in test_dates]
    return train, valid, test


def _dynamic_edge_bonus(rule_metrics: Dict[str, Any], validation_metrics: Dict[str, Any], test_metrics: Dict[str, Any], strength: float) -> float:
    sample_count = _safe_int(rule_metrics.get("sample_count"), 0)
    trade_days = _safe_int(rule_metrics.get("trade_days"), 0)
    win_rate = _safe_float(rule_metrics.get("d5_win_rate_pct"), 0.0) or 0.0
    min_ret = _safe_float(rule_metrics.get("d5_min_return_pct"), 0.0) or 0.0
    v_n = _safe_int(validation_metrics.get("sample_count"), 0)
    v_avg = _safe_float(validation_metrics.get("d5_avg_return_pct"), 0.0) or 0.0
    v_win = _safe_float(validation_metrics.get("d5_win_rate_pct"), 0.0) or 0.0
    t_n = _safe_int(test_metrics.get("sample_count"), 0)
    t_avg = _safe_float(test_metrics.get("d5_avg_return_pct"), 0.0) or 0.0
    t_win = _safe_float(test_metrics.get("d5_win_rate_pct"), 0.0) or 0.0
    cap = 5.0
    if sample_count >= 25 and trade_days >= 8 and v_n >= 6 and v_avg > 0 and v_win >= 50 and t_n >= 5 and t_avg > 0 and t_win >= 50:
        cap = 8.0
    if sample_count >= 35 and trade_days >= 10 and v_n >= 8 and v_avg >= 5 and v_win >= 60 and t_n >= 8 and t_avg >= 3 and t_win >= 55 and min_ret > -30:
        cap = 12.0
    if sample_count >= 50 and trade_days >= 14 and v_n >= 12 and v_avg >= 8 and v_win >= 65 and t_n >= 12 and t_avg >= 5 and t_win >= 60 and min_ret > -25:
        cap = 15.0
    return round(max(2.0, min(cap, strength * 0.55)), 1)


def _build_rule(
    conds: List[Dict[str, Any]],
    matched: List[Dict[str, Any]],
    train_matched: List[Dict[str, Any]],
    validation_matched: List[Dict[str, Any]],
    test_matched: List[Dict[str, Any]],
    baseline: Dict[str, Any],
    validation_baseline: Dict[str, Any],
    test_baseline: Dict[str, Any],
    index: int,
) -> Optional[Dict[str, Any]]:
    values = [v for v in (_safe_float(r.get("d5_return_pct"), None) for r in matched) if v is not None]
    train_values = [v for v in (_safe_float(r.get("d5_return_pct"), None) for r in train_matched) if v is not None]
    valid_values = [v for v in (_safe_float(r.get("d5_return_pct"), None) for r in validation_matched) if v is not None]
    test_values = [v for v in (_safe_float(r.get("d5_return_pct"), None) for r in test_matched) if v is not None]
    stats = _return_stats(values)
    train_stats = _return_stats(train_values)
    valid_stats = _return_stats(valid_values)
    test_stats = _return_stats(test_values)
    if stats["n"] <= 0 or train_stats["n"] <= 0:
        return None
    days = len({r.get("report_date") for r in matched})
    avg = _safe_float(stats.get("avg"), 0.0) or 0.0
    win = _safe_float(stats.get("win_rate"), 0.0) or 0.0
    median = _safe_float(stats.get("median"), 0.0) or 0.0
    base_avg = _safe_float(baseline.get("avg"), 0.0) or 0.0
    base_win = _safe_float(baseline.get("win_rate"), 0.0) or 0.0
    base_median = _safe_float(baseline.get("median"), 0.0) or 0.0
    avg_lift = avg - base_avg
    win_lift = win - base_win
    median_lift = median - base_median
    min_ret = _safe_float(stats.get("min"), 0.0) or 0.0
    drawdown_penalty = max(0.0, abs(min_ret) - 20.0) * 0.18
    sample_penalty = max(0.0, 40.0 - stats["n"]) * 0.06
    validation_bonus = 0.0
    if valid_stats["n"]:
        validation_bonus = ((_safe_float(valid_stats.get("avg"), 0.0) or 0.0) - (_safe_float(validation_baseline.get("avg"), 0.0) or 0.0)) * 0.35
    strength = avg_lift + win_lift * 0.10 + median_lift * 0.20 + validation_bonus - drawdown_penalty - sample_penalty
    desc = " & ".join(_condition_text(c) for c in conds)
    metrics = {
        "sample_count": stats["n"],
        "trade_days": days,
        "d5_avg_return_pct": stats["avg"],
        "d5_median_return_pct": stats["median"],
        "d5_win_rate_pct": stats["win_rate"],
        "d5_min_return_pct": stats["min"],
        "d5_max_return_pct": stats["max"],
        "avg_lift_pct": round(avg_lift, 2),
        "win_lift_pct": round(win_lift, 2),
        "median_lift_pct": round(median_lift, 2),
    }
    validation_metrics = {
        "sample_count": valid_stats["n"],
        "d5_avg_return_pct": valid_stats["avg"],
        "d5_median_return_pct": valid_stats["median"],
        "d5_win_rate_pct": valid_stats["win_rate"],
        "baseline_avg_pct": validation_baseline.get("avg"),
        "baseline_win_rate_pct": validation_baseline.get("win_rate"),
    }
    test_metrics = {
        "sample_count": test_stats["n"],
        "d5_avg_return_pct": test_stats["avg"],
        "d5_median_return_pct": test_stats["median"],
        "d5_win_rate_pct": test_stats["win_rate"],
        "baseline_avg_pct": test_baseline.get("avg"),
        "baseline_win_rate_pct": test_baseline.get("win_rate"),
    }
    bonus = _dynamic_edge_bonus(metrics, validation_metrics, test_metrics, strength)
    return {
        "id": f"edge_{index:03d}",
        "description": desc,
        "conditions": conds,
        "metrics": metrics,
        "train_metrics": {
            "sample_count": train_stats["n"],
            "d5_avg_return_pct": train_stats["avg"],
            "d5_win_rate_pct": train_stats["win_rate"],
        },
        "validation_metrics": validation_metrics,
        "test_metrics": test_metrics,
        "strength_score": round(strength, 2),
        "edge_bonus": bonus,
        "max_edge_bonus": bonus,
        "anti_overfit": {
            "uses_model_output_predicates": any(c.get("field") in {"signal", "score", "rank"} for c in conds),
            "has_out_of_sample_validation": valid_stats["n"] > 0,
            "has_untouched_test": test_stats["n"] > 0,
            "unique_stock_count": len({r.get("stock") for r in matched}),
        },
    }


def _bayesian_shrunk_metrics(values: List[float], baseline: Dict[str, Any], prior_n: int = 20) -> Dict[str, Any]:
    stats = _return_stats(values)
    if not values:
        return {"sample_count": 0, "shrunk_avg_return_pct": None, "shrunk_win_rate_pct": None}
    base_avg = _safe_float(baseline.get("avg"), 0.0) or 0.0
    base_win = _safe_float(baseline.get("win_rate"), 50.0) or 50.0
    avg = _safe_float(stats.get("avg"), 0.0) or 0.0
    win = _safe_float(stats.get("win_rate"), 0.0) or 0.0
    n = len(values)
    return {
        "sample_count": n,
        "prior_sample_count": prior_n,
        "shrunk_avg_return_pct": round((avg * n + base_avg * prior_n) / (n + prior_n), 2),
        "shrunk_win_rate_pct": round((win * n + base_win * prior_n) / (n + prior_n), 2),
    }


def _dynamic_negative_penalty(
    metrics: Dict[str, Any],
    validation_metrics: Dict[str, Any],
    test_metrics: Dict[str, Any],
    baseline: Dict[str, Any],
) -> float:
    base_avg = _safe_float(baseline.get("avg"), 0.0) or 0.0
    base_win = _safe_float(baseline.get("win_rate"), 50.0) or 50.0
    shrunk_avg = _safe_float(metrics.get("shrunk_avg_return_pct"), base_avg) or base_avg
    shrunk_win = _safe_float(metrics.get("shrunk_win_rate_pct"), base_win) or base_win
    severity = max(0.0, base_avg - shrunk_avg) + max(0.0, base_win - shrunk_win) * 0.08
    penalty = 2.0 + severity * 0.55
    if (
        metrics.get("sample_count", 0) >= 35
        and metrics.get("trade_days", 0) >= 8
        and validation_metrics.get("sample_count", 0) >= 5
        and test_metrics.get("sample_count", 0) >= 4
    ):
        penalty += 1.0
    return round(max(2.0, min(8.0, penalty)), 1)


def _build_negative_rule(
    conds: List[Dict[str, Any]],
    matched: List[Dict[str, Any]],
    validation_matched: List[Dict[str, Any]],
    test_matched: List[Dict[str, Any]],
    baseline: Dict[str, Any],
    validation_baseline: Dict[str, Any],
    test_baseline: Dict[str, Any],
    index: int,
) -> Optional[Dict[str, Any]]:
    values = [v for v in (_safe_float(r.get("d5_return_pct"), None) for r in matched) if v is not None]
    if not values:
        return None
    valid_values = [v for v in (_safe_float(r.get("d5_return_pct"), None) for r in validation_matched) if v is not None]
    test_values = [v for v in (_safe_float(r.get("d5_return_pct"), None) for r in test_matched) if v is not None]
    raw = _return_stats(values)
    shrunk = _bayesian_shrunk_metrics(values, baseline)
    valid_raw = _return_stats(valid_values)
    valid_shrunk = _bayesian_shrunk_metrics(valid_values, validation_baseline, prior_n=10)
    test_raw = _return_stats(test_values)
    test_shrunk = _bayesian_shrunk_metrics(test_values, test_baseline, prior_n=10)
    metrics = {
        "sample_count": len(values),
        "trade_days": len({r.get("report_date") for r in matched if r.get("report_date")}),
        "d5_avg_return_pct": raw.get("avg"),
        "d5_median_return_pct": raw.get("median"),
        "d5_win_rate_pct": raw.get("win_rate"),
        "d5_min_return_pct": raw.get("min"),
        **shrunk,
    }
    validation_metrics = {
        "sample_count": len(valid_values),
        "d5_avg_return_pct": valid_raw.get("avg"),
        "d5_win_rate_pct": valid_raw.get("win_rate"),
        **valid_shrunk,
    }
    test_metrics = {
        "sample_count": len(test_values),
        "d5_avg_return_pct": test_raw.get("avg"),
        "d5_win_rate_pct": test_raw.get("win_rate"),
        **test_shrunk,
    }
    penalty = _dynamic_negative_penalty(metrics, validation_metrics, test_metrics, baseline)
    return {
        "id": f"protect_{index:03d}",
        "rule_type": "negative_protection",
        "description": " & ".join(_condition_text(c) for c in conds),
        "conditions": conds,
        "metrics": metrics,
        "validation_metrics": validation_metrics,
        "test_metrics": test_metrics,
        "score_penalty": penalty,
        "watch_only": penalty >= 6.0,
        "anti_overfit": {
            "bayesian_shrinkage": True,
            "prior_sample_count": 20,
            "has_out_of_sample_validation": bool(valid_values),
            "has_untouched_test": bool(test_values),
            "unique_stock_count": len({r.get("stock") for r in matched}),
        },
    }


def _similar_signature(rule: Dict[str, Any]) -> Tuple[str, ...]:
    fields = []
    for cond in rule.get("conditions") or []:
        field = str(cond.get("field") or "")
        if field in {"main_net_flow", "super_net_flow", "ddx_5", "ddy_10", "money_flow_score"}:
            fields.append("money_flow")
        elif field in {"rsi", "close_position_20d", "ma_system", "vol_signal", "tech_score"}:
            fields.append("technical")
        elif field.startswith("pool"):
            fields.append("pool")
        else:
            fields.append(field)
    return tuple(sorted(set(fields)))


def discover_edge_rules(
    priced_rows: List[Dict[str, Any]],
    min_samples: int = 25,
    min_days: int = 8,
    max_rules: int = 10,
) -> Dict[str, Any]:
    d5_values = [_safe_float(r.get("d5_return_pct"), None) for r in priced_rows]
    d5_values = [v for v in d5_values if v is not None]
    baseline = _return_stats(d5_values)
    train_rows, validation_rows, test_rows = _split_train_validation_test(priced_rows)
    validation_values = [_safe_float(r.get("d5_return_pct"), None) for r in validation_rows]
    validation_values = [v for v in validation_values if v is not None]
    validation_baseline = _return_stats(validation_values)
    test_values = [_safe_float(r.get("d5_return_pct"), None) for r in test_rows]
    test_values = [v for v in test_values if v is not None]
    test_baseline = _return_stats(test_values)
    rules: List[Dict[str, Any]] = []
    idx = 1
    for conds in _condition_combos(_predicate_catalog(), max_size=3):
        train_matched = [row for row in train_rows if all(_condition_matches(row, c) for c in conds)]
        if len(train_matched) < min_samples:
            continue
        if len({m.get("report_date") for m in train_matched}) < min_days:
            continue
        matched = [row for row in priced_rows if all(_condition_matches(row, c) for c in conds)]
        validation_matched = [row for row in validation_rows if all(_condition_matches(row, c) for c in conds)]
        test_matched = [row for row in test_rows if all(_condition_matches(row, c) for c in conds)]
        rule = _build_rule(conds, matched, train_matched, validation_matched, test_matched, baseline, validation_baseline, test_baseline, idx)
        if not rule:
            continue
        metrics = rule["metrics"]
        validation = rule.get("validation_metrics") or {}
        validation_avg = _safe_float(validation.get("d5_avg_return_pct"), None)
        validation_win = _safe_float(validation.get("d5_win_rate_pct"), None)
        test_metrics = rule.get("test_metrics") or {}
        test_avg = _safe_float(test_metrics.get("d5_avg_return_pct"), None)
        test_win = _safe_float(test_metrics.get("d5_win_rate_pct"), None)
        if (metrics["avg_lift_pct"] < 5.0 and metrics["win_lift_pct"] < 20.0) or metrics["d5_median_return_pct"] <= 0:
            continue
        if validation.get("sample_count", 0) and (validation_avg is None or validation_avg <= 0 or (validation_win or 0) < 45):
            continue
        if test_rows and (test_metrics.get("sample_count", 0) < 5 or test_avg is None or test_avg <= 0 or (test_win or 0) < 45):
            continue
        if len({row.get("stock") for row in matched}) < max(10, min_samples // 2):
            continue
        rules.append(rule)
        idx += 1
    rules.sort(key=lambda r: (r.get("strength_score", 0), r.get("validation_metrics", {}).get("sample_count", 0), r.get("metrics", {}).get("sample_count", 0)), reverse=True)
    unique: List[Dict[str, Any]] = []
    seen_descriptions = set()
    seen_signatures: Dict[Tuple[str, ...], int] = {}
    for rule in rules:
        desc = rule.get("description")
        if desc in seen_descriptions:
            continue
        sig = _similar_signature(rule)
        if seen_signatures.get(sig, 0) >= 2:
            continue
        seen_signatures[sig] = seen_signatures.get(sig, 0) + 1
        seen_descriptions.add(desc)
        unique.append(rule)
        if len(unique) >= max_rules:
            break
    for i, rule in enumerate(unique, 1):
        rule["id"] = f"edge_{i:03d}"
    return {"baseline": baseline, "validation_baseline": validation_baseline, "test_baseline": test_baseline, "rules": unique}


def discover_negative_rules(
    priced_rows: List[Dict[str, Any]],
    min_samples: int = 20,
    min_days: int = 5,
    max_rules: int = 10,
) -> List[Dict[str, Any]]:
    """Discover stable underperforming combinations for score protection."""
    all_values = [v for v in (_safe_float(r.get("d5_return_pct"), None) for r in priced_rows) if v is not None]
    baseline = _return_stats(all_values)
    train_rows, validation_rows, test_rows = _split_train_validation_test(priced_rows)
    valid_values = [v for v in (_safe_float(r.get("d5_return_pct"), None) for r in validation_rows) if v is not None]
    test_values = [v for v in (_safe_float(r.get("d5_return_pct"), None) for r in test_rows) if v is not None]
    validation_baseline = _return_stats(valid_values)
    test_baseline = _return_stats(test_values)
    rules: List[Dict[str, Any]] = []
    idx = 1
    for conds in _condition_combos(_predicate_catalog(), max_size=3):
        train_matched = [row for row in train_rows if all(_condition_matches(row, c) for c in conds)]
        if len(train_matched) < min_samples:
            continue
        if len({row.get("report_date") for row in train_matched}) < min_days:
            continue
        matched = [row for row in priced_rows if all(_condition_matches(row, c) for c in conds)]
        validation_matched = [row for row in validation_rows if all(_condition_matches(row, c) for c in conds)]
        test_matched = [row for row in test_rows if all(_condition_matches(row, c) for c in conds)]
        rule = _build_negative_rule(
            conds,
            matched,
            validation_matched,
            test_matched,
            baseline,
            validation_baseline,
            test_baseline,
            idx,
        )
        if not rule:
            continue
        metrics = rule.get("metrics") or {}
        shrunk_avg = _safe_float(metrics.get("shrunk_avg_return_pct"), 0.0) or 0.0
        shrunk_win = _safe_float(metrics.get("shrunk_win_rate_pct"), 50.0) or 50.0
        base_avg = _safe_float(baseline.get("avg"), 0.0) or 0.0
        base_win = _safe_float(baseline.get("win_rate"), 50.0) or 50.0
        median = _safe_float(metrics.get("d5_median_return_pct"), 0.0) or 0.0
        if not ((shrunk_avg <= base_avg - 2.0 or shrunk_win <= base_win - 12.0) and median < 0):
            continue
        validation = rule.get("validation_metrics") or {}
        if validation_rows:
            if validation.get("sample_count", 0) < 4:
                continue
            if (_safe_float(validation.get("d5_avg_return_pct"), 0.0) or 0.0) >= (_safe_float(validation_baseline.get("avg"), 0.0) or 0.0):
                continue
        test_metrics = rule.get("test_metrics") or {}
        if test_rows:
            if test_metrics.get("sample_count", 0) < 3:
                continue
            if (_safe_float(test_metrics.get("d5_avg_return_pct"), 0.0) or 0.0) >= (_safe_float(test_baseline.get("avg"), 0.0) or 0.0):
                continue
        if len({row.get("stock") for row in matched}) < 10:
            continue
        rules.append(rule)
        idx += 1
    rules.sort(
        key=lambda rule: (
            float(rule.get("score_penalty") or 0.0),
            int((rule.get("metrics") or {}).get("sample_count") or 0),
        ),
        reverse=True,
    )
    unique: List[Dict[str, Any]] = []
    signatures: Dict[Tuple[str, ...], int] = {}
    for rule in rules:
        signature = _similar_signature(rule)
        if signatures.get(signature, 0) >= 2:
            continue
        signatures[signature] = signatures.get(signature, 0) + 1
        unique.append(rule)
        if len(unique) >= max_rules:
            break
    for i, rule in enumerate(unique, 1):
        rule["id"] = f"protect_{i:03d}"
    return unique


def _is_high_chase_candidate(candidate: Dict[str, Any]) -> bool:
    text = " ".join(
        str(value or "")
        for value in (
            candidate.get("pool"),
            candidate.get("strategy_type"),
            " ".join(candidate.get("source_pools") or []),
            " ".join(candidate.get("strategy_types") or []),
        )
    )
    return any(word in text for word in ("涨停", "首板", "突破新高", "momentum_breakout", "limit_follow"))


def build_chase_policy(priced_rows: List[Dict[str, Any]], baseline: Dict[str, Any]) -> Dict[str, Any]:
    chase_rows = [row for row in priced_rows if _is_high_chase_candidate(row)]
    values = [v for v in (_safe_float(row.get("d5_return_pct"), None) for row in chase_rows) if v is not None]
    days = len({row.get("report_date") for row in chase_rows if row.get("report_date")})
    metrics = _return_stats(values)
    shrunk = _bayesian_shrunk_metrics(values, baseline)
    limit = 2
    status = "insufficient_samples"
    reason = f"追高样本{len(values)}/交易日{days}，维持默认上限2只"
    if len(values) >= 20 and days >= 5:
        status = "active"
        avg = _safe_float(shrunk.get("shrunk_avg_return_pct"), 0.0) or 0.0
        win = _safe_float(shrunk.get("shrunk_win_rate_pct"), 50.0) or 50.0
        base_avg = _safe_float(baseline.get("avg"), 0.0) or 0.0
        base_win = _safe_float(baseline.get("win_rate"), 50.0) or 50.0
        if avg <= min(-2.0, base_avg - 3.0) and win <= min(35.0, base_win - 15.0):
            limit = 0
        elif avg < base_avg or win < min(45.0, base_win):
            limit = 1
        reason = f"追高D5收缩均值{avg:+.2f}%/胜率{win:.1f}%，动态上限{limit}只"
    return {
        "status": status,
        "high_chase_limit": limit,
        "sample_count": len(values),
        "trade_days": days,
        "metrics": metrics,
        "bayesian_metrics": shrunk,
        "reason": reason,
    }


def _previous_month(today: Optional[date] = None) -> str:
    today = today or date.today()
    first = today.replace(day=1)
    prev_last = first - timedelta(days=1)
    return prev_last.strftime("%Y%m")


def build_edge_rules(
    mode: str = "weekly",
    days: int = 20,
    month: Optional[str] = None,
    output_dir: Path = OUTPUT_DIR,
    fetcher=fetch_daily_ohlc,
    min_samples: int = 25,
    min_days: int = 8,
) -> Dict[str, Any]:
    if mode == "monthly":
        month = month or _previous_month()
        report_files = list_report_files(output_dir, month=month)
    else:
        report_files = list_report_files(output_dir, days=days)
    rows = _candidate_rows_from_reports(report_files)
    priced_rows, price_summary = _augment_with_prices(
        rows,
        fetcher=fetcher,
        cache_dir=output_dir / "edge_rules",
    )
    discovered = discover_edge_rules(priced_rows, min_samples=min_samples, min_days=min_days)
    negative_rules = discover_negative_rules(priced_rows, min_samples=20, min_days=5)
    generated_at = datetime.now()
    return {
        "generated_at": generated_at.isoformat(timespec="seconds"),
        "valid_until": (generated_at.date() + timedelta(days=EDGE_RULE_MAX_AGE_DAYS)).isoformat(),
        "mode": mode,
        "month": month if mode == "monthly" else None,
        "source_report_days": sorted({_report_date_from_path(p) for p in report_files}),
        "candidate_count": len(rows),
        "price_summary": price_summary,
        "data_as_of": price_summary.get("latest_bar_date"),
        "baseline": discovered["baseline"],
        "validation_baseline": discovered.get("validation_baseline", {}),
        "test_baseline": discovered.get("test_baseline", {}),
        "rules": discovered["rules"],
        "negative_rules": negative_rules,
        "chase_policy": build_chase_policy(priced_rows, discovered["baseline"]),
        "schema_version": EDGE_RULE_SCHEMA_VERSION,
        "money_flow_semantics_version": MONEY_FLOW_SEMANTICS_VERSION,
    }


def _write_json_atomic(path: Path, data: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
    tmp.replace(path)


def save_edge_rules(payload: Dict[str, Any], rule_dir: Path = EDGE_RULE_DIR) -> Tuple[Path, Path]:
    day = datetime.now().strftime("%Y%m%d")
    mode = payload.get("mode") or "weekly"
    dated = rule_dir / f"{mode}_edge_rules_{day}.json"
    latest = rule_dir / f"{mode}_edge_rules_latest.json"
    _write_json_atomic(dated, payload)
    _write_json_atomic(latest, payload)
    return dated, latest


def build_and_save_edge_rules(
    days: int = 20,
    mode: str = "both",
    month: Optional[str] = None,
    output_dir: Path = OUTPUT_DIR,
    min_samples: int = 25,
    min_days: int = 8,
) -> Dict[str, Any]:
    results: Dict[str, Any] = {}
    modes = ["weekly", "monthly"] if mode == "both" else [mode]
    for item in modes:
        payload = build_edge_rules(
            mode=item,
            days=days,
            month=month,
            output_dir=output_dir,
            min_samples=min_samples,
            min_days=min_days,
        )
        dated, latest = save_edge_rules(payload, output_dir / "edge_rules")
        payload["saved_paths"] = {"dated": str(dated), "latest": str(latest)}
        results[item] = payload
    return results


def load_latest_edge_rule_payloads(output_dir: Path = OUTPUT_DIR) -> Dict[str, Dict[str, Any]]:
    rule_dir = output_dir / "edge_rules"
    payloads: Dict[str, Dict[str, Any]] = {}
    for mode in ("weekly", "monthly"):
        path = rule_dir / f"{mode}_edge_rules_latest.json"
        if not path.exists():
            continue
        data = _read_json(path)
        data_as_of = str(data.get("data_as_of") or "")
        try:
            age_days = (date.today() - datetime.strptime(data_as_of, "%Y%m%d").date()).days
        except Exception:
            age_days = EDGE_RULE_MAX_AGE_DAYS + 1
        if (
            (data.get("rules") or data.get("negative_rules") or data.get("chase_policy"))
            and int(data.get("schema_version") or 0) == EDGE_RULE_SCHEMA_VERSION
            and data.get("money_flow_semantics_version") == MONEY_FLOW_SEMANTICS_VERSION
            and 0 <= age_days <= EDGE_RULE_MAX_AGE_DAYS
        ):
            payloads[mode] = data
    return payloads


def evaluate_candidate_edge(candidate: Dict[str, Any], payloads: Optional[Dict[str, Dict[str, Any]]] = None, output_dir: Path = OUTPUT_DIR) -> Dict[str, Any]:
    payloads = payloads if payloads is not None else load_latest_edge_rule_payloads(output_dir)
    matches: List[Dict[str, Any]] = []
    strongest_by_family_mode: Dict[Tuple[str, str], Dict[str, Any]] = {}
    weights = {"monthly": 0.7, "weekly": 0.3}
    for mode, payload in (payloads or {}).items():
        weight = weights.get(mode, 0.5)
        for rule in payload.get("rules") or []:
            if _rule_matches(candidate, rule):
                bonus = _safe_float(rule.get("edge_bonus"), 0.0) or 0.0
                family = _rule_evidence_family(rule)
                match = {
                    "mode": mode,
                    "evidence_family": family,
                    "id": rule.get("id"),
                    "description": rule.get("description"),
                    "edge_bonus": bonus,
                    "weighted_bonus": round(bonus * weight, 2),
                    "metrics": rule.get("metrics", {}),
                    "selected_for_score": False,
                }
                matches.append(match)
                key = (family, mode)
                previous = strongest_by_family_mode.get(key)
                if previous is None or match["weighted_bonus"] > previous["weighted_bonus"]:
                    strongest_by_family_mode[key] = match
    weighted_bonus = 0.0
    for selected in strongest_by_family_mode.values():
        selected["selected_for_score"] = True
        weighted_bonus += selected["weighted_bonus"]
    edge_score = round(max(0.0, min(8.0, weighted_bonus)), 2)
    matches.sort(key=lambda x: (not x.get("selected_for_score"), -float(x.get("weighted_bonus") or 0)))
    negative_matches: List[Dict[str, Any]] = []
    strongest_negative: Dict[Tuple[str, str], Dict[str, Any]] = {}
    for mode, payload in (payloads or {}).items():
        weight = weights.get(mode, 0.5)
        for rule in payload.get("negative_rules") or []:
            if not _rule_matches(candidate, rule):
                continue
            penalty = _safe_float(rule.get("score_penalty"), 0.0) or 0.0
            family = _rule_evidence_family(rule)
            match = {
                "mode": mode,
                "evidence_family": family,
                "id": rule.get("id"),
                "description": rule.get("description"),
                "score_penalty": penalty,
                "weighted_penalty": round(penalty * weight, 2),
                "metrics": rule.get("metrics", {}),
                "watch_only": bool(rule.get("watch_only")),
                "selected_for_score": False,
            }
            negative_matches.append(match)
            key = (family, mode)
            previous = strongest_negative.get(key)
            if previous is None or match["weighted_penalty"] > previous["weighted_penalty"]:
                strongest_negative[key] = match
    weighted_negative = 0.0
    for selected in strongest_negative.values():
        selected["selected_for_score"] = True
        weighted_negative += selected["weighted_penalty"]
    negative_score = round(max(0.0, min(10.0, weighted_negative)), 2)
    negative_matches.sort(key=lambda x: (not x.get("selected_for_score"), -float(x.get("weighted_penalty") or 0)))
    rsi = _safe_float(_feature(candidate, "rsi"), None)
    close_pos = _safe_float(_feature(candidate, "close_position_20d"), None)
    pool = str(_feature(candidate, "pool") or "")
    chase_penalty = 0.0
    penalty_reasons: List[str] = []
    if rsi is not None and rsi > 85:
        chase_penalty += 5.0
        penalty_reasons.append(f"RSI过热{rsi:.0f}")
    elif rsi is not None and rsi > 80:
        chase_penalty += 3.0
        penalty_reasons.append(f"RSI偏热{rsi:.0f}")
    if close_pos is not None and close_pos >= 98:
        chase_penalty += 4.0
        penalty_reasons.append(f"20日位置{close_pos:.0f}")
    if any(key in pool for key in ("首板", "涨停", "突破新高")) and close_pos is not None and close_pos >= 95:
        chase_penalty += 4.0
        penalty_reasons.append("涨停/新高追高风险")
    watch_only = any(
        item.get("watch_only") and item.get("selected_for_score")
        for item in negative_matches
    )
    if watch_only:
        penalty_reasons.append("历史弱组合要求盘中确认")
    return {
        "score": edge_score,
        "matches": matches[:8],
        "match_count": len(matches),
        "selected_family_count": len({x.get("evidence_family") for x in matches if x.get("selected_for_score")}),
        "score_cap": 8.0,
        "negative_score": negative_score,
        "negative_matches": negative_matches[:8],
        "negative_score_cap": 10.0,
        "chase_risk_penalty": round(min(12.0, chase_penalty), 2),
        "penalty_reasons": penalty_reasons,
        "watch_only": watch_only,
        "payload_modes": sorted((payloads or {}).keys()),
    }


def resolve_dynamic_chase_limit(
    payloads: Optional[Dict[str, Dict[str, Any]]] = None,
    output_dir: Path = OUTPUT_DIR,
) -> Dict[str, Any]:
    payloads = payloads if payloads is not None else load_latest_edge_rule_payloads(output_dir)
    policies = []
    for mode, payload in (payloads or {}).items():
        policy = payload.get("chase_policy") or {}
        if policy.get("status") == "active":
            policies.append({"mode": mode, **policy})
    if not policies:
        return {
            "status": "default",
            "high_chase_limit": 2,
            "reason": "无达到门槛的追高复盘样本，使用默认上限2只",
            "policies": [],
        }
    limit = min(max(0, min(2, int(p.get("high_chase_limit", 2)))) for p in policies)
    reasons = [f"{p.get('mode')}:{p.get('reason')}" for p in policies]
    return {
        "status": "active",
        "high_chase_limit": limit,
        "reason": "；".join(reasons)[:500],
        "policies": policies,
    }


def _shadow_new_score(
    row: Dict[str, Any],
    payloads: Optional[Dict[str, Dict[str, Any]]] = None,
) -> float:
    quant = _safe_float(row.get("quant_base_score"), None)
    if quant is None:
        quant = _safe_float(row.get("pre_edge_score"), _safe_float(row.get("buy_score"), 50.0)) or 50.0
    pm_score = _safe_float(row.get("pm_score") or row.get("llm_buy_score"), quant) or quant
    pm_conf = _safe_float(row.get("pm_confidence") or row.get("llm_confidence") or row.get("confidence"), 50.0) or 50.0
    pm_signal = str(row.get("pm_signal") or row.get("llm_signal") or row.get("signal") or "WATCH").upper()
    residual = pm_score - quant
    residual_adjust = residual * 0.18 * (0.65 + min(1.0, pm_conf / 100.0) * 0.35)
    if pm_signal == "BUY":
        residual_adjust = max(-6.0, min(4.0, residual_adjust))
    elif pm_signal == "WATCH":
        residual_adjust = max(-8.0, min(0.0, residual_adjust))
    elif pm_signal == "AVOID":
        residual_adjust = min(-10.0, residual_adjust)
    else:
        residual_adjust = -25.0
    knowledge = _safe_float(row.get("knowledge_rule_score_adjustment"), 0.0) or 0.0
    knowledge = max(-8.0, min(2.0, knowledge))
    edge = evaluate_candidate_edge(row, payloads=payloads or {})
    positive = _safe_float(edge.get("score"), 0.0) or 0.0
    negative = _safe_float(edge.get("negative_score"), 0.0) or 0.0
    chase = _safe_float(edge.get("chase_risk_penalty"), 0.0) or 0.0
    return round(max(0.0, min(95.0, quant + residual_adjust + knowledge + positive - negative - chase)), 2)


def _shadow_select(rows: List[Dict[str, Any]], score_key: str, high_chase_limit: int) -> List[Dict[str, Any]]:
    ranked = sorted(rows, key=lambda row: -float(row.get(score_key) or 0.0))
    selected: List[Dict[str, Any]] = []
    deferred_sector: List[Dict[str, Any]] = []
    chase_count = 0
    sectors: Dict[str, int] = {}
    seen = set()
    for row in ranked:
        stock = str(row.get("stock") or "")
        if not stock or stock in seen:
            continue
        seen.add(stock)
        is_chase = _is_high_chase_candidate(row)
        sector = str(row.get("sector") or "")
        if is_chase and chase_count >= high_chase_limit:
            continue
        if sector and sectors.get(sector, 0) >= 2:
            deferred_sector.append(row)
            continue
        selected.append(row)
        chase_count += int(is_chase)
        if sector:
            sectors[sector] = sectors.get(sector, 0) + 1
        if len(selected) >= 5:
            return selected
    for row in deferred_sector:
        if len(selected) >= 5:
            break
        is_chase = _is_high_chase_candidate(row)
        if is_chase and chase_count >= high_chase_limit:
            continue
        selected.append(row)
        chase_count += int(is_chase)
    return selected


def _shadow_metrics(selected: List[Dict[str, Any]], benchmark_by_day: Dict[str, Dict[int, float]]) -> Dict[str, Any]:
    result: Dict[str, Any] = {
        "selection_count": len(selected),
        "high_chase_count": sum(_is_high_chase_candidate(row) for row in selected),
    }
    result["high_chase_ratio_pct"] = round(result["high_chase_count"] / len(selected) * 100, 2) if selected else None
    for horizon in (1, 3, 5):
        values = [
            float(row[f"d{horizon}_return_pct"])
            for row in selected
            if row.get(f"d{horizon}_return_pct") is not None
        ]
        alphas = []
        for row in selected:
            value = _safe_float(row.get(f"d{horizon}_return_pct"), None)
            benchmark = (benchmark_by_day.get(str(row.get("report_date") or "")) or {}).get(horizon)
            if value is not None and benchmark is not None:
                alphas.append(value - benchmark)
        result[f"d{horizon}_sample_count"] = len(values)
        result[f"d{horizon}_avg_return_pct"] = round(sum(values) / len(values), 2) if values else None
        result[f"d{horizon}_win_rate_pct"] = round(sum(v > 0 for v in values) / len(values) * 100, 2) if values else None
        result[f"d{horizon}_avg_alpha_pct"] = round(sum(alphas) / len(alphas), 2) if alphas else None
    return result


def build_scoring_shadow_replay(
    output_dir: Path = OUTPUT_DIR,
    start_date: str = "20260601",
    fetcher=fetch_daily_ohlc,
) -> Dict[str, Any]:
    """Causal replay of old vs new scoring over all stored candidates."""
    paths = [path for path in list_report_files(output_dir) if _report_date_from_path(path) >= start_date]
    rows = _candidate_rows_from_reports(paths)
    priced_rows, price_summary = _augment_with_prices(
        rows,
        fetcher=fetcher,
        cache_dir=output_dir / "edge_rules",
    )
    benchmark_rows = fetcher("000300", 220) or []
    benchmark_by_day: Dict[str, Dict[int, float]] = {}
    for idx, bar in enumerate(benchmark_rows):
        day = _normalize_date_key(bar.get("date"))
        entry = _safe_float(bar.get("open") or bar.get("close"), None)
        if not day or not entry:
            continue
        benchmark_by_day[day] = {}
        for horizon in (1, 3, 5):
            if idx + horizon < len(benchmark_rows):
                close = _safe_float(benchmark_rows[idx + horizon].get("close"), None)
                if close:
                    benchmark_by_day[day][horizon] = (close / entry - 1) * 100
    by_day: Dict[str, List[Dict[str, Any]]] = {}
    for row in priced_rows:
        item = dict(row)
        item["shadow_old_score"] = _safe_float(item.get("buy_score"), 0.0) or 0.0
        by_day.setdefault(str(item.get("report_date") or ""), []).append(item)
    old_selected: List[Dict[str, Any]] = []
    new_selected: List[Dict[str, Any]] = []
    policy_log = []
    for day in sorted(by_day):
        try:
            cutoff = (datetime.strptime(day, "%Y%m%d").date() - timedelta(days=7)).strftime("%Y%m%d")
        except Exception:
            cutoff = day
        mature_prior = [row for row in priced_rows if str(row.get("report_date") or "") <= cutoff]
        baseline = _return_stats([
            float(row["d5_return_pct"]) for row in mature_prior if row.get("d5_return_pct") is not None
        ])
        if len(mature_prior) >= 20 and len({row.get("report_date") for row in mature_prior}) >= 5:
            discovered = discover_edge_rules(mature_prior, min_samples=25, min_days=8)
            negative_rules = discover_negative_rules(mature_prior, min_samples=20, min_days=5)
            replay_payloads = {
                "weekly": {
                    "rules": discovered.get("rules") or [],
                    "negative_rules": negative_rules,
                }
            }
        else:
            replay_payloads = {"weekly": {"rules": [], "negative_rules": []}}
        chase_policy = build_chase_policy(mature_prior, baseline)
        chase_limit = int(chase_policy.get("high_chase_limit", 2))
        for row in by_day[day]:
            row["shadow_new_score"] = _shadow_new_score(row, payloads=replay_payloads)
        policy_log.append({
            "report_date": day,
            "high_chase_limit": chase_limit,
            "status": chase_policy.get("status"),
            "positive_rule_count": len((replay_payloads.get("weekly") or {}).get("rules") or []),
            "negative_rule_count": len((replay_payloads.get("weekly") or {}).get("negative_rules") or []),
            "mature_prior_candidates": len(mature_prior),
        })
        old_selected.extend(_shadow_select(by_day[day], "shadow_old_score", 2))
        new_selected.extend(_shadow_select(by_day[day], "shadow_new_score", chase_limit))
    result = {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "start_date": start_date,
        "report_days": sorted(by_day),
        "candidate_count": len(priced_rows),
        "price_summary": price_summary,
        "old": _shadow_metrics(old_selected, benchmark_by_day),
        "new": _shadow_metrics(new_selected, benchmark_by_day),
        "policy_log": policy_log,
        "causal_guards": [
            "每个交易日追高上限只使用至少7个自然日前的成熟样本",
            "历史优势/弱势组合不回填到其生成日前，避免未来数据泄漏",
        ],
    }
    result["comparison"] = {
        key: (
            round(float(result["new"][key]) - float(result["old"][key]), 2)
            if result["new"].get(key) is not None and result["old"].get(key) is not None
            else None
        )
        for key in (
            "d1_avg_return_pct", "d1_win_rate_pct", "d1_avg_alpha_pct",
            "d3_avg_return_pct", "d3_win_rate_pct", "d3_avg_alpha_pct",
            "d5_avg_return_pct", "d5_win_rate_pct", "d5_avg_alpha_pct",
            "high_chase_ratio_pct",
        )
    }
    path = output_dir / "edge_rules" / "scoring_shadow_replay_latest.json"
    _write_json_atomic(path, result)
    result["saved_path"] = str(path)
    return result


def format_edge_rules_text(result: Dict[str, Any]) -> str:
    parts = ["📊 全候选池历史优势组合复盘"]
    for mode in ("weekly", "monthly"):
        payload = result.get(mode) or {}
        if not payload:
            continue
        baseline = payload.get("baseline") or {}
        price = payload.get("price_summary") or {}
        rules = payload.get("rules") or []
        negative_rules = payload.get("negative_rules") or []
        title = "近阶段" if mode == "weekly" else f"月度{payload.get('month') or ''}"
        parts.append(
            f"{title}: 样本{price.get('priced_rows', 0)}/{price.get('loaded_candidates', 0)}，"
            f"基准D5均{baseline.get('avg')}%，胜率{baseline.get('win_rate')}%"
        )
        for i, rule in enumerate(rules[:3], 1):
            m = rule.get("metrics") or {}
            parts.append(
                f"{i}. {rule.get('description')} -> D5均{m.get('d5_avg_return_pct')}%，"
                f"胜率{m.get('d5_win_rate_pct')}%，样本{m.get('sample_count')}，加分+{rule.get('edge_bonus')}"
            )
        if not rules:
            parts.append(f"{title}: 未发现满足样本数和收益约束的稳定优势组合")
        for i, rule in enumerate(negative_rules[:3], 1):
            m = rule.get("metrics") or {}
            parts.append(
                f"保护{i}. {rule.get('description')} -> D5收缩均{m.get('shrunk_avg_return_pct')}%，"
                f"收缩胜率{m.get('shrunk_win_rate_pct')}%，样本{m.get('sample_count')}，扣分-{rule.get('score_penalty')}"
            )
        chase = payload.get("chase_policy") or {}
        if chase:
            parts.append(f"追高策略: {chase.get('reason')}")
    return "\n".join(parts)


def main(argv: Optional[List[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="从全候选池生成历史优势组合规则")
    parser.add_argument("--days", type=int, default=20, help="weekly模式读取最近N个daily_report")
    parser.add_argument("--mode", choices=["weekly", "monthly", "both"], default="both")
    parser.add_argument("--month", help="monthly模式月份，如202606；默认上个自然月")
    parser.add_argument("--min-samples", type=int, default=25)
    parser.add_argument("--min-days", type=int, default=8)
    args = parser.parse_args(argv)
    _load_env()
    _configure_network()
    result = build_and_save_edge_rules(
        days=args.days,
        mode=args.mode,
        month=args.month,
        min_samples=args.min_samples,
        min_days=args.min_days,
    )
    print(format_edge_rules_text(result))
    for mode, payload in result.items():
        print(f"saved_{mode}: {payload.get('saved_paths', {}).get('latest')}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
