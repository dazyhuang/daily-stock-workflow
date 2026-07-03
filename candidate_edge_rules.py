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
        additions = ["127.0.0.1", "localhost", "127.0.0.1"]
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
        if payload.get("success") is False:
            return []
        return _parse_xq_market_data3(payload)
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
        ranked = ((report.get("phase2") or {}).get("ranked_candidates") or [])
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
        return _safe_float(mf.get("main_net_flow"), 0.0)
    if name == "super_net_flow":
        return _safe_float(mf.get("super_net_flow"), 0.0)
    if name == "ddx_5":
        return _safe_float(mf.get("ddx_5"), 0.0)
    if name == "ddy_10":
        return _safe_float(mf.get("ddy_10"), 0.0)
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


def _condition_text(cond: Dict[str, Any]) -> str:
    field = str(cond.get("field") or "")
    op = cond.get("op")
    value = cond.get("value")
    labels = {
        "signal": "信号", "pool": "池", "pool_not": "池非", "score": "做多分",
        "rank": "候选排名", "main_net_flow": "主力净流", "super_net_flow": "超大单净流",
        "ddx_5": "DDX5", "ddy_10": "DDY10", "rsi": "RSI", "close_position_20d": "20日位置",
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
    return [
        {"field": "signal", "op": "eq", "value": "WATCH"},
        {"field": "signal", "op": "eq", "value": "BUY"},
        {"field": "rank", "op": "lte", "value": 20},
        {"field": "score", "op": "gte", "value": 55},
        {"field": "score", "op": "gte", "value": 60},
        {"field": "pool", "op": "contains", "value": "低吸"},
        {"field": "pool", "op": "contains", "value": "强势"},
        {"field": "pool_not", "op": "neq_contains", "value": "突破新高"},
        {"field": "main_net_flow", "op": "gt", "value": 0},
        {"field": "main_net_flow", "op": "gt", "value": 5},
        {"field": "super_net_flow", "op": "gt", "value": 0},
        {"field": "super_net_flow", "op": "gt", "value": 5},
        {"field": "ddx_5", "op": "gt", "value": 2},
        {"field": "ddy_10", "op": "gt", "value": 1},
        {"field": "rsi", "op": "lte", "value": 80},
        {"field": "close_position_20d", "op": "lte", "value": 95},
        {"field": "ma_system", "op": "contains", "value": "多头"},
        {"field": "vol_signal", "op": "contains", "value": "放量"},
        {"field": "money_flow_score", "op": "gte", "value": 60},
        {"field": "tech_score", "op": "gte", "value": 60},
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


def _augment_with_prices(rows: List[Dict[str, Any]], fetcher=fetch_daily_ohlc, pause_sec: float = 0.01) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    cache_path = EDGE_RULE_DIR / "daily_ohlc_cache.json"
    cache: Dict[str, List[Dict[str, Any]]] = _load_price_cache(cache_path)
    priced: List[Dict[str, Any]] = []
    missing_price = 0
    incomplete_d5 = 0
    fetched = 0
    cache_hits = 0
    consecutive_empty = 0
    aborted_fetch = False
    for row in rows:
        stock = row.get("stock")
        report_day = row.get("report_date")
        if not stock:
            missing_price += 1
            continue
        if stock in cache:
            cache_hits += 1
        elif not aborted_fetch:
            bars = fetcher(stock, 160) or []
            cache[stock] = bars
            fetched += 1
            if bars:
                consecutive_empty = 0
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
        out = dict(row)
        out["entry_price"] = entry
        out["price_source"] = bars[idx].get("source") or "xqshare_cache" if stock in cache else "xqshare"
        for h in HORIZONS:
            if idx + h < len(bars):
                close = _safe_float(bars[idx + h].get("close"), None)
                if close and close > 0:
                    out[f"d{h}_return_pct"] = round((close / entry - 1) * 100, 2)
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
        "aborted_fetch": aborted_fetch,
        "max_consecutive_empty": MAX_CONSECUTIVE_EMPTY,
    }


def _build_rule(conds: List[Dict[str, Any]], matched: List[Dict[str, Any]], baseline: Dict[str, Any], index: int) -> Optional[Dict[str, Any]]:
    d5 = [_safe_float(r.get("d5_return_pct"), None) for r in matched]
    values = [v for v in d5 if v is not None]
    days = len({r.get("report_date") for r in matched})
    stats = _return_stats(values)
    if stats["n"] <= 0:
        return None
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
    drawdown_penalty = max(0.0, abs(min_ret) - 20.0) * 0.15
    sample_penalty = max(0.0, 35.0 - stats["n"]) * 0.05
    strength = avg_lift + win_lift * 0.12 + median_lift * 0.25 - drawdown_penalty - sample_penalty
    bonus = max(3.0, min(10.0, round(strength * 0.7, 1)))
    desc = " & ".join(_condition_text(c) for c in conds)
    return {
        "id": f"edge_{index:03d}",
        "description": desc,
        "conditions": conds,
        "metrics": {
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
        },
        "strength_score": round(strength, 2),
        "edge_bonus": bonus,
    }


def discover_edge_rules(
    priced_rows: List[Dict[str, Any]],
    min_samples: int = 25,
    min_days: int = 8,
    max_rules: int = 10,
) -> Dict[str, Any]:
    d5_values = [_safe_float(r.get("d5_return_pct"), None) for r in priced_rows]
    d5_values = [v for v in d5_values if v is not None]
    baseline = _return_stats(d5_values)
    rules: List[Dict[str, Any]] = []
    idx = 1
    for conds in _condition_combos(_predicate_catalog(), max_size=3):
        matched = [row for row in priced_rows if all(_condition_matches(row, c) for c in conds)]
        if len(matched) < min_samples:
            continue
        if len({m.get("report_date") for m in matched}) < min_days:
            continue
        rule = _build_rule(conds, matched, baseline, idx)
        if not rule:
            continue
        metrics = rule["metrics"]
        if (metrics["avg_lift_pct"] < 5.0 and metrics["win_lift_pct"] < 20.0) or metrics["d5_median_return_pct"] <= 0:
            continue
        rules.append(rule)
        idx += 1
    rules.sort(key=lambda r: (r.get("strength_score", 0), r.get("metrics", {}).get("sample_count", 0)), reverse=True)
    unique: List[Dict[str, Any]] = []
    seen_descriptions = set()
    for rule in rules:
        desc = rule.get("description")
        if desc in seen_descriptions:
            continue
        seen_descriptions.add(desc)
        unique.append(rule)
        if len(unique) >= max_rules:
            break
    for i, rule in enumerate(unique, 1):
        rule["id"] = f"edge_{i:03d}"
    return {"baseline": baseline, "rules": unique}


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
    priced_rows, price_summary = _augment_with_prices(rows, fetcher=fetcher)
    discovered = discover_edge_rules(priced_rows, min_samples=min_samples, min_days=min_days)
    return {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "mode": mode,
        "month": month if mode == "monthly" else None,
        "source_report_days": sorted({_report_date_from_path(p) for p in report_files}),
        "candidate_count": len(rows),
        "price_summary": price_summary,
        "baseline": discovered["baseline"],
        "rules": discovered["rules"],
        "schema_version": 1,
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
    if payload.get("rules") or not latest.exists():
        _write_json_atomic(latest, payload)
    else:
        payload.setdefault("save_warnings", []).append("empty_rules_latest_preserved")
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
        if data.get("rules"):
            payloads[mode] = data
    return payloads


def evaluate_candidate_edge(candidate: Dict[str, Any], payloads: Optional[Dict[str, Dict[str, Any]]] = None, output_dir: Path = OUTPUT_DIR) -> Dict[str, Any]:
    payloads = payloads if payloads is not None else load_latest_edge_rule_payloads(output_dir)
    matches: List[Dict[str, Any]] = []
    weighted_bonus = 0.0
    weights = {"monthly": 0.7, "weekly": 0.3}
    for mode, payload in (payloads or {}).items():
        weight = weights.get(mode, 0.5)
        for rule in payload.get("rules") or []:
            if _rule_matches(candidate, rule):
                bonus = _safe_float(rule.get("edge_bonus"), 0.0) or 0.0
                weighted_bonus += bonus * weight
                matches.append({
                    "mode": mode,
                    "id": rule.get("id"),
                    "description": rule.get("description"),
                    "edge_bonus": bonus,
                    "weighted_bonus": round(bonus * weight, 2),
                    "metrics": rule.get("metrics", {}),
                })
    edge_score = round(min(15.0, weighted_bonus), 2)
    rsi = _safe_float(_feature(candidate, "rsi"), None)
    close_pos = _safe_float(_feature(candidate, "close_position_20d"), None)
    pool = str(_feature(candidate, "pool") or "")
    reason = str(candidate.get("reason") or candidate.get("final_decision") or "")
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
    wait_terms = ("等待", "观望", "回调", "确认", "不追", "谨慎")
    watch_only = any(term in reason for term in wait_terms)
    if watch_only:
        chase_penalty += 3.0
        penalty_reasons.append("LLM理由偏等待")
    return {
        "score": edge_score,
        "matches": matches[:5],
        "match_count": len(matches),
        "chase_risk_penalty": round(min(12.0, chase_penalty), 2),
        "penalty_reasons": penalty_reasons,
        "watch_only": watch_only,
        "payload_modes": sorted((payloads or {}).keys()),
    }


def format_edge_rules_text(result: Dict[str, Any]) -> str:
    parts = ["📊 全候选池历史优势组合复盘"]
    for mode in ("weekly", "monthly"):
        payload = result.get(mode) or {}
        if not payload:
            continue
        baseline = payload.get("baseline") or {}
        price = payload.get("price_summary") or {}
        rules = payload.get("rules") or []
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
