#!/usr/bin/env python3
"""Review and attribute the last N trading days of daily Top5 picks.

This is intentionally independent from the weekly review.  Weekly review is
about actual trades; this module reviews every selected Top5 opportunity,
including stocks that were never filled by the intraday-buy task.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import signal
import statistics
import subprocess
import sys
import time
import urllib.parse
import urllib.request
import uuid
from collections import Counter, defaultdict
from datetime import date, datetime
from pathlib import Path
from typing import Any, Callable, Dict, Iterable, List, Optional, Tuple

try:
    import requests
except Exception:  # pragma: no cover - urllib fallback still works for tests.
    requests = None

try:
    from pydantic import BaseModel, Field
except Exception:  # pragma: no cover - pydantic is available in normal runtime.
    BaseModel = None
    Field = None


BASE_DIR = Path(__file__).resolve().parent
OUTPUT_DIR = BASE_DIR / "output"
BENCHMARK_CODE = os.getenv("TOP5_REVIEW_BENCHMARK", "000300")
REVIEW_HORIZONS = (1, 3, 5, 10)


def load_local_env(path: Path = BASE_DIR / ".env") -> None:
    """Load project env for cron jobs that do not inherit the shell session."""
    if not path.exists():
        return
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip().strip('"').strip("'")
        if key and key not in os.environ:
            os.environ[key] = value


def configure_network_for_review() -> None:
    """Make local/China data sources bypass proxy in cron-like environments."""
    try:
        from domestic_network import configure_domestic_direct_network
        configure_domestic_direct_network()
    except Exception:
        bridge_url = os.getenv("XQSHARE_HTTP_BASE", "http://127.0.0.1:8080")
        bridge_host = urllib.parse.urlparse(bridge_url).hostname
        additions = [item for item in (bridge_host, "localhost", "127.0.0.1") if item]
        for key in ("NO_PROXY", "no_proxy"):
            existing = [p.strip() for p in os.environ.get(key, "").split(",") if p.strip()]
            seen = {p.lower() for p in existing}
            for item in additions:
                if item.lower() not in seen:
                    existing.append(item)
                    seen.add(item.lower())
            os.environ[key] = ",".join(existing)


load_local_env()
configure_network_for_review()

XQ_HTTP_BASE = os.getenv("XQSHARE_HTTP_BASE", "http://127.0.0.1:8080").rstrip("/")
DEFAULT_LLM_MODEL = os.getenv("TOP5_REVIEW_LLM_MODEL", "openai/gpt-5.6-sol")
DEFAULT_LLM_FALLBACK_MODEL = os.getenv("TOP5_REVIEW_LLM_FALLBACK_MODEL", "minimax-portal/MiniMax-M3")
DEFAULT_LLM_REASONING_EFFORT = os.getenv("TOP5_REVIEW_LLM_REASONING_EFFORT", "max")
DEFAULT_LLM_TIMEOUT = int(os.getenv("TOP5_REVIEW_LLM_TIMEOUT", "180"))
DEFAULT_LLM_THINKING_BUDGET = int(os.getenv("TOP5_REVIEW_LLM_THINKING_BUDGET", "16000"))
DEFAULT_LLM_MAX_TOKENS = int(os.getenv("TOP5_REVIEW_LLM_MAX_TOKENS", "8192"))
DEFAULT_LLM_RETRIES = int(os.getenv("TOP5_REVIEW_LLM_RETRIES", "3"))
ENABLE_OPENCLAW_LOCAL_FALLBACK = os.getenv("TOP5_REVIEW_ENABLE_OPENCLAW_LOCAL_FALLBACK", "").strip().lower() in {"1", "true", "yes", "on"}
MONEY_FLOW_KEYS = ("main_net_flow", "super_net_flow", "ddx_5", "ddy_10")
MIN_LLM_MATURE_D5_SAMPLES = int(os.getenv("TOP5_REVIEW_MIN_LLM_MATURE_D5_SAMPLES", "10"))
_DAILY_FETCH_CACHE: Dict[str, List[Dict[str, Any]]] = {}
_LOCAL_DAILY_CACHE: Optional[Dict[str, List[Dict[str, Any]]]] = None
_KLINE_SOURCE_FAILURES: Counter = Counter()
_KLINE_SOURCE_DISABLED: set[str] = set()


if BaseModel is not None:
    class Top5ReviewDeepAnalysis(BaseModel):
        overall_conclusion: str = Field(description="最近10个交易日Top5表现的一句话总判断")
        root_causes: List[str] = Field(description="表现不好或波动的核心原因，按重要性排序，3-5条")
        selection_diagnosis: str = Field(description="选股池/Top5排序质量诊断")
        execution_diagnosis: str = Field(description="盘中买入执行、触发、报价与未成交问题诊断")
        scoring_diagnosis: str = Field(description="量化分、LLM修正、置信值、资金流等评分机制诊断")
        market_context_diagnosis: str = Field(description="市场环境、板块轮动和风格适配诊断")
        priority_fixes: List[str] = Field(description="下周优先修复或观察的改进动作，3-5条")
        watchlist_notes: List[str] = Field(description="值得复查的具体股票/日期线索，最多5条")
        risk_warnings: List[str] = Field(description="不要过度优化或需要防范的风险，2-4条")
        confidence: int = Field(ge=0, le=100, description="本次复盘解读置信度")
else:
    Top5ReviewDeepAnalysis = None


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


def _pct_change(start: Optional[float], end: Optional[float]) -> Optional[float]:
    start_f = _safe_float(start)
    end_f = _safe_float(end)
    if not start_f or start_f <= 0 or end_f is None:
        return None
    return round((end_f - start_f) / start_f * 100.0, 2)


def _avg(values: Iterable[Optional[float]]) -> Optional[float]:
    vals = [float(v) for v in values if v is not None]
    if not vals:
        return None
    return round(sum(vals) / len(vals), 2)


def _win_rate(values: Iterable[Optional[float]]) -> Optional[float]:
    vals = [float(v) for v in values if v is not None]
    if not vals:
        return None
    return round(sum(1 for v in vals if v > 0) / len(vals) * 100.0, 1)


def _normalize_code(code: Any) -> str:
    text = re.sub(r"\D", "", str(code or ""))
    return text[-6:].zfill(6) if text else ""


def _to_xt_code(code: str) -> str:
    code = _normalize_code(code)
    sh_indices = {"000001", "000016", "000300", "000688", "000905", "000852"}
    sz_indices = {"399001", "399006", "399005", "399300"}
    if code in sh_indices:
        return f"{code}.SH"
    if code in sz_indices:
        return f"{code}.SZ"
    if code.startswith(("6", "5", "9")):
        return f"{code}.SH"
    return f"{code}.SZ"


def _normalize_date_key(value: Any) -> str:
    text = str(value or "").strip()
    digits = re.sub(r"\D", "", text)
    if len(digits) >= 8:
        return digits[:8]
    return ""


def _iso_date(date_key: str) -> str:
    date_key = _normalize_date_key(date_key)
    if len(date_key) == 8:
        return f"{date_key[:4]}-{date_key[4:6]}-{date_key[6:8]}"
    return ""


def _report_date_from_path(path: Path) -> str:
    match = re.search(r"daily_report_(\d{8})\.json$", path.name)
    return match.group(1) if match else ""


def _read_json(path: Path, default: Any = None) -> Any:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return default


def _read_jsonl(path: Path) -> List[Dict[str, Any]]:
    if not path.exists():
        return []
    rows: List[Dict[str, Any]] = []
    try:
        lines = path.read_text(encoding="utf-8").splitlines()
    except Exception:
        return []
    for line in lines:
        try:
            item = json.loads(line)
        except Exception:
            continue
        if isinstance(item, dict):
            rows.append(item)
    return rows


def _write_json_atomic(path: Path, data: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    tmp.write_text(json.dumps(data, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
    os.replace(tmp, path)


def _env_flag(name: str, default: bool = True) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "on"}


def _money_flow_has_any_value(money_flow: Dict[str, Any]) -> bool:
    if not isinstance(money_flow, dict):
        return False
    return any(money_flow.get(key) is not None for key in MONEY_FLOW_KEYS)


def _money_flow_missing_keys(money_flow: Dict[str, Any]) -> List[str]:
    flow = money_flow if isinstance(money_flow, dict) else {}
    return [key for key in MONEY_FLOW_KEYS if flow.get(key) is None]


def _money_flow_is_complete(money_flow: Dict[str, Any]) -> bool:
    return not _money_flow_missing_keys(money_flow)


def _money_flow_backfill_cache_path(output_dir: Path, report_day: str) -> Path:
    return output_dir / "data_cache" / f"top5_review_money_flow_{report_day}.json"


def _fetch_money_flow_backfill_via_mx(stock: str) -> Dict[str, Any]:
    if not (os.getenv("MX_APIKEY") or os.getenv("MINIMAX_API_KEY")):
        return {}
    try:
        from stock_selection_debate.data_fetcher import _fetch_money_flow_via_mx
        money_flow = _fetch_money_flow_via_mx(stock) or {}
    except Exception:
        return {}
    if not isinstance(money_flow, dict) or not _money_flow_has_any_value(money_flow):
        return {}
    out = {key: money_flow.get(key) for key in MONEY_FLOW_KEYS}
    out["source"] = str(money_flow.get("source") or "mx-data/latest_backfill")
    return out


def _backfill_missing_money_flow(
    picks: List[Dict[str, Any]],
    output_dir: Path,
    report_day: str,
) -> List[Dict[str, Any]]:
    if not _env_flag("TOP5_REVIEW_MONEY_FLOW_BACKFILL", True):
        return picks

    cache_path = _money_flow_backfill_cache_path(output_dir, report_day)
    cache = _read_json(cache_path, {}) or {}
    if not isinstance(cache, dict):
        cache = {}
    changed = False
    pause_sec = _safe_float(os.getenv("TOP5_REVIEW_MX_MONEY_FLOW_PAUSE_SEC"), 6.0) or 0.0
    empty_retry_hours = _safe_float(os.getenv("TOP5_REVIEW_MX_EMPTY_RETRY_HOURS"), 12.0) or 12.0

    def empty_cache_still_fresh(cached: Dict[str, Any]) -> bool:
        try:
            fetched_at = datetime.fromisoformat(str(cached.get("fetched_at") or ""))
            age_hours = (datetime.now() - fetched_at).total_seconds() / 3600.0
            return age_hours < empty_retry_hours
        except Exception:
            return False

    for pick in picks:
        money_flow = pick.get("money_flow") if isinstance(pick.get("money_flow"), dict) else {}
        missing_keys = _money_flow_missing_keys(money_flow)
        pick["money_flow_original_missing"] = len(missing_keys) == len(MONEY_FLOW_KEYS)
        pick["money_flow_original_missing_keys"] = missing_keys
        pick.setdefault("money_flow_backfilled", False)
        pick.setdefault("money_flow_backfill_source", "")
        pick.setdefault("money_flow_backfill_status", "")
        # Historical review must never replace report-time values with today's
        # money flow.  Backfill is diagnostic only and remains separate.
        if not missing_keys:
            continue

        stock = pick.get("stock")
        cached = cache.get(stock) if isinstance(cache.get(stock), dict) else {}
        cached_flow = cached.get("money_flow") if isinstance(cached.get("money_flow"), dict) else {}
        if _money_flow_has_any_value(cached_flow):
            pick["money_flow_backfilled"] = True
            pick["money_flow_backfill_source"] = str(cached.get("source") or cached_flow.get("source") or "mx-data/latest_backfill_cache")
            pick["money_flow_backfill_status"] = "cache_hit"
            pick["money_flow_backfill_snapshot"] = dict(cached_flow)
            continue
        if cached.get("status") == "empty" and empty_cache_still_fresh(cached):
            pick["money_flow_backfill_status"] = "cache_empty"
            continue

        fetched = _fetch_money_flow_backfill_via_mx(stock)
        if _money_flow_has_any_value(fetched):
            pick["money_flow_backfilled"] = True
            pick["money_flow_backfill_source"] = str(fetched.get("source") or "mx-data/latest_backfill")
            pick["money_flow_backfill_status"] = "fetched"
            pick["money_flow_backfill_snapshot"] = fetched
            cache[stock] = {
                "status": "ok",
                "source": pick["money_flow_backfill_source"],
                "money_flow": fetched,
                "fetched_at": datetime.now().isoformat(),
                "note": "Top5复盘回补；若原早报缺字段，此值不等同于原始早报当时值",
            }
        else:
            pick["money_flow_backfill_status"] = "empty"
            cache[stock] = {
                "status": "empty",
                "money_flow": {},
                "fetched_at": datetime.now().isoformat(),
            }
        changed = True
        if pause_sec > 0:
            time.sleep(pause_sec)

    if changed:
        _write_json_atomic(cache_path, cache)
    return picks


def list_recent_report_files(output_dir: Path = OUTPUT_DIR, days: int = 10) -> List[Path]:
    files = []
    for path in output_dir.glob("daily_report_*.json"):
        if "daily_report_push_" in path.name:
            continue
        if _report_date_from_path(path):
            files.append(path)
    return sorted(files, key=_report_date_from_path, reverse=True)[: max(1, days)]


def _xq_http_get(endpoint: str, params: Dict[str, Any], timeout: int = 10) -> Dict[str, Any]:
    url = f"{XQ_HTTP_BASE}{endpoint}?{urllib.parse.urlencode(params)}"
    if requests is not None:
        resp = requests.get(url, timeout=timeout)
        resp.raise_for_status()
        return resp.json() or {}
    req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return json.loads(resp.read().decode("utf-8")) or {}


def _parse_xq_market_data3(payload: Dict[str, Any]) -> List[Dict[str, Any]]:
    data = payload.get("data") if isinstance(payload, dict) else {}
    if not isinstance(data, dict):
        return []
    field_maps = {
        name: data.get(name, {})
        for name in ("open", "high", "low", "close", "volume")
        if isinstance(data.get(name, {}), dict)
    }
    all_dates = sorted({_normalize_date_key(day) for fmap in field_maps.values() for day in fmap.keys()})
    rows = []
    for day in all_dates:
        if not day:
            continue

        def val(name: str) -> Optional[float]:
            fmap = field_maps.get(name, {})
            raw = fmap.get(day) or fmap.get(_iso_date(day)) or {}
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


def fetch_daily_ohlc(stock_code: str, count: int = 80) -> List[Dict[str, Any]]:
    """Fetch daily OHLC through a bounded multi-source route."""
    stock = _normalize_code(stock_code)
    if stock in _DAILY_FETCH_CACHE:
        return _DAILY_FETCH_CACHE[stock]

    candidates: List[List[Dict[str, Any]]] = []

    def keep(source: str, loader: Callable[[], Any], *, circuit_break: bool = True) -> None:
        if source in _KLINE_SOURCE_DISABLED:
            return
        try:
            rows = loader() or []
        except Exception:
            rows = []
        normalized = []
        for row in rows if isinstance(rows, list) else []:
            if not isinstance(row, dict) or _safe_float(row.get("close")) is None:
                continue
            item = dict(row)
            item["date"] = _normalize_date_key(item.get("date"))
            item["source"] = str(item.get("source") or source)
            normalized.append(item)
        if normalized:
            _KLINE_SOURCE_FAILURES[source] = 0
            candidates.append(sorted(normalized, key=lambda x: x.get("date") or "")[-count:])
        else:
            _KLINE_SOURCE_FAILURES[source] += 1
            if circuit_break and _KLINE_SOURCE_FAILURES[source] >= 3:
                _KLINE_SOURCE_DISABLED.add(source)

    def load_xq() -> List[Dict[str, Any]]:
        payload = _xq_http_get(
            "/market_data3",
            {"stock": _to_xt_code(stock), "period": "1d", "count": count},
            timeout=12,
        )
        return [] if payload.get("success") is False else _parse_xq_market_data3(payload)

    keep("xqshare_http", load_xq)
    latest_reports = list_recent_report_files(OUTPUT_DIR, 1)
    expected_latest = _report_date_from_path(latest_reports[0]) if latest_reports else ""
    if candidates and (not expected_latest or _normalize_date_key(candidates[-1][-1].get("date")) >= expected_latest):
        _DAILY_FETCH_CACHE[stock] = candidates[-1]
        return candidates[-1]

    cache_path = OUTPUT_DIR / "edge_rules" / "daily_ohlc_cache.json"
    global _LOCAL_DAILY_CACHE
    if _LOCAL_DAILY_CACHE is None:
        try:
            loaded = (_read_json(cache_path, {}) or {}).get("daily_ohlc", {})
            _LOCAL_DAILY_CACHE = loaded if isinstance(loaded, dict) else {}
        except Exception:
            _LOCAL_DAILY_CACHE = {}
    cached = _LOCAL_DAILY_CACHE.get(stock, [])
    keep("local_daily_cache", lambda: cached, circuit_break=False)

    try:
        from stock_selection_debate.data_fetcher import (
            _fetch_kline_via_http,
            get_kline_via_akshare,
            get_kline_via_mx_data,
            get_kline_via_tencent,
        )
        keep("qmt_http", lambda: _fetch_kline_via_http(stock, days=count))
        keep("tencent", lambda: get_kline_via_tencent(stock, days=count))
        keep("akshare", lambda: get_kline_via_akshare(stock, days=count))
        if _env_flag("TOP5_REVIEW_ENABLE_MX_KLINE", False):
            keep("mx-data", lambda: get_kline_via_mx_data(stock, days=count))
    except Exception:
        pass

    best = max(
        candidates,
        key=lambda rows: (_normalize_date_key(rows[-1].get("date")) if rows else "", len(rows)),
        default=[],
    )
    _DAILY_FETCH_CACHE[stock] = best
    return best


def fetch_minute_reference_price(stock_code: str, report_day: str) -> Optional[float]:
    """Best-effort 09:31 reference from XQShare minute bars.

    Historical minute data is not guaranteed to be retained by XQShare.  The
    caller falls back to daily open when this returns None.
    """
    try:
        payload = _xq_http_get(
            "/market_data3",
            {"stock": _to_xt_code(stock_code), "period": "1m", "count": 300},
            timeout=8,
        )
        target = _normalize_date_key(report_day)
        data = payload.get("data") if isinstance(payload, dict) else {}
        close_map = data.get("close", {}) if isinstance(data, dict) else {}
        if not isinstance(close_map, dict):
            return None
        for raw_time in sorted(close_map.keys()):
            digits = re.sub(r"\D", "", str(raw_time or ""))
            if not digits.startswith(target) or len(digits) < 12:
                continue
            hhmm = digits[8:12]
            if hhmm >= "0931":
                raw = close_map.get(raw_time)
                if isinstance(raw, dict) and raw:
                    return _safe_float(next(iter(raw.values())))
                return _safe_float(raw)
    except Exception:
        return None
    return None


PriceFetcher = Callable[[str, int], List[Dict[str, Any]]]
MinuteFetcher = Callable[[str, str], Optional[float]]


def _row_by_date(rows: List[Dict[str, Any]], report_day: str) -> Tuple[Optional[int], Optional[Dict[str, Any]]]:
    target = _normalize_date_key(report_day)
    for idx, row in enumerate(rows):
        if _normalize_date_key(row.get("date")) == target:
            return idx, row
    return None, None


def _future_row(rows: List[Dict[str, Any]], start_idx: Optional[int], horizon: int) -> Optional[Dict[str, Any]]:
    if start_idx is None:
        return None
    idx = start_idx + horizon
    if idx < len(rows):
        return rows[idx]
    return None


def calculate_price_performance(
    stock_code: str,
    report_day: str,
    *,
    daily_fetcher: PriceFetcher = fetch_daily_ohlc,
    minute_fetcher: MinuteFetcher = fetch_minute_reference_price,
) -> Dict[str, Any]:
    rows = daily_fetcher(stock_code, 80) or []
    rows = sorted(rows, key=lambda r: _normalize_date_key(r.get("date")))
    idx, day_row = _row_by_date(rows, report_day)
    quality = []
    if not rows:
        quality.append("DAILY_OHLC_MISSING")
    if day_row is None:
        quality.append("REPORT_DAY_OHLC_MISSING")

    reference_price = minute_fetcher(stock_code, report_day)
    ref_source = "minute_0931"
    if not reference_price:
        reference_price = _safe_float((day_row or {}).get("open"))
        ref_source = "daily_open" if reference_price is not None else "unavailable"
        quality.append("MINUTE_0931_MISSING")

    open_price = _safe_float((day_row or {}).get("open"))
    high_price = _safe_float((day_row or {}).get("high"))
    low_price = _safe_float((day_row or {}).get("low"))
    close_price = _safe_float((day_row or {}).get("close"))

    returns: Dict[str, Any] = {}
    return_dates: Dict[str, Any] = {}
    return_complete: Dict[str, bool] = {}
    for horizon in REVIEW_HORIZONS:
        frow = _future_row(rows, idx, horizon)
        key = f"d{horizon}"
        returns[key] = _pct_change(reference_price, (frow or {}).get("close"))
        return_dates[key] = _iso_date(_normalize_date_key((frow or {}).get("date")))
        return_complete[key] = bool(idx is not None and idx + horizon < len(rows))

    return {
        "price_data_quality": "ok" if not quality else ",".join(sorted(set(quality))),
        "ohlc": {
            "date": _iso_date(_normalize_date_key((day_row or {}).get("date"))),
            "open": open_price,
            "high": high_price,
            "low": low_price,
            "close": close_price,
        },
        "reference_price": reference_price,
        "reference_source": ref_source,
        "intraday_high_return_pct": _pct_change(reference_price, high_price),
        "intraday_drawdown_pct": _pct_change(reference_price, low_price),
        "same_day_close_return_pct": _pct_change(reference_price, close_price),
        "future_returns_pct": returns,
        "future_return_dates": return_dates,
        "future_return_complete": return_complete,
    }


def embedded_backtest_performance(pick: Dict[str, Any]) -> Dict[str, Any]:
    """Fallback when live OHLC is unavailable in a sandboxed automation run."""
    selection = pick.get("selection_backtest") if isinstance(pick.get("selection_backtest"), dict) else {}
    strategy = pick.get("strategy_backtest") if isinstance(pick.get("strategy_backtest"), dict) else {}
    selection_ret = _safe_float(selection.get("return_pct"))
    strategy_ret = _safe_float(strategy.get("return_pct"))
    reference = _safe_float(selection.get("entry") or strategy.get("entry_price"))
    exit_price = _safe_float(selection.get("exit") or strategy.get("exit_price"))
    usable_ret = selection_ret if selection_ret is not None else strategy_ret
    quality = []
    if selection_ret is None:
        quality.append("SELECTION_BACKTEST_MISSING")
    if strategy_ret is None:
        quality.append("STRATEGY_BACKTEST_MISSING")
    if usable_ret is None:
        quality.append("EMBEDDED_BACKTEST_MISSING")
    return {
        "embedded_selection_return_pct": selection_ret,
        "embedded_strategy_return_pct": strategy_ret,
        "embedded_strategy_status": strategy.get("status") or "",
        "embedded_entry_date": strategy.get("entry_date") or "",
        "embedded_exit_date": strategy.get("exit_date") or "",
        "embedded_entry_price": _safe_float(strategy.get("entry_price") or selection.get("entry")),
        "embedded_exit_price": _safe_float(strategy.get("exit_price") or selection.get("exit")),
        "embedded_return_pct": usable_ret,
        "embedded_reference_price": reference,
        "embedded_exit_price_effective": exit_price,
        "embedded_data_quality": "ok" if not quality else ",".join(quality),
    }


def _apply_embedded_price_fallback(price_perf: Dict[str, Any], pick: Dict[str, Any]) -> Dict[str, Any]:
    embedded = embedded_backtest_performance(pick)
    if price_perf.get("reference_price") is not None:
        price_perf.update(embedded)
        return price_perf
    fallback_ret = embedded.get("embedded_return_pct")
    if fallback_ret is None:
        price_perf.update(embedded)
        return price_perf
    returns = dict(price_perf.get("future_returns_pct") or {})
    complete = dict(price_perf.get("future_return_complete") or {})
    dates = dict(price_perf.get("future_return_dates") or {})
    # Strategy simulation is historical context, never realized post-selection
    # performance.  Keep it separate and leave every D+N return unavailable.
    price_perf.update({
        "price_data_quality": f"{price_perf.get('price_data_quality')},EMBEDDED_BACKTEST_CONTEXT_ONLY",
        "future_returns_pct": returns,
        "future_return_complete": complete,
        "future_return_dates": dates,
    })
    price_perf.update(embedded)
    return price_perf


def _extract_top_picks(report: Dict[str, Any]) -> List[Dict[str, Any]]:
    phase2 = report.get("phase2") if isinstance(report, dict) else {}
    picks = (phase2 or {}).get("top_picks") or []
    result = []
    for idx, pick in enumerate(picks[:5], 1):
        if not isinstance(pick, dict):
            continue
        stock = _normalize_code(pick.get("stock") or pick.get("stock_code"))
        if not stock:
            continue
        copied = {
            "rank": idx,
            "stock": stock,
            "name": pick.get("name") or pick.get("stock_name") or stock,
            "signal": pick.get("signal") or pick.get("action"),
            "confidence": _safe_float(pick.get("confidence")),
            "ranking_score": _safe_float(pick.get("ranking_score") or pick.get("final_score")),
            "quant_base_score": _safe_float(pick.get("quant_base_score")),
            "llm_risk_adjustment": _safe_float(pick.get("llm_risk_adjustment")),
            "pool_score": _safe_float(pick.get("pool_score")),
            "pool": pick.get("pool") or pick.get("source") or "未知",
            "source_pools": pick.get("source_pools") or [],
            "money_flow": pick.get("money_flow") or {},
            "money_flow_original_missing": len(_money_flow_missing_keys(pick.get("money_flow") or {})) == len(MONEY_FLOW_KEYS),
            "money_flow_original_missing_keys": _money_flow_missing_keys(pick.get("money_flow") or {}),
            "money_flow_backfilled": False,
            "money_flow_backfill_source": "",
            "money_flow_backfill_status": "",
            "strategy_backtest": pick.get("strategy_backtest") or {},
            "selection_backtest": pick.get("selection_backtest") or {},
            "data_quality_flags": pick.get("data_quality_flags") or [],
            "quant_score_detail": pick.get("quant_score_detail") or {},
            "reason": pick.get("reason") or pick.get("final_decision") or "",
        }
        result.append(copied)
    return result


def _load_timing_state(output_dir: Path, report_day: str) -> Dict[str, Any]:
    path = output_dir / f"intraday_buy_timing_{report_day}.json"
    state = _read_json(path, {}) or {}
    stocks = state.get("stocks") if isinstance(state, dict) else {}
    if not isinstance(stocks, dict):
        stocks = {}
    selected = {
        _normalize_code(item.get("stock"))
        for item in state.get("selected_signals", []) or []
        if isinstance(item, dict)
    }
    return {"path": str(path), "exists": path.exists(), "stocks": stocks, "selected": selected}


def _extract_timing_info(timing_state: Dict[str, Any], stock: str) -> Dict[str, Any]:
    entry = (timing_state.get("stocks") or {}).get(stock) or {}
    last_decision = entry.get("last_decision") if isinstance(entry.get("last_decision"), dict) else {}
    last_order = entry.get("last_order") if isinstance(entry.get("last_order"), dict) else {}
    entered = bool(entry) or stock in (timing_state.get("selected") or set())
    status = str(entry.get("status") or ("missing" if not entered else "open"))
    return {
        "timing_state_exists": bool(timing_state.get("exists")),
        "entered_watch_pool": entered,
        "timing_status": status,
        "filled": status.lower() == "filled" or bool(entry.get("filled_at")),
        "filled_at": entry.get("filled_at"),
        "filled_price": _safe_float(entry.get("filled_price")),
        "filled_quantity": _safe_int(entry.get("filled_quantity") or entry.get("recorded_quantity")),
        "technical_trigger": last_decision.get("technical_trigger") or entry.get("buy_trigger"),
        "last_decision": last_decision,
        "last_order": last_order,
        "submitted_order_count": _safe_int(entry.get("submitted_order_count")),
        "decision_count": _safe_int(entry.get("decision_count")),
        "not_bought_reason": last_decision.get("reason") if status.lower() != "filled" else "",
    }


def _load_trade_records(output_dir: Path) -> List[Dict[str, Any]]:
    trades = _read_json(output_dir / "trades.json", {}) or {}
    records = trades.get("records") if isinstance(trades, dict) else []
    return [r for r in records or [] if isinstance(r, dict)]


def _trade_matches_day(record: Dict[str, Any], stock: str, report_day: str) -> bool:
    if _normalize_code(record.get("stock")) != stock:
        return False
    return _normalize_date_key(record.get("buy_date")) == report_day


def _extract_trade_info(records: List[Dict[str, Any]], stock: str, report_day: str, latest_close: Optional[float]) -> Dict[str, Any]:
    matched = [r for r in records if _trade_matches_day(r, stock, report_day)]
    if not matched:
        return {
            "actual_bought": False,
            "actual_buy_price": None,
            "actual_quantity": 0,
            "actual_remaining_quantity": 0,
            "actual_return_pct_to_latest": None,
            "actual_source": "",
            "sell_count": 0,
        }
    total_qty = 0
    total_remaining = 0
    total_cost = 0.0
    realized_amt = 0.0
    sold_qty = 0
    sell_count = 0
    sources = set()
    for rec in matched:
        buy_price = _safe_float(rec.get("buy_price"))
        qty = _safe_int(rec.get("quantity"))
        remaining = _safe_int(rec.get("remaining_quantity"), qty)
        if not buy_price or qty <= 0:
            continue
        total_qty += qty
        total_remaining += max(0, remaining)
        total_cost += buy_price * qty
        if rec.get("source"):
            sources.add(str(rec.get("source")))
        sells = rec.get("sells") if isinstance(rec.get("sells"), list) else []
        sell_count += len(sells)
        for sell in sells:
            sell_qty = _safe_int(sell.get("quantity"))
            sell_price = _safe_float(sell.get("price"))
            if sell_price and sell_qty > 0:
                realized_amt += (sell_price - buy_price) * sell_qty
                sold_qty += sell_qty
    unrealized_amt = 0.0
    if latest_close and total_remaining > 0 and total_qty > 0:
        remaining_cost = sum(
            (_safe_float(rec.get("buy_price")) or 0.0)
            * max(0, _safe_int(rec.get("remaining_quantity"), _safe_int(rec.get("quantity"))))
            for rec in matched
        )
        unrealized_amt = latest_close * total_remaining - remaining_cost
    actual_return = round((realized_amt + unrealized_amt) / total_cost * 100.0, 2) if total_cost > 0 else None
    return {
        "actual_bought": True,
        "actual_buy_price": round(total_cost / total_qty, 4) if total_qty > 0 else None,
        "actual_quantity": total_qty,
        "actual_remaining_quantity": total_remaining,
        "actual_return_pct_to_latest": actual_return,
        "actual_source": ",".join(sorted(sources)),
        "actual_lot_count": len(matched),
        "sell_count": sell_count,
        "sold_quantity": sold_qty,
    }


def _money_flow_strength(money_flow: Dict[str, Any]) -> str:
    vals = {
        "main": _safe_float(money_flow.get("main_net_flow")),
        "super": _safe_float(money_flow.get("super_net_flow")),
        "ddx": _safe_float(money_flow.get("ddx_5")),
        "ddy": _safe_float(money_flow.get("ddy_10")),
    }
    if all(v is None for v in vals.values()):
        return "missing"
    score = 0
    score += 1 if (vals["main"] or 0) > 0.3 else 0
    score += 1 if (vals["super"] or 0) > 0.2 else 0
    score += 1 if (vals["ddx"] or 0) > 0 else 0
    score += 1 if (vals["ddy"] or 0) > 0 else 0
    neg = 0
    neg += 1 if vals["main"] is not None and vals["main"] < -0.2 else 0
    neg += 1 if vals["super"] is not None and vals["super"] < -0.2 else 0
    neg += 1 if vals["ddx"] is not None and vals["ddx"] < 0 else 0
    neg += 1 if vals["ddy"] is not None and vals["ddy"] < 0 else 0
    if score >= 3:
        return "strong"
    if neg >= 3:
        return "weak"
    return "neutral"


def _complete_future_return(item: Dict[str, Any], key: str) -> Optional[float]:
    complete = item.get("future_return_complete") or {}
    value = (item.get("future_returns_pct") or {}).get(key)
    return _safe_float(value) if complete.get(key) is True else None


def _is_future_strong(item: Dict[str, Any]) -> bool:
    values = [
        _complete_future_return(item, "d3"),
        _complete_future_return(item, "d5"),
        _complete_future_return(item, "d10"),
        item.get("intraday_high_return_pct") if "REPORT_DAY_OHLC_MISSING" not in str(item.get("price_data_quality") or "") else None,
    ]
    return any(v is not None and float(v) >= 5.0 for v in values) or any(
        (_complete_future_return(item, k) is not None and float(_complete_future_return(item, k)) >= 3.0)
        for k in ("d5", "d10")
    )


def _is_future_weak(item: Dict[str, Any]) -> bool:
    d5 = _complete_future_return(item, "d5")
    d10 = _complete_future_return(item, "d10")
    if d5 is not None and d10 is not None:
        return float(d5) <= -2.0 and float(d10) <= 0.0
    available = [v for v in (_complete_future_return(item, "d3"), d5, d10) if v is not None]
    return bool(available) and statistics.mean(available) <= -2.0


def assign_attribution_labels(item: Dict[str, Any]) -> List[str]:
    labels: List[str] = []
    fut = item.get("future_returns_pct") or {}
    strong = _is_future_strong(item)
    weak = _is_future_weak(item)
    alpha = item.get("alpha_pct") or {}
    bought = bool(item.get("actual_bought") or item.get("filled"))
    timing_missing = not item.get("timing_state_exists") or not item.get("entered_watch_pool")
    price_quality = str(item.get("price_data_quality") or "")
    money_strength = _money_flow_strength(item.get("money_flow") or {})

    severe_price_gap = "REPORT_DAY_OHLC_MISSING" in price_quality
    if severe_price_gap or timing_missing or money_strength == "missing" or item.get("money_flow_original_missing_keys"):
        labels.append("DATA_QUALITY_ISSUE")

    alpha_values = [v for v in alpha.values() if v is not None]
    if weak and (not alpha_values or statistics.mean(alpha_values) < 0):
        labels.append("SELECTION_WEAK")

    if not bought and strong:
        if item.get("submitted_order_count", 0) > 0 or item.get("technical_trigger"):
            labels.append("ENTRY_MISSED")
        else:
            labels.append("GOOD_BUT_NOT_BOUGHT")

    actual_buy = item.get("actual_buy_price") or item.get("filled_price")
    same_day_from_actual = _pct_change(actual_buy, (item.get("ohlc") or {}).get("close"))
    d1_reference_return = _complete_future_return(item, "d1")
    d1_close = None
    if d1_reference_return is not None and item.get("reference_price"):
        d1_close = float(item["reference_price"]) * (1 + d1_reference_return / 100.0)
    d1_from_actual = _pct_change(actual_buy, d1_close)
    if bought and ((same_day_from_actual is not None and same_day_from_actual <= -2.0) or (d1_from_actual is not None and d1_from_actual <= -3.0)):
        labels.append("ENTRY_TOO_LATE")

    quant_base = item.get("quant_base_score")
    ranking_score = item.get("ranking_score")
    llm_adj = item.get("llm_risk_adjustment")
    quant_high = any(v is not None and float(v) >= 70.0 for v in (quant_base, ranking_score))
    if quant_high and llm_adj is not None and float(llm_adj) <= -8.0 and strong:
        labels.append("MODEL_OVER_RISK")
    if quant_high and weak:
        labels.append("QUANT_OVER_SCORE")
    if (money_strength == "strong" and weak) or (money_strength == "weak" and strong):
        labels.append("MONEY_FLOW_MISLEAD")

    return labels or ["NO_OBVIOUS_ISSUE"]


def data_quality_reasons(item: Dict[str, Any]) -> List[str]:
    reasons = []
    price_quality = str(item.get("price_data_quality") or "")
    if "REPORT_DAY_OHLC_MISSING" in price_quality:
        reasons.append("日线行情缺失")
    if "MINUTE_0931_MISSING" in price_quality:
        reasons.append("09:31分钟价缺失(有日线时使用日开盘价)")
    if not item.get("timing_state_exists"):
        reasons.append("盘中状态文件缺失")
    elif not item.get("entered_watch_pool"):
        reasons.append("未进入盘中观察池")
    missing_flow_keys = item.get("money_flow_original_missing_keys") or []
    if item.get("money_flow_strength") == "missing":
        reasons.append("资金流缺失")
    elif missing_flow_keys:
        reasons.append("资金流部分缺失:" + ",".join(missing_flow_keys))
    elif item.get("money_flow_original_missing") and item.get("money_flow_backfilled"):
        reasons.append("资金流由mx-data回补(非原始早报字段)")
    if item.get("quant_base_score") is None:
        reasons.append("量化基分缺失(历史早报)")
    if item.get("llm_risk_adjustment") is None:
        reasons.append("LLM修正缺失(历史早报)")
    return reasons


def data_quality_issue_reasons(item: Dict[str, Any]) -> List[str]:
    reasons = []
    price_quality = str(item.get("price_data_quality") or "")
    if "REPORT_DAY_OHLC_MISSING" in price_quality:
        reasons.append("日线行情缺失")
    if not item.get("timing_state_exists"):
        reasons.append("盘中状态文件缺失")
    elif not item.get("entered_watch_pool"):
        reasons.append("未进入盘中观察池")
    if item.get("money_flow_strength") == "missing":
        reasons.append("资金流缺失")
    elif item.get("money_flow_original_missing_keys"):
        reasons.append("资金流部分缺失")
    return reasons


def _benchmark_returns(report_day: str, daily_fetcher: PriceFetcher) -> Dict[str, Optional[float]]:
    perf = calculate_price_performance(
        BENCHMARK_CODE,
        report_day,
        daily_fetcher=daily_fetcher,
        minute_fetcher=lambda _code, _day: None,
    )
    return perf.get("future_returns_pct") or {}


def _build_pick_review(
    pick: Dict[str, Any],
    report_day: str,
    timing_state: Dict[str, Any],
    trade_records: List[Dict[str, Any]],
    benchmark_returns: Dict[str, Optional[float]],
    *,
    daily_fetcher: PriceFetcher,
    minute_fetcher: MinuteFetcher,
) -> Dict[str, Any]:
    stock = pick["stock"]
    price_perf = calculate_price_performance(
        stock,
        report_day,
        daily_fetcher=daily_fetcher,
        minute_fetcher=minute_fetcher,
    )
    price_perf = _apply_embedded_price_fallback(price_perf, pick)
    latest_close = None
    fut = price_perf.get("future_returns_pct") or {}
    for horizon in reversed(REVIEW_HORIZONS):
        if fut.get(f"d{horizon}") is not None and price_perf.get("reference_price"):
            latest_close = price_perf["reference_price"] * (1 + fut[f"d{horizon}"] / 100.0)
            break
    if latest_close is None:
        latest_close = (price_perf.get("ohlc") or {}).get("close")

    timing = _extract_timing_info(timing_state, stock)
    trade = _extract_trade_info(trade_records, stock, report_day, latest_close)
    alpha = {}
    for horizon in REVIEW_HORIZONS:
        key = f"d{horizon}"
        stock_ret = (price_perf.get("future_returns_pct") or {}).get(key)
        bench_ret = benchmark_returns.get(key)
        alpha[key] = round(stock_ret - bench_ret, 2) if stock_ret is not None and bench_ret is not None else None

    item = {
        "date": _iso_date(report_day),
        **pick,
        **price_perf,
        **timing,
        **trade,
        "benchmark": BENCHMARK_CODE,
        "benchmark_returns_pct": benchmark_returns,
        "alpha_pct": alpha,
        "money_flow_strength": _money_flow_strength(pick.get("money_flow") or {}),
    }
    item["attribution_labels"] = assign_attribution_labels(item)
    item["data_quality_reasons"] = data_quality_reasons(item)
    item["data_quality_issue_reasons"] = data_quality_issue_reasons(item)
    item["primary_attribution"] = item["attribution_labels"][0]
    return item


def _group_summary(items: List[Dict[str, Any]], group_key: str, return_key: str = "d5") -> Dict[str, Any]:
    groups: Dict[str, List[Optional[float]]] = defaultdict(list)
    for item in items:
        raw_key = item.get(group_key)
        if isinstance(raw_key, list):
            raw_key = raw_key[0] if raw_key else "未知"
        key = str(raw_key if raw_key not in (None, "") else "未知")
        groups[key].append(_complete_future_return(item, return_key))
    return {
        key: {
            "count": len(vals),
            "mature_sample_count": sum(v is not None for v in vals),
            "avg_return_pct": _avg(vals),
            "win_rate_pct": _win_rate(vals),
        }
        for key, vals in sorted(groups.items())
    }


def _score_bucket(value: Optional[float], high: float, low_label: str, high_label: str) -> str:
    if value is None:
        return "missing"
    return high_label if float(value) >= high else low_label


def build_summary(items: List[Dict[str, Any]], report_days: List[str]) -> Dict[str, Any]:
    label_counts = Counter(label for item in items for label in item.get("attribution_labels", []))
    summary = {
        "review_days": [_iso_date(day) for day in report_days],
        "days_count": len(report_days),
        "top5_count": len(items),
        "label_counts": dict(label_counts.most_common()),
        "data_quality_reason_counts": {},
        "data_quality_issue_reason_counts": {},
        "returns": {},
        "bought_vs_unbought": {},
        "by_signal": _group_summary(items, "signal"),
        "by_pool": _group_summary(items, "pool"),
        "by_money_flow_strength": _group_summary(items, "money_flow_strength"),
        "by_confidence_bucket": {},
        "by_quant_bucket": {},
        "by_llm_adjustment_bucket": {},
        "opportunity_missed_top3": [],
        "score_mismatch_top3": [],
        "data_gap_fix_hints_top3": [],
        "money_flow_backfill": {},
    }
    quality_reason_counts = Counter(reason for item in items for reason in item.get("data_quality_reasons", []))
    summary["data_quality_reason_counts"] = dict(quality_reason_counts.most_common())
    issue_reason_counts = Counter(reason for item in items for reason in item.get("data_quality_issue_reasons", []))
    summary["data_quality_issue_reason_counts"] = dict(issue_reason_counts.most_common())
    fix_map = {
        "资金流缺失": "已启用mx-data串行回补；剩余缺口通常是mx-data也无有效值、接口限频或原始历史早报字段为空。",
        "未进入盘中观察池": "检查当日intraday_buy_timing状态和Top5继承逻辑，确认早报Top5是否被写入观察池。",
        "盘中状态文件缺失": "需要恢复或重建对应日期intraday_buy_timing文件，否则只能做行情机会复盘。",
        "日线行情缺失": "优先检查XQShare日线接口，其次补本地行情缓存。",
    }
    summary["data_gap_fix_hints_top3"] = [
        {"reason": reason, "count": count, "fix_hint": fix_map.get(reason, "需要按缺口来源补齐对应原始数据。")}
        for reason, count in issue_reason_counts.most_common(3)
    ]
    backfill_status = Counter(str(item.get("money_flow_backfill_status") or "not_needed") for item in items)
    summary["money_flow_backfill"] = {
        "original_missing_count": sum(1 for item in items if item.get("money_flow_original_missing")),
        "original_partial_count": sum(
            1
            for item in items
            if item.get("money_flow_original_missing_keys") and not item.get("money_flow_original_missing")
        ),
        "backfilled_count": sum(1 for item in items if item.get("money_flow_backfilled")),
        "status_counts": dict(backfill_status.most_common()),
    }
    for horizon in REVIEW_HORIZONS:
        key = f"d{horizon}"
        vals = [_complete_future_return(item, key) for item in items]
        alphas = [
            (item.get("alpha_pct") or {}).get(key)
            if (item.get("future_return_complete") or {}).get(key) is True
            else None
            for item in items
        ]
        summary["returns"][key] = {
            "mature_sample_count": sum(v is not None for v in vals),
            "immature_sample_count": len(items) - sum(v is not None for v in vals),
            "avg_return_pct": _avg(vals),
            "win_rate_pct": _win_rate(vals),
            "avg_alpha_pct": _avg(alphas),
            "beat_benchmark_rate_pct": _win_rate(alphas),
        }

    for bought_state, label in ((True, "bought"), (False, "not_bought")):
        subset = [item for item in items if bool(item.get("actual_bought") or item.get("filled")) is bought_state]
        summary["bought_vs_unbought"][label] = {
            "count": len(subset),
            "d5_mature_sample_count": sum(_complete_future_return(item, "d5") is not None for item in subset),
            "d5_avg_return_pct": _avg(_complete_future_return(item, "d5") for item in subset),
            "d10_avg_return_pct": _avg(_complete_future_return(item, "d10") for item in subset),
            "actual_avg_return_pct": _avg(item.get("actual_return_pct_to_latest") for item in subset),
        }

    for item in items:
        item["_confidence_bucket"] = _score_bucket(item.get("confidence"), 70, "low_confidence", "high_confidence")
        item["_quant_bucket"] = _score_bucket(item.get("quant_base_score") or item.get("ranking_score"), 70, "low_quant", "high_quant")
        adj = item.get("llm_risk_adjustment")
        item["_llm_adjustment_bucket"] = "missing" if adj is None else ("llm_positive_or_neutral" if float(adj) >= 0 else "llm_negative")
    summary["by_confidence_bucket"] = _group_summary(items, "_confidence_bucket")
    summary["by_quant_bucket"] = _group_summary(items, "_quant_bucket")
    summary["by_llm_adjustment_bucket"] = _group_summary(items, "_llm_adjustment_bucket")

    missed = [
        item for item in items
        if not bool(item.get("actual_bought") or item.get("filled")) and _is_future_strong(item)
    ]
    summary["opportunity_missed_top3"] = [
        {
            "stock": item["stock"],
            "name": item.get("name"),
            "date": item.get("date"),
            "d5_return_pct": _complete_future_return(item, "d5"),
            "d10_return_pct": _complete_future_return(item, "d10"),
            "intraday_high_return_pct": item.get("intraday_high_return_pct"),
            "reason": item.get("not_bought_reason") or item.get("primary_attribution"),
        }
        for item in sorted(missed, key=lambda x: max(v for v in [
            _complete_future_return(x, "d5"),
            _complete_future_return(x, "d10"),
            x.get("intraday_high_return_pct"),
        ] if v is not None), reverse=True)[:3]
    ]

    mismatches = [
        item for item in items
        if any(label in item.get("attribution_labels", []) for label in ("MODEL_OVER_RISK", "QUANT_OVER_SCORE", "MONEY_FLOW_MISLEAD"))
    ]
    mismatches.sort(
        key=lambda item: min(
            [v for v in (_complete_future_return(item, "d5"), _complete_future_return(item, "d10")) if v is not None]
            or [0.0]
        )
    )
    summary["score_mismatch_top3"] = [
        {
            "stock": item["stock"],
            "name": item.get("name"),
            "date": item.get("date"),
            "labels": item.get("attribution_labels"),
            "quant_base_score": item.get("quant_base_score"),
            "llm_risk_adjustment": item.get("llm_risk_adjustment"),
            "money_flow_strength": item.get("money_flow_strength"),
            "d5_return_pct": _complete_future_return(item, "d5"),
            "d10_return_pct": _complete_future_return(item, "d10"),
        }
        for item in mismatches[:3]
    ]

    for item in items:
        item.pop("_confidence_bucket", None)
        item.pop("_quant_bucket", None)
        item.pop("_llm_adjustment_bucket", None)
    return summary


def _compact_items_for_llm(items: List[Dict[str, Any]], limit: int = 50) -> List[Dict[str, Any]]:
    compact = []
    for item in items[:limit]:
        fut = item.get("future_returns_pct") or {}
        alpha = item.get("alpha_pct") or {}
        decision = item.get("last_decision") or {}
        compact.append({
            "date": item.get("date"),
            "rank": item.get("rank"),
            "stock": item.get("stock"),
            "name": item.get("name"),
            "signal": item.get("signal"),
            "confidence": item.get("confidence"),
            "ranking_score": item.get("ranking_score"),
            "quant_base_score": item.get("quant_base_score"),
            "llm_risk_adjustment": item.get("llm_risk_adjustment"),
            "pool_score": item.get("pool_score"),
            "pool": item.get("pool"),
            "money_flow_strength": item.get("money_flow_strength"),
            "money_flow_original_missing": item.get("money_flow_original_missing"),
            "money_flow_backfilled": item.get("money_flow_backfilled"),
            "money_flow_backfill_source": item.get("money_flow_backfill_source"),
            "price_data_quality": item.get("price_data_quality"),
            "actual_bought": bool(item.get("actual_bought") or item.get("filled")),
            "timing_status": item.get("timing_status"),
            "technical_trigger": item.get("technical_trigger"),
            "last_action": decision.get("action"),
            "last_reason": str(decision.get("reason") or item.get("not_bought_reason") or "")[:160],
            "intraday_high_return_pct": item.get("intraday_high_return_pct"),
            "same_day_close_return_pct": item.get("same_day_close_return_pct"),
            "d1": fut.get("d1"),
            "d3": fut.get("d3"),
            "d5": fut.get("d5"),
            "d10": fut.get("d10"),
            "future_return_complete": item.get("future_return_complete") or {},
            "future_return_dates": item.get("future_return_dates") or {},
            "alpha_d5": alpha.get("d5"),
            "alpha_d10": alpha.get("d10"),
            "actual_return_pct_to_latest": item.get("actual_return_pct_to_latest"),
            "labels": item.get("attribution_labels"),
        })
    return compact


def _build_deep_analysis_prompt(review: Dict[str, Any]) -> str:
    summary = review.get("summary") or {}
    intraday_review = review.get("intraday_buy_review") or {}
    try:
        from intraday_buy_weekly_review import compact_intraday_review_for_llm
        compact_intraday = compact_intraday_review_for_llm(intraday_review)
    except Exception:
        compact_intraday = {"status": "failed_to_compact"}
    payload = {
        "generated_at": review.get("generated_at"),
        "benchmark": review.get("benchmark"),
        "summary": summary,
        "per_day": review.get("per_day") or {},
        "items": _compact_items_for_llm(review.get("items") or []),
        "intraday_buy_review": compact_intraday,
    }
    return (
        "你是A股短线/波段交易系统的复盘负责人。请基于下面的事实底稿，"
        "对最近10个交易日早报Top5进行深度复盘归因。\n\n"
        "重要要求：\n"
        "1. 只基于事实底稿，不要编造行情、成交或新闻。\n"
        "2. 明确区分选股质量、盘中执行、评分机制、市场环境四类原因。\n"
        "3. 如果数据质量不足，要直接指出，不要假装有完整结论。\n"
        "4. 建议必须能转化为下周可执行的规则或观察项。\n"
        "5. 输出中文，简洁但要有判断力。\n\n"
        "6. 盘中买入复盘必须区分选股问题、技术触发问题、LLM判断问题、订单问题和进程问题；PARTIAL证据只能作为线索。\n"
        "7. 影子策略结果只用于对照，不得建议系统自动修改实盘规则。\n\n"
        "事实底稿JSON：\n"
        f"{json.dumps(payload, ensure_ascii=False, default=str)}"
    )


def _model_display_name(model: str) -> str:
    text = str(model or "")
    if "gpt-5.6-sol" in text.lower():
        return "GPT-5.6 Sol"
    if "minimax" in text.lower() or "m3" in text.lower():
        return "MiniMax-M3"
    return text or "unknown"


def _parse_deep_analysis_text(text: str, model: str, error: str = "") -> Dict[str, Any]:
    try:
        from stock_selection_debate.providers import extract_json_object
        data = extract_json_object(
            text,
            required_keys={
                "overall_conclusion",
                "root_causes",
                "selection_diagnosis",
                "execution_diagnosis",
                "scoring_diagnosis",
                "market_context_diagnosis",
                "priority_fixes",
                "watchlist_notes",
                "risk_warnings",
                "confidence",
            },
            wrapper_key="Top5ReviewDeepAnalysis",
        )
    except Exception:
        data = None
    if not isinstance(data, dict):
        return {
            "status": "failed",
            "model": _model_display_name(model),
            "error": error or "LLM返回内容无法解析为深度复盘JSON",
            "raw_text_preview": str(text or "")[:1000],
        }
    data["status"] = "ok"
    data["model"] = _model_display_name(model)
    return data


def _deep_analysis_model_dump(structured: Any, model: str, fallback_used: bool) -> Dict[str, Any]:
    data = structured.model_dump() if hasattr(structured, "model_dump") else structured.dict()
    data["status"] = "ok"
    data["model"] = _model_display_name(model)
    data["fallback_used"] = fallback_used
    return data


def _call_structured_deep_analysis(prompt: str, model: str, *, fallback_used: bool) -> Dict[str, Any]:
    from stock_selection_debate.providers import call_structured

    structured = call_structured(
        prompt,
        Top5ReviewDeepAnalysis,
        model=model,
        timeout=DEFAULT_LLM_TIMEOUT,
        retries=0,
        thinking_budget=DEFAULT_LLM_THINKING_BUDGET,
        max_tokens=DEFAULT_LLM_MAX_TOKENS,
        allow_fallback=False,
        reasoning_effort=DEFAULT_LLM_REASONING_EFFORT,
    )
    if structured is None:
        return {
            "status": "failed",
            "model": _model_display_name(model),
            "error": f"{_model_display_name(model)} structured返回空结果",
        }
    return _deep_analysis_model_dump(structured, model, fallback_used=fallback_used)


def _call_minimax_deep_analysis(prompt: str, model: str) -> Dict[str, Any]:
    """MiniMax deep analysis follows the PM path: portal OAuth + structured JSON."""
    return _call_structured_deep_analysis(prompt, model, fallback_used=True)


def _call_minimax_text_repair_deep_analysis(prompt: str, model: str) -> Dict[str, Any]:
    from stock_selection_debate.providers import call_llm
    text = call_llm(
        prompt=prompt,
        model=model,
        timeout=DEFAULT_LLM_TIMEOUT,
        retries=2,
        max_tokens=DEFAULT_LLM_MAX_TOKENS,
        thinking_budget=DEFAULT_LLM_THINKING_BUDGET,
        temperature=0,
    )
    return _parse_deep_analysis_text(text, model)


def _deep_analysis_json_instruction() -> str:
    return """

请只输出一个 JSON 对象，不要 markdown，不要代码块。字段必须包含：
{
  "overall_conclusion": "一句话总判断",
  "root_causes": ["核心原因1", "核心原因2", "核心原因3"],
  "selection_diagnosis": "选股质量诊断",
  "execution_diagnosis": "盘中执行诊断",
  "scoring_diagnosis": "评分机制诊断",
  "market_context_diagnosis": "市场环境诊断",
  "priority_fixes": ["优先改进1", "优先改进2", "优先改进3"],
  "watchlist_notes": ["具体股票/日期线索"],
  "risk_warnings": ["风险提示"],
  "confidence": 0
}
"""


def _call_openclaw_agent_deep_analysis(prompt: str) -> Dict[str, Any]:
    session_id = f"top5-review-{uuid.uuid4().hex[:10]}"
    cmd = [
        "openclaw",
        "agent",
        "--local",
        "--session-id",
        session_id,
        "--message",
        prompt + _deep_analysis_json_instruction(),
        "--timeout",
        str(DEFAULT_LLM_TIMEOUT),
    ]
    proc = None
    try:
        proc = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            env={**os.environ},
            preexec_fn=os.setsid if hasattr(os, "setsid") else None,
        )
        stdout, stderr = proc.communicate(timeout=DEFAULT_LLM_TIMEOUT + 15)
        if proc.returncode != 0:
            return {
                "status": "failed",
                "model": "OpenClaw-local",
                "error": f"openclaw agent failed: {(stderr or stdout or '')[:1000]}",
            }
        parsed = _parse_deep_analysis_text(stdout, "OpenClaw-local")
        if parsed.get("status") == "ok":
            parsed["model"] = "OpenClaw-local"
        return parsed
    except subprocess.TimeoutExpired:
        if proc is not None:
            try:
                if hasattr(os, "killpg"):
                    os.killpg(os.getpgid(proc.pid), signal.SIGTERM)
                else:
                    proc.terminate()
            except Exception:
                pass
        return {"status": "failed", "model": "OpenClaw-local", "error": "openclaw agent timeout"}
    except Exception as exc:
        return {"status": "failed", "model": "OpenClaw-local", "error": str(exc)}


def run_llm_deep_analysis(
    review: Dict[str, Any],
    *,
    model: str = DEFAULT_LLM_MODEL,
    fallback_model: str = DEFAULT_LLM_FALLBACK_MODEL,
) -> Dict[str, Any]:
    if Top5ReviewDeepAnalysis is None:
        return {
            "status": "failed",
            "model": "",
            "error": "pydantic不可用，无法启用结构化LLM深度解读",
        }
    prompt = _build_deep_analysis_prompt(review)
    errors = []

    for attempt in range(max(1, DEFAULT_LLM_RETRIES)):
        try:
            primary = _call_structured_deep_analysis(prompt, model, fallback_used=False)
            if primary.get("status") == "ok":
                return primary
            errors.append(f"{_model_display_name(model)}第{attempt + 1}次失败: {primary.get('error')}")
        except Exception as exc:
            errors.append(f"{_model_display_name(model)}第{attempt + 1}次失败: {exc}")
        if attempt < max(1, DEFAULT_LLM_RETRIES) - 1:
            time.sleep(1)

    for attempt in range(max(1, DEFAULT_LLM_RETRIES)):
        try:
            fallback = _call_minimax_deep_analysis(prompt, fallback_model)
            if fallback.get("status") == "ok":
                if errors:
                    fallback["primary_error"] = "; ".join(errors)[-1000:]
                return fallback
            errors.append(f"{_model_display_name(fallback_model)} structured第{attempt + 1}次失败: {fallback.get('error') or '返回空结果'}")
        except Exception as exc:
            errors.append(f"{_model_display_name(fallback_model)} structured第{attempt + 1}次失败: {exc}")
        if attempt < max(1, DEFAULT_LLM_RETRIES) - 1:
            time.sleep(1)

    try:
        repaired = _call_minimax_text_repair_deep_analysis(prompt + _deep_analysis_json_instruction(), fallback_model)
        if repaired.get("status") == "ok":
            repaired["fallback_used"] = True
            repaired["model"] = f"{_model_display_name(fallback_model)}-text-repair"
            if errors:
                repaired["primary_error"] = "; ".join(errors)[-1000:]
            return repaired
        errors.append(f"{_model_display_name(fallback_model)}文本JSON修复失败: {repaired.get('error') or repaired.get('raw_text_preview') or '无法解析'}")
    except Exception as exc:
        errors.append(f"{_model_display_name(fallback_model)}文本JSON修复失败: {exc}")

    if ENABLE_OPENCLAW_LOCAL_FALLBACK:
        local = _call_openclaw_agent_deep_analysis(prompt)
        local["fallback_used"] = True
        if errors:
            local["primary_error"] = "; ".join(errors)[-1500:]
        if local.get("status") == "ok":
            return local
        errors.append(f"OpenClaw-local失败: {local.get('error')}")
    else:
        errors.append("OpenClaw-local兜底默认关闭，避免只读状态数据库导致任务误报")
    return {
        "status": "failed",
        "model": f"{_model_display_name(model)}->{_model_display_name(fallback_model)}",
        "fallback_used": True,
        "error": "; ".join(errors)[-1500:],
    }


def build_review(
    *,
    base_dir: Path = BASE_DIR,
    days: int = 10,
    daily_fetcher: PriceFetcher = fetch_daily_ohlc,
    minute_fetcher: MinuteFetcher = fetch_minute_reference_price,
    include_llm: bool = False,
    llm_model: str = DEFAULT_LLM_MODEL,
    llm_fallback_model: str = DEFAULT_LLM_FALLBACK_MODEL,
    enable_money_flow_backfill: bool = True,
) -> Dict[str, Any]:
    output_dir = base_dir / "output"
    report_files = list_recent_report_files(output_dir, days)
    report_days = [_report_date_from_path(path) for path in report_files]
    trade_records = _load_trade_records(output_dir)
    items: List[Dict[str, Any]] = []
    per_day: Dict[str, Any] = {}

    for path in sorted(report_files, key=_report_date_from_path):
        report_day = _report_date_from_path(path)
        report = _read_json(path, {}) or {}
        picks = _extract_top_picks(report)
        if enable_money_flow_backfill:
            picks = _backfill_missing_money_flow(picks, output_dir, report_day)
        timing_state = _load_timing_state(output_dir, report_day)
        benchmark = _benchmark_returns(report_day, daily_fetcher)
        day_items = [
            _build_pick_review(
                pick,
                report_day,
                timing_state,
                trade_records,
                benchmark,
                daily_fetcher=daily_fetcher,
                minute_fetcher=minute_fetcher,
            )
            for pick in picks
        ]
        items.extend(day_items)
        per_day[_iso_date(report_day)] = {
            "top5_count": len(day_items),
            "d5_mature_sample_count": sum(_complete_future_return(item, "d5") is not None for item in day_items),
            "d5_avg_return_pct": _avg(_complete_future_return(item, "d5") for item in day_items),
            "label_counts": dict(Counter(label for item in day_items for label in item.get("attribution_labels", []))),
        }

    report_days_sorted = sorted(report_days)
    summary = build_summary(items, report_days_sorted)
    review = {
        "generated_at": datetime.now().isoformat(),
        "days_requested": days,
        "benchmark": BENCHMARK_CODE,
        "summary": summary,
        "per_day": per_day,
        "items": items,
    }
    try:
        from intraday_buy_weekly_review import build_intraday_buy_weekly_review
        review["intraday_buy_review"] = build_intraday_buy_weekly_review(
            output_dir=output_dir,
            selection_items=items,
            rolling_days=20,
        )
    except Exception as exc:
        review["intraday_buy_review"] = {
            "status": "failed",
            "error": f"{type(exc).__name__}: {exc}"[:500],
        }
    if include_llm:
        mature_d5 = ((summary.get("returns") or {}).get("d5") or {}).get("mature_sample_count", 0)
        intraday_full = int((((review.get("intraday_buy_review") or {}).get("summary") or {}).get("full_quality_count") or 0))
        if mature_d5 < MIN_LLM_MATURE_D5_SAMPLES and intraday_full < 10:
            review["llm_deep_analysis"] = {
                "status": "skipped",
                "model": "",
                "error": (
                    f"成熟D+5样本仅{mature_d5}只且完整盘中样本仅{intraday_full}只，"
                    f"均不足以支持深度策略结论"
                ),
            }
        else:
            review["llm_deep_analysis"] = run_llm_deep_analysis(
                review,
                model=llm_model,
                fallback_model=llm_fallback_model,
            )
    return review


def build_forward_memory_backfill(
    *,
    base_dir: Path = BASE_DIR,
    days: int = 30,
    horizon: str = "d10",
    daily_fetcher: PriceFetcher = fetch_daily_ohlc,
    minute_fetcher: MinuteFetcher = fetch_minute_reference_price,
) -> Dict[str, Any]:
    """Backfill mature forward returns outside the 10-day display window.

    Only unresolved Top5 records are considered.  A report day is queried only
    after the benchmark or local report calendar proves that the requested
    horizon has matured, avoiding repeated requests for recent picks.
    """
    output_dir = base_dir / "output"
    memory_path = output_dir / "selection_memory.jsonl"
    memory_rows = _read_jsonl(memory_path)
    pending_keys = {
        (_normalize_date_key(item.get("report_date")), _normalize_code(item.get("stock")))
        for item in memory_rows
        if item.get("top5_rank")
        and item.get(f"return_{horizon}_complete") is not True
        and _normalize_date_key(item.get("report_date"))
        and _normalize_code(item.get("stock"))
    }
    report_files = list_recent_report_files(output_dir, max(1, days))
    all_report_days = sorted({
        _report_date_from_path(path)
        for path in output_dir.glob("daily_report_*.json")
        if "daily_report_push_" not in path.name and _report_date_from_path(path)
    })
    horizon_steps = max(1, _safe_int(str(horizon).lower().lstrip("d"), 10))
    trade_records = _load_trade_records(output_dir)
    items: List[Dict[str, Any]] = []
    eligible_days: List[str] = []
    immature_days: List[str] = []
    unavailable_records = 0
    pending_in_window = 0

    for path in sorted(report_files, key=_report_date_from_path):
        report_day = _report_date_from_path(path)
        report = _read_json(path, {}) or {}
        picks = [
            pick for pick in _extract_top_picks(report)
            if (report_day, pick.get("stock")) in pending_keys
        ]
        if not picks:
            continue
        pending_in_window += len(picks)

        newer_report_count = sum(1 for day in all_report_days if day > report_day)
        benchmark_perf = calculate_price_performance(
            BENCHMARK_CODE,
            report_day,
            daily_fetcher=daily_fetcher,
            minute_fetcher=lambda _code, _day: None,
        )
        benchmark_complete = (benchmark_perf.get("future_return_complete") or {}).get(horizon) is True
        if newer_report_count < horizon_steps and not benchmark_complete:
            immature_days.append(report_day)
            continue

        eligible_days.append(report_day)
        benchmark_returns = benchmark_perf.get("future_returns_pct") or {}
        timing_state = _load_timing_state(output_dir, report_day)
        for pick in picks:
            item = _build_pick_review(
                pick,
                report_day,
                timing_state,
                trade_records,
                benchmark_returns,
                daily_fetcher=daily_fetcher,
                minute_fetcher=minute_fetcher,
            )
            if (item.get("future_return_complete") or {}).get(horizon) is True:
                items.append(item)
            else:
                unavailable_records += 1

    completed = sum(
        1 for item in items
        if (item.get("future_return_complete") or {}).get(horizon) is True
    )
    if completed:
        status = "ok"
    elif pending_in_window:
        status = "no_mature_pending_samples"
    else:
        status = "no_pending_records"
    return {
        "generated_at": datetime.now().isoformat(),
        "items": items,
        "summary": {
            "status": status,
            "horizon": horizon,
            "days_requested": max(1, days),
            "report_days_scanned": len(report_files),
            "pending_records_in_window": pending_in_window,
            "eligible_report_days": [_iso_date(day) for day in sorted(set(eligible_days))],
            "immature_report_days": [_iso_date(day) for day in sorted(set(immature_days))],
            "records_calculated": len(items),
            "completed_records": completed,
            "unavailable_records": unavailable_records,
        },
    }


def format_feishu_text(review: Dict[str, Any]) -> str:
    summary = review.get("summary") or {}
    returns = summary.get("returns") or {}
    d5 = returns.get("d5") or {}
    d10 = returns.get("d10") or {}
    labels = summary.get("label_counts") or {}
    top_labels = "、".join(f"{k}:{v}" for k, v in list(labels.items())[:3]) or "暂无明显问题"
    issue_reasons = summary.get("data_quality_issue_reason_counts") or {}
    top_issue_quality = "、".join(f"{k}:{v}" for k, v in list(issue_reasons.items())[:3])
    gap_hints = summary.get("data_gap_fix_hints_top3") or []
    backfill = summary.get("money_flow_backfill") or {}
    quality_reasons = summary.get("data_quality_reason_counts") or {}
    top_quality_note = "、".join(
        f"{k}:{v}"
        for k, v in list(quality_reasons.items())[:3]
        if k not in issue_reasons
    )
    missed = summary.get("opportunity_missed_top3") or []
    mismatch = summary.get("score_mismatch_top3") or []

    mature_d5 = int(d5.get("mature_sample_count") or 0)
    title = "📊" if mature_d5 >= MIN_LLM_MATURE_D5_SAMPLES else "⚠️"
    lines = [
        f"{title} 最近{summary.get('days_count', 0)}个交易日Top5复盘归因",
        f"样本: {summary.get('top5_count', 0)}只 | 成熟D+5 {mature_d5}只 | 成熟D+10 {d10.get('mature_sample_count', 0)}只",
        f"D+5均值 {d5.get('avg_return_pct')}% | D+10均值 {d10.get('avg_return_pct')}%",
        f"D+5胜率 {d5.get('win_rate_pct')}% | 跑赢沪深300 {d5.get('beat_benchmark_rate_pct')}%",
        f"主要问题: {top_labels}",
    ]
    if mature_d5 < MIN_LLM_MATURE_D5_SAMPLES:
        lines.insert(1, f"复盘数据不足：成熟D+5样本低于{MIN_LLM_MATURE_D5_SAMPLES}只，本次不生成策略结论、不写入未成熟收益。")
    if top_issue_quality:
        lines.append(f"触发数据问题: {top_issue_quality}")
    if backfill:
        lines.append(
            "资金流回补: "
            f"原始缺失{backfill.get('original_missing_count', 0)}只，"
            f"部分缺失{backfill.get('original_partial_count', 0)}只，"
            f"mx-data诊断快照{backfill.get('backfilled_count', 0)}只(不替代历史值)"
        )
    forward_backfill = review.get("forward_memory_backfill") or {}
    if forward_backfill:
        lines.append(
            "前向样本回填: "
            f"扫描{forward_backfill.get('report_days_scanned', 0)}个报告日，"
            f"补齐{str(forward_backfill.get('horizon') or 'd10').upper()} "
            f"{forward_backfill.get('completed_records', 0)}条，"
            f"写入记忆{forward_backfill.get('records_updated', 0)}条"
        )
    if gap_hints:
        lines.append("缺口修复Top3: " + "；".join(
            f"{x.get('reason')}({x.get('count')}): {x.get('fix_hint')}"
            for x in gap_hints[:3]
        ))
    if top_quality_note:
        lines.append(f"其它数据说明: {top_quality_note}")
    if missed:
        lines.append("机会错过Top3: " + "；".join(
            f"{x['stock']} {x.get('name','')} D5 {x.get('d5_return_pct')}%/日内高点 {x.get('intraday_high_return_pct')}%"
            for x in missed[:3]
        ))
    if mismatch:
        lines.append("评分失真Top3: " + "；".join(
            f"{x['stock']} {x.get('name','')} {','.join(x.get('labels') or [])}"
            for x in mismatch[:3]
        ))
    intraday_review = review.get("intraday_buy_review") or {}
    try:
        from intraday_buy_weekly_review import format_intraday_review_text
        lines.append("")
        lines.append(format_intraday_review_text(intraday_review))
    except Exception as exc:
        lines.append(f"盘中买入复盘生成失败: {type(exc).__name__}: {exc}")
    llm = review.get("llm_deep_analysis") or {}
    if llm:
        if llm.get("status") == "ok":
            lines.append(f"🤖 LLM深度解读({llm.get('model')}): {llm.get('overall_conclusion')}")
            if llm.get("execution_diagnosis"):
                lines.append(f"盘中执行诊断: {llm.get('execution_diagnosis')}")
            causes = llm.get("root_causes") or []
            if causes:
                lines.append("核心归因: " + "；".join(str(x) for x in causes[:3]))
            fixes = llm.get("priority_fixes") or []
            if fixes:
                lines.append("优先改进: " + "；".join(str(x) for x in fixes[:3]))
        else:
            lines.append(f"🤖 LLM深度解读失败: {llm.get('error', 'unknown')}")
    lines.append("建议: 优先检查机会错过、量化高分走弱、LLM过度扣分这三类样本。")
    return "\n".join(lines)


def push_feishu_text_detailed(text: str, webhook: Optional[str] = None) -> Tuple[bool, str]:
    webhook = webhook or os.getenv("FEISHU_WEBHOOK_URL")
    if not webhook:
        return False, "FEISHU_WEBHOOK_URL not configured"
    payload = {"msg_type": "text", "content": {"text": text}}
    try:
        if requests is not None:
            resp = requests.post(webhook, json=payload, timeout=10)
            body = resp.text[:500]
            if not (200 <= resp.status_code < 300):
                return False, f"HTTP {resp.status_code}: {body}"
            try:
                data = resp.json()
            except Exception:
                data = {}
            code = data.get("code")
            if code not in (None, 0):
                return False, f"Feishu code={code} msg={data.get('msg') or data.get('message') or body}"
            return True, "ok"
        data = json.dumps(payload).encode("utf-8")
        req = urllib.request.Request(webhook, data=data, headers={"Content-Type": "application/json"})
        with urllib.request.urlopen(req, timeout=10) as resp:
            body = resp.read().decode("utf-8", errors="replace")[:500]
            if not (200 <= resp.status < 300):
                return False, f"HTTP {resp.status}: {body}"
            try:
                parsed = json.loads(body)
            except Exception:
                parsed = {}
            code = parsed.get("code")
            if code not in (None, 0):
                return False, f"Feishu code={code} msg={parsed.get('msg') or parsed.get('message') or body}"
            return True, "ok"
    except Exception as exc:
        return False, f"{type(exc).__name__}: {exc}"


def push_feishu_text(text: str, webhook: Optional[str] = None) -> bool:
    ok, _reason = push_feishu_text_detailed(text, webhook=webhook)
    return ok


def save_review(
    review: Dict[str, Any],
    output_dir: Path = OUTPUT_DIR,
    memory_backfill_review: Optional[Dict[str, Any]] = None,
) -> Tuple[Path, Path]:
    today_key = date.today().strftime("%Y%m%d")
    dated = output_dir / f"top5_review_{today_key}.json"
    latest = output_dir / "top5_review_latest.json"
    _write_json_atomic(dated, review)
    _write_json_atomic(latest, review)
    try:
        from selection_memory import update_selection_memory_from_top5_review
        memory_path = output_dir / "selection_memory.jsonl"
        updated = update_selection_memory_from_top5_review(review, path=memory_path)
        review.setdefault("selection_memory", {})["records_updated"] = updated
        backfill_updated = 0
        if memory_backfill_review:
            backfill_updated = update_selection_memory_from_top5_review(
                memory_backfill_review,
                path=memory_path,
            )
            forward_summary = review.setdefault("forward_memory_backfill", {})
            forward_summary["records_updated"] = backfill_updated
        review.setdefault("selection_memory", {})["forward_records_updated"] = backfill_updated
        review.setdefault("selection_memory", {})["total_records_updated"] = updated + backfill_updated
        _write_json_atomic(dated, review)
        _write_json_atomic(latest, review)
    except Exception as exc:
        review.setdefault("selection_memory", {})["update_error"] = str(exc)[:300]
        _write_json_atomic(dated, review)
        _write_json_atomic(latest, review)
    try:
        from intraday_buy_weekly_review import update_intraday_policy_memory
        updated = update_intraday_policy_memory(review.get("intraday_buy_review") or {}, output_dir)
        review.setdefault("intraday_buy_review", {}).setdefault("policy_memory", {})["records_updated"] = updated
    except Exception as exc:
        review.setdefault("intraday_buy_review", {}).setdefault("policy_memory", {})["update_error"] = str(exc)[:300]
    _write_json_atomic(dated, review)
    _write_json_atomic(latest, review)
    return dated, latest


def main(argv: Optional[List[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="最近N个交易日Top5事后复盘归因")
    parser.add_argument("--days", type=int, default=10, help="读取最近N个daily_report交易日")
    parser.add_argument("--push", action="store_true", help="推送简洁飞书文本")
    parser.add_argument("--no-save", action="store_true", help="只打印摘要，不写output文件")
    parser.add_argument("--no-llm", action="store_true", help="禁用LLM深度解读，只做规则归因")
    parser.add_argument("--llm-model", default=DEFAULT_LLM_MODEL, help="深度解读主模型，默认GPT-5.6 Sol")
    parser.add_argument("--llm-fallback-model", default=DEFAULT_LLM_FALLBACK_MODEL, help="深度解读兜底模型，默认MiniMax M3")
    parser.add_argument("--no-edge-rules", action="store_true", help="不生成全候选池历史优势组合规则")
    parser.add_argument("--edge-days", type=int, default=20, help="生成weekly优势规则时读取最近N个daily_report")
    parser.add_argument("--edge-min-samples", type=int, default=25, help="优势规则最小样本数")
    parser.add_argument("--edge-min-days", type=int, default=8, help="优势规则最少覆盖交易日数")
    parser.add_argument("--forward-backfill-days", type=int, default=30, help="额外扫描最近N个报告日，补齐已成熟D+10前向样本；0=禁用")
    args = parser.parse_args(argv)

    start = time.time()
    review = build_review(
        days=args.days,
        include_llm=not args.no_llm,
        llm_model=args.llm_model,
        llm_fallback_model=args.llm_fallback_model,
    )
    exit_reasons: List[str] = []
    memory_backfill_review: Optional[Dict[str, Any]] = None
    if not args.no_save and args.forward_backfill_days > 0:
        try:
            memory_backfill_review = build_forward_memory_backfill(
                days=max(args.days + 1, args.forward_backfill_days),
            )
            review["forward_memory_backfill"] = dict(memory_backfill_review.get("summary") or {})
        except Exception as exc:
            review["forward_memory_backfill"] = {
                "status": "failed",
                "error": f"{type(exc).__name__}: {exc}"[:500],
            }
            exit_reasons.append(f"forward_backfill_failed:{type(exc).__name__}")
    intraday_status = str((review.get("intraday_buy_review") or {}).get("status") or "missing")
    if intraday_status in {"failed", "missing", "no_data"}:
        intraday_error = str((review.get("intraday_buy_review") or {}).get("error") or intraday_status)
        exit_reasons.append(f"intraday_buy_review_{intraday_status}:{intraday_error[:120]}")
    mature_d5 = int((((review.get("summary") or {}).get("returns") or {}).get("d5") or {}).get("mature_sample_count") or 0)
    if mature_d5 < MIN_LLM_MATURE_D5_SAMPLES:
        exit_reasons.append(f"mature_d5_samples={mature_d5}<{MIN_LLM_MATURE_D5_SAMPLES}")
    edge_text = ""
    if not args.no_edge_rules:
        try:
            from candidate_edge_rules import (
                build_and_save_edge_rules,
                build_scoring_shadow_replay,
                format_edge_rules_text,
            )
            edge_result = build_and_save_edge_rules(
                days=args.edge_days,
                mode="both",
                min_samples=args.edge_min_samples,
                min_days=args.edge_min_days,
            )
            review["candidate_edge_rules"] = {
                mode: {
                    "saved_paths": payload.get("saved_paths", {}),
                    "candidate_count": payload.get("candidate_count"),
                    "price_summary": payload.get("price_summary", {}),
                    "baseline": payload.get("baseline", {}),
                    "top_rules": (payload.get("rules") or [])[:5],
                    "negative_rules": (payload.get("negative_rules") or [])[:5],
                    "chase_policy": payload.get("chase_policy", {}),
                }
                for mode, payload in edge_result.items()
            }
            try:
                review["scoring_shadow_replay"] = build_scoring_shadow_replay(
                    output_dir=OUTPUT_DIR,
                    start_date="20260601",
                )
            except Exception as shadow_exc:
                review["scoring_shadow_replay"] = {
                    "status": "failed",
                    "error": f"{type(shadow_exc).__name__}: {shadow_exc}"[:500],
                }
                exit_reasons.append(f"shadow_replay_failed:{type(shadow_exc).__name__}")
            edge_text = format_edge_rules_text(edge_result)
            shadow = review.get("scoring_shadow_replay") or {}
            if shadow.get("old") and shadow.get("new"):
                old_metrics = shadow.get("old") or {}
                new_metrics = shadow.get("new") or {}
                shadow_lines = ["影子回放（2026-06起，全候选池，逐日无未来数据）"]
                for horizon in (1, 3, 5):
                    shadow_lines.append(
                        f"D+{horizon}: 旧均收益{old_metrics.get(f'd{horizon}_avg_return_pct')}%/"
                        f"胜率{old_metrics.get(f'd{horizon}_win_rate_pct')}% -> "
                        f"新均收益{new_metrics.get(f'd{horizon}_avg_return_pct')}%/"
                        f"胜率{new_metrics.get(f'd{horizon}_win_rate_pct')}%"
                    )
                shadow_lines.append(
                    f"高追涨占比: {old_metrics.get('high_chase_ratio_pct')}% -> "
                    f"{new_metrics.get('high_chase_ratio_pct')}%"
                )
                edge_text = edge_text + "\n" + "\n".join(shadow_lines)
        except Exception as exc:
            review["candidate_edge_rules"] = {"status": "failed", "error": str(exc)[:500]}
            edge_text = f"📊 全候选池历史优势组合复盘失败: {str(exc)[:200]}"
            exit_reasons.append(f"edge_rules_failed:{type(exc).__name__}")
    if not args.no_save:
        dated, latest = save_review(review, memory_backfill_review=memory_backfill_review)
        print(f"saved: {dated}")
        print(f"saved: {latest}")
    text = format_feishu_text(review)
    if edge_text:
        text = text + "\n\n" + edge_text
    print(text)
    if args.push:
        ok, reason = push_feishu_text_detailed(text)
        print(f"feishu_push={'ok' if ok else 'failed'} reason={reason}")
        if not ok:
            exit_reasons.append(f"feishu_push_failed:{reason[:120]}")
    llm = review.get("llm_deep_analysis") or {}
    if not args.no_llm and llm.get("status") != "ok":
        exit_reasons.append(f"llm_{llm.get('status') or 'missing'}:{str(llm.get('error') or '')[:120]}")
    print(f"elapsed={time.time() - start:.1f}s")
    if exit_reasons:
        print("task_status=failed reasons=" + " | ".join(exit_reasons))
        return 2
    print("task_status=ok")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
