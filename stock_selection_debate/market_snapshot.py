"""Verified market snapshot helpers for stock-selection debate packets.

The snapshot is intentionally deterministic: it only uses fields already in the
debate packet, mostly the cached daily K-line bars. LLM nodes can cite this
object as the canonical technical fact sheet instead of inferring indicators
from prose.
"""

from __future__ import annotations

import re
from datetime import date
from typing import Any, Dict, Iterable, List, Optional

from .technical_indicators import compute_macd


MARKET_SNAPSHOT_VERSION = "2026-07-09.verified-market-snapshot-v1"


def _safe_float(value: Any, default: Optional[float] = None) -> Optional[float]:
    try:
        if value in (None, ""):
            return default
        return float(value)
    except (TypeError, ValueError):
        return default


def _round(value: Any, ndigits: int = 2) -> Optional[float]:
    num = _safe_float(value, None)
    return round(num, ndigits) if num is not None else None


def _normalize_date(value: Any) -> str:
    digits = re.sub(r"\D", "", str(value or ""))
    if len(digits) >= 8:
        return digits[:8]
    return str(value or "")[:10]


def _bars_from_packet(packet: Dict[str, Any]) -> List[Dict[str, Any]]:
    rows = []
    for item in packet.get("kline_raw") or []:
        if not isinstance(item, dict):
            continue
        close = _safe_float(item.get("close"), None)
        if close is None or close <= 0:
            continue
        row = dict(item)
        row["close"] = close
        for key in ("open", "high", "low", "volume"):
            val = _safe_float(row.get(key), None)
            if val is not None:
                row[key] = val
        row["date"] = _normalize_date(row.get("date"))
        rows.append(row)
    return rows[-160:]


def _ma(values: List[float], window: int) -> Optional[float]:
    if len(values) < window:
        return None
    return sum(values[-window:]) / window


def _pct(now: Optional[float], before: Optional[float]) -> Optional[float]:
    if now is None or before in (None, 0):
        return None
    return (now / before - 1) * 100


def _ema(values: List[float], span: int) -> Optional[float]:
    if not values:
        return None
    alpha = 2 / (span + 1)
    ema = values[0]
    for value in values[1:]:
        ema = alpha * value + (1 - alpha) * ema
    return ema


def _macd(values: List[float]) -> Dict[str, Optional[float] | str]:
    result = compute_macd(values)
    return {
        "dif": result.get("dif"),
        "dea": result.get("dea"),
        "hist": result.get("hist"),
        "signal": result.get("state"),
        "state": result.get("state"),
        "cross_event": result.get("cross_event"),
        "hist_slope": result.get("hist_slope"),
    }


def _rsi(values: List[float], period: int = 14) -> Optional[float]:
    if len(values) <= period:
        return None
    deltas = [values[i] - values[i - 1] for i in range(1, len(values))]
    recent = deltas[-period:]
    gains = [x if x > 0 else 0.0 for x in recent]
    losses = [-x if x < 0 else 0.0 for x in recent]
    avg_gain = sum(gains) / period
    avg_loss = sum(losses) / period
    if avg_loss == 0:
        return 100.0
    rs = avg_gain / avg_loss
    return 100 - 100 / (1 + rs)


def _kdj(rows: List[Dict[str, Any]], period: int = 9) -> Dict[str, Optional[float] | str]:
    if len(rows) < period:
        return {"k": None, "d": None, "j": None, "signal": "数据不足"}
    k = 50.0
    d = 50.0
    for idx in range(period - 1, len(rows)):
        window = rows[idx - period + 1 : idx + 1]
        highs = [_safe_float(x.get("high"), None) for x in window]
        lows = [_safe_float(x.get("low"), None) for x in window]
        highs = [x for x in highs if x is not None]
        lows = [x for x in lows if x is not None]
        close = _safe_float(rows[idx].get("close"), None)
        if close is None or not highs or not lows or max(highs) == min(lows):
            continue
        rsv = (close - min(lows)) / (max(highs) - min(lows)) * 100
        k = 2 / 3 * k + 1 / 3 * rsv
        d = 2 / 3 * d + 1 / 3 * k
    j = 3 * k - 2 * d
    if k > d and k < 85:
        signal = "偏多"
    elif k < d and k > 15:
        signal = "偏空"
    elif k >= 85:
        signal = "高位"
    else:
        signal = "低位"
    return {"k": _round(k, 2), "d": _round(d, 2), "j": _round(j, 2), "signal": signal}


def _atr(rows: List[Dict[str, Any]], period: int = 14) -> Optional[float]:
    if len(rows) <= period:
        return None
    true_ranges: List[float] = []
    for idx in range(1, len(rows)):
        high = _safe_float(rows[idx].get("high"), None)
        low = _safe_float(rows[idx].get("low"), None)
        prev_close = _safe_float(rows[idx - 1].get("close"), None)
        if high is None or low is None or prev_close is None:
            continue
        true_ranges.append(max(high - low, abs(high - prev_close), abs(low - prev_close)))
    if len(true_ranges) < period:
        return None
    return sum(true_ranges[-period:]) / period


def _limit_pct(stock_code: str, name: str) -> float:
    code = str(stock_code or "")
    upper_name = str(name or "").upper()
    if "ST" in upper_name:
        return 0.05
    if code.startswith(("300", "301", "688", "689")):
        return 0.20
    if code.startswith(("8", "4")):
        return 0.30
    return 0.10


def _ma_alignment(ma: Dict[str, Optional[float]]) -> str:
    ma5, ma10, ma20, ma60 = ma.get("ma5"), ma.get("ma10"), ma.get("ma20"), ma.get("ma60")
    if all(x is not None for x in (ma5, ma10, ma20, ma60)):
        if ma5 > ma10 > ma20 > ma60:
            return "多头排列"
        if ma5 < ma10 < ma20 < ma60:
            return "空头排列"
    return "混合"


def build_verified_market_snapshot(packet: Dict[str, Any]) -> Dict[str, Any]:
    rows = _bars_from_packet(packet)
    contract = packet.get("data_contract") or {}
    kline_contract = contract.get("kline") or {}
    flags = list(dict.fromkeys(packet.get("data_quality_flags") or []))
    missing_fields: List[str] = []
    if not rows:
        return {
            "version": MARKET_SNAPSHOT_VERSION,
            "status": "missing",
            "source": kline_contract.get("source") or "none",
            "as_of": date.today().strftime("%Y%m%d"),
            "bar_count": 0,
            "quality_flags": list(dict.fromkeys(flags + ["KLINE_MISSING"])),
            "missing_fields": ["daily_ohlc"],
            "evidence_fields": {},
        }

    closes = [_safe_float(x.get("close"), 0.0) or 0.0 for x in rows]
    highs = [_safe_float(x.get("high"), x.get("close")) for x in rows]
    lows = [_safe_float(x.get("low"), x.get("close")) for x in rows]
    volumes = [_safe_float(x.get("volume"), None) for x in rows]
    latest = rows[-1]
    prev = rows[-2] if len(rows) >= 2 else {}
    latest_close = _safe_float(latest.get("close"), None)
    prev_close = _safe_float(prev.get("close"), None)
    ma = {f"ma{n}": _round(_ma(closes, n), 2) for n in (5, 10, 20, 60, 120)}
    alignment = _ma_alignment(ma)
    pct_1d = _pct(latest_close, prev_close)
    pct_5d = _pct(latest_close, closes[-6] if len(closes) >= 6 else None)
    pct_10d = _pct(latest_close, closes[-11] if len(closes) >= 11 else None)
    pct_20d = _pct(latest_close, closes[-21] if len(closes) >= 21 else None)
    high20 = max([x for x in highs[-20:] if x is not None], default=None)
    low20 = min([x for x in lows[-20:] if x is not None], default=None)
    close_pos = None
    if latest_close is not None and high20 is not None and low20 is not None and high20 > low20:
        close_pos = (latest_close - low20) / (high20 - low20) * 100
    vol_recent = [x for x in volumes[-5:] if x is not None]
    vol_base = [x for x in volumes[-20:] if x is not None]
    vol_ratio = None
    if vol_recent and vol_base and sum(vol_base) > 0:
        vol_ratio = (sum(vol_recent) / len(vol_recent)) / (sum(vol_base) / len(vol_base))
    rsi14 = _round(_rsi(closes, 14), 2)
    macd = _macd(closes)
    kdj = _kdj(rows)
    atr14 = _round(_atr(rows, 14), 3)
    limit = _limit_pct(packet.get("stock_code") or packet.get("stock"), packet.get("name") or packet.get("stock_name"))
    limit_up = _round(prev_close * (1 + limit), 2) if prev_close else None
    limit_down = _round(prev_close * (1 - limit), 2) if prev_close else None
    if len(rows) < 60:
        missing_fields.append("daily_ohlc_60")
    if ma["ma120"] is None:
        missing_fields.append("ma120")
    status = "ok" if len(rows) >= 60 and not missing_fields else "partial"
    if kline_contract.get("status") == "missing":
        status = "missing"
    if alignment == "多头排列" and (pct_5d or 0) > 0:
        trend_state = "多头上行"
    elif alignment == "空头排列" and (pct_5d or 0) < 0:
        trend_state = "空头下行"
    elif (pct_20d or 0) > 8 and (vol_ratio or 0) >= 1:
        trend_state = "强势震荡"
    elif (pct_20d or 0) < -8:
        trend_state = "弱势修复"
    else:
        trend_state = "震荡"
    evidence = {
        "latest_close": _round(latest_close, 2),
        "pct_change_1d": _round(pct_1d, 2),
        "pct_change_5d": _round(pct_5d, 2),
        "pct_change_20d": _round(pct_20d, 2),
        "ma_alignment": alignment,
        "close_position_20d": _round(close_pos, 2),
        "volume_ratio_5_20": _round(vol_ratio, 2),
        "rsi14": rsi14,
        "macd_signal": macd.get("signal"),
        "kdj_signal": kdj.get("signal"),
        "atr14": atr14,
    }
    return {
        "version": MARKET_SNAPSHOT_VERSION,
        "status": status,
        "source": kline_contract.get("source") or "kline_raw",
        "as_of": latest.get("date") or kline_contract.get("as_of") or date.today().strftime("%Y%m%d"),
        "bar_count": len(rows),
        "latest_date": latest.get("date"),
        "latest_close": _round(latest_close, 2),
        "previous_close": _round(prev_close, 2),
        "open": _round(latest.get("open"), 2),
        "high": _round(latest.get("high"), 2),
        "low": _round(latest.get("low"), 2),
        "volume": _round(latest.get("volume"), 2),
        "pct_change_1d": _round(pct_1d, 2),
        "pct_change_5d": _round(pct_5d, 2),
        "pct_change_10d": _round(pct_10d, 2),
        "pct_change_20d": _round(pct_20d, 2),
        "ma": ma,
        "ma_alignment": alignment,
        "trend_state": trend_state,
        "high_20d": _round(high20, 2),
        "low_20d": _round(low20, 2),
        "close_position_20d": _round(close_pos, 2),
        "volume_ratio_5_20": _round(vol_ratio, 2),
        "macd": macd,
        "kdj": kdj,
        "rsi14": rsi14,
        "atr14": atr14,
        "limit_pct": round(limit * 100, 2),
        "limit_up": limit_up,
        "limit_down": limit_down,
        "quality_flags": flags,
        "missing_fields": missing_fields,
        "evidence_fields": {k: v for k, v in evidence.items() if v not in (None, "")},
    }


def attach_verified_market_snapshot(packet: Dict[str, Any]) -> Dict[str, Any]:
    packet["verified_market_snapshot"] = build_verified_market_snapshot(packet)
    packet["market_snapshot_version"] = MARKET_SNAPSHOT_VERSION
    return packet
