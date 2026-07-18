"""Canonical technical indicators shared by selection workflow components."""

from __future__ import annotations

from typing import Any, Dict, Iterable, List, Optional


def _finite_floats(values: Iterable[Any]) -> List[float]:
    result: List[float] = []
    for value in values:
        try:
            number = float(value)
        except (TypeError, ValueError):
            continue
        if number == number and number not in (float("inf"), float("-inf")):
            result.append(number)
    return result


def ema_series(values: Iterable[Any], span: int) -> List[float]:
    """Return a full EMA series using the first value as the seed."""
    numbers = _finite_floats(values)
    if not numbers or span <= 0:
        return []
    alpha = 2.0 / (span + 1.0)
    output = [numbers[0]]
    for value in numbers[1:]:
        output.append(alpha * value + (1.0 - alpha) * output[-1])
    return output


def compute_macd(values: Iterable[Any]) -> Dict[str, Any]:
    """Compute DIF/DEA/histogram plus state and actual crossover event."""
    closes = _finite_floats(values)
    if len(closes) < 35:
        return {
            "dif": None,
            "dea": None,
            "hist": None,
            "state": "数据不足",
            "cross_event": "无",
            "hist_slope": "无法判断",
        }

    ema12 = ema_series(closes, 12)
    ema26 = ema_series(closes, 26)
    dif_series = [fast - slow for fast, slow in zip(ema12, ema26)]
    dea_series = ema_series(dif_series, 9)
    hist_series = [dif - dea for dif, dea in zip(dif_series, dea_series)]

    dif = dif_series[-1]
    dea = dea_series[-1]
    hist = hist_series[-1]
    prev_dif = dif_series[-2]
    prev_dea = dea_series[-2]
    cross_event = "无"
    if prev_dif <= prev_dea and dif > dea:
        cross_event = "金叉"
    elif prev_dif >= prev_dea and dif < dea:
        cross_event = "死叉"

    hist_delta = hist - hist_series[-2]
    epsilon = max(1e-8, abs(hist_series[-2]) * 0.01)
    hist_slope = "扩张" if hist_delta > epsilon else "收缩" if hist_delta < -epsilon else "持平"
    return {
        "dif": round(dif, 4),
        "dea": round(dea, 4),
        "hist": round(hist, 4),
        "state": "多头" if dif > dea else "空头",
        "cross_event": cross_event,
        "hist_slope": hist_slope,
    }


def legacy_macd_signal(macd: Dict[str, Any]) -> str:
    """Keep a readable legacy field without mislabelling a persistent state as a new cross."""
    event = str(macd.get("cross_event") or "")
    if event in {"金叉", "死叉"}:
        return event
    state = str(macd.get("state") or "")
    return "多头区" if state == "多头" else "空头区" if state == "空头" else "数据不足"
