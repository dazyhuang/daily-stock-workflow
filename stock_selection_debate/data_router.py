"""Data-router metadata for daily stock-selection packets.

Existing fetchers still do the actual IO. This module standardizes the
observable contract so downstream prompts, reports, and resume logic can see
which route supplied each data class and whether it is complete enough.
"""

from __future__ import annotations

import re
import sys
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List


DATA_ROUTER_VERSION = "2026-07-17.freshness-contract-v3"

ROUTE_ORDER: Dict[str, List[str]] = {
    "kline": ["debate_data_cache", "xqshare", "qmt_http", "workflow", "mx-data", "akshare", "tencent"],
    "money_flow": ["mx", "mx-data", "eastmoney", "ak", "akshare", "ak_rank", "pool_seed"],
    "financial": ["xqshare", "phase1_cache", "cache", "akshare"],
    "sector": ["xqshare", "sector_cache", "mx-data", "akshare", "eastmoney"],
    "news": ["mx-search", "akshare", "debate_data_cache"],
}


def _today_key() -> str:
    return date.today().strftime("%Y%m%d")


def _expected_market_key() -> str:
    today = date.today()
    if datetime.now().hour >= 17:
        candidate = today
    else:
        candidate = today - timedelta(days=1)
    for _ in range(15):
        key = candidate.strftime("%Y%m%d")
        try:
            shared = Path.home() / ".openclaw" / "agents" / "shared"
            if str(shared) not in sys.path:
                sys.path.insert(0, str(shared))
            from trading_calendar import get_a_share_trading_day_status
            if get_a_share_trading_day_status(key).get("is_trading_day"):
                return key
        except Exception:
            if candidate.weekday() < 5:
                return key
        candidate -= timedelta(days=1)
    return candidate.strftime("%Y%m%d")


def _contract_is_stale(category: str, as_key: str) -> bool:
    if not as_key:
        return False
    if category in {"kline", "money_flow"}:
        return as_key < _expected_market_key()
    try:
        observed = datetime.strptime(as_key, "%Y%m%d").date()
        age_days = (date.today() - observed).days
    except Exception:
        return False
    if category == "financial":
        return age_days > 200
    if category == "sector":
        return age_days > 30
    if category == "news":
        return age_days > 7
    return False


def _as_key(value: Any) -> str:
    digits = re.sub(r"\D", "", str(value or ""))
    return digits[:8] if len(digits) >= 8 else ""


def _route_position(category: str, source: str) -> int | None:
    order = ROUTE_ORDER.get(category, [])
    source_text = str(source or "").lower()
    for idx, name in enumerate(order, 1):
        if name.lower() in source_text:
            return idx
    return None


def normalize_contract_item(category: str, item: Dict[str, Any] | None) -> Dict[str, Any]:
    src = dict(item or {})
    source = str(src.get("source") or "none")
    status = str(src.get("status") or "unknown")
    checked_at = src.get("checked_at") or ""
    content_as_of = src.get("content_as_of") or src.get("latest_item_at") or ""
    as_of = (checked_at if category == "news" and checked_at else src.get("as_of")) or ""
    as_key = _as_key(as_of)
    stale = bool(src.get("is_stale")) or _contract_is_stale(category, as_key)
    content_key = _as_key(content_as_of or (src.get("as_of") if category == "news" else ""))
    content_is_old = bool(category == "news" and content_key and _contract_is_stale("news", content_key))
    flags = []
    for flag in src.get("quality_flags") or []:
        if flag not in flags:
            flags.append(flag)
    if not as_key and status == "ok":
        status = "partial"
        if "DATE_UNKNOWN" not in flags:
            flags.append("DATE_UNKNOWN")
    if category == "news" and checked_at and content_is_old:
        status = "checked_fresh_no_recent_items"
        if "NEWS_NO_RECENT_ITEMS" not in flags:
            flags.append("NEWS_NO_RECENT_ITEMS")
    position = _route_position(category, source)
    out = {
        "source": source,
        "status": status,
        "error": str(src.get("error") or "")[:300],
        "as_of": as_of,
        "age_minutes": src.get("age_minutes"),
        "is_stale": stale,
        "quality_flags": flags,
        "route_order": ROUTE_ORDER.get(category, []),
        "route_position": position,
        "router_version": DATA_ROUTER_VERSION,
    }
    if category == "news":
        out["checked_at"] = checked_at or as_of
        out["content_as_of"] = content_as_of or (src.get("as_of") or "")
        out["content_is_old"] = content_is_old
    for key in ("field_status", "field_sources", "field_as_of", "units", "diagnostics"):
        if isinstance(src.get(key), dict):
            out[key] = dict(src[key])
    if position and position > 1:
        out["fallback_used"] = True
    return out


def normalize_data_contract(contract: Dict[str, Any] | None) -> Dict[str, Dict[str, Any]]:
    contract = contract or {}
    normalized: Dict[str, Dict[str, Any]] = {}
    for category in ("kline", "money_flow", "financial", "sector", "news"):
        normalized[category] = normalize_contract_item(category, contract.get(category) or {})
    for category, item in contract.items():
        if category not in normalized and isinstance(item, dict):
            normalized[category] = normalize_contract_item(category, item)
    return normalized


def summarize_data_router(contract: Dict[str, Dict[str, Any]]) -> Dict[str, Any]:
    counts: Dict[str, int] = {}
    fallback_used = 0
    stale = 0
    missing: List[str] = []
    partial: List[str] = []
    for category, item in (contract or {}).items():
        status = str((item or {}).get("status") or "unknown")
        counts[status] = counts.get(status, 0) + 1
        if (item or {}).get("fallback_used"):
            fallback_used += 1
        if (item or {}).get("is_stale"):
            stale += 1
        if status == "missing":
            missing.append(category)
        elif status == "partial":
            partial.append(category)
    return {
        "version": DATA_ROUTER_VERSION,
        "status_counts": counts,
        "fallback_used_count": fallback_used,
        "stale_count": stale,
        "missing_categories": missing,
        "partial_categories": partial,
    }


def _packet_has_kline(packet: Dict[str, Any]) -> bool:
    return any(isinstance(x, dict) and x.get("close") not in (None, "") for x in (packet.get("kline_raw") or []))


def _packet_has_money_flow(packet: Dict[str, Any]) -> bool:
    mf = packet.get("money_flow") or {}
    return any(
        mf.get(k) is not None
        for k in (
            "main_net_flow",
            "super_net_flow",
            "ddx_5",
            "ddy_10",
            "main_net_flow_5d",
            "main_net_flow_10d",
        )
    )


def _packet_has_financial(packet: Dict[str, Any]) -> bool:
    fin = packet.get("financial") or {}
    return any(fin.get(k) is not None for k in ("roe", "revenue_growth", "net_profit_growth", "pe_ttm", "pb"))


def _upgrade_contract_from_packet(packet: Dict[str, Any], contract: Dict[str, Dict[str, Any]]) -> None:
    checks = {
        "kline": _packet_has_kline(packet),
        "money_flow": _packet_has_money_flow(packet),
        "financial": _packet_has_financial(packet),
        "sector": bool(packet.get("sector")),
        "news": bool(packet.get("news")),
    }
    for category, has_value in checks.items():
        item = contract.setdefault(category, normalize_contract_item(category, {}))
        if has_value and item.get("status") in {"missing", "unknown", ""}:
            item["status"] = "partial"
            if item.get("source") in {"", "none"}:
                item["source"] = "packet"
            flags = list(item.get("quality_flags") or [])
            if "DATE_UNKNOWN" not in flags:
                flags.append("DATE_UNKNOWN")
            item["quality_flags"] = flags
        if category == "kline" and has_value:
            count = sum(1 for x in (packet.get("kline_raw") or []) if isinstance(x, dict) and x.get("close") not in (None, ""))
            if count < 60:
                item["status"] = "partial"
                flags = list(item.get("quality_flags") or [])
                if "KLINE_SHORT" not in flags:
                    flags.append("KLINE_SHORT")
                item["quality_flags"] = flags


def attach_data_router_metadata(packet: Dict[str, Any]) -> Dict[str, Any]:
    contract = normalize_data_contract(packet.get("data_contract") or {})
    _upgrade_contract_from_packet(packet, contract)
    packet["data_contract"] = contract
    packet["data_router_version"] = DATA_ROUTER_VERSION
    packet["data_router_summary"] = summarize_data_router(contract)
    return packet
