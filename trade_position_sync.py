#!/usr/bin/env python3
"""Keep local trades.json aligned with the mock trading account positions."""

import json
import os
import sys
from copy import deepcopy
from datetime import date, datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import requests


BASE_DIR = Path(__file__).parent
OUTPUT_DIR = BASE_DIR / "output"
TRADES_FILE = OUTPUT_DIR / "trades.json"
ENV_FILE = BASE_DIR / ".env"
DEFAULT_API_URL = "https://mkapi2.dfcfs.com/finskillshub"


def load_local_env(path: Path = ENV_FILE) -> None:
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


def _api_key() -> str:
    load_local_env()
    return os.environ.get("MX_APIKEY", "").strip()


def _api_url() -> str:
    load_local_env()
    return os.environ.get("MX_API_URL", DEFAULT_API_URL).rstrip("/")


def mx_api_post(endpoint: str, payload: Dict[str, Any], timeout: int = 30) -> Dict[str, Any]:
    # 盘中任务会同时读持仓/委托；优先复用 intraday_executor 的跨进程缓存与限速冷却。
    try:
        main_mod = sys.modules.get("__main__")
        shared_mx_api_post = getattr(main_mod, "mx_api_post", None)
        if callable(shared_mx_api_post) and getattr(main_mod, "BASE_DIR", None) == BASE_DIR:
            return shared_mx_api_post(endpoint, payload, retries=1)
    except Exception:
        pass
    try:
        from intraday_executor import mx_api_post as shared_mx_api_post
        if shared_mx_api_post is not mx_api_post:
            return shared_mx_api_post(endpoint, payload, retries=1)
    except Exception:
        pass

    key = _api_key()
    if not key:
        raise RuntimeError("MX_APIKEY 未设置，无法读取模拟账户")
    response = requests.post(
        f"{_api_url()}{endpoint}",
        headers={"Content-Type": "application/json", "apikey": key},
        json=payload,
        timeout=timeout,
    )
    response.raise_for_status()
    data = response.json() or {}
    code = str(data.get("code") or data.get("status") or "")
    if data.get("success") is False or (code and code != "200"):
        raise RuntimeError(f"模拟账户接口失败 {endpoint}: {data.get('message') or data}")
    return data


def _price(value: Any, dec: Any, default_dec: int = 2) -> float:
    try:
        return float(value or 0) / pow(10, int(dec if dec is not None else default_dec))
    except Exception:
        return 0.0


def fetch_mock_positions(*, include_zero: bool = False) -> List[Dict[str, Any]]:
    data = mx_api_post("/api/claw/mockTrading/positions", {"moneyUnit": 1})
    pos_data = data.get("data") or {}
    raw_positions = pos_data.get("posList") or []
    result = []
    for p in raw_positions:
        quantity = int(p.get("count", p.get("totalQuantity", 0)) or 0)
        if quantity <= 0 and not include_zero:
            continue
        result.append({
            "stock": p.get("secCode") or p.get("stockCode") or "",
            "name": p.get("secName") or p.get("stockName") or "",
            "quantity": quantity,
            "avail_quantity": int(p.get("availCount", p.get("availQuantity", 0)) or 0),
            "cost_price": _price(p.get("costPrice"), p.get("costPriceDec"), 3),
            "current_price": _price(p.get("price"), p.get("priceDec"), 2),
            "raw": p,
        })
    return [p for p in result if p["stock"]]


def load_trades(path: Path = TRADES_FILE) -> Dict[str, Any]:
    if not path.exists():
        return {"records": [], "updated": ""}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {"records": [], "updated": ""}


def save_trades(data: Dict[str, Any], path: Path = TRADES_FILE) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    data["updated"] = datetime.now().isoformat()
    tmp = path.with_name(f"{path.name}.{os.getpid()}.tmp")
    tmp.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
    tmp.replace(path)


def _tracked_remaining(record: Dict[str, Any]) -> int:
    buy_records = record.get("buy_records") or []
    if buy_records:
        return sum(max(0, int(br.get("remaining", 0) or 0)) for br in buy_records)
    return max(0, int(record.get("remaining_quantity", 0) or 0))


def aggregate_trades_positions(trades: Optional[Dict[str, Any]] = None) -> Dict[str, Dict[str, Any]]:
    trades = trades if trades is not None else load_trades()
    agg: Dict[str, Dict[str, Any]] = {}
    for rec in trades.get("records", []) or []:
        stock = rec.get("stock", "")
        qty = _tracked_remaining(rec)
        if not stock or qty <= 0:
            continue
        item = agg.setdefault(stock, {"qty": 0, "name": rec.get("name", stock), "records": 0, "cost_value": 0.0})
        item["qty"] += qty
        item["records"] += 1
        lots = rec.get("buy_records") or []
        if lots:
            for lot in lots:
                lot_qty = max(0, int(lot.get("remaining", 0) or 0))
                item["cost_value"] += lot_qty * float(lot.get("price", 0) or 0)
        else:
            item["cost_value"] += qty * float(rec.get("buy_price", 0) or 0)
        if rec.get("name"):
            item["name"] = rec.get("name")
    for item in agg.values():
        qty = int(item.get("qty", 0) or 0)
        item["cost_price"] = item["cost_value"] / qty if qty > 0 else 0.0
    return agg


def aggregate_mock_positions(positions: Optional[List[Dict[str, Any]]] = None) -> Dict[str, Dict[str, Any]]:
    positions = positions if positions is not None else fetch_mock_positions()
    return {
        p["stock"]: {
            "qty": int(p.get("quantity", 0) or 0),
            "name": p.get("name") or p["stock"],
            "cost_price": float(p.get("cost_price", 0) or 0),
            "current_price": float(p.get("current_price", 0) or 0),
            "avail_quantity": int(p.get("avail_quantity", 0) or 0),
        }
        for p in positions
        if int(p.get("quantity", 0) or 0) > 0
    }


def _record_date(record: Dict[str, Any]) -> str:
    return str(record.get("buy_date") or "")


def _sync_events(record: Dict[str, Any]) -> List[Dict[str, Any]]:
    events = record.setdefault("sync_events", [])
    if not isinstance(events, list):
        events = []
        record["sync_events"] = events
    return events


def _zero_record(record: Dict[str, Any], actual_qty: int, source: str) -> None:
    before = _tracked_remaining(record)
    if before <= 0:
        return
    record["remaining_quantity"] = 0
    for br in record.get("buy_records") or []:
        br["remaining"] = 0
    _sync_events(record).append({
        "date": date.today().isoformat(),
        "source": source,
        "before": before,
        "actual_quantity": actual_qty,
        "note": "模拟账户无持仓，本地剩余归零",
    })


def _ensure_buy_records(record: Dict[str, Any], fallback_price: float, source: str) -> List[Dict[str, Any]]:
    lots = record.get("buy_records")
    if not isinstance(lots, list):
        lots = []
        record["buy_records"] = lots
    if lots:
        return lots
    rem = max(0, int(record.get("remaining_quantity", 0) or 0))
    if rem <= 0:
        return lots
    px = float(record.get("buy_price", 0) or fallback_price or 0)
    if not record.get("buy_price") and px > 0:
        record["buy_price"] = px
    lots.append({
        "date": record.get("buy_date") or date.today().isoformat(),
        "price": px,
        "quantity": rem,
        "remaining": rem,
        "source": record.get("source") or source,
    })
    return lots


def _merge_records_into_active(records: List[Dict[str, Any]], active: Dict[str, Any], source: str, fallback_price: float) -> None:
    active_lots = _ensure_buy_records(active, fallback_price, source)
    active_sells = active.setdefault("sells", [])
    # bug 修 (2026-06-02): 加 buy_records 去重，否则 reconcile 多次跑会重复 append
    seen_lot = {
        (
            str(l.get("date", "")),
            float(l.get("price", 0) or 0),
            int(l.get("quantity", 0) or 0),
        )
        for l in active_lots
    }
    seen_sell = {
        (
            str(s.get("date", "")),
            float(s.get("price", 0) or 0),
            int(s.get("quantity", 0) or 0),
            str(s.get("reason", "")),
        )
        for s in active_sells
    }
    for rec in records:
        if rec is active:
            continue
        for lot in _ensure_buy_records(rec, fallback_price, source):
            qty = max(0, int(lot.get("quantity", 0) or lot.get("remaining", 0) or 0))
            rem = max(0, int(lot.get("remaining", 0) or 0))
            if qty <= 0 and rem <= 0:
                continue
            lot_key = (
                str(lot.get("date", "") or rec.get("buy_date") or date.today().isoformat()),
                float(lot.get("price", 0) or rec.get("buy_price", 0) or fallback_price or 0),
                qty if qty > 0 else rem,
            )
            if lot_key in seen_lot:
                continue
            seen_lot.add(lot_key)
            active_lots.append({
                "date": lot.get("date") or rec.get("buy_date") or date.today().isoformat(),
                "price": float(lot.get("price", 0) or rec.get("buy_price", 0) or fallback_price or 0),
                "quantity": qty if qty > 0 else rem,
                "remaining": rem,
                "source": lot.get("source") or rec.get("source") or source,
                "executed_tp_tiers": list(lot.get("executed_tp_tiers") or []),
            })
        for sell in rec.get("sells") or []:
            key = (
                str(sell.get("date", "")),
                float(sell.get("price", 0) or 0),
                int(sell.get("quantity", 0) or 0),
                str(sell.get("reason", "")),
            )
            if key not in seen_sell:
                active_sells.append(sell)
                seen_sell.add(key)
    active_lots.sort(key=lambda lot: str(lot.get("date") or ""))


def _align_record_to_actual_quantity(record: Dict[str, Any], actual_qty: int, fallback_price: float, source: str) -> None:
    before = _tracked_remaining(record)
    if before == actual_qty:
        return
    sold_qty = sum(int(s.get("quantity", 0) or 0) for s in record.get("sells", []) or [])
    inherited_tiers = []
    for sell in record.get("sells", []) or []:
        reason = str(sell.get("reason", ""))
        if "止盈第1档" in reason and 1 not in inherited_tiers:
            inherited_tiers.append(1)
        if "止盈第2档" in reason and 2 not in inherited_tiers:
            inherited_tiers.append(2)
        if "止盈第3档" in reason and 3 not in inherited_tiers:
            inherited_tiers.append(3)
    lots = _ensure_buy_records(record, fallback_price, source)
    gap = int(actual_qty) - int(before)
    if gap < 0:
        to_reduce = -gap
        reduce_order = (
            [lot for lot in reversed(lots) if str(lot.get("source") or "") == "position_reconcile"]
            + [lot for lot in reversed(lots) if str(lot.get("source") or "") != "position_reconcile"]
        )
        for lot in reduce_order:
            if to_reduce <= 0:
                break
            rem = max(0, int(lot.get("remaining", 0) or 0))
            if rem <= 0:
                continue
            cut = min(rem, to_reduce)
            lot["remaining"] = rem - cut
            to_reduce -= cut
    elif gap > 0:
        if lots:
            last = lots[-1]
            last_rem = max(0, int(last.get("remaining", 0) or 0))
            last_qty = max(0, int(last.get("quantity", 0) or 0))
            last["remaining"] = last_rem + gap
            last["quantity"] = max(last_qty, last_rem + gap)
        else:
            price = float(record.get("buy_price", 0) or fallback_price or 0)
            lots.append({
                "date": record.get("buy_date") or date.today().isoformat(),
                "price": price,
                "quantity": gap,
                "remaining": gap,
                "source": source,
                "executed_tp_tiers": inherited_tiers,
            })
            if not record.get("buy_price") and price > 0:
                record["buy_price"] = price
    record["remaining_quantity"] = actual_qty
    record["quantity"] = max(int(record.get("quantity", 0) or 0), sold_qty + actual_qty, actual_qty)
    _sync_events(record).append({
        "date": date.today().isoformat(),
        "source": source,
        "before": before,
        "actual_quantity": actual_qty,
        "note": "按模拟账户数量校准（保留原始买入批次价格）",
    })


def reconcile_trades_with_positions(
    trades: Optional[Dict[str, Any]] = None,
    positions: Optional[List[Dict[str, Any]]] = None,
    *,
    source: str = "position_sync",
) -> Tuple[Dict[str, Any], Dict[str, Any]]:
    trades = deepcopy(trades if trades is not None else load_trades())
    trades.setdefault("records", [])
    actual = aggregate_mock_positions(positions)

    records_by_stock: Dict[str, List[Dict[str, Any]]] = {}
    for rec in trades.get("records", []) or []:
        stock = rec.get("stock", "")
        if stock:
            records_by_stock.setdefault(stock, []).append(rec)

    before = aggregate_trades_positions(trades)
    report = {
        "source": source,
        "before": {k: v["qty"] for k, v in before.items()},
        "actual": {k: v["qty"] for k, v in actual.items()},
        "fixed": [],
        "created": [],
    }

    for stock in sorted(set(records_by_stock) | set(actual)):
        records = records_by_stock.get(stock, [])
        actual_item = actual.get(stock)
        actual_qty = int((actual_item or {}).get("qty", 0) or 0)
        actual_cost = float((actual_item or {}).get("cost_price", 0) or 0)
        tracked_qty = sum(_tracked_remaining(r) for r in records)
        if tracked_qty == actual_qty and (tracked_qty > 0 or actual_item is None):
            if actual_item and actual_qty > 0 and records:
                active = sorted(records, key=_record_date)[-1]
                price_missing = not active.get("buy_price") and actual_cost > 0
                _ensure_buy_records(active, actual_cost, source)
                name_changed = bool(actual_item.get("name")) and active.get("name") != actual_item.get("name")
                if len(records) > 1 or name_changed or price_missing:
                    report["fixed"].append({
                        "stock": stock,
                        "name": actual_item.get("name") or active.get("name") or stock,
                        "before": tracked_qty,
                        "actual": actual_qty,
                        "reason": "重复记录/名称/买入价校准",
                    })
                    _merge_records_into_active(records, active, source, actual_cost)
                    for rec in records:
                        if rec is not active:
                            _zero_record(rec, actual_qty, source)
                    _align_record_to_actual_quantity(active, actual_qty, actual_cost, source)
                    active["name"] = actual_item.get("name") or active.get("name") or stock
            continue

        report["fixed"].append({
            "stock": stock,
            "name": (actual_item or {}).get("name") or (records[-1].get("name") if records else stock),
            "before": tracked_qty,
            "actual": actual_qty,
        })

        if not records and actual_item and actual_qty > 0:
            record = {
                "stock": stock,
                "name": actual_item.get("name", stock),
                "buy_date": date.today().isoformat(),
                "buy_price": actual_item.get("cost_price", 0),
                "quantity": actual_qty,
                "remaining_quantity": actual_qty,
                "action": "SYNC",
                "source": source,
                "reason": "模拟账户有持仓，本地 trades.json 缺失，自动补建",
                "buy_records": [],
                "sells": [],
            }
            _align_record_to_actual_quantity(record, actual_qty, actual_cost, source)
            trades["records"].append(record)
            report["created"].append(stock)
            continue

        if actual_qty <= 0:
            for rec in records:
                _zero_record(rec, actual_qty, source)
            continue

        active = sorted(records, key=_record_date)[-1]
        _merge_records_into_active(records, active, source, actual_cost)
        for rec in records:
            if rec is not active:
                _zero_record(rec, actual_qty, source)
        active["name"] = (actual_item or {}).get("name") or active.get("name") or stock
        _align_record_to_actual_quantity(active, actual_qty, actual_cost, source)

    after = aggregate_trades_positions(trades)
    report["after"] = {k: v["qty"] for k, v in after.items()}
    report["is_consistent"] = report["after"] == report["actual"]
    return trades, report


def reconcile_trades_file_with_account(*, source: str = "position_sync") -> Dict[str, Any]:
    positions = fetch_mock_positions()
    trades, report = reconcile_trades_with_positions(positions=positions, source=source)
    save_trades(trades)
    return report
