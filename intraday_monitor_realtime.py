#!/usr/bin/env python3
"""
盘中实时监控进程（规则触发卖出）
- 交易日 09:30-14:50 独立进程，14:50 执行尾盘兜底快照后退出
- QMT HTTP /full_tick 实时行情轮询（无 xtquant 依赖）
- 规则触发即卖，卖出数量按规则固定，不再调用 LLM
- 推送消息到飞书群 oc_47d71d764d80f6a580faca781cb4fd34
"""

import os
import sys
import json
import time
import datetime
import threading
import requests
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Optional, Dict, List

# ========== 项目路径 ==========
WORKSPACE = Path(".")
STATE_FILE = WORKSPACE / "output" / "intraday_monitor_state.json"
TRADES_FILE = WORKSPACE / "output" / "trades.json"
OUTPUT_DIR = WORKSPACE / "output"
LOCK_FILE = WORKSPACE / "output" / "intraday_monitor_realtime.pid"

OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
sys.path.insert(0, str(WORKSPACE))
from trade_position_sync import load_local_env, load_trades, save_trades, reconcile_trades_file_with_account

load_local_env()

# ── 进程锁：防止重复启动 ─────────────────────────────
def _acquire_lock() -> bool:
    """返回 True 表示获得锁（无人运行），False 表示已有进程在跑"""
    import atexit
    if LOCK_FILE.exists():
        try:
            pid = int(LOCK_FILE.read_text().strip())
            # 检查进程是否还活着
            import subprocess
            ret = subprocess.run(["ps", "-p", str(pid), "-o", "pid="], capture_output=True)
            if ret.returncode == 0:
                # 进程仍在跑，不重复启动
                return False
        except Exception:
            pass
        # pid 文件过期或进程已死，删除重写
        try:
            LOCK_FILE.unlink(missing_ok=True)
        except Exception:
            pass
    try:
        LOCK_FILE.write_text(str(os.getpid()))
        atexit.register(lambda: LOCK_FILE.unlink(missing_ok=True))
        return True
    except Exception:
        return False


# ========== 飞书 Webhook ==========
FEISHU_WEBHOOK_URL = os.getenv("FEISHU_WEBHOOK_URL", "")

# ========== QMT HTTP ==========
XQ_HTTP_BASE = os.getenv("XQSHARE_HTTP_BASE", "http://127.0.0.1:8080").rstrip("/")

# ========== 全局配置 ==========
TAKE_PROFIT_1 = 0.10
TAKE_PROFIT_2 = 0.20
TAKE_PROFIT_3 = 0.50

# ATR 移动止损分档（基于持仓期最高价）
# (peak_pnl阈值, 当前浮盈止损线)
# stop_line=None → 使用 max(成本-2ATR, 成本-3%)
# 其余 → pnl_pct <= stop_line 触发止损
ATR_TIERS = [
    (0.03, None),   # peak_pnl≥3%: max(成本-2ATR, 成本-3%)
    (0.05, 0.0),    # peak_pnl≥5%: 跌回成本价(0%盈利)则止损
    (0.10, 0.05),   # peak_pnl≥10%: 浮盈回撤至≤5%则止损
    (0.20, 0.10),   # peak_pnl≥20%: 浮盈回撤至≤10%则止损
    (0.30, 0.25),   # peak_pnl≥30%: 浮盈回撤至≤25%则止损
]

POLL_INTERVAL = int(os.getenv("INTRADAY_MONITOR_POLL_INTERVAL", "60"))  # 默认每分钟
FINAL_SNAPSHOT_TIME = datetime.time(14, 50)
FINAL_SNAPSHOT_GRACE_SECONDS = int(os.getenv("INTRADAY_MONITOR_FINAL_GRACE_SECONDS", "60"))
LUNCH_BREAK_START = datetime.time(11, 30)  # 午休开始
LUNCH_BREAK_END = datetime.time(13, 0)    # 午休结束
SELL_PENDING_COOLDOWN = int(os.getenv("INTRADAY_SELL_PENDING_COOLDOWN", "120"))
QUOTE_MAX_WORKERS = max(1, int(os.getenv("INTRADAY_MONITOR_QUOTE_MAX_WORKERS", "1")))
KLINE_MAX_WORKERS = max(1, int(os.getenv("INTRADAY_MONITOR_KLINE_MAX_WORKERS", "1")))
XQ_REQUEST_GAP_SECONDS = float(os.getenv("INTRADAY_MONITOR_XQ_REQUEST_GAP_SECONDS", "0.2"))
STARTUP_RECONCILE_ENABLED = os.getenv("INTRADAY_MONITOR_STARTUP_RECONCILE", "0") == "1"

# ========== 导入项目模块 ==========
from intraday_executor import (
    get_current_positions,
    sell_stock,
    mx_api_post,
    get_today_orders,
    _build_order_id_map,
    _extract_order_id,
    _is_pending_order,
    _latest_order_for_stock,
)

# ========== 日志 ==========
import logging
logging.basicConfig(
    format="%(asctime)s [实时监控] %(levelname)s %(message)s",
    level=logging.INFO,
    handlers=[
        logging.FileHandler(OUTPUT_DIR / "intraday_monitor_realtime.log"),
        logging.StreamHandler(),
    ],
)
log = logging.getLogger(__name__)


# ============================================================================
# 工具函数
# ============================================================================

def today_str() -> str:
    return datetime.date.today().strftime("%Y-%m-%d")


def _xq_http_get(endpoint: str, params: Dict = None, timeout: int = 6) -> Dict:
    url = f"{XQ_HTTP_BASE}{endpoint}"
    resp = requests.get(url, params=params or {}, timeout=timeout)
    resp.raise_for_status()
    return resp.json() or {}


def _sleep_between_xq_requests() -> None:
    if XQ_REQUEST_GAP_SECONDS > 0:
        time.sleep(XQ_REQUEST_GAP_SECONDS)


def _to_xt_code(code: str) -> str:
    code = str(code or "").strip().upper()
    if "." in code:
        return code
    if len(code) == 6 and code.isdigit():
        if code.startswith(("0", "3")):
            return f"{code}.SZ"
        if code.startswith(("6", "5", "9")):
            return f"{code}.SH"
        if code.startswith(("4", "8", "2")):
            return f"{code}.BJ"
    return code


def _stock_limit_pct(stock_code: str) -> float:
    code = str(stock_code or "").strip()
    if code.startswith(("300", "301", "688")):
        return 0.20
    if code.startswith(("4", "8", "920")):
        return 0.30
    return 0.10


def _as_float(value, default=None):
    if isinstance(value, (list, tuple)):
        value = value[0] if value else None
    if value in (None, "", "--", "None"):
        return default
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _position_current_price(pos: Dict) -> float:
    try:
        price = float(pos.get("price", 0) or 0)
        dec = int(pos.get("priceDec", 2) or 2)
        return price / pow(10, dec)
    except Exception:
        return 0.0


def _position_cost_price(pos: Dict) -> float:
    try:
        price = float(pos.get("costPrice", 0) or 0)
        dec = int(pos.get("costPriceDec", 3) or 3)
        return price / pow(10, dec)
    except Exception:
        return 0.0


def _sell_order_price_from_latest(latest_price: float, limit_down: float = 0.0, discount: float = 0.015) -> float:
    latest_price = float(latest_price or 0)
    if latest_price <= 0:
        return 0.0
    order_price = round(latest_price * (1 - discount), 2)
    if limit_down and limit_down > 0:
        order_price = max(order_price, round(float(limit_down), 2))
    return order_price


def _normalize_xq_quote(raw: Dict) -> Optional[Dict]:
    if not isinstance(raw, dict):
        return None
    price = _as_float(raw.get("lastPrice") or raw.get("last_price") or raw.get("close"))
    if not price or price <= 0:
        return None
    prev_close = _as_float(raw.get("lastClose") or raw.get("preClose"))
    if prev_close and prev_close > 0:
        change_pct = round((price - prev_close) / prev_close * 100, 2)
    else:
        change_pct = _as_float(raw.get("changePct") or raw.get("pctChg"), 0.0)
    high = _as_float(raw.get("high") or raw.get("highPrice"))
    low = _as_float(raw.get("low") or raw.get("lowPrice"))
    open_price = _as_float(raw.get("open") or raw.get("openPrice"))
    return {
        "current": price,
        "change_pct": change_pct or 0.0,
        "high": high,
        "low": low,
        "open": open_price,
        "prev_close": prev_close,
    }


def get_xq_realtime_quote(stock_code: str) -> Optional[Dict]:
    xt_code = _to_xt_code(stock_code)
    try:
        payload = _xq_http_get("/full_tick", {"stocks": xt_code}, timeout=6)
        if payload.get("success"):
            data = payload.get("data", {})
            raw = data.get(xt_code) or data.get(stock_code) or next(iter(data.values()), None)
            if raw:
                quote = _normalize_xq_quote(raw)
                if quote:
                    return quote
    except Exception as e:
        log.debug(f"XQShare /full_tick 失败 {stock_code}: {e}")
    finally:
        _sleep_between_xq_requests()

    try:
        payload = _xq_http_get("/realtime_quote", {"stock": xt_code}, timeout=6)
        if payload.get("success"):
            return {
                "current": payload.get("price", 0),
                "high": payload.get("high", 0),
                "low": payload.get("low", 0),
                "open": payload.get("open", 0),
                "change_pct": payload.get("change_pct", 0),
                "prev_close": payload.get("last_close", 0),
            }
    except Exception as e:
        log.debug(f"XQShare /realtime_quote 失败 {stock_code}: {e}")
    finally:
        _sleep_between_xq_requests()

    return None


def _get_realtime_quote_batch(codes: List[str]) -> Dict[str, Dict]:
    result = {}
    if QUOTE_MAX_WORKERS <= 1:
        for code in codes:
            quote = get_xq_realtime_quote(code)
            if quote and quote.get("current", 0) > 0:
                result[code] = quote
        return result

    with ThreadPoolExecutor(max_workers=min(len(codes), QUOTE_MAX_WORKERS)) as ex:
        futures = {ex.submit(get_xq_realtime_quote, c): c for c in codes}
        for future in as_completed(futures):
            code = futures[future]
            quote = future.result()
            if quote and quote.get("current", 0) > 0:
                result[code] = quote
    return result


def _parse_http_kline(code: str, period: str = "1d", count: int = 500,
                     start_date: str = "", end_date: str = "") -> Optional[List[Dict]]:
    xt_code = _to_xt_code(code)
    params = f"stock={xt_code}&period={period}&count={count}"
    if start_date:
        params += f"&start_date={start_date}"
    if end_date:
        params += f"&end_date={end_date}"
    url = f"{XQ_HTTP_BASE}/market_data3?{params}"
    try:
        r = requests.get(url, timeout=10)
        # API 返回 JSON 格式：{"success":true,"data":{"close":{"日期":{"股票代码":值},...},...}}
        d = r.json()
        if not d.get("success") or "data" not in d:
            return None
        data = d["data"]
        # 获取所有日期（从任一字段的 keys）
        dates = set()
        for field in data.values():
            if isinstance(field, dict):
                dates.update(field.keys())
        if not dates:
            return None
        rows = []
        for dt in sorted(dates):
            row = {"date": dt}
            for fname, field_data in data.items():
                if isinstance(field_data, dict) and dt in field_data:
                    # 取第一个股票代码的值（通常只有一个）
                    for stock_val in field_data[dt].values():
                        row[fname] = str(stock_val)
                        break
            rows.append(row)
        return rows if rows else None
    except:
        return None
    finally:
        _sleep_between_xq_requests()


def _get_peak_price_since_buy(stock_code: str, buy_dates: List[str]) -> float:
    if not buy_dates:
        return 0.0
    start = min(buy_dates).replace("-", "")
    end = datetime.date.today().strftime("%Y%m%d")
    rows = _parse_http_kline(stock_code, period="1d", count=500,
                              start_date=start, end_date=end)
    if not rows:
        return 0.0
    highs = []
    for row in rows:
        # 过滤 buy_date 之前的 K 线，避免老高点混入（ATR lot 独立触发需要）
        row_dt = str(row.get("date", "")).replace("-", "")
        if row_dt and row_dt < start:
            continue
        h = row.get("high", "")
        try:
            highs.append(float(h))
        except:
            pass
    return max(highs) if highs else 0.0


def _calc_atr_ma20_http(stock_code: str) -> tuple:
    rows = _parse_http_kline(stock_code, period="1d", count=60)
    if not rows or len(rows) < 20:
        return 0.0, 0.0
    try:
        highs = [float(r.get("high", 0)) for r in rows if r.get("high")]
        lows = [float(r.get("low", 0)) for r in rows if r.get("low")]
        closes = [float(r.get("close", 0)) for r in rows if r.get("close")]
        if len(closes) < 20:
            return 0.0, 0.0
        trs = []
        for i in range(1, len(closes)):
            h = highs[i] if i < len(highs) else closes[i]
            l = lows[i] if i < len(lows) else closes[i]
            prev_c = closes[i - 1]
            tr = max(h - l, abs(h - prev_c), abs(l - prev_c))
            trs.append(tr)
        atr = sum(trs[-14:]) / min(14, len(trs)) if trs else 0.0
        ma20 = sum(closes[-20:]) / 20
        return atr, ma20
    except:
        return 0.0, 0.0



def _get_limit_down(stock_code: str) -> float:
    try:
        rows = _parse_http_kline(stock_code, period="1d", count=2)
        if rows:
            prev_close = float(rows[0].get("close", 0))
            if prev_close > 0:
                return round(prev_close * (1 - _stock_limit_pct(stock_code)), 2)
    except:
        pass
    return 0.0


def market_open_today() -> bool:
    """判断今天是否为 A 股交易日（走共享模块 trading_calendar）"""
    try:
        import os
        _SHARED = os.path.expanduser("~/.openclaw/agents/shared")
        if _SHARED not in sys.path:
            sys.path.insert(0, _SHARED)
        from trading_calendar import is_a_share_trading_day
        return is_a_share_trading_day()
    except Exception as e:
        log.warning(f"共享交易日历判断失败: {e}，本轮不启动")
        return False


# ============================================================================
# 状态文件管理
# ============================================================================

def load_state() -> Dict:
    if STATE_FILE.exists():
        try:
            with open(STATE_FILE) as f:
                state = json.load(f)
            if state.get("date") != today_str():
                log.info("状态文件日期不是今天，重建")
                state = {"date": today_str(), "positions": {}}
        except:
            state = {"date": today_str(), "positions": {}}
    else:
        state = {"date": today_str(), "positions": {}}
    return state


def save_state(state: Dict):
    with open(STATE_FILE, "w") as f:
        json.dump(state, f, ensure_ascii=False, indent=2)


def _as_tier_set(value) -> set:
    try:
        return {int(v) for v in (value or []) if int(v) > 0}
    except Exception:
        return set()


def _tier_threshold(tier: int) -> float:
    return {
        1: TAKE_PROFIT_1,
        2: TAKE_PROFIT_2,
        3: TAKE_PROFIT_3,
    }.get(int(tier or 0), 999.0)


def _trigger_tier(trigger: Dict) -> int:
    kind = str((trigger or {}).get("trigger") or "")
    if kind.startswith("tp") and kind[-1:].isdigit():
        return int(kind[-1])
    reason = str((trigger or {}).get("reason") or "")
    if "止盈第1档" in reason:
        return 1
    if "止盈第2档" in reason:
        return 2
    if "止盈第3档" in reason:
        return 3
    return 0


def _record_take_profit_tiers(record: Dict) -> set:
    tiers = set()
    for sell in (record or {}).get("sells", []) or []:
        reason = str(sell.get("reason", ""))
        if "止盈第1档" in reason:
            tiers.add(1)
        if "止盈第2档" in reason:
            tiers.add(2)
        if "止盈第3档" in reason:
            tiers.add(3)
    return tiers


def _apply_confirmed_sell_to_state_lots(position: Dict, quote: Dict, trigger: Dict, quantity: int) -> None:
    tier = _trigger_tier(trigger)
    lots = position.get("lots") or []
    eligible_lots = set()
    if tier:
        tiers = _as_tier_set(position.get("executed_tp_tiers"))
        tiers.add(tier)
        position["executed_tp_tiers"] = sorted(tiers)

        current = float((quote or {}).get("current", 0) or 0)
        threshold = _tier_threshold(tier)
        for idx, lot in enumerate(lots):
            lot_rem = max(0, int(lot.get("remaining", 0) or 0))
            lot_bp = float(lot.get("buy_price", 0) or 0)
            if lot_rem <= 0 or lot_bp <= 0 or current <= 0:
                continue
            if (current - lot_bp) / lot_bp < threshold:
                continue
            eligible_lots.add(idx)
            lot_tiers = _as_tier_set(lot.get("executed_tp_tiers"))
            lot_tiers.add(tier)
            lot["executed_tp_tiers"] = sorted(lot_tiers)

    remaining_to_reduce = max(0, int(quantity or 0))
    for idx, lot in enumerate(lots):
        if remaining_to_reduce <= 0:
            break
        lot_rem = max(0, int(lot.get("remaining", 0) or 0))
        if lot_rem <= 0:
            continue
        cut = min(lot_rem, remaining_to_reduce)
        if tier and (not eligible_lots or idx in eligible_lots):
            lot_tiers = _as_tier_set(lot.get("executed_tp_tiers"))
            lot_tiers.add(tier)
            lot["executed_tp_tiers"] = sorted(lot_tiers)
        lot["remaining"] = lot_rem - cut
        remaining_to_reduce -= cut


def _inherit_old_take_profit_tiers(position: Dict, old_position: Dict) -> None:
    old_tiers = _as_tier_set(old_position.get("executed_tp_tiers"))
    current_tiers = _as_tier_set(position.get("executed_tp_tiers"))
    inherited = current_tiers | old_tiers
    position["executed_tp_tiers"] = sorted(inherited)
    if not inherited:
        return

    old_dates = {str(d) for d in old_position.get("buy_dates", []) if d}
    current_dates = {str(d) for d in position.get("buy_dates", []) if d}
    can_apply_to_lots = not current_dates or not old_dates or current_dates.issubset(old_dates)
    if not can_apply_to_lots:
        return

    for lot in position.get("lots") or []:
        lot_tiers = _as_tier_set(lot.get("executed_tp_tiers"))
        lot["executed_tp_tiers"] = sorted(lot_tiers | inherited)


def _confirmed_sell_quantity_from_report(stock: str, requested_quantity: int, report: Dict) -> int:
    requested_quantity = max(0, int(requested_quantity or 0))
    for item in (report or {}).get("fixed", []) or []:
        if str(item.get("stock") or "") != str(stock):
            continue
        before = int(item.get("before", 0) or 0)
        actual = int(item.get("actual", 0) or 0)
        if before > actual:
            return min(requested_quantity, before - actual)
    return 0


def _parse_iso_datetime(value) -> Optional[datetime.datetime]:
    if value in (None, ""):
        return None
    try:
        return datetime.datetime.fromisoformat(str(value).replace("Z", "+00:00")).replace(tzinfo=None)
    except Exception:
        return None


def _sell_order_filled_quantity(order: Optional[Dict]) -> int:
    if not isinstance(order, dict):
        return 0
    return max(0, int(order.get("quantity", 0) or 0))


def _find_sell_order(stock: str, pending_sell: Dict, sell_orders: List[Dict]) -> Optional[Dict]:
    order_id = pending_sell.get("order_id") or _extract_order_id(pending_sell)
    if order_id not in (None, ""):
        by_id = _build_order_id_map(sell_orders)
        matched = by_id.get(str(order_id))
        if matched:
            return matched
        return None

    sent_at = float(pending_sell.get("sent_at", 0) or 0)
    requested_qty = int(pending_sell.get("quantity", 0) or 0)
    candidates = []
    for order in sell_orders or []:
        if str(order.get("stock") or order.get("stockCode") or order.get("code") or "").strip() != str(stock):
            continue
        if requested_qty > 0:
            order_qty = int(order.get("order_quantity", order.get("order_count", 0)) or 0)
            filled_qty = int(order.get("quantity", 0) or 0)
            if order_qty and order_qty != requested_qty:
                continue
            if not order_qty and filled_qty and filled_qty > requested_qty:
                continue
        order_time = _parse_iso_datetime(order.get("order_time") or order.get("time"))
        if sent_at and order_time and order_time.timestamp() + 2 < sent_at:
            continue
        candidates.append(order)
    return _latest_order_for_stock(candidates, stock)


def _mark_sell_pending(position: Dict, quantity: int, reason: str, order_price: float, result: Dict = None) -> None:
    result = result or {}
    position["sell_pending_until"] = time.time() + SELL_PENDING_COOLDOWN
    position["pending_sell"] = {
        "sent_at": time.time(),
        "reason": reason,
        "quantity": int(quantity or 0),
        "order_price": float(order_price or 0),
        "order_id": _extract_order_id(result),
    }


def _confirm_pending_sell_from_orders(stock: str, position: Dict, quote: Dict, pending_sell: Dict) -> bool:
    """Confirm one pending sell with one order snapshot; return True to skip fresh sell checks."""
    try:
        today_orders = get_today_orders(force=True)
    except Exception as e:
        log.warning(f"{stock} 待确认卖单回查失败，延后确认: {e}")
        position["sell_pending_until"] = time.time() + SELL_PENDING_COOLDOWN
        return True

    if today_orders.get("_ok") is False:
        log.warning(f"{stock} 待确认卖单回查失败，延后确认")
        position["sell_pending_until"] = time.time() + SELL_PENDING_COOLDOWN
        return True

    order = _find_sell_order(stock, pending_sell, today_orders.get("sells", []))
    if not order:
        log.warning(f"{stock} 待确认卖单未在今日委托中找到，延后确认避免重复卖出")
        position["sell_pending_until"] = time.time() + SELL_PENDING_COOLDOWN
        return True

    if _is_pending_order(order):
        log.info(f"{stock} 卖出委托仍未完成，继续等待成交确认")
        position["sell_pending_until"] = time.time() + SELL_PENDING_COOLDOWN
        return True

    confirmed_qty = _sell_order_filled_quantity(order)
    if confirmed_qty <= 0:
        log.warning(f"{stock} 卖出委托已结束但成交为0，清除pending后允许重新判断")
        position["pending_sell"] = None
        position["sell_pending_until"] = 0
        return False

    reason = str(pending_sell.get("reason") or "实时监控卖出")
    trigger = {
        "reason": reason,
        "quantity_rule": int(pending_sell.get("quantity", confirmed_qty) or confirmed_qty),
        "pct": 0,
    }
    _apply_confirmed_sell_to_state_lots(position, quote, trigger, confirmed_qty)
    position["avail"] = max(0, int(position.get("avail", 0) or 0) - confirmed_qty)
    position["sell_pending_until"] = 0
    position["pending_sell"] = None
    saved = _append_realtime_sell_record(
        stock=stock,
        price=float(order.get("trade_price") or pending_sell.get("order_price") or quote.get("current") or 0),
        quantity=confirmed_qty,
        reason=f"[实时监控] {reason}",
    )
    if saved:
        log.info(f"{stock} 实时卖出记录已补写: {confirmed_qty}股, reason={reason}")
    _push_message(
        f"【实时监控】✅ 卖出成交已确认\n"
        f"股票：{position.get('name', stock)}({stock})\n"
        f"成交：{confirmed_qty}股\n"
        f"触发：{reason}"
    )
    return True


def _append_realtime_sell_record(stock: str, price: float, quantity: int, reason: str) -> bool:
    quantity = int(quantity or 0)
    if quantity <= 0:
        return False
    trades = load_trades(TRADES_FILE)
    def tracked_remaining(rec: Dict) -> int:
        buy_records = rec.get("buy_records") or []
        if buy_records:
            return int(sum(max(0, int(lot.get("remaining", 0) or 0)) for lot in buy_records))
        return int(rec.get("remaining_quantity", 0) or 0)

    records = [
        rec for rec in trades.get("records", [])
        if str(rec.get("stock") or "") == str(stock) and tracked_remaining(rec) > 0
    ]
    if not records:
        log.error(f"{stock} 实时卖出已成交但 trades.json 找不到记录，无法补写卖出原因")
        return False
    record = sorted(records, key=lambda rec: str(rec.get("buy_date") or ""))[-1]
    today = datetime.date.today().isoformat()
    sells = record.setdefault("sells", [])
    for sell in sells:
        if (
            sell.get("date") == today
            and sell.get("source") == "intraday_realtime_sell"
            and str(sell.get("reason") or "") == str(reason)
            and int(sell.get("quantity", 0) or 0) == quantity
            and abs(float(sell.get("price", 0) or 0) - float(price or 0)) < 0.005
        ):
            return False

    before_remaining = tracked_remaining(record)
    tier = 0
    if "止盈第1档" in str(reason):
        tier = 1
    elif "止盈第2档" in str(reason):
        tier = 2
    elif "止盈第3档" in str(reason):
        tier = 3
    matched_cost = 0.0
    matched_qty = 0
    to_reduce = min(quantity, before_remaining) if before_remaining > 0 else quantity
    buy_records = record.get("buy_records") or []
    if buy_records:
        remaining_to_reduce = to_reduce
        for lot in buy_records:
            if remaining_to_reduce <= 0:
                break
            lot_remaining = max(0, int(lot.get("remaining", 0) or 0))
            if lot_remaining <= 0:
                continue
            cut = min(lot_remaining, remaining_to_reduce)
            lot_price = float(lot.get("price", 0) or record.get("buy_price", 0) or 0)
            matched_cost += lot_price * cut
            matched_qty += cut
            if tier:
                tiers = _as_tier_set(lot.get("executed_tp_tiers"))
                tiers.add(tier)
                lot["executed_tp_tiers"] = sorted(tiers)
            lot["remaining"] = lot_remaining - cut
            remaining_to_reduce -= cut
    else:
        buy_price = float(record.get("buy_price", 0) or 0)
        matched_qty = to_reduce
        matched_cost = buy_price * matched_qty

    after_remaining = max(0, before_remaining - to_reduce)
    record["remaining_quantity"] = after_remaining
    if to_reduce < quantity:
        log.warning(f"{stock} 实时卖出成交{quantity}股，本地仅匹配{to_reduce}股，已将本地剩余归零")

    buy_price_used = matched_cost / matched_qty if matched_qty > 0 else float(record.get("buy_price", 0) or 0)
    pnl_pct = round((float(price or 0) - buy_price_used) / buy_price_used * 100, 2) if buy_price_used > 0 else 0
    sells.append({
        "date": today,
        "price": round(float(price or 0), 3),
        "quantity": quantity,
        "pnl_pct": pnl_pct,
        "reason": reason,
        "buy_price_used": round(buy_price_used, 4) if buy_price_used else 0,
        "source": "intraday_realtime_sell",
    })
    save_trades(trades, TRADES_FILE)
    return True


# ============================================================================
# 持仓加载
# ============================================================================

def load_positions() -> Dict[str, Dict]:
    if STARTUP_RECONCILE_ENABLED:
        try:
            report = reconcile_trades_file_with_account(source="intraday_realtime_start")
            if report.get("fixed"):
                log.warning(
                    f"启动时已按模拟账户同步 trades.json: fixed={len(report.get('fixed', []))}, "
                    f"consistent={report.get('is_consistent')}"
                )
        except Exception as e:
            log.error(f"启动时持仓同步失败: {e}")
            return {}
    else:
        log.info("启动时跳过 trades.json 账户同步，仅读取一次模拟账户持仓作为监控基准")

    trades_map = {}
    if TRADES_FILE.exists():
        with open(TRADES_FILE) as f:
            trades_data = json.load(f)
        for rec in trades_data.get("records", []):
            stock = rec.get("stock", "")
            if not stock:
                continue
            record_tiers = _record_take_profit_tiers(rec)
            lots_raw = rec.get("buy_records") or []
            if not lots_raw:
                rem = int(rec.get("remaining_quantity", 0) or 0)
                if rem > 0:
                    lots_raw = [{
                        "date": rec.get("buy_date"),
                        "price": rec.get("buy_price", 0),
                        "quantity": rem,
                        "remaining": rem,
                    }]
            lots = []
            for lot in lots_raw:
                rem = int(lot.get("remaining", 0) or 0)
                qty = int(lot.get("quantity", 0) or rem or 0)
                price = float(lot.get("price", 0) or rec.get("buy_price", 0) or 0)
                if qty <= 0 and rem <= 0:
                    continue
                lots.append({
                    "date": lot.get("date") or rec.get("buy_date"),
                    "buy_price": price,
                    "original_quantity": max(qty, rem),
                    "remaining": max(0, rem),
                    "source": lot.get("source") or rec.get("source") or "",
                    "executed_tp_tiers": _as_tier_set(lot.get("executed_tp_tiers")),
                })
            # 用FIFO把历史卖出映射回各批次，恢复每批已执行止盈档位
            fifo = [{"left": int(l["original_quantity"]), "ref": l} for l in lots]
            for sell in rec.get("sells", []):
                reason = str(sell.get("reason", ""))
                tier = 0
                if "止盈第1档" in reason:
                    tier = 1
                elif "止盈第2档" in reason:
                    tier = 2
                elif "止盈第3档" in reason:
                    tier = 3
                qty = int(sell.get("quantity", 0) or 0)
                if qty <= 0:
                    continue
                for node in fifo:
                    if qty <= 0:
                        break
                    left = int(node["left"] or 0)
                    if left <= 0:
                        continue
                    cut = min(left, qty)
                    node["left"] = left - cut
                    qty -= cut
                    if tier:
                        node["ref"]["executed_tp_tiers"].add(tier)
            tiers_union = set()
            for lot in lots:
                if str(lot.get("source") or "") == "position_reconcile":
                    lot["executed_tp_tiers"] = _as_tier_set(lot.get("executed_tp_tiers")) | record_tiers
                tiers_union |= lot["executed_tp_tiers"]
            trades_map[stock] = {
                "bp": rec.get("buy_price", 0),
                "buy_dates": [l.get("date") for l in lots if l.get("date")],
                "name": rec.get("name", ""),
                "original_quantity": rec.get("quantity", 0),
                "executed_tp_tiers": sorted(tiers_union),
                "lots": lots,
            }

    api_positions = get_current_positions()
    if not api_positions:
        log.warning("API 返回空持仓，不使用本地持仓快照缓存")
        return {}

    result = {}
    with ThreadPoolExecutor(max_workers=KLINE_MAX_WORKERS) as ex:
        futures = {}
        for pos in api_positions:
            stock = pos["stockCode"]
            trade_meta = trades_map.get(stock, {})
            lots = trade_meta.get("lots") or []
            if not lots:
                qty = int(pos["totalQuantity"] or 0)
                price = float(trade_meta.get("bp") or _position_cost_price(pos) or 0)
                lots = [{
                    "date": datetime.date.today().strftime("%Y-%m-%d"),
                    "buy_price": price,
                    "original_quantity": qty,
                    "remaining": int(pos.get("availQuantity", qty) or 0),
                    "executed_tp_tiers": set(),
                }]
            buy_dates = [l.get("date") for l in lots if l.get("date")]
            if not buy_dates:
                buy_dates = [datetime.date.today().strftime("%Y-%m-%d")]

            f_peak = ex.submit(_get_peak_price_since_buy, stock, buy_dates)
            f_atr = ex.submit(_calc_atr_ma20_http, stock)
            lot_peak_futures = {}
            for idx, lot in enumerate(lots):
                lot_date = lot.get("date")
                if lot_date:
                    lot_peak_futures[idx] = ex.submit(_get_peak_price_since_buy, stock, [lot_date])
            futures[stock] = {
                "pos": pos, "lots": lots, "buy_dates": buy_dates,
                "f_peak": f_peak, "f_atr": f_atr,
                "lot_peak_futures": lot_peak_futures,
            }

        for stock, info in futures.items():
            pos = info["pos"]
            lots = info["lots"]
            buy_dates = info["buy_dates"]
            peak_price = 0.0
            atr = 0.0
            ma20 = 0.0
            try:
                peak_price = info["f_peak"].result() or 0.0
            except:
                pass
            try:
                atr, ma20 = info["f_atr"].result() or (0.0, 0.0)
            except:
                pass
            for idx, ft in info.get("lot_peak_futures", {}).items():
                try:
                    lots[idx]["peak_price"] = float(ft.result() or 0)
                except Exception:
                    lots[idx]["peak_price"] = 0.0
            rem_cost = 0.0
            rem_qty = 0
            for lot in lots:
                lot_rem = max(0, int(lot.get("remaining", 0) or 0))
                lot_bp = float(lot.get("buy_price", 0) or 0)
                if lot_rem > 0 and lot_bp > 0:
                    rem_qty += lot_rem
                    rem_cost += lot_bp * lot_rem
            bp = rem_cost / rem_qty if rem_qty > 0 else float(_position_cost_price(pos) or 0)

            result[stock] = {
                "bp": bp,
                "peak_price": peak_price,
                "quantity": pos["totalQuantity"],
                "original_quantity": trades_map.get(stock, {}).get("original_quantity") or pos["totalQuantity"],
                "avail": pos["availQuantity"],
                "account_price": _position_current_price(pos),
                "ma20": ma20,
                "atr": atr,
                "name": pos["stockName"],
                "buy_dates": buy_dates,
                "executed_tp_tiers": trades_map.get(stock, {}).get("executed_tp_tiers", []),
                "lots": [
                    {
                        **lot,
                        "executed_tp_tiers": sorted(list(lot.get("executed_tp_tiers") or [])),
                    }
                    for lot in lots
                ],
            }

    return result


# ============================================================================
# 触发条件检查
# ============================================================================

def _is_star_market(stock: str) -> bool:
    return str(stock or "").startswith("688")


def _min_sell_quantity(stock: str) -> int:
    # 科创板卖出 200 股起，超过 200 股部分可 1 股递增；其它 A 股按整百卖。
    return 200 if _is_star_market(stock) else 100


def _normalize_full_exit_quantity(stock: str, avail: int) -> int:
    """Normalize full-exit sell quantity for stop-loss/ATR/TP3."""
    avail = int(avail or 0)
    if avail <= 0:
        return 0
    min_qty = _min_sell_quantity(stock)
    if avail < min_qty:
        return avail
    if _is_star_market(stock):
        return avail
    return (avail // 100) * 100


def _normalize_partial_take_profit_quantity(stock: str, avail: int, raw_qty: int) -> int:
    """Normalize TP1/TP2 quantity; return 0 when the partial sell is not tradable."""
    avail = int(avail or 0)
    raw_qty = int(raw_qty or 0)
    if avail <= 0 or raw_qty <= 0:
        return 0
    min_qty = _min_sell_quantity(stock)
    if avail < min_qty or raw_qty > avail:
        return 0
    if _is_star_market(stock):
        return raw_qty if raw_qty >= min_qty else 0
    qty = (raw_qty // 100) * 100
    return qty if qty >= min_qty else 0


def _normalize_target_sell_quantity(stock: str, avail: int, raw_qty: int) -> int:
    """Normalize arbitrary target sell quantity under board lot constraints."""
    avail = int(avail or 0)
    raw_qty = int(raw_qty or 0)
    if avail <= 0 or raw_qty <= 0:
        return 0
    min_qty = _min_sell_quantity(stock)
    target = min(avail, raw_qty)
    if avail < min_qty:
        return avail
    if _is_star_market(stock):
        return target if target >= min_qty else 0
    qty = (target // 100) * 100
    return qty if qty >= min_qty else 0


def _is_valid_sell_quantity(stock: str, avail: int, quantity: int) -> bool:
    avail = int(avail or 0)
    quantity = int(quantity or 0)
    if quantity <= 0 or quantity > avail:
        return False
    min_qty = _min_sell_quantity(stock)
    if avail < min_qty:
        return quantity == avail
    if _is_star_market(stock):
        return quantity >= min_qty
    return quantity >= min_qty and quantity % 100 == 0


def check_triggers(stock: str, info: Dict, quote: Dict) -> List[Dict]:
    triggers = []
    bp = info["bp"]
    current = quote.get("current", 0)
    atr = info["atr"]
    ma20 = info["ma20"]
    avail = info["avail"]
    lots = info.get("lots") or []
    if not lots and bp > 0:
        lots = [{
            "date": "",
            "buy_price": bp,
            "original_quantity": int(info.get("original_quantity") or avail or 0),
            "remaining": int(avail or 0),
            "executed_tp_tiers": set(info.get("executed_tp_tiers") or []),
            "peak_price": float(info.get("peak_price", 0) or 0),
        }]

    if current <= 0 or bp <= 0:
        return []

    pnl_pct = (current - bp) / bp

    if ma20 > 0 and current < ma20:
        raw_qty = sum(max(0, int(l.get("remaining", 0) or 0)) for l in lots) or int(avail or 0)
        quantity = _normalize_target_sell_quantity(stock, avail, raw_qty)
        if quantity > 0:
            quantity_note = "" if quantity == avail else f"，按交易单位由{avail}股调整为{quantity}股"
            triggers.append({
                "trigger": "ma20",
                "reason": f"MA20止损（现价{current:.3f} < MA20 {ma20:.2f}{quantity_note}）",
                "quantity_rule": quantity,
                "pct": pnl_pct,
            })

    atr_raw = 0
    atr_messages = []
    tp1_raw = 0
    tp2_raw = 0
    tp3_raw = 0
    for lot in lots:
        lot_remaining = max(0, int(lot.get("remaining", 0) or 0))
        lot_bp = float(lot.get("buy_price", 0) or 0)
        if lot_remaining <= 0 or lot_bp <= 0:
            continue
        lot_pnl = (current - lot_bp) / lot_bp
        lot_peak = max(float(lot.get("peak_price", 0) or 0), current)
        if lot_peak <= lot_bp:
            lot_peak_pnl = 0.0
        else:
            lot_peak_pnl = (lot_peak - lot_bp) / lot_bp
        lot_tiers = set(lot.get("executed_tp_tiers") or [])
        if len(lots) == 1:
            lot_tiers |= set(info.get("executed_tp_tiers") or [])
        lot_date = lot.get("date") or "-"

        triggered_atr = False
        for threshold, stop_line in sorted(ATR_TIERS, key=lambda item: item[0], reverse=True):
            if lot_peak_pnl < threshold:
                continue
            if stop_line is None:
                if atr and atr > 0:
                    stop_price = max(lot_bp - 2 * atr, lot_bp * 0.97)
                    effective_stop_pct = (stop_price - lot_bp) / lot_bp
                else:
                    effective_stop_pct = -0.03
            else:
                effective_stop_pct = stop_line
            if lot_pnl <= effective_stop_pct:
                atr_raw += lot_remaining
                atr_messages.append(
                    f"{lot_date}批次: 最高{lot_peak_pnl*100:.1f}%→当前{lot_pnl*100:.1f}%≤{effective_stop_pct*100:.1f}%"
                )
                triggered_atr = True
            break
        if triggered_atr:
            continue

        lot_original = max(int(lot.get("original_quantity", lot_remaining) or lot_remaining), lot_remaining)
        if 1 not in lot_tiers and lot_pnl >= TAKE_PROFIT_1:
            tp1_raw += int(lot_original * 0.3)
        if 2 not in lot_tiers and lot_pnl >= TAKE_PROFIT_2:
            tp2_raw += int(lot_original * 0.2)
        if 3 not in lot_tiers and lot_pnl >= TAKE_PROFIT_3:
            tp3_raw += lot_remaining

    if atr_raw > 0:
        quantity = _normalize_target_sell_quantity(stock, avail, atr_raw)
        if quantity > 0:
            reason = "ATR止损（分批）"
            if atr_messages:
                reason += "：" + "；".join(atr_messages[:3])
            triggers.append({
                "trigger": "atr",
                "reason": reason,
                "quantity_rule": quantity,
                "pct": pnl_pct,
            })

    if tp1_raw > 0:
        tp1_qty = _normalize_partial_take_profit_quantity(stock, avail, tp1_raw)
        if tp1_qty > 0:
            triggers.append({
                "trigger": "tp1",
                "reason": f"止盈第1档(分批合计触发)卖原始仓位30%",
                "quantity_rule": tp1_qty,
                "pct": pnl_pct,
            })
    if tp2_raw > 0:
        tp2_qty = _normalize_partial_take_profit_quantity(stock, avail, tp2_raw)
        if tp2_qty > 0:
            triggers.append({
                "trigger": "tp2",
                "reason": f"止盈第2档(分批合计触发)卖原始仓位20%",
                "quantity_rule": tp2_qty,
                "pct": pnl_pct,
            })
    if tp3_raw > 0:
        quantity = _normalize_target_sell_quantity(stock, avail, tp3_raw)
        if quantity > 0:
            triggers.append({
                "trigger": "tp3",
                "reason": f"止盈第3档(分批触发)卖对应批次剩余",
                "quantity_rule": quantity,
                "pct": pnl_pct,
            })

    return triggers


# ============================================================================
# 卖出执行 + 飞书推送
# ============================================================================

def do_sell_and_push(stock: str, info: Dict, quote: Dict, trigger: Dict, decision: str, state: Dict):
    name = info["name"]
    current = quote.get("current", 0)
    avail = int(info.get("avail", 0) or 0)
    quantity = int(trigger["quantity_rule"] or 0)
    reason = trigger["reason"]
    limit_down = _get_limit_down(stock)
    order_price = _sell_order_price_from_latest(current, limit_down)
    pct = trigger["pct"]

    if decision != "SELL":
        log.warning(f"{stock} 收到非 SELL 决策 {decision}，按规则仍执行卖出")

    if not _is_valid_sell_quantity(stock, avail, quantity):
        quantity = _normalize_target_sell_quantity(stock, avail, quantity)
        if quantity <= 0:
            _push_message(
                f"【实时监控】跳过废单\n"
                f"股票：{name}({stock})\n"
                f"触发：{reason}\n"
                f"原因：卖出数量{quantity}股不满足交易单位要求，可卖{avail}股"
            )
            return

    if not _is_valid_sell_quantity(stock, avail, quantity):
        _push_message(
            f"【实时监控】跳过废单\n"
            f"股票：{name}({stock})\n"
            f"触发：{reason}\n"
            f"原因：规范化后卖出数量{quantity}股仍不满足交易单位要求，可卖{avail}股"
        )
        return

    try:
        result = sell_stock(
            stock_code=stock,
            stock_name=name,
            price=current,
            quantity=quantity,
            reason=f"[实时监控] {reason}",
        )
        success = (
            result.get("success") is True
            or result.get("status") in {"submitted", "success", "dry_run"}
        )
        if success:
            _push_message(
                f"【实时监控】📤 卖出委托已提交，等待成交确认\n"
                f"股票：{name}({stock})\n"
                f"触发：{reason}\n"
                f"委托：{quantity}股 @ {order_price:.3f}元\n"
                f"浮盈：{pct*100:.1f}%\n"
                f"时间：{datetime.datetime.now().strftime('%H:%M:%S')}"
            )
            position = state["positions"][stock]
            _mark_sell_pending(position, quantity, reason, order_price, result)
            _push_message(
                f"【实时监控】⚠️ 卖出委托待成交回查\n"
                f"股票：{name}({stock})\n"
                f"触发：{reason}\n"
                f"处理：暂不重复卖出，{SELL_PENDING_COOLDOWN}秒后用订单回查确认"
            )
        else:
            _push_message(
                f"【实时监控】❌ 卖出失败\n"
                f"股票：{name}({stock})\n"
                f"触发：{reason}\n"
                f"原因：{result.get('error', '未知错误')}"
            )
    except Exception as e:
        log.error(f"卖出异常: {e}")
        _push_message(f"【实时监控】❌ 卖出异常 {name}({stock})：{e}")


def _push_message(text: str):
    if not FEISHU_WEBHOOK_URL:
        log.error("FEISHU_WEBHOOK_URL 未设置，跳过推送")
        return
    try:
        import urllib.request
        payload = json.dumps({
            "msg_type": "text",
            "content": {"text": text}
        }).encode()
        req = urllib.request.Request(
            FEISHU_WEBHOOK_URL,
            data=payload,
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        with urllib.request.urlopen(req, timeout=10):
            log.info(f"飞书推送成功: {text[:50]}")
    except Exception as e:
        log.error(f"飞书推送失败: {e}")


# ============================================================================
# 主轮询
# ============================================================================

def _refresh_position_before_sell(
    stock: str,
    info: Dict,
    quote: Dict,
    api_positions: Optional[List[Dict]] = None,
) -> bool:
    """Refresh account position only after a rule has triggered a sell."""
    try:
        api_pos = api_positions if api_positions is not None else get_current_positions()
    except Exception as e:
        log.warning(f"{stock} 卖出前刷新持仓失败，跳过本轮卖出: {e}")
        return False

    pos = next((p for p in api_pos if p.get("stockCode") == stock), None)
    if not pos:
        log.warning(f"{stock} 卖出前账户已无持仓，跳过本轮卖出")
        info["avail"] = 0
        return False

    avail = int(pos.get("availQuantity", 0) or 0)
    info["avail"] = avail
    info["quantity"] = int(pos.get("totalQuantity", info.get("quantity", 0)) or 0)

    account_price = _position_current_price(pos)
    if account_price > 0:
        info["account_price"] = account_price
        quote["current"] = account_price
        quote["source"] = "mock_position_pre_sell"

    log.info(f"{stock} 卖出前刷新持仓: 可卖={avail}, 账户价={account_price:.3f}")
    return avail > 0


def poll_once(state: Dict):
    stocks = list(state["positions"].keys())
    if not stocks:
        return

    state_dirty = False
    quotes = _get_realtime_quote_batch(stocks)
    account_positions_snapshot = None

    for stock, info in list(state["positions"].items()):
        if info.get("avail", 0) <= 0:
            continue

        sell_pending_until = info.get("sell_pending_until", 0)
        if sell_pending_until and time.time() < sell_pending_until:
            continue
        pending_sell = info.get("pending_sell")
        if pending_sell:
            quote_for_confirm = quotes.get(stock, {})
            if _confirm_pending_sell_from_orders(stock, info, quote_for_confirm, pending_sell):
                state_dirty = True
                continue
            state_dirty = True

        quote = quotes.get(stock, {})
        if not quote:
            continue

        current = quote.get("current", 0)
        if current <= 0:
            continue

        if current > info["peak_price"]:
            state["positions"][stock]["peak_price"] = current
            state_dirty = True
            log.info(f"{stock} 持仓期最高价更新: {current:.3f}")

        triggers = check_triggers(stock, info, quote)
        if not triggers:
            continue

        if account_positions_snapshot is None:
            try:
                account_positions_snapshot = get_current_positions()
            except Exception as e:
                log.warning(f"卖出前刷新账户持仓失败，本轮触发卖出全部跳过: {e}")
                state_dirty = True
                break

        if not _refresh_position_before_sell(stock, info, quote, account_positions_snapshot):
            state_dirty = True
            continue

        triggers = check_triggers(stock, info, quote)
        if not triggers:
            state_dirty = True
            continue

        trigger = triggers[0]

        # 规则触发即卖，不再调 LLM 二次确认
        decision = "SELL"
        log.info(f"{stock} 触发{trigger['reason']}，按规则执行卖出")
        do_sell_and_push(stock, info, quote, trigger, decision, state)
        save_state(state)

    if state_dirty:
        save_state(state)


# ============================================================================
# 主进程
# ============================================================================

def main():
    log.info("盘中实时监控进程启动 " + "=" * 30)
    log.info(f"时间: {datetime.datetime.now()}")

    if not _acquire_lock():
        log.info("已有进程在运行，本轮退出")
        return

    if not market_open_today():
        log.info("今天非交易日，退出")
        return

    state = load_state()
    positions = load_positions()
    if not positions:
        log.info("无持仓，退出")
        return

    # 止盈档位只从 trades.json 的已确认成交记录恢复；旧 state 只继承未成交卖单冷却。
    old = state.get("positions", {})
    for stock, pinfo in positions.items():
        if stock in old:
            pinfo["sell_pending_until"] = old[stock].get("sell_pending_until", 0)
            pinfo["pending_sell"] = old[stock].get("pending_sell")
    state["positions"] = positions
    save_state(state)
    stock_list = ", ".join([f"{info['name']}({code})" for code, info in positions.items()])
    log.info(f"加载持仓 {len(positions)} 只：{list(positions.keys())}")
    _push_message(
        f"【实时监控】盘中实时监控进程启动\n"
        f"监控 {len(positions)} 只股票：{stock_list}\n"
        f"执行窗口：09:30-14:50，14:50执行尾盘兜底快照\n"
        f"时间：{datetime.datetime.now().strftime('%H:%M:%S')}"
    )

    while True:
        now = datetime.datetime.now()
        t = now.time()
        if t >= FINAL_SNAPSHOT_TIME:
            log.info("已到 14:50，退出实时轮询并执行兜底快照")
            break


        # 午休期间不发送卖出委托，只更新价格（持仓期最高价继续跟踪）
        if LUNCH_BREAK_START <= t < LUNCH_BREAK_END:
            wait_seconds = (datetime.datetime.combine(now.date(), LUNCH_BREAK_END) - now).total_seconds()
            log.info(f"[午休中 11:30-13:00] 跳过卖出判断，{wait_seconds:.0f}秒后恢复")
            time.sleep(min(wait_seconds, POLL_INTERVAL))
            continue

        poll_once(state)
        now = datetime.datetime.now()
        seconds_to_final = (
            datetime.datetime.combine(now.date(), FINAL_SNAPSHOT_TIME) - now
        ).total_seconds()
        if seconds_to_final <= 0:
            break
        time.sleep(min(POLL_INTERVAL, max(1, seconds_to_final)))

    snapshot_delay = (
        datetime.datetime.now() - datetime.datetime.combine(datetime.date.today(), FINAL_SNAPSHOT_TIME)
    ).total_seconds()
    if snapshot_delay <= FINAL_SNAPSHOT_GRACE_SECONDS:
        log.info("14:50 尾盘兜底快照")
        try:
            from intraday_executor import run_monitor_mode
            run_monitor_mode()
            _push_message("【实时监控】14:50 尾盘兜底快照执行完毕")
        except Exception as e:
            log.error(f"兜底快照失败: {e}")
    else:
        log.warning(f"已超过14:50兜底窗口{snapshot_delay:.0f}秒，跳过下单型兜底快照")
        _push_message("【实时监控】已超过14:50兜底窗口，跳过下单型兜底快照")

    log.info("盘中实时监控进程退出")


if __name__ == "__main__":
    main()
