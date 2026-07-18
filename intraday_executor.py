#!/usr/bin/env python3
import os
import sys
import logging
import argparse
import gzip
import json
import re
import requests
import shlex
import subprocess
import time
import urllib.request
from pathlib import Path
from datetime import date, datetime, time as dt_time, timedelta
from typing import Any, Dict, List, Literal, Optional
from workflow_common import coerce_bool, setup_file_logging

try:
    from pydantic import BaseModel, Field
except ModuleNotFoundError:
    class BaseModel:
        def __init__(self, **data):
            for key, value in data.items():
                setattr(self, key, value)

        def model_dump(self) -> Dict[str, Any]:
            return dict(self.__dict__)

        def dict(self) -> Dict[str, Any]:
            return dict(self.__dict__)

    def Field(default=..., **_kwargs):
        return default

# ── XQShare 客户端（延迟初始化）─────────────────────────
_XQ_CLIENT = None

# ─── 分时任务配置 ───────────────────────────────────────
SELL_MONITOR_START_TIME = dt_time(10, 0)
SELL_MONITOR_FINAL_TIME = dt_time(14, 50)
SELL_MONITOR_FINAL_GRACE_SECONDS = int(os.getenv("INTRADAY_SELL_FINAL_GRACE_SECONDS", "60"))

def _get_xq_client():
    """延迟连接 XQShare（仅当日线数据获取时使用）"""
    global _XQ_CLIENT
    if _XQ_CLIENT is not None:
        return _XQ_CLIENT
    try:
        sys.path.insert(0, str(BASE_DIR.parent / "knowledge-base" / "xqshare"))
        from client import XtQuantRemote
        host = os.environ.get("XQSHARE_REMOTE_HOST", "127.0.0.1")
        port = int(os.environ.get("XQSHARE_PORT", "18812"))
        _XQ_CLIENT = XtQuantRemote(host=host, port=port, log_level="WARNING")
        if hasattr(_XQ_CLIENT, "connect"):
            _XQ_CLIENT.connect()
        return _XQ_CLIENT
    except Exception as e:
        logging.getLogger("intraday_executor").warning(f"XQShare连接失败: {e}")
        _XQ_CLIENT = None
        return None


# ── 路径配置 ──────────────────────────────────────────────
BASE_DIR = Path(__file__).parent
OUTPUT_DIR = BASE_DIR / "output"
LOG_DIR = BASE_DIR / "logs"

_BUY_TIMING_MARKET_BUFFER: Dict[str, Any] = {}
_BUY_TIMING_MARKET_LAST_FLUSH = 0.0

from trade_position_sync import load_local_env, reconcile_trades_file_with_account, reconcile_trades_with_positions

load_local_env()

logger = logging.getLogger("intraday_executor")
_LOGGING_READY = False


def setup_logging() -> logging.Logger:
    global logger, _LOGGING_READY
    if not _LOGGING_READY:
        logger = setup_file_logging(
            logger_name="intraday_executor",
            log_dir=LOG_DIR,
            filename_prefix="intraday",
        )
        _LOGGING_READY = True
    return logger

# ── mx-moni / 任务配置 ───────────────────────────────────
API_URL = os.getenv("MX_API_URL", "https://mkapi2.dfcfs.com/finskillshub")
API_KEY = os.getenv("MX_APIKEY")
WEBHOOK_URL = os.getenv("FEISHU_WEBHOOK_URL")
PORTFOLIO_VALUE = float(os.getenv("PORTFOLIO_VALUE", "1000000"))
MAX_POSITIONS = int(os.getenv("MAX_POSITIONS", "5"))
POSITION_SIZE_PCT = float(os.getenv("POSITION_SIZE_PCT", "0.20"))
XQ_HTTP_BASE = os.getenv("XQSHARE_HTTP_BASE", "http://127.0.0.1:8080").rstrip("/")

PARAM_FILE = BASE_DIR / "params.json"


def _load_params() -> Dict:
    defaults = {
        "position_size_pct": POSITION_SIZE_PCT,
        "max_positions": MAX_POSITIONS,
        "stop_loss_pct": -0.03,
        "take_profit_1": 0.10,
        "take_profit_2": 0.20,
        "take_profit_3": 0.50,
    }
    if PARAM_FILE.exists():
        try:
            with open(PARAM_FILE, encoding="utf-8") as f:
                saved = json.load(f)
            if isinstance(saved, dict):
                defaults.update(saved)
        except Exception as e:
            logger.warning(f"params.json 读取失败，使用默认参数: {e}")
    return defaults


PARAMS = _load_params()
MAX_POSITIONS = int(PARAMS["max_positions"])
POSITION_SIZE_PCT = float(PARAMS["position_size_pct"])
STOP_LOSS_PCT = float(PARAMS["stop_loss_pct"])
TAKE_PROFIT_1 = float(PARAMS["take_profit_1"])
TAKE_PROFIT_2 = float(PARAMS["take_profit_2"])
TAKE_PROFIT_3 = float(PARAMS["take_profit_3"])
INTRADAY_MONITOR_RECONCILE_ENABLED = os.getenv("INTRADAY_MONITOR_RECONCILE", "0") == "1"

# ATR 移动止损分档（基于持仓期最高价）—— 与 intraday_monitor_realtime.py:77 保持一致
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


def _normalize_pct_param(value: Any, default: float) -> float:
    """Normalize config pct: 20 means 20%, 0.2 means 20%; absurd values fall back."""
    try:
        raw = float(value)
    except (TypeError, ValueError):
        return float(default)
    if abs(raw) <= 1:
        return raw
    if abs(raw) <= 100:
        return raw / 100.0
    return float(default)


def _calc_trailing_stop_pct(
    buy_price: float,
    current_price: float,
    atr: Optional[float] = None,
    peak_price: Optional[float] = None,
) -> float:
    buy_price = float(buy_price or 0)
    current_price = float(current_price or 0)
    if buy_price <= 0:
        return STOP_LOSS_PCT
    peak = max(float(peak_price or 0), current_price)
    peak_pnl = (peak - buy_price) / buy_price if peak > buy_price else 0.0
    for threshold, stop_line in sorted(ATR_TIERS, key=lambda item: item[0], reverse=True):
        if peak_pnl < threshold:
            continue
        if stop_line is not None:
            return float(stop_line)
        if atr and atr > 0:
            return max(((buy_price - 2 * float(atr)) - buy_price) / buy_price, STOP_LOSS_PCT)
        return STOP_LOSS_PCT
    return STOP_LOSS_PCT

INTRADAY_LLM_MODEL = os.getenv("INTRADAY_LLM_MODEL", "minimax-portal/MiniMax-M3")
INTRADAY_LLM_FALLBACK_MODEL = os.getenv("INTRADAY_LLM_FALLBACK_MODEL", "openai/gpt-5.6-sol")
INTRADAY_BUY_TIMING_LLM_MODEL = os.getenv("INTRADAY_BUY_TIMING_LLM_MODEL", INTRADAY_LLM_MODEL)
INTRADAY_BUY_TIMING_LLM_FALLBACK_MODEL = os.getenv("INTRADAY_BUY_TIMING_LLM_FALLBACK_MODEL", INTRADAY_LLM_FALLBACK_MODEL)


class IntradayBuyTimingDecision(BaseModel):
    action: Literal[
        "BUY_NOW", "WAIT", "SKIP_TODAY", "KEEP_ORDER",
        "CANCEL_WAIT", "CANCEL_REBUY", "CANCEL_SKIP_TODAY",
    ] = Field(description="本轮买入/挂单处理动作")
    price_mode: Literal["NONE", "FOLLOW", "PASSIVE", "DIP", "CUSTOM"] = Field(description="报价模式；实际报价由程序统一计算")
    limit_price: Optional[float] = Field(default=None, description="保留字段，实际报价不用它")
    max_premium_pct: float = Field(ge=-3.0, le=1.5, description="相对最新价的报价偏离百分比")
    confidence: int = Field(ge=0, le=100, description="本轮动作置信度")
    reason: str = Field(description="一句话说明本轮判断")


def _with_intraday_feishu_prefix(msg: str) -> str:
    prefix = os.getenv("INTRADAY_FEISHU_PREFIX", "【实时监控】【盘中交易】")
    text = str(msg or "")
    if not prefix:
        return text
    if text.startswith(prefix) or text.startswith("【实时监控】") or text.startswith("【盘中交易】"):
        return text
    if text.startswith("[DRY-RUN] "):
        return "[DRY-RUN] " + prefix + "\n" + text[len("[DRY-RUN] "):]
    return prefix + "\n" + text


def feishu_push(msg: str, webhook: str = None) -> bool:
    webhook = webhook or WEBHOOK_URL
    if not webhook:
        logger.debug("飞书未配置，跳过推送")
        return False
    msg = _with_intraday_feishu_prefix(msg)
    raw_delays = os.getenv("FEISHU_PUSH_RETRY_DELAYS", "5,15,30")
    try:
        retry_delays = [max(0, int(x.strip())) for x in raw_delays.split(",") if x.strip()]
    except Exception:
        retry_delays = [5, 15, 30]
    max_attempts = 1 + len(retry_delays)

    last_error = ""
    for attempt in range(1, max_attempts + 1):
        feishu_code = None
        feishu_msg = ""
        body = ""
        try:
            resp = requests.post(webhook, json={"msg_type": "text", "content": {"text": msg}}, timeout=10)
            body = (resp.text or "")[:500]
            ok = resp.status_code == 200
            try:
                data = resp.json()
                feishu_code = data.get("code", data.get("errcode", data.get("StatusCode")))
                feishu_msg = data.get("msg", data.get("errmsg", data.get("StatusMessage", "")))
                if feishu_code not in (None, 0, "0"):
                    ok = False
                logger.info(
                    f"飞书推送: attempt={attempt}/{max_attempts} "
                    f"http={resp.status_code} code={feishu_code} msg={feishu_msg} body={body[:200]}"
                )
            except Exception:
                logger.info(f"飞书推送: attempt={attempt}/{max_attempts} http={resp.status_code} body={body[:200]}")
            if ok:
                return True
            last_error = f"http={resp.status_code} code={feishu_code} msg={feishu_msg} body={body}"
            rate_limited = (
                resp.status_code == 429
                or str(feishu_code) == "11232"
                or "frequency limited" in str(feishu_msg).lower()
                or "frequency limited" in body.lower()
            )
            if not rate_limited or attempt >= max_attempts:
                break
            delay = retry_delays[attempt - 1]
            logger.warning(f"飞书推送被限流，{delay}s 后重试: {last_error[:200]}")
            time.sleep(delay)
        except Exception as e:
            last_error = str(e)
            if attempt >= max_attempts:
                break
            delay = retry_delays[attempt - 1]
            logger.warning(f"飞书推送异常，{delay}s 后重试: {e}")
            time.sleep(delay)
    logger.error(f"飞书推送失败: {last_error}")
    return False


def _should_skip_non_trading_day(task_label: str, now: datetime = None) -> bool:
    """Return True when intraday tasks should quietly skip a non-trading day."""
    now = now or datetime.now()
    today_str = now.date().strftime("%Y%m%d")
    try:
        from execute_debate_result import is_trading_day
        if is_trading_day():
            return False
        logger.info(f"今日({today_str})为非交易日,跳过{task_label}")
        if os.getenv("INTRADAY_PUSH_NON_TRADING_SKIP", "0") == "1":
            feishu_push(f"📅 {now.date()} 非交易日,跳过{task_label}")
        return True
    except Exception as e:
        logger.warning(f"交易日判断失败,默认继续: {e}")
        return False


def _is_success_response(result: Dict) -> bool:
    if not isinstance(result, dict) or not result:
        return False
    if result.get("success") is False:
        return False
    code = result.get("code", result.get("status"))
    if code in (None, ""):
        return True
    try:
        return int(code) in (0, 200)
    except Exception:
        return str(code).lower() in {"ok", "success", "submitted"}


def _env_flag(name: str, default: bool = False) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    return str(value).strip().lower() in {"1", "true", "yes", "on"}


def _orders_payload_from_response(result: Dict) -> Optional[List[Dict]]:
    """Return the raw orders list only when the API response is trustworthy."""
    if not _is_success_response(result):
        return None
    order_data = result.get("data", {}) if isinstance(result, dict) else {}
    if not isinstance(order_data, dict):
        return None
    if "orders" in order_data:
        orders = order_data.get("orders")
    elif "orderList" in order_data:
        orders = order_data.get("orderList")
    else:
        return None
    return orders if isinstance(orders, list) else None


def _extract_api_error(result: Dict, default: str = "API请求失败") -> str:
    if not isinstance(result, dict) or not result:
        return default
    return str(result.get("message") or result.get("msg") or result.get("error") or result.get("errmsg") or default)


def _get_historical_prices(stock_code: str, days: int = 30) -> Optional[list]:
    try:
        xt_code = _to_xt_code(stock_code)
        url = f"{XQ_HTTP_BASE}/market_data3?stock={xt_code}&period=1d&count={days}"
        r = requests.get(url, timeout=10)
        d = r.json() or {}
        if not d.get("success") or not isinstance(d.get("data"), dict):
            return None
        data = d["data"]
        dates = set()
        for field in data.values():
            if isinstance(field, dict):
                dates.update(field.keys())
        rows = []
        for day in sorted(dates)[-days:]:
            def val(name):
                raw = data.get(name, {}).get(day, {}) if isinstance(data.get(name), dict) else {}
                if isinstance(raw, dict) and raw:
                    return float(next(iter(raw.values())) or 0)
                return 0.0
            rows.append([day, val("open"), val("close"), val("high"), val("low"), val("volume")])
        return rows or None
    except Exception as e:
        logger.debug(f"历史K线获取失败 {stock_code}: {e}")
        return None


def _calc_atr(hist: list) -> Optional[float]:
    if not hist or len(hist) < 15:
        return None
    trs = []
    for i in range(1, len(hist)):
        high = float(hist[i][3] or hist[i][2] or 0)
        low = float(hist[i][4] or hist[i][2] or 0)
        prev_close = float(hist[i - 1][2] or 0)
        if high <= 0 or low <= 0 or prev_close <= 0:
            continue
        trs.append(max(high - low, abs(high - prev_close), abs(low - prev_close)))
    return sum(trs[-14:]) / min(14, len(trs)) if trs else None


def _calc_ma20(hist: list) -> Optional[float]:
    closes = [float(row[2] or 0) for row in (hist or []) if float(row[2] or 0) > 0]
    if len(closes) < 20:
        return None
    return sum(closes[-20:]) / 20


def _get_atr_and_ma20(stock_code: str) -> tuple:
    hist = _get_historical_prices(stock_code, days=60)
    if not hist:
        return None, None
    return _calc_atr(hist), _calc_ma20(hist)

# ── mx-moni API ─────────────────────────────────────────
def mx_api_post(endpoint: str, payload: Dict, retries: int = 3) -> Dict:
    """调用 mx-moni API；不再使用本地缓存，避免盘中任务读到旧快照。"""
    if not API_KEY:
        raise RuntimeError("MX_APIKEY 未设置")

    url = f"{API_URL}{endpoint}"
    headers = {"Content-Type": "application/json", "apikey": API_KEY}
    for attempt in range(retries):
        try:
            resp = requests.post(url, headers=headers, json=payload, timeout=20)
            resp.raise_for_status()
            result = resp.json() or {}
            if result.get("code") == 112 or result.get("status") == 112:
                wait = min((attempt + 1) * 5, 30)
                logger.warning(f"API 限速(112) [{endpoint}]，不使用缓存，等待 {wait}s 后重试 ({attempt+1}/{retries})")
                time.sleep(wait)
                continue
            code = str(result.get("code") or result.get("status") or "")
            if result.get("success") is False or (code and code != "200"):
                raise RuntimeError(f"API业务失败 [{endpoint}]: {result.get('message') or result}")
            return result
        except requests.exceptions.HTTPError as e:
            if e.response is not None and e.response.status_code == 429:
                wait = min((attempt + 1) * 10, 30)
                logger.warning(f"HTTP 429 [{endpoint}]，不使用缓存，等待 {wait}s 后重试 ({attempt+1}/{retries})")
                time.sleep(wait)
                continue
            logger.warning(f"API 请求失败 [{endpoint}]: {e}")
            return {}
        except Exception as e:
            logger.warning(f"API 请求异常 [{endpoint}]: {e}")
            if attempt < retries - 1:
                time.sleep((attempt + 1) * 3)
                continue
            raise
    logger.warning(f"API 限速重试耗尽 [{endpoint}]")
    return {}


TRADES_FILE = OUTPUT_DIR / "trades.json"


def _load_trades() -> Dict:
    """加载交易记录文件"""
    if TRADES_FILE.exists():
        try:
            with open(TRADES_FILE) as f:
                return json.load(f)
        except (IOError, json.JSONDecodeError, Exception):
            pass
    return {"records": []}


def _save_trades(data: Dict):
    """保存交易记录文件"""
    with open(TRADES_FILE, "w") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


def _to_xt_code(stock_code: str) -> str:
    """Convert 6-digit A-share code to XtQuant/XQShare code."""
    code = str(stock_code or "").strip().upper()
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


def _ensure_suffix(stock_code: str) -> str:
    """兼容旧调用：返回 XtQuant/XQShare 后缀代码。"""
    return _to_xt_code(stock_code)


def _first_present(data: Dict, *keys, default=None):
    if not isinstance(data, dict):
        return default
    for key in keys:
        if key in data and data.get(key) not in (None, ""):
            return data.get(key)
    return default


def _as_float(value, default=None):
    if isinstance(value, (list, tuple)):
        value = value[0] if value else None
    if value in (None, "", "--", "None"):
        return default
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _as_price(value, default=None):
    """价格字段读取：A 股最小变动 0.01 元，自动 round 到 2 位消除 IEEE 754 精度尾巴。

    只用于 close/open/high/low/prev_close/limit_up/limit_down 等价格字段，
    不要用于 volume/amount/change_pct/score/timestamp 等非价格数值。
    """
    f = _as_float(value, default)
    if f is None:
        return None
    return round(f, 2)


def _level1_value(value):
    if isinstance(value, (list, tuple)):
        return _as_float(value[0] if value else None)
    return _as_float(value)


def _build_buy_order_map(orders: List[Dict]) -> Dict[str, Dict]:
    """按股票聚合同日买入/卖出订单，避免同一股票多笔订单互相覆盖。"""
    order_map: Dict[str, Dict] = {}
    for order in orders or []:
        if not isinstance(order, dict):
            continue
        stock = str(order.get("stock") or order.get("stockCode") or order.get("code") or "").strip()
        if not stock:
            continue
        existing = order_map.get(stock)
        if not existing:
            item = dict(order)
            item["stock"] = stock
            item["order_count"] = int(item.get("order_count", item.get("order_quantity", item.get("quantity", 0))) or 0)
            item["quantity"] = int(item.get("quantity", 0) or 0)
            item["duplicate_order_count"] = 1
            order_map[stock] = item
            continue

        old_qty = int(existing.get("quantity", 0) or 0)
        new_qty = int(order.get("quantity", 0) or 0)
        total_qty = old_qty + new_qty
        if total_qty > 0:
            existing["trade_price"] = round(
                (
                    float(existing.get("trade_price", 0) or 0) * old_qty
                    + float(order.get("trade_price", 0) or 0) * new_qty
                ) / total_qty,
                4,
            )
        existing["quantity"] = total_qty
        existing["order_quantity"] = int(existing.get("order_quantity", existing.get("order_count", 0)) or 0) + int(order.get("order_quantity", order.get("order_count", 0)) or 0)
        existing["order_count"] = int(existing.get("order_count", 0) or 0) + int(order.get("order_count", order.get("order_quantity", 0)) or 0)
        existing["duplicate_order_count"] = int(existing.get("duplicate_order_count", 1) or 1) + 1
        if order.get("order_time") and str(order.get("order_time")) > str(existing.get("order_time", "")):
            existing["order_time"] = order.get("order_time")
        if order.get("status"):
            existing["status"] = order.get("status")
        if order.get("dbStatus") is not None:
            existing["dbStatus"] = order.get("dbStatus")
    return order_map


def _build_order_id_map(orders: List[Dict]) -> Dict[str, Dict]:
    """按委托号索引订单；撤单重报时优先用它匹配本地 pending/last_order。"""
    result = {}
    for order in orders or []:
        order_id = _extract_order_id(order)
        if order_id not in (None, ""):
            result[str(order_id)] = order
    return result


def _latest_order_for_stock(orders: List[Dict], stock: str) -> Optional[Dict]:
    """Fallback when no order id is available: use the latest order for this stock."""
    matches = [
        o for o in orders or []
        if str(o.get("stock") or o.get("stockCode") or o.get("code") or "").strip() == str(stock)
    ]
    if not matches:
        return None
    return sorted(matches, key=lambda o: str(o.get("order_time") or o.get("time") or ""))[-1]


def _best_existing_buy_order_for_stock(orders: List[Dict], stock: str) -> Optional[Dict]:
    """Pick the order that matters most for duplicate-buy protection."""
    matches = [
        o for o in orders or []
        if str(o.get("stock") or o.get("stockCode") or o.get("code") or "").strip() == str(stock)
    ]
    if not matches:
        return None
    pending = [o for o in matches if _is_pending_order(o)]
    if pending:
        return _latest_order_for_stock(pending, stock) or pending[-1]
    filled = [o for o in matches if _is_terminal_filled_order(o) and int(o.get("quantity", 0) or 0) > 0]
    if filled:
        return _latest_order_for_stock(filled, stock) or filled[-1]
    return _latest_order_for_stock(matches, stock) or matches[-1]


def _entry_order_id(entry: Dict):
    for key in ("pending_order", "last_order"):
        order_id = _extract_order_id(entry.get(key))
        if order_id not in (None, ""):
            return str(order_id)
    submitted_orders = entry.get("submitted_orders") or []
    if submitted_orders:
        order_id = _extract_order_id(submitted_orders[-1])
        if order_id not in (None, ""):
            return str(order_id)
    return None


def _extract_order_id(obj: Any):
    """Best-effort extraction of broker order id from nested API responses."""
    keys = {
        "id",
        "orderId", "order_id", "orderID", "orderNo", "order_no",
        "entrustNo", "entrust_no", "entrustId", "entrust_id",
    }
    if isinstance(obj, dict):
        for key in keys:
            value = obj.get(key)
            if value not in (None, ""):
                return value
        for value in obj.values():
            found = _extract_order_id(value)
            if found not in (None, ""):
                return found
    elif isinstance(obj, list):
        for item in obj:
            found = _extract_order_id(item)
            if found not in (None, ""):
                return found
    return None


def _order_status_text(order: Dict) -> str:
    status = _first_present(
        order,
        "status", "orderStatus", "order_status", "state", "entrustStatus", "businessStatus",
        default="",
    )
    return str(status or "").strip()


def _is_sell_order_raw(order: Dict) -> bool:
    raw = str(_first_present(order, "drt", "direction", "type", "bsFlag", default="")).lower()
    return raw in {"2", "sell", "s", "卖", "卖出"}


def _order_db_status(order: Dict) -> int:
    """Extract dbStatus, checking both top-level and nested raw dict."""
    v = int(order.get("dbStatus", 0) or 0)
    if v:
        return v
    raw = order.get("raw")
    if isinstance(raw, dict):
        return int(raw.get("dbStatus", 0) or 0)
    return 0


def _parse_order_time(order: Dict) -> Optional[datetime]:
    for key in ("time", "orderTime", "tradeTime", "createdAt", "createTime", "entrustTime", "businessTime"):
        raw = order.get(key)
        if raw in (None, "", 0, "0"):
            continue
        if isinstance(raw, datetime):
            return raw
        text = str(raw).strip()
        if text.isdigit() and len(text) == 14:
            try:
                return datetime.strptime(text, "%Y%m%d%H%M%S")
            except Exception:
                pass
        if isinstance(raw, (int, float)) or str(raw).isdigit():
            try:
                ts = int(raw)
                if ts > 10_000_000_000:
                    ts = ts / 1000
                return datetime.fromtimestamp(ts)
            except Exception:
                continue
        for fmt in ("%Y-%m-%d %H:%M:%S", "%Y/%m/%d %H:%M:%S", "%Y%m%d%H%M%S"):
            try:
                return datetime.strptime(text, fmt)
            except Exception:
                pass
        try:
            return datetime.fromisoformat(text.replace("Z", "+00:00")).replace(tzinfo=None)
        except Exception:
            continue
    return None


def _is_pending_order(order: Dict) -> bool:
    db_status = _order_db_status(order)
    if db_status in {4, 5, 6, 7, 8, 100, 200}:
        return False
    status_str = _order_status_text(order).lower()
    terminal_words = [
        "已成", "全部成交", "filled", "done",
        "已撤", "撤单", "cancel", "canceled", "cancelled",
        "废单", "拒绝", "失败", "rejected", "invalid", "error",
    ]
    if any(word in status_str for word in terminal_words):
        return False
    pending_words = [
        "未成交", "未成", "已报", "待报", "正报", "部成", "部分成交",
        "pending", "submitted", "accepted", "partial", "open", "queued",
    ]
    order_qty = int(order.get("order_quantity", order.get("order_count", 0)) or 0)
    filled_qty = int(order.get("quantity", 0) or 0)
    if order_qty > 0:
        return filled_qty < order_qty
    if any(word in status_str for word in pending_words):
        return True
    if filled_qty > 0:
        return False
    return False


def _is_terminal_filled_order(order: Dict) -> bool:
    """判断是否已全部成交（非撤单/废单）."""
    db_status = _order_db_status(order)
    if db_status == 200:
        return True
    filled_qty = int(order.get("quantity", 0) or 0)
    order_qty = int(order.get("order_quantity", order.get("order_count", 0)) or 0)
    status_str = _order_status_text(order).lower()
    if filled_qty > 0 and any(word in status_str for word in ("已成", "全部成交", "filled", "done")):
        return True
    return order_qty > 0 and filled_qty > 0 and filled_qty >= order_qty


_TODAY_ORDERS_CACHE = {"ts": 0.0, "data": {"buys": [], "sells": []}}


def get_today_orders(force: bool = False) -> Dict:
    """获取今日成交记录；短时缓存避免同一轮反复打 /orders 导致 112 限速。"""
    ttl = float(os.getenv("INTRADAY_ORDERS_CACHE_TTL_SEC", "6"))
    now_mono = time.monotonic()
    if not force and ttl > 0 and now_mono - float(_TODAY_ORDERS_CACHE.get("ts", 0.0) or 0.0) < ttl:
        cached = _TODAY_ORDERS_CACHE.get("data") or {"buys": [], "sells": []}
        return {
            "buys": list(cached.get("buys", [])),
            "sells": list(cached.get("sells", [])),
            "_ok": bool(cached.get("_ok", True)),
        }
    if not API_KEY:
        return {"buys": [], "sells": [], "_ok": False}
    try:
        data = mx_api_post("/api/claw/mockTrading/orders", {})
        orders = _orders_payload_from_response(data)
        if orders is None:
            error_msg = _extract_api_error(data, "今日委托响应缺少orders/orderList")
            logger.warning(f"获取今日成交记录失败: {error_msg}")
            fallback = {"buys": [], "sells": [], "_ok": False}
            _TODAY_ORDERS_CACHE["ts"] = now_mono
            _TODAY_ORDERS_CACHE["data"] = fallback
            return fallback
        today = date.today()
        buys = []
        sells = []
        for o in orders:
            order_time = _parse_order_time(o)
            if order_time and order_time.date() != today:
                continue
            price_dec = pow(10, int(o.get("priceDec", 2) or 2))
            trade_price_dec = pow(10, int(o.get("tradePriceDec", o.get("priceDec", 2)) or 2))
            record = {
                "stock": o.get("secCode", ""),
                "name": o.get("secName", ""),
                "order_price": float(o.get("price", 0) or 0) / price_dec,
                "trade_price": float(o.get("tradePrice", 0) or 0) / trade_price_dec,
                "quantity": int(o.get("tradeCount", 0)),
                "order_quantity": int(o.get("count", o.get("orderCount", 0)) or 0),
                "order_time": order_time.isoformat() if order_time else "",
                "order_id": _extract_order_id(o),
                "status": _order_status_text(o),
                "raw": o,
            }
            if _is_sell_order_raw(o):
                sells.append(record)
            else:
                buys.append(record)
        result = {"buys": buys, "sells": sells, "_ok": True}
        _TODAY_ORDERS_CACHE["ts"] = now_mono
        _TODAY_ORDERS_CACHE["data"] = result
        return {"buys": list(buys), "sells": list(sells), "_ok": True}
    except Exception as e:
        logger.warning(f"获取今日成交记录失败: {e}")
        fallback = {"buys": [], "sells": [], "_ok": False}
        _TODAY_ORDERS_CACHE["ts"] = now_mono
        _TODAY_ORDERS_CACHE["data"] = fallback
        return fallback


def query_pending_buy_orders(stock_code: str = None, force: bool = False) -> List[Dict]:
    """Return today's accepted-but-not-filled buy orders."""
    orders = get_today_orders(force=force).get("buys", [])
    pending = [o for o in orders if _is_pending_order(o)]
    if stock_code:
        pending = [o for o in pending if o.get("stock") == stock_code]
    return pending


def query_pending_sell_orders(stock_code: str = None, today_orders: Dict = None, force: bool = False) -> List[Dict]:
    """Return today's accepted-but-not-filled sell orders."""
    snapshot = today_orders if today_orders is not None else get_today_orders(force=force)
    orders = snapshot.get("sells", []) if isinstance(snapshot, dict) else []
    pending = [o for o in orders if _is_pending_order(o)]
    if stock_code:
        pending = [o for o in pending if str(o.get("stock") or "") == str(stock_code)]
    return pending


def cancel_buy_order(order_id, stock_code: str = "", reason: str = "") -> Dict:
    """Cancel a single pending buy order. Fails closed if no order id is known."""
    if not order_id:
        return {"status": "error", "error": "缺少委托号，不能安全撤单", "stock": stock_code}
    if _env_flag("DRY_RUN"):
        logger.info(f"[DRY-RUN] CANCEL BUY order_id={order_id} stock={stock_code} reason={reason}")
        return {"status": "dry_run", "order_id": order_id, "stock": stock_code}
    try:
        result = mx_api_post("/api/claw/mockTrading/cancel", {"type": "order", "orderId": order_id, "stockCode": stock_code})
        if not _is_success_response(result):
            error_msg = _extract_api_error(result, "撤单失败")
            logger.warning(f"撤单失败 {stock_code} order_id={order_id}: {error_msg}")
            return {"status": "error", "error": error_msg, "order_id": order_id, "stock": stock_code}
        logger.info(f"撤单成功 {stock_code} order_id={order_id}")
        return {"status": "submitted", "response": result, "order_id": order_id, "stock": stock_code}
    except Exception as e:
        logger.warning(f"撤单异常 {stock_code} order_id={order_id}: {e}")
        return {"status": "error", "error": str(e), "order_id": order_id, "stock": stock_code}


def get_portfolio() -> Dict:
    """获取当前持仓"""
    try:
        return mx_api_post("/api/claw/mockTrading/portfolio", {})
    except Exception as e:
        logger.error(f"获取持仓失败: {e}")
        return {}


def get_realtime_price(stock_code: str) -> Optional[float]:
    """
    获取股票实时价格
    主:mx-data;备:腾讯行情API
    """
    # 方案1:mx-data
    try:
        cmd = [
            sys.executable,
            str(Path(__file__).parent.parent / "skills/mx-data" / "mx_data.py"),
            f"{stock_code} 当前最新价",
        ]
        r = subprocess.run(cmd, capture_output=True, text=True, timeout=20)
        output = r.stdout
        if "上限" in output or "frequency" in output.lower() or "调用次数" in output:
            raise RuntimeError("mx-data限额")
        # 支持两种格式
        matches = re.findall(r'\|\s*([\d.]+)\s*\|', output)
        if not matches:
            matches = re.findall(r'(\d+\.\d+)\s*元', output)
        if matches:
            return float(matches[0])
    except Exception:
        pass
    # 方案2:腾讯行情API
    try:
        prefix = "sh" if stock_code.startswith(('6', '5', '9')) else "sz"
        url = f"https://qt.gtimg.cn/q={prefix}{stock_code}"
        req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0", "Referer": "https://gu.qq.com"})
        with urllib.request.urlopen(req, timeout=10) as resp:
            data = resp.read().decode("gbk")
        parts = data.split("~")
        if len(parts) > 3:
            return float(parts[3])
    except Exception:
        pass
    return None


def _stock_limit_pct(stock_code: str, stock_name: str = "") -> float:
    code = str(stock_code or "")
    name = str(stock_name or "").upper()
    if "ST" in name:
        return 0.05
    if code.startswith(("300", "301", "688")):
        return 0.20
    if code.startswith(("4", "8", "920")):
        return 0.30
    return 0.10




def _is_buy_quote_limit_down(quote: Dict, tolerance: float = 0.001) -> bool:
    """True only when current price is at/under the stock-specific down-limit price."""
    if not isinstance(quote, dict):
        return False
    price = _as_float(quote.get("price"))
    limit_down = _as_float(quote.get("limit_down"))
    if not price or not limit_down or price <= 0 or limit_down <= 0:
        return False
    return price <= limit_down * (1 + max(0.0, tolerance))


def _limit_down_block_reason(quote: Dict) -> str:
    price = _as_float((quote or {}).get("price"))
    limit_down = _as_float((quote or {}).get("limit_down"))
    change_pct = _as_float((quote or {}).get("change_pct"))
    parts = []
    if price is not None:
        parts.append(f"现价{price:.2f}")
    if limit_down is not None:
        parts.append(f"跌停价{limit_down:.2f}")
    if change_pct is not None:
        parts.append(f"涨跌幅{change_pct:+.2f}%")
    return "已到个股跌停价，禁止买入" + ("（" + "，".join(parts) + "）" if parts else "")


def _normalize_realtime_quote(stock_code: str, raw: Dict, source: str = "") -> Optional[Dict]:
    """Normalize XQShare/Tencent-like quote dictionaries into executor quote shape."""
    if not isinstance(raw, dict):
        return None
    price = _as_price(_first_present(raw, "lastPrice", "last_price", "latestPrice", "price", "close"))
    if not price or price <= 0:
        return None

    prev_close = _as_price(_first_present(raw, "lastClose", "preClose", "prevClose", "pre_close", "y_close"))
    change_pct = _as_float(_first_present(raw, "changePct", "pctChg", "change_pct", "pct_change"))
    if prev_close and prev_close > 0:
        change_pct = round((price - prev_close) / prev_close * 100, 2)
    elif change_pct is not None and change_pct > -99:
        prev_close = price / (1 + change_pct / 100) if change_pct != -100 else None
    else:
        change_pct = 0.0

    name = str(_first_present(raw, "name", "secName", default="") or "")
    limit_pct = _stock_limit_pct(stock_code, name)
    limit_up = _as_price(_first_present(raw, "upperLimit", "limitUp", "limit_up"))
    limit_down = _as_price(_first_present(raw, "lowerLimit", "limitDown", "limit_down"))
    if prev_close and prev_close > 0:
        limit_up = limit_up or round(prev_close * (1 + limit_pct), 2)
        limit_down = limit_down or round(prev_close * (1 - limit_pct), 2)
    if not limit_up:
        limit_up = round(price * (1 + limit_pct), 2)
    if not limit_down:
        limit_down = round(price * (1 - limit_pct), 2)

    open_price = _as_price(_first_present(raw, "open", "openPrice"))
    high = _as_price(_first_present(raw, "high", "highPrice"))
    low = _as_price(_first_present(raw, "low", "lowPrice"))
    volume = _as_float(_first_present(raw, "volume", "vol"), 0.0)
    amount = _as_float(_first_present(raw, "amount", "turnover"), 0.0)
    bid1 = _level1_value(_first_present(raw, "bidPrice", "bid_price", "bid1"))
    ask1 = _level1_value(_first_present(raw, "askPrice", "ask_price", "ask1"))

    return {
        "price": round(price, 4),
        "change_pct": float(change_pct or 0.0),
        "is_limit_up": bool(limit_up and price >= limit_up - 0.01),
        "limit_up": round(float(limit_up), 4),
        "limit_down": round(float(limit_down), 4),
        "open": open_price,
        "high": high,
        "low": low,
        "prev_close": prev_close,
        "volume": volume,
        "amount": amount,
        "bid1": bid1,
        "ask1": ask1,
        "source": source,
        "raw_time": _first_present(raw, "time", "timestamp", "datetime"),
    }


def _get_peak_price_since_buy(stock_code: str, buy_date: str) -> Optional[float]:
    """获取自买入以来的历史最高价，供批次移动止盈兜底使用。"""
    if not buy_date:
        return None
    try:
        hist = _get_historical_prices(stock_code, days=365)
        if not hist:
            return None
        buy_dt = datetime.strptime(str(buy_date)[:10], "%Y-%m-%d")
        peak = 0.0
        for row in hist:
            row_dt = datetime.strptime(str(row[0])[:10], "%Y-%m-%d")
            high = float(row[3] or 0)
            if row_dt >= buy_dt and high > peak:
                peak = high
        return peak if peak > 0 else None
    except Exception:
        return None


def _get_intraday_peak_price(stock_code: str, current_price: float) -> float:
    """Prefer today's realtime high for trailing-stop peak; fall back to current price."""
    current_price = float(current_price or 0)
    try:
        quote = get_xq_realtime_quote(stock_code) or {}
        high = float(quote.get("high", 0) or 0)
        return max(current_price, high)
    except Exception:
        return current_price


def _get_post_buy_peak_price(stock_code: str, record: Dict, current_price: float) -> float:
    """Historical fallback peak from buy date(s) to today, including current price."""
    current_price = float(current_price or 0)
    buy_dates = []
    for lot in (record or {}).get("buy_records", []) or []:
        if lot.get("date"):
            buy_dates.append(str(lot.get("date"))[:10])
    if not buy_dates and (record or {}).get("buy_date"):
        buy_dates.append(str(record.get("buy_date"))[:10])
    if not buy_dates:
        return current_price
    try:
        earliest = min(datetime.strptime(d, "%Y-%m-%d") for d in buy_dates)
        hist = _get_historical_prices(stock_code, days=365) or []
        peak = current_price
        for row in hist:
            row_dt = datetime.strptime(str(row[0])[:10], "%Y-%m-%d")
            if row_dt >= earliest:
                peak = max(peak, float(row[3] or 0))
        return peak
    except Exception:
        return current_price


def _xq_http_get(endpoint: str, params: Dict = None, timeout: int = 6) -> Dict:
    url = f"{XQ_HTTP_BASE}{endpoint}"
    resp = requests.get(url, params=params or {}, timeout=timeout)
    resp.raise_for_status()
    return resp.json() or {}


def _quote_from_market_data3(stock_code: str, xt_code: str, payload: Dict) -> Optional[Dict]:
    data = payload.get("data", {}) if isinstance(payload, dict) else {}
    close_map = data.get("close", {}) if isinstance(data, dict) else {}
    if not close_map:
        return None
    dates = sorted(close_map.keys())
    if not dates:
        return None
    latest_date = dates[-1]
    prev_date = dates[-2] if len(dates) >= 2 else None

    def _field_value(field: str, day: str):
        if not day:
            return None
        return _as_float(((data.get(field, {}) or {}).get(day, {}) or {}).get(xt_code))

    price = _field_value("close", latest_date)
    if not price or price <= 0:
        return None
    raw = {
        "close": price,
        "open": _field_value("open", latest_date),
        "high": _field_value("high", latest_date),
        "low": _field_value("low", latest_date),
        "volume": _field_value("volume", latest_date),
        "lastClose": _field_value("close", prev_date) if prev_date else None,
        "time": latest_date,
    }
    return _normalize_realtime_quote(stock_code, raw, "xq_market_data3")


def get_xq_realtime_quote(stock_code: str) -> Optional[Dict]:
    """Get realtime quote from local XQShare HTTP server, with market_data3 fallback."""
    xt_code = _to_xt_code(stock_code)
    try:
        payload = _xq_http_get("/full_tick", {"stocks": xt_code})
        data = payload.get("data", {}) if payload.get("success") else {}
        raw = data.get(xt_code) or data.get(stock_code) or next(iter(data.values()), None)
        quote = _normalize_realtime_quote(stock_code, raw, "xq_full_tick")
        if quote:
            return quote
    except Exception as e:
        logger.debug(f"XQShare full_tick 获取失败 {stock_code}: {e}")

    try:
        payload = _xq_http_get("/market_data3", {"stock": xt_code, "period": "1d", "count": 2})
        quote = _quote_from_market_data3(stock_code, xt_code, payload)
        if quote:
            return quote
    except Exception as e:
        logger.debug(f"XQShare market_data3 获取失败 {stock_code}: {e}")
    return None


def get_intraday_buy_quote(stock_code: str) -> Optional[Dict]:
    """Buy timing quote source: XQShare first, existing quote stack second."""
    quote = get_xq_realtime_quote(stock_code)
    if quote:
        return quote
    quote = get_realtime_quote(stock_code)
    if quote:
        quote.setdefault("source", "legacy_quote")
    return quote


def get_realtime_quote(stock_code: str, retries: int = 3) -> Optional[Dict]:
    """
    获取股票实时报价(含最新价、涨跌幅、是否涨停、涨跌停价)
    主:mx-data(东方财富妙想,数据最全)
    备:腾讯行情API(不限次数,快速)
    返回 {"price": float, "change_pct": float, "is_limit_up": bool, "limit_up": float, "limit_down": float}
    """

    # ── 方案1: QMT HTTP API (本地xtquant数据,毫秒级,无限速) ──
    def _qmt_http_quote(code: str) -> Optional[Dict]:
        """GET /realtime_quote via QMT HTTP服务 (Windows本地)"""
        try:
            import urllib.request
            full_code = _ensure_suffix(code)
            url = f"{XQ_HTTP_BASE}/realtime_quote?stock={full_code}"
            req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
            with urllib.request.urlopen(req, timeout=10) as r:
                data = json.loads(r.read().decode("utf-8"))
            if not data.get('success'):
                return None
            return {
                'price': data.get('price'),
                'change_pct': data.get('change_pct'),
                'last_close': data.get('last_close'),
                'limit_up': data.get('limit_up'),
                'limit_down': data.get('limit_down'),
                'is_limit_up': data.get('price') and data.get('limit_up') and
                               data['price'] >= data['limit_up'] * 0.999,
                'volume': data.get('volume'),
                'amount': data.get('amount'),
            }
        except Exception:
            return None

    # ── 主方案:mx-data(数据最全,有限速) ─────────────────
    def _mx_quote(code: str) -> Optional[Dict]:
        try:
            cmd = [
                sys.executable,
                str(Path(__file__).parent.parent / "skills/mx-data/mx_data.py"),
                f"{code} 当前最新价",
            ]
            r = subprocess.run(cmd, capture_output=True, text=True, timeout=20)
            output = r.stdout
            # 检测限流
            if "上限" in output or "frequency" in output.lower() or "调用次数" in output:
                return None
            price_matches = re.findall(r'(\d+\.\d+)\s*元', output)
            if not price_matches:
                price_matches = re.findall(r'\|\s*([\d.]+)\s*\|', output)
            change_matches = re.findall(r'涨跌幅[::]\s*([+-]?\d+\.?\d*)%?', output)
            price = float(price_matches[0]) if price_matches else None
            change_pct = float(change_matches[0]) if change_matches else 0.0
            if price is None:
                return None
            y_close_est = price / (1 + change_pct / 100)
            # 涨跌停价:ST ±5%, 创业板/科创板 ±20%, 主板 ±10%
            if code.startswith(("300", "301", "688", "920")):
                limit_pct = 0.20
            else:
                limit_pct = 0.10  # mx-data无法判断ST,用10%作为默认
            is_limit_up = change_pct >= (limit_pct * 100 - 0.5)
            limit_up = round(y_close_est * (1 + limit_pct), 2)
            limit_down = round(y_close_est * (1 - limit_pct), 2)
            return {"price": price, "change_pct": change_pct, "is_limit_up": is_limit_up,
                    "limit_up": limit_up, "limit_down": limit_down}
        except Exception:
            return None

    # ── 备用方案:腾讯行情API(不限流,快速兜底) ───────────
    def _tencent_quote(code: str) -> Optional[Dict]:
        """腾讯行情API,不限次数;从中提取昨收价计算涨跌停"""
        try:
            prefix = "sh" if code.startswith(('6', '5', '9')) else "sz"
            url = f"https://qt.gtimg.cn/q={prefix}{code}"
            req = urllib.request.Request(url, headers={
                "User-Agent": "Mozilla/5.0",
                "Referer": "https://gu.qq.com"
            })
            with urllib.request.urlopen(req, timeout=10) as r:
                data = r.read().decode("gbk")
            if "none" in data.lower() or not data:
                return None
            parts = data.split("~")
            if len(parts) < 10:
                return None
            price = float(parts[3])
            y_close = float(parts[4])
            change_pct = round((price - y_close) / y_close * 100, 2) if y_close else 0.0
            is_st = "ST" in parts[1] or "*ST" in parts[1] or "S*" in parts[1]
            if is_st:
                limit_pct = 0.05
            elif code.startswith(("300", "301", "688", "920")):
                limit_pct = 0.20
            else:
                limit_pct = 0.10
            limit_up = round(y_close * (1 + limit_pct), 2)
            limit_down = round(y_close * (1 - limit_pct), 2)
            return {"price": price, "change_pct": change_pct, "is_limit_up": change_pct >= (limit_pct * 100 - 0.5),
                    "limit_up": limit_up, "limit_down": limit_down}
        except Exception:
            return None

    # 1. QMT HTTP (本地xtquant数据,最快最稳)
    for attempt in range(retries):
        result = _qmt_http_quote(stock_code)
        if result:
            return result
        if attempt < retries - 1:
            time.sleep(1)

    # 2. mx-data (东方财富数据全,有限速)
    for attempt in range(retries):
        result = _mx_quote(stock_code)
        if result:
            return result
        if attempt < retries - 1:
            time.sleep(3)

    # 3. 腾讯行情API (兜底,不限流)
    result = _tencent_quote(stock_code)
    if result:
        return result

    logger.error(f"{stock_code} 价格获取失败(mx-data+腾讯均失败)")
    return None


def get_current_positions() -> List[Dict]:
    """获取当前持仓"""
    try:
        data = mx_api_post("/api/claw/mockTrading/positions", {})
        pos_list = data.get("data", {}).get("posList", [])
        # 标准化字段名:API返回secCode/count/availCount,代码期望stockCode/totalQuantity/availQuantity
        normalized = []
        for p in pos_list:
            total_qty = int(p.get("count", 0) or 0)
            if total_qty <= 0:
                continue
            normalized.append({
                "stockCode": p.get("secCode", ""),
                "stockName": p.get("secName", ""),
                "totalQuantity": total_qty,
                "availQuantity": p.get("availCount", 0),
                "price": p.get("price", 0),          # 元(API返回分,构造函数已转换)
                "costPrice": p.get("costPrice", 0),  # 元(API返回分,构造函数已转换)
                "priceDec": p.get("priceDec", 2),    # 价格小数位数
                "costPriceDec": p.get("costPriceDec", 3),  # 成本价小数位数
                "profit": p.get("profit", 0),
                "profitPct": p.get("profitPct", 0),
                "dayProfit": p.get("dayProfit", 0),
                "dayProfitPct": p.get("dayProfitPct", 0),
                "secMkt": p.get("secMkt", 0),
            })
        return normalized
    except Exception as e:
        logger.error(f"获取当前持仓失败: {e}")
        raise


def buy_stock(stock_code: str, stock_name: str, price: float, quantity: int, reason: str = "", order_price: float = None) -> Dict:
    """买入股票。只有委托被 API 接受后才推送，避免先报买入后废单。"""
    mode = "DRY-RUN" if _env_flag("DRY_RUN") else "REAL"
    order_price = order_price or price
    logger.info(f"[{mode}] BUY {stock_code} @{order_price} x {quantity} ({reason})")

    name_display = f"{stock_name}({stock_code})" if stock_name else stock_code
    msg = (f"🟢 买入委托已提交 {name_display}\n"
           f"💰 报价: {order_price}\n"
           f"📊 数量: {quantity}股\n"
           f"💵 金额: {order_price * quantity:.2f}元\n"
           f"📝 理由: {reason}\n"
           f"⚠️ 成交状态等待订单确认")

    if _env_flag("DRY_RUN"):
        feishu_push("[DRY-RUN] " + msg)
        return {"status": "dry_run", "stock": stock_code, "price": order_price, "quantity": quantity}

    try:
        result = mx_api_post("/api/claw/mockTrading/trade", {
            "type": "buy",
            "stockCode": stock_code,
            "price": order_price,
            "quantity": quantity,
            "useMarketPrice": order_price is None,
        })
        logger.info(f"买入委托结果: {result}")
        if not _is_success_response(result):
            error_msg = _extract_api_error(result, "买入委托失败")
            logger.error(f"买入委托失败 {stock_code}: {error_msg}")
            feishu_push(f"❌ 买入委托失败 {stock_code}: {error_msg}")
            return {"status": "error", "error": error_msg, "stock": stock_code}
        order_id = _extract_order_id(result)
        feishu_push(msg)
        return {"status": "submitted", "response": result, "stock": stock_code,
                "price": order_price, "quantity": quantity, "order_id": order_id}
    except Exception as e:
        logger.error(f"买入失败 {stock_code}: {e}")
        feishu_push(f"❌ 买入失败 {stock_code}: {e}")
        return {"status": "error", "error": str(e)}


def sell_stock(stock_code: str, stock_name: str, price: float, quantity: int, reason: str = "", discount: float = 0.015) -> Dict:
    """卖出股票
    discount: 卖单限价折扣,默认 -1.5%(卖价 = current_price * 0.985)
    """
    mode = "DRY-RUN" if _env_flag("DRY_RUN") else "REAL"

    # 获取实时行情用于判断跌停状态
    quote = get_realtime_quote(stock_code)
    limit_down = float(quote.get("limit_down", 0) or 0) if quote else 0
    change_pct = float(quote.get("change_pct", 0) or 0) if quote else 0
    base_price = float(price or 0)
    current_price = base_price if base_price > 0 else (float(quote.get("price", 0) or 0) if quote else 0)

    # 跌停市价卖保护：已跌停时用市价单确保能成交
    use_market_price = False
    if limit_down > 0 and current_price <= limit_down:
        use_market_price = True
        order_price = round(limit_down, 2)  # 以跌停价挂单
        logger.warning(f"{stock_code} 已跌停(现价={current_price} <= 跌停价={limit_down}), 改用市价/跌停价卖出")
    else:
        order_price = round(current_price * (1 - discount), 2)  # 限价折扣

    # 跌停价保护:确保卖出价不低于跌停价（兜底）
    if limit_down > 0 and order_price < limit_down:
        logger.warning(f"{stock_code} 折后价{order_price} < 跌停价{limit_down},调整为跌停价")
        order_price = limit_down

    logger.info(f"[{mode}] SELL {stock_code} @{order_price} x {quantity} (原价{price}, 折扣{discount*100:.1f}%, {reason})")

    name_display = f"{stock_name}({stock_code})" if stock_name else stock_code
    msg = (f"🔴 卖出 {name_display}\n"
           f"💰 价格: {order_price} (市价×0.985)\n"
           f"📊 数量: {quantity}股\n"
           f"📝 理由: {reason}")
    feishu_push(msg)

    if _env_flag("DRY_RUN"):
        return {"status": "dry_run", "stock": stock_code, "price": price, "quantity": quantity}

    try:
        result = mx_api_post("/api/claw/mockTrading/trade", {
            "type": "sell",
            "stockCode": stock_code,
            "price": order_price,
            "quantity": quantity,
            "useMarketPrice": use_market_price,
        })
        logger.info(f"卖出委托结果: {result}")
        if not result:
            error_msg = "模拟盘卖出接口无有效响应，不能确认委托已提交"
            logger.error(f"卖出委托失败 {stock_code}: {error_msg}")
            feishu_push(f"❌ 卖出委托失败 {stock_code}: {error_msg}")
            return {"status": "error", "error": error_msg, "stock": stock_code}
        # 验证成交状态(code 可能返回字符串 '200' 或整数 200,需统一转为 int 比较)
        raw_code = result.get("code") or result.get("status")
        try:
            code = int(raw_code) if raw_code is not None else None
        except (ValueError, TypeError):
            code = raw_code
        if code is not None and code != 0 and code != 200:
            error_msg = result.get("msg") or result.get("message") or str(code)
            logger.error(f"卖出委托失败 {stock_code}: {error_msg}")
            feishu_push(f"❌ 卖出委托失败 {stock_code}: {error_msg}")
            return {"status": "error", "error": error_msg, "stock": stock_code}
        # API 接受委托但不一定已成交,先标记为 submitted,后续由 monitor 验证
        feishu_push(f"📤 卖出委托已提交 {stock_code} {quantity}股 @{order_price}\n📝 {reason}\n⚠️ 成交状态等待确认")
        return {
            "status": "submitted",
            "response": result,
            "stock": stock_code,
            "price": order_price,
            "quantity": quantity,
            "order_id": _extract_order_id(result),
        }
    except Exception as e:
        logger.error(f"卖出失败 {stock_code}: {e}")
        feishu_push(f"❌ 卖出失败 {stock_code}: {e}")
        return {"status": "error", "error": str(e)}




def _parse_signal_for_buy(s: Dict) -> str:
    """解析信号：优先读结构化字段，再解析 final_decision 文本。"""
    sig = str(s.get("signal") or s.get("action") or "").strip().upper()
    if sig in ("BUY", "WATCH", "AVOID"):
        return sig
    dec = str(s.get("final_decision") or "")
    dec_upper = dec.upper()
    dec_upper_compact = re.sub(r"\s+", "", dec_upper)
    if any(neg in dec_upper for neg in ["不给BUY", "不支撑BUY", "不推荐BUY", "不建议BUY", "不足以BUY"]):
        return "WATCH"
    if any(neg in dec_upper_compact for neg in ["不给BUY", "不支撑BUY", "不推荐BUY", "不建议BUY", "不足以BUY"]):
        return "WATCH"
    m = re.search(r"(?:\*\*)?\s*最终信号\s*(?:\*\*)?\s*[:：=]\s*(BUY|WATCH|AVOID)", dec, re.I)
    if not m:
        m = re.search(r"\b(?:signal|action)\s*[:：=]\s*(BUY|WATCH|AVOID)\b", dec, re.I)
    if m:
        return m.group(1).upper()
    if "仓位建议" in dec and "0%" in dec:
        return "AVOID"
    return "WATCH"

def _confidence_value(signal: Dict) -> float:
    for key in ("confidence", "total_score", "final_score", "score"):
        try:
            value = signal.get(key)
            if value not in (None, ""):
                if isinstance(value, str):
                    m = re.search(r"-?\d+(?:\.\d+)?", value)
                    if not m:
                        continue
                    return float(m.group(0))
                return float(value)
        except (TypeError, ValueError):
            continue
    dec = str(signal.get("final_decision") or "")
    for pattern in (
        r"(?:置信度|confidence)(?:\s|\*)*[:：=]?\s*(-?\d+(?:\.\d+)?)\s*(?:分|%)?",
        r"\bconf\s*[:：=]\s*(-?\d+(?:\.\d+)?)\b",
    ):
        m = re.search(pattern, dec, re.I)
        if m:
            try:
                return float(m.group(1))
            except (TypeError, ValueError):
                continue
    return 0.0


def _legacy_buy_position_pct(action: Any, confidence: int = None) -> float:
    """
    盘中买入下单仓位沿用旧逻辑：按 BUY/WATCH 和置信度分档。
    position_ratio 不参与实际下单金额。
    """
    if isinstance(action, dict):
        signal = action
        action = _parse_signal_for_buy(signal)
        if confidence is None:
            confidence = _confidence_value(signal)
    action = str(action or "WATCH").upper()
    if action == "AVOID":
        return 0.0
    try:
        confidence = int(confidence or 0)
    except (TypeError, ValueError):
        confidence = 0
    stars = 3 if confidence >= 75 else (2 if confidence >= 60 else 1)
    light = action == "WATCH"
    table = {
        (3, False): 0.20,
        (3, True): 0.10,
        (2, False): 0.15,
        (2, True): 0.07,
        (1, False): 0.10,
        (1, True): 0.05,
    }
    return table.get((stars, light), 0.0)


def _get_position_pct(signal: Dict) -> float:
    """
    Compatibility helper for the legacy immediate-buy path/tests.
    The active buy-timing path uses confidence tiers via _legacy_buy_position_pct.
    """
    text = str((signal or {}).get("final_decision") or "")
    match = re.search(r"(?:position_ratio|仓位建议)\s*[:：=]?\s*([0-9]+(?:\.[0-9]+)?)\s*%", text, re.I)
    if not match:
        return 0.0
    try:
        return max(0.0, min(float(match.group(1)) / 100.0, 1.0))
    except (TypeError, ValueError):
        return 0.0


def _buy_score_value(signal: Dict) -> float:
    for key in ("buy_score", "long_score"):
        try:
            value = signal.get(key)
            if value not in (None, ""):
                return max(0, min(100, float(value)))
        except (TypeError, ValueError):
            continue
    action = _parse_signal_for_buy(signal)
    conf = _confidence_value(signal)
    if action == "BUY":
        return max(70, conf)
    if action == "WATCH":
        return min(max(55, conf), 69)
    return min(conf, 54)


def _normalize_buy_signal(signal: Dict) -> Dict:
    normalized = dict(signal)
    action = _parse_signal_for_buy(normalized)
    normalized["signal"] = action
    normalized["action"] = action
    normalized["confidence"] = _confidence_value(normalized)
    if normalized.get("buy_score") in (None, ""):
        normalized["buy_score"] = _buy_score_value(normalized)
    gate = normalized.get("execution_gate")
    if not gate:
        gate = "DIRECT_BUY_ALLOWED" if action == "BUY" else "INTRADAY_CONFIRMATION_REQUIRED" if action == "WATCH" else "NO_BUY"
    normalized["execution_gate"] = gate
    normalized["intraday_execution_gate"] = gate
    normalized["intraday_entry_condition"] = normalized.get("entry_condition") or (
        "开盘强势或盘中强势可买" if gate == "DIRECT_BUY_ALLOWED" else "盘中放量突破或回踩承接确认"
    )
    normalized["intraday_block_buy_reason"] = normalized.get("block_buy_reason") or ";".join(str(x) for x in (normalized.get("signal_blockers") or [])[:2])
    normalized["allow_direct_buy"] = coerce_bool(normalized.get("allow_direct_buy"), gate == "DIRECT_BUY_ALLOWED")
    normalized["needs_intraday_confirmation"] = coerce_bool(normalized.get("needs_intraday_confirmation"), gate != "DIRECT_BUY_ALLOWED")
    return normalized


def _has_buyable_data_quality(signal: Dict) -> bool:
    flags = set(signal.get("data_quality_flags") or [])
    return not flags.intersection({"KLINE_MISSING", "KLINE_SHORT"})


def _select_intraday_timing_pool(report: Dict, target: int = 5, allow_ranked_fallback: bool = False) -> List[Dict]:
    """Return the morning Top5 observation pool without using its advice/data gaps as buy filters."""
    phase2 = report.get("phase2", {}) if report else {}
    candidates = phase2.get("top_picks") or (phase2.get("ranked_candidates") if allow_ranked_fallback else []) or []
    signals = []
    seen = set()
    for s in candidates:
        stock = s.get("stock")
        if not stock or stock in seen:
            continue
        seen.add(stock)
        if _parse_signal_for_buy(s) == "AVOID":
            continue
        signals.append(_normalize_buy_signal(s))
        if len(signals) >= target:
            break
    return signals


def _select_intraday_buy_signals(report: Dict, target: int = 5) -> List[Dict]:
    """Backward-compatible entry point for the current Top5 timing pool."""
    return _select_intraday_timing_pool(report, target, allow_ranked_fallback=True)


def _buy_timing_state_date_from_path(path: Path) -> Optional[date]:
    try:
        return datetime.strptime(path.stem.replace("intraday_buy_timing_", ""), "%Y%m%d").date()
    except Exception:
        return None


def _latest_previous_buy_timing_state_file(today: date = None) -> Optional[Path]:
    today = today or date.today()
    prev_trading_day = _previous_a_share_trading_day(today)
    if not prev_trading_day:
        return None
    path = _buy_timing_state_file(prev_trading_day)
    return path if path.exists() else None


def _previous_a_share_trading_day(today: date = None, lookback_days: int = 14) -> Optional[date]:
    today = today or date.today()
    try:
        shared_path = os.path.expanduser("~/.openclaw/agents/shared")
        if shared_path not in sys.path:
            sys.path.insert(0, shared_path)
        from trading_calendar import is_a_share_trading_day
    except Exception:
        is_a_share_trading_day = None
    cur = today - timedelta(days=1)
    for _ in range(max(1, lookback_days)):
        if is_a_share_trading_day is None:
            if cur.weekday() < 5:
                return cur
        else:
            try:
                if is_a_share_trading_day(cur.isoformat()):
                    return cur
            except Exception:
                if cur.weekday() < 5:
                    return cur
        cur -= timedelta(days=1)
    return None


def _load_report_for_day(day: date) -> Dict:
    path = OUTPUT_DIR / f"daily_report_{day.strftime('%Y%m%d')}.json"
    if not path.exists():
        return {}
    try:
        with open(path, encoding="utf-8") as f:
            return json.load(f)
    except Exception as e:
        logger.warning(f"读取历史早报失败 {path}: {e}")
        return {}


def _carryover_intraday_timing_signals(today_signals: List[Dict], today: date = None) -> List[Dict]:
    """Carry previous day's not-filled Top5 forward once, without storing full signals in state."""
    today = today or date.today()
    prev_file = _latest_previous_buy_timing_state_file(today)
    if not prev_file:
        return []
    prev_state = _load_buy_timing_state(prev_file)
    prev_day_obj = _buy_timing_state_date_from_path(prev_file)
    prev_day = prev_state.get("date") or (prev_day_obj or today).isoformat()
    try:
        prev_report_day = datetime.fromisoformat(str(prev_day)).date()
    except Exception:
        prev_report_day = prev_day_obj or today
    prev_signals = [
        _normalize_buy_signal(s)
        for s in (prev_state.get("selected_signals") or [])
        if isinstance(s, dict) and s.get("stock")
    ]
    if not prev_signals:
        prev_signals = _select_intraday_timing_pool(_load_report_for_day(prev_report_day), 5)
    prev_signal_map = {str(s.get("stock")): s for s in prev_signals if s.get("stock")}
    today_seen = {s.get("stock") for s in today_signals if s.get("stock")}
    already_carried_once = set(prev_state.get("carryover_stocks") or [])
    carryovers = []
    for stock in prev_state.get("selected_stocks", []):
        if not stock or stock in today_seen:
            continue
        if stock in already_carried_once:
            continue
        entry = (prev_state.get("stocks") or {}).get(stock, {})
        if entry.get("status") == "filled":
            continue
        signal = prev_signal_map.get(str(stock))
        if not signal:
            continue
        carried = _normalize_buy_signal(signal)
        carried["carryover_from"] = prev_day
        carried["carryover_reason"] = "previous_day_unfilled"
        carryovers.append(carried)
    return carryovers


def _merge_intraday_timing_pool(today_signals: List[Dict], carryovers: List[Dict]) -> List[Dict]:
    merged = []
    seen = set()
    for signal in list(today_signals) + list(carryovers):
        stock = signal.get("stock")
        if not stock or stock in seen:
            continue
        seen.add(stock)
        merged.append(_normalize_buy_signal(signal))
    return merged

# ── Mode: buy ────────────────────────────────────────────

def _parse_hhmm(value: str, default: dt_time) -> dt_time:
    try:
        hh, mm = value.split(":", 1)
        return dt_time(int(hh), int(mm))
    except Exception:
        return default


def _report_wait_seconds_with_optional_override(default_seconds: int) -> int:
    """
    By default buy tasks wait until their trading cutoff. A stale short
    INTRADAY_BUY_REPORT_WAIT_SECONDS value should not make them abandon a day.
    """
    wait_seconds = max(0, int(default_seconds or 0))
    if os.getenv("INTRADAY_BUY_REPORT_WAIT_SECONDS_OVERRIDE") != "1":
        return wait_seconds
    configured_wait = os.getenv("INTRADAY_BUY_REPORT_WAIT_SECONDS")
    if not configured_wait:
        return wait_seconds
    try:
        return min(max(0, int(configured_wait)), wait_seconds)
    except (TypeError, ValueError):
        logger.warning(f"INTRADAY_BUY_REPORT_WAIT_SECONDS 无效: {configured_wait}, 使用截止时间等待")
        return wait_seconds


def _buy_window() -> tuple:
    start = _parse_hhmm(os.getenv("INTRADAY_BUY_WINDOW_START", "09:31"), dt_time(9, 31))
    end = _parse_hhmm(os.getenv("INTRADAY_BUY_WINDOW_END", "10:00"), dt_time(10, 0))
    return start, end


def _is_in_buy_window(now: datetime = None) -> bool:
    if os.getenv("ALLOW_BUY_OUTSIDE_WINDOW") == "1":
        return True
    now = now or datetime.now()
    start, end = _buy_window()
    return start <= now.time() <= end


def _seconds_until_buy_window_end(now: datetime = None) -> int:
    now = now or datetime.now()
    _, end = _buy_window()
    end_dt = datetime.combine(now.date(), end)
    return max(0, int((end_dt - now).total_seconds()))


def _load_ready_daily_report(report_file: Path, wait_seconds: int = None, poll_seconds: int = 30) -> Optional[Dict]:
    """Wait briefly for today's report to become complete enough for buying."""
    if wait_seconds is None:
        wait_seconds = _report_wait_seconds_with_optional_override(_seconds_until_buy_window_end())
    deadline = time.time() + max(0, wait_seconds)
    last_reason = ""

    while True:
        if not _is_in_buy_window():
            logger.warning("已超出买入窗口，停止等待选股报告")
            feishu_push("⏳ 选股报告未在买入窗口内准备完成，今日盘中买入跳过")
            return None

        if not report_file.exists():
            last_reason = f"今日报告不存在: {report_file}"
        else:
            try:
                with open(report_file, encoding="utf-8") as f:
                    report = json.load(f)
                ranked = report.get("phase2", {}).get("ranked_candidates", [])
                if ranked:
                    return report
                last_reason = "今日报告已存在，但 ranked_candidates 为空，可能仍在生成"
            except Exception as e:
                last_reason = f"今日报告读取失败，可能仍在写入: {e}"

        if time.time() >= deadline:
            logger.error(last_reason)
            feishu_push(f"⚠️ {last_reason}\n盘中买入跳过")
            return None
        logger.info(f"{last_reason}，等待 {poll_seconds}s 后重试")
        time.sleep(poll_seconds)


def run_buy_mode():
    setup_logging()
    """
    盘中买入模式(每天 09:35 触发,cron: intraday-buy)
    1. 交易日检查
    2. 读取今日选股报告
    3. 过滤 BUY 信号
    4. 获取实时价格
    5. 计算仓位
    6. 执行买入
    """
    logger.info("=" * 50)
    logger.info("Phase 4: 盘中买入模式")
    logger.info("=" * 50)

    if _should_skip_non_trading_day("盘中买入"):
        return

    if not API_KEY:
        logger.error("MX_APIKEY 未设置")
        feishu_push("⚠️ MX_APIKEY 未配置,无法执行买入")
        return

    # Step 0: 交易日检查
    today_str = date.today().strftime("%Y%m%d")
    is_holiday = False

    if not _is_in_buy_window():
        start, end = _buy_window()
        logger.warning(f"当前不在买入窗口({start.strftime('%H:%M')}-{end.strftime('%H:%M')}),跳过买入")
        feishu_push(f"⏰ 当前不在买入窗口({start.strftime('%H:%M')}-{end.strftime('%H:%M')}),跳过盘中买入")
        return

    # Step 1: 读取今日报告
    report_file = OUTPUT_DIR / f"daily_report_{today_str}.json"
    report = _load_ready_daily_report(report_file)
    if not report:
        return

    # 优先读取 phase2.top_picks，确保盘中买入与早报展示 Top5 保持一致。
    # 老报告没有 top_picks 时，才回退到 ranked_candidates 重新筛选。
    ranked = report.get("phase2", {}).get("ranked_candidates", [])
    top_picks = report.get("phase2", {}).get("top_picks", [])
    if not top_picks and not ranked:
        logger.warning("top_picks/ranked_candidates 均为空,跳过买入")
        return

    # 构建股票代码→名称映射
    name_map = {}
    for s in list(ranked) + list(top_picks):
        stock_code = s.get("stock")
        if stock_code:
            name_map[stock_code] = s.get("name", stock_code)

    # ── 买入股票来源：优先早报Top5 ────────────────────────
    TARGET = 5
    signals = _select_intraday_buy_signals(report, TARGET)

    if not signals:
        logger.info("今日无 BUY/WATCH 信号,跳过买入")
        feishu_push(f"📋 {date.today()} 盘中\n今日无 BUY/WATCH 信号,跳过买入")
        return

    source_name = "top_picks" if top_picks else "ranked_candidates"
    logger.info(f"可买信号({source_name}): {[s['stock'] for s in signals]}")

    # Step 3: 计算可用资金
    pos_resp = mx_api_post("/api/claw/mockTrading/positions", {})
    pos_data = pos_resp.get("data", {}) if pos_resp else {}
    unit = pos_data.get("currencyUnit", 1) if pos_data else 1
    avail_balance = pos_data.get("availBalance") if pos_data else None
    if avail_balance is None:
        logger.error("无法获取真实可用资金，fail-closed 跳过买入")
        feishu_push("⚠️ 无法获取真实可用资金，为避免误买，今日盘中买入跳过")
        return
    available_cash = float(avail_balance) / unit
    logger.info(f"可用资金: {available_cash:.2f} 元")
    initial_cash = available_cash

    # Step 4: 遍历信号,涨停直接跳过,不顺位替补
    results = []
    skipped_results = []
    bought_count = 0

    for signal in signals:
        if bought_count >= MAX_POSITIONS:
            break

        stock = signal["stock"]
        name = name_map.get(stock, stock)
        # 优先用 bull_argument(多方看多论点),其次从 debate_history 提取多方首段
        # ★ 6-04 老板拍板：截断 200 字符，避免单次推送超 30KB
        reason = (signal.get("bull_argument", "") or signal.get("reason", ""))[:200]
        if not reason:
            # 从 debate_history 提取多方分析师第一段发言(【多方分析师】开头到【空方分析师】之前)
            dh = signal.get("debate_history", "") or ""
            m = re.search(r'【多方分析师】(.+?)(?=【空方分析师】|$)', dh, re.DOTALL)
            if m:
                reason = re.sub(r'\s+', ' ', m.group(1).strip())[:100]
        if not reason:
            dec = signal.get("final_decision", "") or ""
            pos_m = re.search(r'仓位建议[::]\s*([^\n]+)', dec)
            conf_m = re.search(r'置信度[::]\s*(\d+)', dec)
            sig_m = signal.get("signal", "WATCH")
            score = signal.get("final_score", 0)
            pos_str = pos_m.group(1).strip() if pos_m else sig_m
            conf_str = f"置信{conf_m.group(1)}分" if conf_m else f"评分{score}分"
            reason = f"{pos_str} | {conf_str}"

        # 获取实时报价
        quote = get_realtime_quote(stock)
        if not quote:
            logger.warning(f"无法获取 {stock} 报价,跳过")
            feishu_push(f"⚠️ 无法获取 {stock}({name}) 报价,跳过")
            skipped_results.append({"stock": stock, "name": name, "reason": "报价获取失败"})
            continue

        price = quote["price"]
        change_pct = quote["change_pct"]

        # 涨停检查:直接跳过,不替补
        if quote["is_limit_up"]:
            logger.warning(f"{stock} 今日涨停(+{change_pct:.2f}%),跳过")
            feishu_push(f"⚠️ {stock}({name}) 今日涨停(+{change_pct:.2f}%),跳过")
            skipped_results.append({"stock": stock, "name": name, "reason": f"涨停+{change_pct:.2f}%"})
            continue

        # 实际下单仓位沿用旧的 signal + confidence 分档逻辑，不使用早报仓位建议。
        sig = _parse_signal_for_buy(signal)
        conf = int(_confidence_value(signal) or 60)
        pos_pct = _legacy_buy_position_pct(sig, conf)
        max_per_stock = min(initial_cash * pos_pct, available_cash)
        if max_per_stock <= 0:
            logger.warning(f"{stock} 可用资金不足,跳过")
            skipped_results.append({"stock": stock, "name": name, "reason": "可用资金不足"})
            continue
        limit_up = quote.get("limit_up", price * 1.10)
        buffered_price = round(price * 1.015, 2)
        order_price = min(buffered_price, limit_up)
        if order_price >= limit_up:
            logger.warning(f"{stock} 缓冲价{buffered_price}>=涨停价{limit_up},以涨停价报价")

        # 最低买入数量
        min_shares = _buy_min_shares(stock)
        quantity = _buy_quantity_for_amount(stock, max_per_stock, order_price)
        if quantity < min_shares:
            cost_min = order_price * min_shares
            if cost_min <= available_cash:
                quantity = min_shares
            else:
                logger.warning(f"{stock} 仓位不足({cost_min:.0f}元 > 可用{available_cash:.0f}元),跳过")
                skipped_results.append({"stock": stock, "name": name, "reason": f"不足最低买入数量({cost_min:.0f}元 > 可用{available_cash:.0f}元)"})
                continue

        result = buy_stock(stock, name, order_price, quantity, reason)
        results.append({**signal, "price": order_price, "quantity": quantity, "result": result, "name": name})
        bought_count += 1
        if result.get("status") in ("submitted", "dry_run"):
            available_cash = max(0.0, available_cash - order_price * quantity)
            logger.info(f"买入后剩余可用资金(本轮估算): {available_cash:.2f} 元")
        time.sleep(3)

    # 从API获取真实成交数据,替换计划数据
    today_orders = get_today_orders()
    order_map = _build_buy_order_map(today_orders.get("buys", []))
    duplicate_orders = [stock for stock, order in order_map.items() if order.get("duplicate_order_count", 1) > 1]
    if duplicate_orders:
        logger.warning(f"今日成交回查发现同股多笔买入委托，已按股票汇总确认: {duplicate_orders}")

    # 用真实成交数据更新results
    confirmed_results = []
    for r in results:
        stock = r.get("stock", "")
        api_order = order_map.get(stock)
        if api_order and api_order.get("quantity", 0) > 0:
            confirmed_results.append({
                **r,  # 保留原始signal数据(action, score, reason, pool)
                "price": api_order["trade_price"],       # 实际成交价
                "quantity": api_order["quantity"],        # 实际成交数量
                "order_price": api_order["order_price"],  # 委托价
                "order_time": api_order["order_time"],     # 成交时间
                "result": r.get("result", {}),
                "filled": True,
            })
        else:
            # 没有成交(废单/被拒/零成交)
            confirmed_results.append({
                **r,
                "price": r.get("price", 0),
                "quantity": 0,
                "order_time": None,
                "result": r.get("result", {}),
                "filled": False,
                "order_seen": bool(api_order),
            })

    # 汇总推送(用实际成交价,含废单纠正)
    if confirmed_results:
        summary = f"📋 买入执行完毕 {date.today()}\n"
        for r in confirmed_results:
            filled = r.get("filled", False)
            if filled:
                summary += f"✅ {r['stock']} {r.get('name', '')} {r.get('quantity', 0)}股 @ {r.get('price', 0)}\n"
            else:
                # 废单/零成交:发纠正通知,覆盖之前的"委托已提交"推送
                order_price = r.get("price", 0)
                reason = r.get("reason", "")[:30]
                summary += f"❌ {r['stock']} {r.get('name', '')} 未成交(报价{order_price})\n"
                feishu_push(f"⚠️ 纠正:{r['stock']}({r.get('name','')}) 买入未成交\n报价{order_price},订单未成交或未出现在成交记录\n原因: {reason}")
        if skipped_results:
            summary += "跳过:\n"
            for s in skipped_results:
                summary += f"⚠️ {s['stock']} {s.get('name', '')}: {s.get('reason', '')}\n"
        feishu_push(summary)
    elif skipped_results:
        summary = f"📋 买入执行完毕 {date.today()}\n今日未提交买入委托，跳过原因:\n"
        for s in skipped_results:
            summary += f"⚠️ {s['stock']} {s.get('name', '')}: {s.get('reason', '')}\n"
        feishu_push(summary)

    # 追加买入记录到 trades.json
    trades = _load_trades()
    for r in confirmed_results:
        if r.get("quantity", 0) > 0:
            trades["records"].append({
                "stock": r.get("stock", ""),
                "name": r.get("name", ""),
                "buy_date": date.today().isoformat(),
                "buy_price": r.get("price", 0),
                "quantity": r.get("quantity", 0),
                "remaining_quantity": r.get("quantity", 0),
                "action": r.get("action", "WATCH"),
                "confidence": r.get("confidence", 60),
                "reason": r.get("reason", ""),
                "pool": r.get("pool", ""),
                "source_pools": r.get("source_pools", []),
                "strategy_type": r.get("strategy_type", ""),
                "entry_bias": r.get("entry_bias", ""),
                "source": "intraday_executor",
                "buy_records": [{
                    "date": date.today().isoformat(),
                    "price": r.get("price", 0),
                    "quantity": r.get("quantity", 0),
                    "remaining": r.get("quantity", 0),
                    "source": "intraday_executor",
                }],
                "sells": [],
            })
    try:
        _save_trades(trades)
        logger.info(f"✅ 买入执行完成: {len(confirmed_results)} 笔,已追加到 trades.json")
    except Exception as e:
        logger.error(f"❌ trades.json 买入记录写入失败: {e}")


# ── Mode: buy-timing (LLM 分时买入) ──────────────────────

def _buy_timing_start() -> dt_time:
    return _parse_hhmm(os.getenv("INTRADAY_BUY_TIMING_START", "09:31"), dt_time(9, 31))


def _buy_timing_cutoff() -> dt_time:
    return _parse_hhmm(os.getenv("INTRADAY_BUY_TIMING_CUTOFF", "14:57"), dt_time(14, 57))


def _buy_timing_skip_earliest() -> dt_time:
    return _parse_hhmm(os.getenv("INTRADAY_BUY_SKIP_EARLIEST", "14:57"), dt_time(14, 57))


def _buy_timing_launch_earliest() -> dt_time:
    return _parse_hhmm(os.getenv("INTRADAY_BUY_LAUNCH_EARLIEST", "09:20"), dt_time(9, 20))


def _buy_timing_llm_interval_minutes(now: datetime = None) -> int:
    now = now or datetime.now()
    if now.time() < dt_time(10, 0):
        return max(1, int(os.getenv("INTRADAY_BUY_LLM_INTERVAL_BEFORE_10", "3")))
    return max(1, int(os.getenv("INTRADAY_BUY_LLM_INTERVAL_AFTER_10", "10")))


def _buy_timing_realtime_thread_enabled() -> bool:
    """Legacy fast thread is permanently disabled.

    The main loop already checks technical triggers every minute. Re-enabling the
    old thread would duplicate 1m/120m K-line pulls and can make the task appear
    stuck or crashed under QMT/API pressure.
    """
    if os.getenv("INTRADAY_BUY_ENABLE_REALTIME_THREAD") == "1":
        logger.warning("INTRADAY_BUY_ENABLE_REALTIME_THREAD=1 已忽略；旧实时硬触发线程已永久禁用")
    return False


def _buy_timing_index_data_enabled() -> bool:
    """Index context is optional; stock-level same-day technicals drive buy timing."""
    return os.getenv("INTRADAY_BUY_INCLUDE_INDEX_DATA", "0") == "1"


def _parse_state_datetime(value) -> Optional[datetime]:
    if not value:
        return None
    if isinstance(value, datetime):
        return value
    try:
        return datetime.fromisoformat(str(value))
    except Exception:
        return None


def _is_buy_timing_llm_due(entry: Dict, now: datetime = None) -> bool:
    now = now or datetime.now()
    last = _parse_state_datetime(entry.get("last_llm_check_at"))
    if not last:
        return True
    interval = timedelta(minutes=_buy_timing_llm_interval_minutes(now))
    return now - last >= interval


def _is_lunch_break(now: datetime = None) -> bool:
    now = now or datetime.now()
    return dt_time(11, 30) < now.time() < dt_time(13, 0)


def _is_in_buy_timing_session(now: datetime = None) -> bool:
    if os.getenv("ALLOW_BUY_OUTSIDE_WINDOW") == "1":
        return True
    now = now or datetime.now()
    return _buy_timing_start() <= now.time() < _buy_timing_cutoff() and not _is_lunch_break(now)


def _seconds_until_buy_timing_cutoff(now: datetime = None) -> int:
    now = now or datetime.now()
    cutoff_dt = datetime.combine(now.date(), _buy_timing_cutoff())
    return max(0, int((cutoff_dt - now).total_seconds()))


def _next_buy_timing_check(now: datetime = None) -> Optional[datetime]:
    now = now or datetime.now()
    start_dt = datetime.combine(now.date(), _buy_timing_start())
    cutoff_dt = datetime.combine(now.date(), _buy_timing_cutoff())
    if now < start_dt:
        return start_dt
    if now >= cutoff_dt:
        return None
    if _is_lunch_break(now):
        return datetime.combine(now.date(), dt_time(13, 0))

    candidate = now.replace(second=0, microsecond=0) + timedelta(minutes=1)
    if dt_time(11, 30) < candidate.time() < dt_time(13, 0):
        candidate = datetime.combine(now.date(), dt_time(13, 0))
    if candidate > cutoff_dt:
        candidate = cutoff_dt
    return candidate


def _buy_timing_report_wait_seconds(now: datetime = None) -> int:
    return _report_wait_seconds_with_optional_override(_seconds_until_buy_timing_cutoff(now))


def _load_daily_report_with_top_picks(report_file: Path) -> Optional[Dict]:
    """Return today's report only when the morning Top5 is available."""
    if not report_file.exists():
        return None
    try:
        with open(report_file, encoding="utf-8") as f:
            report = json.load(f)
        if (report.get("phase2") or {}).get("top_picks"):
            return report
    except Exception as e:
        logger.debug(f"分时买入读取早报Top5失败，可能仍在写入: {e}")
    return None


def _load_ready_daily_report_for_timing(report_file: Path, wait_seconds: int = None, poll_seconds: int = 30) -> Optional[Dict]:
    if wait_seconds is None:
        wait_seconds = _buy_timing_report_wait_seconds()
    deadline = time.time() + max(0, wait_seconds)
    last_reason = ""
    while True:
        if datetime.now().time() >= _buy_timing_cutoff():
            logger.warning("已超过分时买入截止时间，停止等待选股报告")
            feishu_push(f"⏳ 选股报告未在{_buy_timing_cutoff().strftime('%H:%M')}前准备完成，今日分时买入跳过")
            return None

        if not report_file.exists():
            last_reason = f"今日报告不存在: {report_file}"
        else:
            try:
                report = _load_daily_report_with_top_picks(report_file)
                if report:
                    return report
                last_reason = "今日报告已存在，但早报Top5(top_picks)为空，可能仍在生成"
            except Exception as e:
                last_reason = f"今日报告读取失败，可能仍在写入: {e}"

        if time.time() >= deadline:
            logger.error(last_reason)
            feishu_push(f"⚠️ {last_reason}\n分时买入跳过")
            return None
        logger.info(f"{last_reason}，等待 {poll_seconds}s 后重试")
        time.sleep(poll_seconds)


def _buy_timing_state_file(day: date = None) -> Path:
    day = day or date.today()
    return OUTPUT_DIR / f"intraday_buy_timing_{day.strftime('%Y%m%d')}.json"


def _buy_timing_lock_dir(day: date = None) -> Path:
    day = day or date.today()
    return OUTPUT_DIR / f"intraday_buy_timing_{day.strftime('%Y%m%d')}.lockdir"


def _buy_timing_pid_file() -> Path:
    return OUTPUT_DIR / "buy_timing.pid"


def _buy_timing_event_file(day: date = None) -> Path:
    day = day or date.today()
    return OUTPUT_DIR / f"intraday_buy_events_{day.strftime('%Y%m%d')}.jsonl"


def _buy_timing_market_file(day: date = None) -> Path:
    day = day or date.today()
    return OUTPUT_DIR / f"intraday_buy_market_{day.strftime('%Y%m%d')}.json.gz"


def _compact_audit_market(quote: Dict = None, technical_snapshot: Dict = None) -> Dict:
    quote = quote or {}
    snapshot = technical_snapshot or {}
    result = {
        "price": _as_price(quote.get("price") or snapshot.get("latest")),
        "open": _as_price(quote.get("open") or snapshot.get("day_open")),
        "high": _as_price(quote.get("high") or snapshot.get("high")),
        "low": _as_price(quote.get("low") or snapshot.get("low")),
        "prev_close": _as_price(quote.get("prev_close")),
        "limit_up": _as_price(quote.get("limit_up")),
        "limit_down": _as_price(quote.get("limit_down")),
        "bid1": _as_price(quote.get("bid1")),
        "ask1": _as_price(quote.get("ask1")),
        "change_pct": _as_float(quote.get("change_pct") or snapshot.get("change_pct")),
        "source": quote.get("source") or snapshot.get("source"),
        "bar_count": int(snapshot.get("bar_count", 0) or 0),
        "ma": snapshot.get("ma") or {},
        "above_ma": snapshot.get("above_ma") or [],
        "crossed_up_ma": snapshot.get("crossed_up_ma") or [],
        "ma120": _as_price(snapshot.get("ma120_1m") or snapshot.get("ma120")),
        "vwap": _as_price(snapshot.get("vwap")),
        "rsi14": _as_float(snapshot.get("rsi14")),
        "macd_dif": _as_float(snapshot.get("macd_dif")),
        "macd_dea": _as_float(snapshot.get("macd_dea")),
        "macd_hist": _as_float(snapshot.get("macd_hist")),
        "kdj_k": _as_float(snapshot.get("kdj_k")),
        "kdj_d": _as_float(snapshot.get("kdj_d")),
        "kdj_j": _as_float(snapshot.get("kdj_j")),
        "high_retreat_pct": _as_float(snapshot.get("high_retreat_pct")),
        "vwap_distance_pct": _as_float(snapshot.get("vwap_distance_pct")),
    }
    return {key: value for key, value in result.items() if value not in (None, "", [], {})}


def _append_buy_timing_event(
    event_type: str,
    *,
    stock: str = "",
    now: datetime = None,
    decision: Dict = None,
    market: Dict = None,
    order: Dict = None,
    error: Any = None,
    details: Dict = None,
) -> None:
    if os.getenv("INTRADAY_BUY_AUDIT_ENABLED", "1") != "1":
        return
    now = now or datetime.now()
    event = {
        "schema_version": 1,
        "event_id": f"{now.strftime('%Y%m%d%H%M%S%f')}:{os.getpid()}:{time.time_ns()}",
        "date": now.date().isoformat(),
        "time": now.isoformat(),
        "event_type": str(event_type),
        "stock": str(stock or ""),
    }
    if decision:
        compact = _compact_timing_decision(decision, now)
        compact["llm_status"] = decision.get("_llm_status") or decision.get("llm_status")
        compact["llm_error_type"] = decision.get("_llm_error_type") or decision.get("llm_error_type")
        compact["llm_started_at"] = decision.get("_llm_started_at") or decision.get("llm_started_at")
        compact["llm_finished_at"] = decision.get("_llm_finished_at") or decision.get("llm_finished_at")
        compact["llm_latency_seconds"] = decision.get("_llm_latency_seconds") or decision.get("llm_latency_seconds")
        event["decision"] = {key: value for key, value in compact.items() if value not in (None, "")}
    if market:
        event["market"] = market
    if order:
        event["order"] = order
    if error not in (None, ""):
        event["error"] = str(error)[:1000]
    if details:
        event["details"] = details
    path = _buy_timing_event_file(now.date())
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        with open(path, "a", encoding="utf-8") as handle:
            handle.write(json.dumps(event, ensure_ascii=False, default=str) + "\n")
            handle.flush()
            if event_type in {"TASK_START", "TASK_END", "TASK_ERROR", "ORDER_SUBMIT", "ORDER_CANCEL", "ORDER_FILL"}:
                os.fsync(handle.fileno())
    except Exception as exc:
        logger.warning(f"盘中买入审计事件写入失败 {event_type} {stock}: {exc}")


def _load_buy_timing_market_buffer(day: date) -> Dict[str, Any]:
    global _BUY_TIMING_MARKET_BUFFER
    if _BUY_TIMING_MARKET_BUFFER.get("date") == day.isoformat():
        return _BUY_TIMING_MARKET_BUFFER
    path = _buy_timing_market_file(day)
    loaded: Dict[str, Any] = {}
    if path.exists():
        try:
            with gzip.open(path, "rt", encoding="utf-8") as handle:
                loaded = json.load(handle)
        except Exception as exc:
            logger.warning(f"盘中分钟行情包读取失败，将重建: {exc}")
    if not isinstance(loaded, dict) or loaded.get("date") != day.isoformat():
        loaded = {"schema_version": 1, "date": day.isoformat(), "stocks": {}}
    loaded.setdefault("stocks", {})
    _BUY_TIMING_MARKET_BUFFER = loaded
    return _BUY_TIMING_MARKET_BUFFER


def _flush_buy_timing_market(day: date = None, force: bool = False) -> None:
    global _BUY_TIMING_MARKET_LAST_FLUSH
    if os.getenv("INTRADAY_BUY_AUDIT_ENABLED", "1") != "1":
        return
    day = day or date.today()
    buffer = _load_buy_timing_market_buffer(day)
    interval = max(30, int(os.getenv("INTRADAY_BUY_MARKET_FLUSH_SECONDS", "300")))
    if not force and time.monotonic() - _BUY_TIMING_MARKET_LAST_FLUSH < interval:
        return
    path = _buy_timing_market_file(day)
    path.parent.mkdir(parents=True, exist_ok=True)
    buffer["updated_at"] = datetime.now().isoformat()
    tmp_path = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    try:
        with gzip.open(tmp_path, "wt", encoding="utf-8") as handle:
            json.dump(buffer, handle, ensure_ascii=False, separators=(",", ":"), default=str)
        os.replace(tmp_path, path)
        _BUY_TIMING_MARKET_LAST_FLUSH = time.monotonic()
    except Exception as exc:
        logger.warning(f"盘中分钟行情包写入失败: {exc}")
        try:
            if tmp_path.exists():
                tmp_path.unlink()
        except Exception:
            pass


def _cache_buy_timing_market(stock: str, bars: List[Dict], quote: Dict, now: datetime) -> None:
    if os.getenv("INTRADAY_BUY_AUDIT_ENABLED", "1") != "1":
        return
    if not stock:
        return
    buffer = _load_buy_timing_market_buffer(now.date())
    stock_data = buffer.setdefault("stocks", {}).setdefault(stock, {"bars": []})
    merged = {
        str(row.get("time")): row
        for row in stock_data.get("bars") or []
        if isinstance(row, dict) and row.get("time")
    }
    for bar in bars or []:
        bar_time = _bar_time_value(bar.get("time"))
        if not bar_time or bar_time.date() != now.date():
            continue
        close = _as_price(bar.get("close"))
        if close is None or close <= 0:
            continue
        merged[bar_time.isoformat()] = {
            "time": bar_time.isoformat(),
            "open": _as_price(bar.get("open"), close),
            "high": _as_price(bar.get("high"), close),
            "low": _as_price(bar.get("low"), close),
            "close": close,
            "volume": _as_float(bar.get("volume"), 0.0),
        }
    stock_data["bars"] = [merged[key] for key in sorted(merged)]
    stock_data["last_quote"] = _compact_audit_market(quote)
    stock_data["updated_at"] = now.isoformat()
    _flush_buy_timing_market(now.date())


def _pid_alive(pid: Any) -> bool:
    try:
        pid_int = int(pid)
    except Exception:
        return False
    if pid_int <= 0:
        return False
    try:
        os.kill(pid_int, 0)
        return True
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    except Exception:
        return False


def _pid_looks_like_buy_timing(pid: Any) -> bool:
    if not _pid_alive(pid):
        return False
    try:
        ret = subprocess.run(
            ["ps", "-p", str(int(pid)), "-o", "command="],
            capture_output=True,
            text=True,
            timeout=2,
        )
        cmd = (ret.stdout or "").strip()
        if not cmd:
            return False
        return _command_is_buy_timing(cmd)
    except Exception:
        return False


def _command_is_buy_timing(cmd: str) -> bool:
    if "intraday_executor.py" not in (cmd or ""):
        return False
    try:
        parts = shlex.split(cmd)
    except Exception:
        parts = str(cmd).split()
    modes: list[str] = []
    for idx, part in enumerate(parts):
        if part == "--mode" and idx + 1 < len(parts):
            modes.append(parts[idx + 1])
        elif part.startswith("--mode="):
            modes.append(part.split("=", 1)[1])
    if not modes:
        return True
    return any(mode in {"buy", "buy-timing"} for mode in modes)


def _remove_buy_timing_lock(lock_dir: Path) -> bool:
    try:
        for child in lock_dir.iterdir():
            child.unlink()
        lock_dir.rmdir()
        return True
    except FileNotFoundError:
        return True
    except Exception as e:
        logger.warning(f"分时买入锁清理失败: {e}")
        return False


def _cleanup_stale_buy_timing_pid() -> None:
    pid_file = _buy_timing_pid_file()
    if not pid_file.exists():
        return
    try:
        pid = pid_file.read_text(encoding="utf-8").strip()
        if not _pid_looks_like_buy_timing(pid):
            pid_file.unlink()
            logger.info(f"已清理过期盘中买入PID文件: {pid_file}")
    except Exception as e:
        logger.warning(f"盘中买入PID文件清理失败: {e}")


def _acquire_buy_timing_process_lock() -> Optional[Dict[str, Any]]:
    lock_dir = _buy_timing_lock_dir()
    owner_file = lock_dir / "owner.json"
    pid_file = _buy_timing_pid_file()
    _cleanup_stale_buy_timing_pid()
    payload = {
        "pid": os.getpid(),
        "started_at": datetime.now().isoformat(),
        "date": date.today().isoformat(),
        "cwd": str(BASE_DIR),
        "lock_dir": str(lock_dir),
    }
    while True:
        try:
            lock_dir.mkdir(mode=0o755)
            owner_file.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
            pid_file.write_text(str(os.getpid()), encoding="utf-8")
            return {"lock_dir": lock_dir, "owner": payload}
        except FileExistsError:
            owner = {}
            try:
                owner = json.loads(owner_file.read_text(encoding="utf-8"))
            except Exception:
                owner = {}
            pid = owner.get("pid")
            if _pid_looks_like_buy_timing(pid):
                logger.warning(f"已有今日盘中买入进程正在运行: pid={pid}, started_at={owner.get('started_at')}")
                return None
            logger.warning(f"发现过期盘中买入锁，准备清理: {lock_dir}")
            if not _remove_buy_timing_lock(lock_dir):
                return None


def _release_buy_timing_process_lock(lock: Optional[Dict[str, Any]]) -> None:
    if not lock:
        return
    lock_dir = Path(lock.get("lock_dir", ""))
    owner = lock.get("owner") or {}
    try:
        owner_file = lock_dir / "owner.json"
        current = json.loads(owner_file.read_text(encoding="utf-8")) if owner_file.exists() else {}
        if current.get("pid") == owner.get("pid"):
            _remove_buy_timing_lock(lock_dir)
            pid_file = _buy_timing_pid_file()
            if pid_file.exists() and pid_file.read_text(encoding="utf-8").strip() == str(owner.get("pid")):
                pid_file.unlink()
    except Exception as e:
        logger.warning(f"分时买入锁释放失败: {e}")


def _compact_timing_decision(decision: Dict, now: datetime = None) -> Dict:
    now = now or datetime.now()
    return {
        "time": now.isoformat(),
        "action": str((decision or {}).get("action") or "WAIT").upper(),
        "price_mode": str((decision or {}).get("price_mode") or "NONE").upper(),
        "confidence": int((decision or {}).get("confidence", 0) or 0),
        "reason": str((decision or {}).get("reason", "")).strip()[:300],
        "quote_price": (decision or {}).get("quote_price"),
        "quote_source": (decision or {}).get("quote_source"),
        "technical_trigger": (decision or {}).get("technical_trigger"),
        "trigger_detail": (decision or {}).get("trigger_detail"),
        "skip_reason": (decision or {}).get("skip_reason"),
        "anti_chase_reason": (decision or {}).get("anti_chase_reason"),
        "llm_skipped": bool((decision or {}).get("llm_skipped")),
        "realtime_triggered": bool((decision or {}).get("realtime_triggered")),
        "llm_model": (decision or {}).get("_llm_model") or (decision or {}).get("llm_model"),
        "llm_path": (decision or {}).get("_llm_path") or (decision or {}).get("llm_path"),
    }


def _compact_order(order: Dict) -> Dict:
    if not isinstance(order, dict):
        return {}
    result = order.get("result") if isinstance(order.get("result"), dict) else {}
    intended_quantity = order.get("order_quantity")
    if intended_quantity in (None, "", 0, "0"):
        intended_quantity = order.get("order_count")
    if intended_quantity in (None, "", 0, "0"):
        intended_quantity = order.get("quantity")
    return {
        "time": order.get("time") or order.get("order_time"),
        "order_id": _extract_order_id(order),
        "order_price": order.get("order_price") or order.get("price") or order.get("trade_price"),
        "quantity": intended_quantity,
        "quote_price": order.get("quote_price"),
        "price_mode": order.get("price_mode"),
        "status": order.get("status") or result.get("status"),
        "error": order.get("error") or result.get("error"),
    }


def _pending_missing_grace_seconds() -> int:
    try:
        return max(0, int(os.getenv("INTRADAY_BUY_PENDING_MISSING_GRACE_SEC", "180")))
    except (TypeError, ValueError):
        return 180


def _pending_order_age_seconds(entry: Dict, now: datetime = None) -> Optional[float]:
    now = now or datetime.now()
    order = entry.get("pending_order") if isinstance(entry.get("pending_order"), dict) else None
    if not order:
        order = entry.get("last_order") if isinstance(entry.get("last_order"), dict) else None
    if not order:
        return None
    order_time = _parse_state_datetime(order.get("time") or order.get("order_time"))
    if not order_time:
        return None
    return max(0.0, (now - order_time).total_seconds())


def _should_keep_missing_pending_order(entry: Dict, now: datetime = None) -> bool:
    grace = _pending_missing_grace_seconds()
    if grace <= 0:
        return False
    age = _pending_order_age_seconds(entry, now)
    return age is not None and age < grace


def _record_buy_timing_decision(entry: Dict, decision: Dict, now: datetime = None) -> None:
    now = now or datetime.now()
    compact = _compact_timing_decision(decision, now)
    entry["last_decision"] = compact
    entry["last_decision_at"] = entry["last_decision"]["time"]
    entry["decision_count"] = int(entry.get("decision_count", 0) or 0) + 1
    if not compact.get("llm_skipped") and (compact.get("llm_model") or compact.get("llm_path")):
        entry["last_llm_decision"] = dict(compact)
        entry["last_llm_decision_at"] = compact.get("time")
    is_llm_decision = bool(
        compact.get("llm_model")
        or compact.get("llm_path")
        or decision.get("_llm_status")
        or decision.get("llm_status")
    )
    event_type = "LLM_DECISION" if is_llm_decision else "RULE_DECISION"
    if compact.get("llm_skipped"):
        event_type = "POLL_SKIP"
    market = decision.get("market_snapshot") if isinstance(decision.get("market_snapshot"), dict) else None
    audit_now = _parse_state_datetime(decision.get("_llm_finished_at") or decision.get("llm_finished_at")) or now
    _append_buy_timing_event(
        event_type,
        stock=str(entry.get("stock") or ""),
        now=audit_now,
        decision=decision,
        market=market,
    )


def _compact_buy_timing_state(state: Dict) -> Dict:
    state = dict(state or {})
    state["date"] = state.get("date") or date.today().isoformat()
    state["version"] = 2
    stocks = state.setdefault("stocks", {})
    state.pop("rounds", None)
    selected_signals = []
    for signal in state.get("selected_signals") or []:
        if not isinstance(signal, dict):
            continue
        compact_signal = {
            k: signal.get(k)
            for k in (
                "stock",
                "name",
                "signal",
                "action",
                "confidence",
                "buy_score",
                "final_decision",
                "carryover_from",
                "carryover_reason",
            )
            if signal.get(k) not in (None, "")
        }
        if compact_signal.get("stock"):
            selected_signals.append(compact_signal)
    state["selected_signals"] = selected_signals
    state["carryover_stocks"] = [
        str(s) for s in (state.get("carryover_stocks") or [])
        if s
    ]
    state.pop("_round_index_data", None)
    state.pop("_round_board_data", None)
    state.pop("_timing_stock_locks", None)
    for entry in stocks.values():
        if not isinstance(entry, dict):
            continue
        _repair_filled_timing_entry_identity(entry)
        old_decisions = entry.pop("decisions", None) or []
        if old_decisions and not entry.get("last_decision"):
            entry["last_decision"] = _compact_timing_decision(old_decisions[-1])
            entry["last_decision_at"] = entry["last_decision"].get("time")
            entry["decision_count"] = max(int(entry.get("decision_count", 0) or 0), len(old_decisions))
        old_orders = entry.pop("submitted_orders", None) or []
        if old_orders and not entry.get("last_order"):
            entry["last_order"] = _compact_order(old_orders[-1])
            entry["submitted_order_count"] = max(int(entry.get("submitted_order_count", 0) or 0), len(old_orders))
        old_cancellations = entry.pop("cancellations", None) or []
        if old_cancellations and not entry.get("last_cancellation"):
            entry["last_cancellation"] = _compact_order(old_cancellations[-1].get("order", {}))
            entry["last_cancellation"]["time"] = old_cancellations[-1].get("time")
            entry["last_cancellation"]["reason"] = old_cancellations[-1].get("reason")
            entry["cancellation_count"] = max(int(entry.get("cancellation_count", 0) or 0), len(old_cancellations))
        if isinstance(entry.get("pending_order"), dict):
            entry["pending_order"] = _compact_order(entry.get("pending_order"))
        if isinstance(entry.get("last_decision"), dict):
            entry["last_decision"].pop("technical_snapshot", None)
        if isinstance(entry.get("last_llm_decision"), dict):
            entry["last_llm_decision"].pop("technical_snapshot", None)
    return state


def _load_buy_timing_state(path: Path = None) -> Dict:
    path = path or _buy_timing_state_file()
    if path.exists():
        try:
            with open(path, encoding="utf-8") as f:
                state = json.load(f)
            if isinstance(state, dict) and not state.get("date"):
                state_day = _buy_timing_state_date_from_path(path)
                if state_day:
                    state["date"] = state_day.isoformat()
            return _compact_buy_timing_state(state)
        except Exception as e:
            logger.warning(f"分时买入状态读取失败，重新创建: {e}")
    return {"date": date.today().isoformat(), "version": 2, "stocks": {}}


def _save_buy_timing_state(state: Dict, path: Path = None):
    path = path or _buy_timing_state_file()
    state = _compact_buy_timing_state(state)
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    with open(tmp_path, "w", encoding="utf-8") as f:
        json.dump(state, f, ensure_ascii=False, indent=2, default=str)
    os.replace(tmp_path, path)


def _state_entry(state: Dict, stock: str) -> Dict:
    entry = state.setdefault("stocks", {}).setdefault(stock, {})
    entry.setdefault("stock", stock)
    entry.setdefault("status", "open")
    entry.setdefault("reprice_count", 0)
    return entry


def _claim_buy_timing_stock(state: Dict, stock: str, owner: str, ttl_seconds: int = 90) -> bool:
    """Lightweight in-process guard to avoid double-evaluating one stock in parallel."""
    if not stock:
        return False
    now_ts = time.time()
    locks = state.setdefault("_timing_stock_locks", {})
    lock = locks.get(stock)
    if isinstance(lock, dict):
        age = now_ts - float(lock.get("ts", 0) or 0)
        if age < ttl_seconds and lock.get("owner") != owner:
            return False
    locks[stock] = {"owner": owner, "ts": now_ts}
    return True


def _release_buy_timing_stock(state: Dict, stock: str, owner: str):
    locks = state.get("_timing_stock_locks") or {}
    lock = locks.get(stock)
    if isinstance(lock, dict) and lock.get("owner") == owner:
        locks.pop(stock, None)


def _release_buy_timing_owner_claims(state: Dict, owner: str):
    locks = state.get("_timing_stock_locks") or {}
    for stock, lock in list(locks.items()):
        if isinstance(lock, dict) and lock.get("owner") == owner:
            locks.pop(stock, None)


def _get_available_cash() -> Optional[float]:
    pos_resp = mx_api_post("/api/claw/mockTrading/positions", {})
    pos_data = pos_resp.get("data", {}) if pos_resp else {}
    unit = pos_data.get("currencyUnit", 1) if pos_data else 1
    avail_balance = pos_data.get("availBalance") if pos_data else None
    if avail_balance is None:
        return None
    return float(avail_balance) / unit


def _timing_quote_features(quote: Dict) -> Dict:
    price = float(quote.get("price", 0) or 0)
    open_price = quote.get("open")
    high = quote.get("high")
    low = quote.get("low")
    intraday_position = None
    if high and low and float(high) > float(low):
        intraday_position = round((price - float(low)) / (float(high) - float(low)), 3)
    vwap = None
    vol = float(quote.get("volume") or 0)
    amt = float(quote.get("amount") or 0)
    if vol > 0 and amt > 0:
        vwap_raw = amt / vol
        # amount单位可能是分(厘)而非元，当数值明显异常时自动修正
        if price > 0 and vwap_raw > price * 10:
            vwap_raw /= 100
        vwap = round(vwap_raw, 4)
    return {
        "price": price,
        "change_pct": quote.get("change_pct"),
        "open": open_price,
        "high": high,
        "low": low,
        "prev_close": quote.get("prev_close"),
        "intraday_position_0_low_1_high": intraday_position,
        "bid1": quote.get("bid1"),
        "ask1": quote.get("ask1"),
        "volume": quote.get("volume"),
        "amount": quote.get("amount"),
        "vwap": vwap,
        "source": quote.get("source"),
    }


def _bar_time_value(raw_time) -> Optional[datetime]:
    if raw_time in (None, "", "NaT"):
        return None
    if isinstance(raw_time, datetime):
        return raw_time
    try:
        if hasattr(raw_time, "to_pydatetime"):
            return raw_time.to_pydatetime()
    except Exception:
        pass
    text = str(raw_time)
    try:
        return datetime.fromisoformat(text.replace("Z", "+00:00")).replace(tzinfo=None)
    except Exception:
        pass
    for fmt in ("%Y%m%d%H%M%S", "%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M:%S.%f", "%Y-%m-%d"):
        try:
            return datetime.strptime(text, fmt)
        except Exception:
            continue
    return None


def _row_float(row, key: str, default=None):
    try:
        if hasattr(row, "get"):
            return _as_float(row.get(key), default)
        return _as_float(row[key], default)
    except Exception:
        return default


def get_intraday_1m_bars(stock_code: str, count: int = 180) -> List[Dict]:
    """Fetch recent 1-minute bars from XQShare. Used only for same-day technical timing."""
    xt = _get_xq_client()
    if xt is None:
        return []
    xt_code = _to_xt_code(stock_code)
    try:
        requested_count = int(os.getenv("INTRADAY_BUY_1M_BAR_COUNT", str(count or 500)))
    except (TypeError, ValueError):
        requested_count = int(count or 500)
    requested_count = max(180, min(requested_count, 1000))
    try:
        # 必须先 download_history_data 才能在后续 get_market_data 拿到当日1分钟数据
        xt.xtdata.download_history_data(stock_code=xt_code, period="1m", start_time="", end_time="")
        data = xt.xtdata.get_market_data(
            field_list=["time", "open", "high", "low", "close", "volume"],
            stock_list=[xt_code],
            period="1m",
            count=requested_count,
            end_time="",
            start_time="",
        )
    except Exception as e:
        logger.debug(f"{stock_code} 1分钟K线获取失败: {e}")
        return []

    bars = []
    try:
        if hasattr(data, "iterrows"):
            for idx, row in data.iterrows():
                bar_time = _bar_time_value(idx)
                if bar_time is None and hasattr(row, "get"):
                    bar_time = _bar_time_value(row.get("time"))
                close = _row_float(row, "close")
                if close is None or close <= 0:
                    continue
                bars.append({
                    "time": bar_time,
                    "open": _row_float(row, "open", close),
                    "high": _row_float(row, "high", close),
                    "low": _row_float(row, "low", close),
                    "close": close,
                    "volume": _row_float(row, "volume", 0.0),
                })
        elif isinstance(data, dict):
            close_df = data.get("close")
            if hasattr(close_df, "items"):
                for raw_time, close_map in close_df.items():
                    close = _as_float(close_map.get(xt_code) if isinstance(close_map, dict) else close_map)
                    if close is None or close <= 0:
                        continue
                    bars.append({"time": _bar_time_value(raw_time), "open": close, "high": close, "low": close, "close": close, "volume": 0.0})
    except Exception as e:
        logger.debug(f"{stock_code} 1分钟K线解析失败: {e}")
        return []

    # 所有bar按时间排序（最老在前）；无法解析时间的脏数据不参与日内技术判断。
    valid_bars = [b for b in bars if b.get("time") is not None]
    return sorted(valid_bars, key=lambda b: b["time"])


def _sma(values: List[float], window: int) -> Optional[float]:
    if len(values) < window:
        return None
    return sum(values[-window:]) / window


def _sma_or_available(values: List[float], window: int) -> Optional[float]:
    """SMA that falls back to all available data when fewer than `window` values."""
    if not values:
        return None
    n = min(window, len(values))
    if n <= 0:
        return None
    return sum(values[-n:]) / n


def _ma_closes_for_window(today_closes: List[float], last_day_closes: List[float], window: int) -> List[float]:
    """Build the correct close list for a given MA window.
    Uses today_bars first; if insufficient, supplements with last trading day's most recent closes.
    """
    n_today = len(today_closes)
    if n_today >= window:
        return today_closes[-window:]
    need = window - n_today
    # last_day_closes is already oldest-first within that day
    return last_day_closes[-need:] + today_closes



def _calc_rsi(closes: List[float], period: int = 14) -> Optional[float]:
    if len(closes) < period + 1:
        return None
    gains, losses = [], []
    for i in range(1, len(closes)):
        d = closes[i] - closes[i - 1]
        gains.append(d if d > 0 else 0.0)
        losses.append(abs(d) if d < 0 else 0.0)
    avg_gain = sum(gains[-period:]) / period
    avg_loss = sum(losses[-period:]) / period
    if avg_loss == 0:
        return 100.0
    rs = avg_gain / avg_loss
    return round(100.0 - 100.0 / (1.0 + rs), 2)


def _calc_macd(closes: List[float], fast: int = 12, slow: int = 26, signal: int = 9) -> tuple:
    if len(closes) < slow + signal:
        return None, None, None
    k_fast = 2.0 / (fast + 1)
    k_slow = 2.0 / (slow + 1)
    k_sig = 2.0 / (signal + 1)
    ema_f = closes[0]
    ema_s = closes[0]
    dif_list = []
    for v in closes[1:]:
        ema_f = v * k_fast + ema_f * (1.0 - k_fast)
        ema_s = v * k_slow + ema_s * (1.0 - k_slow)
        dif_list.append(ema_f - ema_s)
    dea_val = dif_list[0]
    for d in dif_list[1:]:
        dea_val = d * k_sig + dea_val * (1.0 - k_sig)
    dif = dif_list[-1]
    macd_hist = 2.0 * (dif - dea_val)
    return round(dif, 4), round(dea_val, 4), round(macd_hist, 4)


def _calc_kdj(closes: List[float], highs: List[float], lows: List[float], n: int = 9, m1: int = 3, m2: int = 3) -> tuple:
    if len(closes) < n + 1:
        return None, None, None
    rsv_list = []
    for i in range(n - 1, len(closes)):
        high_n = max(highs[i - n + 1 : i + 1])
        low_n = min(lows[i - n + 1 : i + 1])
        close_c = closes[i]
        if high_n == low_n:
            rsv = 50.0
        else:
            rsv = (close_c - low_n) / (high_n - low_n) * 100.0
        rsv_list.append(rsv)
    if len(rsv_list) < m1:
        return None, None, None
    k_val = 50.0
    d_val = 50.0
    for r in rsv_list[:m1]:
        k_val = (2.0 / 3.0) * k_val + (1.0 / 3.0) * r
        d_val = (2.0 / 3.0) * d_val + (1.0 / 3.0) * k_val
    for r in rsv_list[m1:]:
        k_val = (2.0 / 3.0) * k_val + (1.0 / 3.0) * r
        d_val = (2.0 / 3.0) * d_val + (1.0 / 3.0) * k_val
    j_val = 3.0 * k_val - 2.0 * d_val
    return round(k_val, 2), round(d_val, 2), round(j_val, 2)


def _calc_bollinger(closes: List[float], period: int = 20, k: float = 2.0) -> tuple:
    if len(closes) < period:
        return None, None, None
    last_n = closes[-period:]
    middle = sum(last_n) / period
    variance = sum((x - middle) ** 2 for x in last_n) / period
    std = variance ** 0.5
    return round(middle + k * std, 4), round(middle, 4), round(middle - k * std, 4)


def _intraday_technical_snapshot(stock: str, quote: Dict, bars: List[Dict], trading_day: date = None) -> Dict:
    from datetime import date

    all_bars_sorted = sorted([b for b in bars if b.get("time") is not None], key=lambda b: b["time"])
    today = trading_day or date.today()
    today_bars = [b for b in all_bars_sorted if b.get("time") and b["time"].date() == today]
    other_bars = [b for b in all_bars_sorted if b.get("time") and b["time"].date() != today]

    if other_bars:
        last_day_dates = sorted(set(b["time"].date() for b in other_bars), reverse=True)
        last_day = last_day_dates[0] if last_day_dates else None
        last_day_bars = [b for b in other_bars if b["time"].date() == last_day] if last_day else []
        last_day_bars.sort(key=lambda b: b["time"])
    else:
        last_day_bars = []

    today_closes = [float(b["close"]) for b in today_bars if b.get("close")]
    today_highs  = [float(b["high"])  for b in today_bars if b.get("high")]
    today_lows   = [float(b["low"])   for b in today_bars if b.get("low")]
    last_day_closes = [float(b["close"]) for b in last_day_bars if b.get("close")]

    latest = float(quote.get("price") or (today_closes[-1] if today_closes else 0) or 0)
    day_open = _as_float(quote.get("open"))
    if day_open is None and today_bars:
        day_open = _as_float(today_bars[0].get("open"))

    ma_windows = [5, 10, 20, 30, 60, 120]
    mas = {}
    for w in ma_windows:
        ma_closes = _ma_closes_for_window(today_closes, last_day_closes, w)
        mas[f"ma{w}"] = _sma(ma_closes, w)

    above = [k for k, v in mas.items() if v and latest > v]
    crossed = []
    if len(today_closes) >= 2:
        prev_close = today_closes[-2]
        for w in ma_windows:
            prev_today = today_closes[:-1]
            prev_ma_closes = _ma_closes_for_window(prev_today, last_day_closes, w)
            prev_ma = _sma(prev_ma_closes, w)
            curr_ma = mas[f"ma{w}"]
            if prev_ma and curr_ma and prev_close <= prev_ma and latest > curr_ma:
                crossed.append(f"ma{w}")

    # ── RSI / MACD / KDJ / 布林带（本地实时计算，早盘数据不足时拼接昨天）──
    # 与均线同样的拼接逻辑：今日不够则向前拼接昨天尾段
    def _concat_for_indicator(today_vals, last_day_vals, need):
        if len(today_vals) >= need:
            return today_vals
        need_from_last = need - len(today_vals)
        return last_day_vals[-need_from_last:] + today_vals

    all_closes = _concat_for_indicator(today_closes, last_day_closes, max(26, 60))
    all_highs  = _concat_for_indicator(today_highs,  last_day_closes, max(26, 60))  # 用close代替high作近似
    all_lows   = _concat_for_indicator(today_lows,   last_day_closes, max(26, 60))  # 用close代替low作近似

    rsi14         = _calc_rsi(all_closes, 14)
    macd_dif, macd_dea, macd_hist = _calc_macd(all_closes)
    kdj_k,  kdj_d,  kdj_j        = _calc_kdj(all_closes, all_highs, all_lows)
    bb_upper, bb_middle, bb_lower = _calc_bollinger(_concat_for_indicator(today_closes, last_day_closes, 60))

    prev_bar_close = today_closes[-2] if len(today_closes) >= 2 else None
    high = _as_float(quote.get("high"))
    low  = _as_float(quote.get("low"))
    high_retreat_pct = round((latest - high) / high * 100, 3) if high and high > 0 else None
    vwap = None
    total_amount = 0.0
    total_volume = 0.0
    for b in today_bars:
        vol = _as_float(b.get("volume") or b.get("vol"))
        close = _as_float(b.get("close"))
        high_b = _as_float(b.get("high"))
        low_b = _as_float(b.get("low"))
        if vol and vol > 0 and close:
            typical = ((high_b or close) + (low_b or close) + close) / 3.0
            total_amount += typical * vol
            total_volume += vol
    if total_volume > 0:
        vwap = round(total_amount / total_volume, 4)
    vwap_distance_pct = round((latest - vwap) / vwap * 100, 3) if latest and vwap else None

    return {
        "stock": stock,
        "bar_count": len(today_bars),
        "last_day_bar_count": len(last_day_bars),
        "first_today_bar_time": today_bars[0]["time"].isoformat() if today_bars else None,
        "last_today_bar_time": today_bars[-1]["time"].isoformat() if today_bars else None,
        "opening_sequence_ready": bool(today_bars and len(last_day_bars) >= 120),
        "latest": latest,
        "day_open": day_open,
        "prev_bar_close": prev_bar_close,
        "change_pct": quote.get("change_pct"),
        "high": high,
        "low": low,
        "high_retreat_pct": high_retreat_pct,
        "ma": {k: round(v, 4) for k, v in mas.items() if v},
        "above_ma": above,
        "crossed_up_ma": crossed,
        "ma120": mas.get("ma120"),
        "ma120_1m": mas.get("ma120"),
        "vwap": vwap,
        "vwap_distance_pct": vwap_distance_pct,
        "source": "1m_kline",
        # ── 新增技术指标（本地实时计算）─────────────────────────
        "rsi14": rsi14,
        "macd_dif": macd_dif,
        "macd_dea": macd_dea,
        "macd_hist": macd_hist,
        "kdj_k": kdj_k,
        "kdj_d": kdj_d,
        "kdj_j": kdj_j,
        "bb_upper": bb_upper,
        "bb_middle": bb_middle,
        "bb_lower": bb_lower,
    }


def _is_opening_chase_time(now: datetime, entry: Dict) -> bool:
    if entry.get("opening_chase_evaluated_at"):
        return False
    if entry.get("status") in {"pending", "filled", "skip_today", "cancelled"}:
        return False
    if entry.get("pending_order") or entry.get("last_order") or entry.get("submitted_order_count"):
        return False
    return dt_time(9, 31) <= now.time() < dt_time(9, 34)


def _opening_strong_buy_decision(snapshot: Dict) -> Optional[Dict]:
    latest = _as_float(snapshot.get("latest"))
    day_open = _as_float(snapshot.get("day_open"))
    prev_bar_close = _as_float(snapshot.get("prev_bar_close"))
    if not latest or not day_open:
        return None
    rising = latest > day_open and (prev_bar_close is None or latest >= prev_bar_close)
    above_ma_count = len(snapshot.get("above_ma") or [])
    crossed_count = len(snapshot.get("crossed_up_ma") or [])
    close_to_high = (snapshot.get("high_retreat_pct") is None) or snapshot.get("high_retreat_pct") >= -1.2
    if (
        snapshot.get("opening_sequence_ready")
        and rising
        and close_to_high
        and (above_ma_count >= 3 or crossed_count >= 2)
    ):
        ma_text = ",".join(snapshot.get("above_ma") or snapshot.get("crossed_up_ma") or ["开盘上涨"])
        return {
            "action": "BUY_NOW",
            "price_mode": "FOLLOW",
            "limit_price": None,
            "max_premium_pct": float(os.getenv("INTRADAY_BUY_MAX_PREMIUM_PCT", "1.5")),
            "confidence": 90,
            "reason": f"09:31开盘强势：最新价{latest:.2f}高于开盘{day_open:.2f}，1分钟走势站上/上穿{ma_text}，直接追入",
            "technical_trigger": "OPENING_STRONG",
        }
    return None


def _opening_snapshot_ready(snapshot: Dict) -> bool:
    return bool(
        _as_float(snapshot.get("latest"))
        and _as_float(snapshot.get("day_open"))
        and snapshot.get("opening_sequence_ready")
    )




def _intraday_anti_chase_reason(snapshot: Dict) -> str:
    latest = _as_float(snapshot.get("latest"))
    bb_upper = _as_float(snapshot.get("bb_upper"))
    rsi = _as_float(snapshot.get("rsi14"))
    kdj_j = _as_float(snapshot.get("kdj_j"))
    retreat = _as_float(snapshot.get("high_retreat_pct"))
    vwap_dist = _as_float(snapshot.get("vwap_distance_pct"))
    change_pct = _as_float(snapshot.get("change_pct"))
    reasons = []
    if rsi is not None and rsi >= 84:
        reasons.append(f"RSI{rsi:.0f}超买")
    if kdj_j is not None and kdj_j >= 110:
        reasons.append(f"KDJ-J{kdj_j:.0f}极端")
    if latest and bb_upper and latest >= bb_upper * 0.995:
        reasons.append("接近布林上轨")
    if retreat is not None and retreat <= -1.8:
        reasons.append(f"距日内高点回落{abs(retreat):.1f}%")
    if vwap_dist is not None and vwap_dist >= 3.5:
        reasons.append(f"偏离VWAP{vwap_dist:.1f}%")
    if change_pct is not None and change_pct >= 8 and (retreat is None or retreat <= -0.8):
        reasons.append("高涨幅且冲高回落")
    return "、".join(reasons)


def _force_llm_review_decision(trigger: str, reason: str, confidence: int = 0, detail: Dict = None) -> Dict:
    return {
        "action": "LLM_REVIEW",
        "price_mode": "NONE",
        "limit_price": None,
        "max_premium_pct": 0.0,
        "confidence": confidence,
        "reason": reason,
        "technical_trigger": trigger,
        "force_llm_review": True,
        "trigger_detail": detail or {},
    }


def _opening_continuation_buy_decision(snapshot: Dict, now: datetime, entry: Dict) -> Optional[Dict]:
    if not (dt_time(9, 32) <= now.time() <= dt_time(9, 40)):
        return None
    if entry.get("pending_order") or entry.get("last_order") or entry.get("submitted_order_count"):
        return None
    latest = _as_float(snapshot.get("latest"))
    day_open = _as_float(snapshot.get("day_open"))
    if not latest or not day_open or latest <= day_open:
        return None
    anti_reason = _intraday_anti_chase_reason(snapshot)
    if anti_reason:
        return {
            "action": "WAIT",
            "price_mode": "NONE",
            "limit_price": None,
            "max_premium_pct": 0.0,
            "confidence": 0,
            "reason": f"早盘强势但触发反追高保护：{anti_reason}，等待回踩承接",
            "technical_trigger": "OPENING_STRENGTH_CONTINUATION_BLOCKED",
            "anti_chase_reason": anti_reason,
        }
    above_count = len(snapshot.get("above_ma") or [])
    crossed_count = len(snapshot.get("crossed_up_ma") or [])
    retreat = _as_float(snapshot.get("high_retreat_pct"))
    macd_hist = _as_float(snapshot.get("macd_hist"))
    kdj_k = _as_float(snapshot.get("kdj_k"))
    kdj_d = _as_float(snapshot.get("kdj_d"))
    vwap = _as_float(snapshot.get("vwap"))
    vwap_ok = not vwap or latest >= vwap
    momentum_ok = (macd_hist is None or macd_hist >= -0.01) and (kdj_k is None or kdj_d is None or kdj_k >= kdj_d - 3)
    if vwap_ok and momentum_ok and (above_count >= 3 or crossed_count >= 2) and (retreat is None or retreat >= -1.2):
        return _force_llm_review_decision(
            "OPENING_STRENGTH_CONTINUATION",
            f"09:32-09:40早盘强势延续：最新价{latest:.2f}高于开盘{day_open:.2f}，站上/上穿多条分钟均线，触发LLM复核",
            confidence=0,
            detail={"above_ma": snapshot.get("above_ma"), "crossed_up_ma": snapshot.get("crossed_up_ma"), "vwap": vwap},
        )
    return None


def _pullback_resume_buy_decision(snapshot: Dict, now: datetime, entry: Dict) -> Optional[Dict]:
    if not (dt_time(9, 40) < now.time() < _buy_timing_cutoff()):
        return None
    latest = _as_float(snapshot.get("latest"))
    prev_close = _as_float(snapshot.get("prev_bar_close"))
    if not latest or not prev_close:
        return None
    anti_reason = _intraday_anti_chase_reason(snapshot)
    if anti_reason:
        return None
    ma = snapshot.get("ma") or {}
    ma5 = _as_float(ma.get("ma5"))
    ma20 = _as_float(ma.get("ma20"))
    vwap = _as_float(snapshot.get("vwap"))
    support_refs = [x for x in (vwap, ma5, ma20) if x]
    if not support_refs:
        return None
    stood_back = any(prev_close <= ref and latest > ref for ref in support_refs)
    still_supported = any(latest >= ref and abs((latest - ref) / ref * 100) <= 1.2 for ref in support_refs if ref)
    above_count = len(snapshot.get("above_ma") or [])
    crossed = set(snapshot.get("crossed_up_ma") or [])
    macd_hist = _as_float(snapshot.get("macd_hist"))
    kdj_k = _as_float(snapshot.get("kdj_k"))
    kdj_d = _as_float(snapshot.get("kdj_d"))
    momentum_ok = (macd_hist is None or macd_hist >= -0.01) and (kdj_k is None or kdj_d is None or kdj_k >= kdj_d - 2)
    if momentum_ok and above_count >= 2 and (stood_back or still_supported or crossed.intersection({"ma5", "ma10", "ma20"})):
        return _force_llm_review_decision(
            "PULLBACK_RESUME",
            f"回踩承接后再上攻：最新价{latest:.2f}重新站回VWAP/短均线附近，触发LLM复核",
            confidence=0,
            detail={"vwap": vwap, "ma5": ma5, "ma20": ma20, "crossed_up_ma": list(crossed)},
        )
    return None


def _pending_order_review_decision(entry: Dict, pending_order: Dict, now: datetime) -> Optional[Dict]:
    if not pending_order:
        return None
    age = _pending_order_age_seconds(entry, now)
    if age is None or age < int(os.getenv("INTRADAY_BUY_PENDING_REVIEW_SECONDS", "90")):
        return None
    return _force_llm_review_decision(
        "PENDING_ORDER_REVIEW",
        "未成交挂单超过复判阈值，触发LLM判断KEEP_ORDER/CANCEL_REBUY/CANCEL_WAIT",
        detail={"pending_age_seconds": round(age, 1)},
    )


def _is_technical_llm_due(entry: Dict, trigger: str, now: datetime) -> bool:
    if trigger == "MA120_CROSS_UP":
        return True
    last_trigger = entry.get("last_technical_llm_trigger")
    last_at = _parse_state_datetime(entry.get("last_technical_llm_check_at"))
    interval = int(os.getenv("INTRADAY_BUY_TECH_TRIGGER_REVIEW_SECONDS", "120"))
    if last_trigger != trigger or not last_at:
        return True
    return (now - last_at).total_seconds() >= max(30, interval)


def _ma120_cross_buy_decision(snapshot: Dict) -> Optional[Dict]:
    latest = _as_float(snapshot.get("latest"))
    ref_ma120 = _as_float(snapshot.get("ma120_1m") or snapshot.get("ma120"))
    crossed_ma = snapshot.get("crossed_up_ma") or []
    if ref_ma120 and latest and "ma120" in crossed_ma:
        return {
            "action": "LLM_REVIEW",
            "price_mode": "NONE",
            "limit_price": None,
            "max_premium_pct": 0.0,
            "confidence": 0,
            "reason": f"1分钟K线最新价{latest:.2f}上穿1分钟MA120({ref_ma120:.2f})，触发LLM买入判断",
            "technical_trigger": "MA120_CROSS_UP",
            "force_llm_review": True,
        }
    return None


def _get_realtime_prices(stock_list: list) -> dict:
    """Legacy realtime polling helper: disabled by design."""
    return {}


def _check_hard_triggers_for_stock(signal: dict, name_map: dict, state: dict, now: datetime) -> Optional[dict]:
    """Legacy hard-trigger helper: main loop owns every minute technical checks."""
    return None


def _execute_buy_timing_action(
    stock: str,
    name: str,
    signal: Dict,
    quote: Dict,
    decision: Dict,
    entry: Dict,
    initial_cash: float,
    available_cash: Optional[float],
    now: datetime,
) -> Optional[float]:
    """Execute a sanitized buy-timing decision and persist entry state."""
    action = str(decision.get("action") or "WAIT").upper()
    if action == "SKIP_TODAY":
        entry["status"] = "skip_today"
        return available_cash
    if action != "BUY_NOW":
        if entry.get("status") != "pending":
            entry["status"] = "open"
        return available_cash

    if available_cash is None:
        entry["status"] = "open"
        entry["last_error"] = "无法获取可用资金"
        return available_cash
    if not initial_cash or initial_cash <= 0:
        initial_cash = available_cash

    price = float(quote.get("price", 0) or 0)
    limit_down = float(quote.get("limit_down", 0) or 0)
    change_pct = float(quote.get("change_pct", 0) or 0)
    if _is_buy_quote_limit_down(quote):
        logger.warning(f"{stock} 跌停价截断买入: price={price} limit_down={limit_down} change_pct={change_pct}")
        _record_buy_timing_decision(entry, {
            "action": "WAIT",
            "price_mode": "NONE",
            "confidence": 0,
            "reason": _limit_down_block_reason(quote),
            "quote_price": quote.get("price"),
        }, now)
        entry["status"] = "open"
        return available_cash

    order_plan = _calc_timing_buy_order(signal, quote, initial_cash, available_cash, decision)
    if not order_plan.get("ok"):
        entry["status"] = "open"
        entry["last_skip_reason"] = order_plan.get("reason")
        return available_cash

    reason_prefix = "技术触发买入" if decision.get("technical_trigger") else "LLM分时买入"
    reason = f"{reason_prefix}: {decision.get('reason', '')}"
    result = buy_stock(stock, name, order_plan["order_price"], order_plan["quantity"], reason)
    if result.get("status") in {"submitted", "dry_run"}:
        pending_record = {
            "time": now.isoformat(),
            "order_id": result.get("order_id"),
            "order_price": order_plan["order_price"],
            "raw_order_price": order_plan.get("raw_order_price"),
            "price_mode": order_plan.get("price_mode"),
            "price_guard": order_plan.get("price_guard"),
            "price_bounds": order_plan.get("price_bounds"),
            "quantity": order_plan["quantity"],
            "quote_price": quote.get("price"),
            "result": result,
        }
        entry["status"] = "pending"
        entry["pending_order"] = _compact_order(pending_record)
        entry["last_order"] = _compact_order(pending_record)
        entry["submitted_order_count"] = int(entry.get("submitted_order_count", 0) or 0) + 1
        # 与主轮询保持一致：资金余额只信真实账户查询，不在内存中手动扣减。
        return available_cash

    entry["status"] = "open"
    entry["last_error"] = result.get("error", result.get("status"))
    return available_cash


def _execute_hard_trigger_action(trigger_result: dict, state: dict, initial_cash: float, now: datetime):
    """Legacy hard-trigger executor: kept as inert compatibility shim."""
    logger.warning("旧实时硬触发执行入口已禁用；主轮询负责技术触发和买入执行")


def _run_realtime_hard_trigger_loop(signals: list, name_map: dict, state: dict, initial_cash: float, now: datetime, stop_event=None):
    """Legacy realtime loop: kept inert so old callers cannot revive double polling."""
    logger.warning("旧实时硬触发快轮询入口已禁用；主轮询每分钟检查技术触发")
    return None

def _technical_buy_timing_decision(signal: Dict, quote: Dict, bars: List[Dict], entry: Dict, now: datetime, pending_order: Dict = None) -> Optional[Dict]:
    snapshot = _intraday_technical_snapshot(signal.get("stock", ""), quote, bars, trading_day=now.date())
    pending_decision = _pending_order_review_decision(entry, pending_order or {}, now)
    if pending_decision:
        return pending_decision
    if _is_opening_chase_time(now, entry):
        # Only a successful quote/snapshot pass consumes the opening-strength check.
        # A transient quote/open-price failure before this point should still be
        # allowed to retry within the 09:31-09:34 opening window.
        if not _opening_snapshot_ready(snapshot):
            return None
        entry["opening_chase_evaluated_at"] = now.isoformat()
        decision = _opening_strong_buy_decision(snapshot)
        if decision:
            return decision
    elif now.time() < _buy_timing_cutoff():
        for builder in (
            lambda: _opening_continuation_buy_decision(snapshot, now, entry),
            lambda: _ma120_cross_buy_decision(snapshot),
            lambda: _pullback_resume_buy_decision(snapshot, now, entry),
        ):
            decision = builder()
            if decision:
                return decision
    return None


def _build_buy_timing_prompt(signal: Dict, quote: Dict, pending_order: Dict = None, now: datetime = None, technical_snapshot: Dict = None) -> str:
    now = now or datetime.now()
    stock = signal.get("stock", "")
    # 跌停保护：如果当前价已是跌停价，强制WAIT不许买
    price = float(quote.get("price", 0) or 0)
    limit_down = float(quote.get("limit_down", 0) or 0)
    change_pct = float(quote.get("change_pct", 0) or 0)
    is_limit_down = _is_buy_quote_limit_down(quote)
    payload = {
        "time": now.strftime("%H:%M:%S"),
        "stock": stock,
        "name": signal.get("name", stock),
        "quote": _timing_quote_features(quote),
        "is_limit_down": is_limit_down,
        "one_minute_technical": technical_snapshot or {},
        "pending_order": pending_order or None,
        "rules": [
            "观察池固定来自当天选股早报Top5，但本轮只判断当天盘中技术面，不考虑早报信号、建议、策略标签、基本面、估值、新闻或长期逻辑。",
            "你只决定今天是否现在买、继续等待、跳过，以及如何处理未成交挂单；买入报价由程序统一计算。",
            "判断口径保持中性：既不为了买入而买入，也不因为早报建议保守就消极等待；只有当天技术面给出清晰买点才BUY_NOW。",
            "09:31开盘强势由程序直接买；09:31之后的MA120上穿、早盘强势延续、回踩后再上攻只提示你进行LLM判断，由你决定BUY_NOW或WAIT。",
            "不要因为股票早报原本是WATCH/低吸/等待确认而拒绝买入；WATCH表示等待盘中确认，技术确认后可以买。",
            "允许明显转弱时WAIT；不要为了凑满Top5而强行买。",
            f"SKIP_TODAY 是当天永久放弃，只能在 {_buy_timing_skip_earliest().strftime('%H:%M')} 之后，且趋势、盘口都明确破坏、反弹概率很低时使用；在此之前用WAIT继续观察。",
            "如果决定买入或撤单重报，price_mode可填FOLLOW；limit_price不参与实际报价；程序统一按最新价×1.015且不超过当天涨停价报价。",
            "如果已有未成交买单，同一股票不能重复挂单，只能 KEEP_ORDER/CANCEL_WAIT/CANCEL_REBUY/CANCEL_SKIP_TODAY。",
            "重点关注RSI/MACD/KDJ/布林带的超买超卖信号，结合大盘走势综合判断；如输入里提供板块涨跌数据，可作为辅助参考。",
            "只关注当天日内趋势、1分钟均线、MA120、VWAP/MA5/MA20承接、MACD/KDJ、布林带、量价配合、日内高低位、回踩承接、冲高回落和涨停不可追等技术因素。",
            "反追高：高开过大、RSI/KDJ极端、接近布林上轨、明显冲高回落、显著偏离VWAP/MA5时，优先WAIT等回踩确认。",
            "【硬规则】若当前价已到个股跌停价（price <= limit_down，允许极小报价误差），必须返回WAIT或SKIP_TODAY，禁止返回BUY_NOW；不要用固定-9.5%判断创业板/科创板跌停。",
        ],
    }
    use_thinking = os.getenv("INTRADAY_BUY_TIMING_THINKING", "1") == "1"
    if use_thinking:
        output_instruction = (
            "输出格式：先用中文写出分析思路（1-3句话），然后最后一行按以下格式输出结构化决策：\n"
            "action: BUY_NOW（或其他动作）\n"
            "price_mode: NONE（或其他模式）\n"
            "limit_price: null（或其他数值）\n"
            "max_premium_pct: 0.5（百分数数值）\n"
            "confidence: 75（0-100整数）\n"
            "reason: 一句话判断理由\n"
        )
    else:
        output_instruction = (
            "只能输出 JSON：action、price_mode、limit_price、max_premium_pct、confidence、reason。\n"
        )
    return (
        "你是盘中买入执行员。根据早报Top5观察池中的股票和实时行情，只用当天技术面判断本轮动作和买入报价。\n"
        + output_instruction + "\n"
        "action 可选：BUY_NOW、WAIT、SKIP_TODAY、KEEP_ORDER、CANCEL_WAIT、CANCEL_REBUY、CANCEL_SKIP_TODAY。\n"
        "price_mode 保留结构化字段即可，实际买入报价统一由程序按最新价×1.015且不超过涨停价计算。\n"
        "输入数据：\n"
        f"{json.dumps(payload, ensure_ascii=False, default=str)}"
    )


def _decision_to_dict(decision) -> Dict:
    if not decision:
        return {
            "action": "WAIT",
            "price_mode": "NONE",
            "limit_price": None,
            "max_premium_pct": 0.0,
            "confidence": 0,
            "reason": "LLM无有效结构化输出",
        }
    if hasattr(decision, "model_dump"):
        return decision.model_dump()
    if hasattr(decision, "dict"):
        return decision.dict()
    return dict(decision)


def _call_thinking_minimax(prompt: str, timeout: int = 90, thinking_budget: int = 4000, max_tokens: int = 8192) -> str:
    """Call MiniMax M3 through OpenClaw portal OAuth and return raw text response."""
    from stock_selection_debate.providers import call_llm
    thinking_prompt = (
        f"{prompt}\n\n"
        "请使用深度思考，但最终回答必须以一个独立 JSON 对象结尾，"
        "不要在 JSON 后再输出任何文字。JSON 字段固定为："
        "action、price_mode、limit_price、max_premium_pct、confidence、reason。"
    )
    return call_llm(
        prompt=thinking_prompt,
        model="minimax-portal/MiniMax-M3",
        timeout=timeout,
        retries=1,
        max_tokens=max_tokens,
        thinking_budget=thinking_budget,
        temperature=0,
    ).strip()


def _parse_buy_timing_text_response(text: str) -> Optional[Dict]:
    """Parse structured fields from thinking-mode text response."""
    if not text:
        return None

    def _first_payload_value(payload: Dict, *keys):
        for key in keys:
            if key in payload and payload.get(key) not in (None, ""):
                return payload.get(key)
        return None

    def _normalize_action_value(value) -> str:
        raw = str(value or "").strip().upper()
        compact = re.sub(r"\s+", "", raw)
        alias_map = {
            "买入": "BUY_NOW",
            "立即买入": "BUY_NOW",
            "直接买入": "BUY_NOW",
            "追入": "BUY_NOW",
            "追买": "BUY_NOW",
            "等待": "WAIT",
            "观望": "WAIT",
            "继续观察": "WAIT",
            "不买": "WAIT",
            "保留挂单": "KEEP_ORDER",
            "继续挂单": "KEEP_ORDER",
            "撤单等待": "CANCEL_WAIT",
            "撤单观望": "CANCEL_WAIT",
            "撤单重报": "CANCEL_REBUY",
            "撤单后重报": "CANCEL_REBUY",
            "撤单追高": "CANCEL_REBUY",
            "撤单后买入": "CANCEL_REBUY",
            "今日跳过": "SKIP_TODAY",
            "今日放弃": "SKIP_TODAY",
            "跳过": "SKIP_TODAY",
            "撤单今日跳过": "CANCEL_SKIP_TODAY",
            "撤单放弃": "CANCEL_SKIP_TODAY",
        }
        return alias_map.get(compact, raw)

    def _normalize_price_mode_value(value) -> str:
        raw = str(value or "NONE").strip().upper()
        compact = re.sub(r"\s+", "", raw)
        alias_map = {
            "无": "NONE",
            "不报价": "NONE",
            "跟随": "FOLLOW",
            "跟随最新价": "FOLLOW",
            "追价": "FOLLOW",
            "被动": "PASSIVE",
            "低吸": "DIP",
            "自定义": "CUSTOM",
        }
        return alias_map.get(compact, raw)

    def _normalize_payload(payload: Dict) -> Optional[Dict]:
        if not isinstance(payload, dict):
            return None
        action = _normalize_action_value(_first_payload_value(payload, "action", "动作", "操作", "决策", "结论"))
        valid_actions = {"BUY_NOW", "WAIT", "SKIP_TODAY", "KEEP_ORDER", "CANCEL_WAIT", "CANCEL_REBUY", "CANCEL_SKIP_TODAY"}
        if action not in valid_actions:
            return None
        price_mode = _normalize_price_mode_value(_first_payload_value(payload, "price_mode", "报价模式", "价格模式") or "NONE")
        valid_pm = {"NONE", "FOLLOW", "PASSIVE", "DIP", "CUSTOM"}
        if price_mode not in valid_pm:
            price_mode = "NONE"
        limit_price = _as_float(_first_payload_value(payload, "limit_price", "限价", "报价"))
        max_premium_pct = _normalize_price_pct_points(_first_payload_value(payload, "max_premium_pct", "最大溢价", "最大追价"), 0.0)
        try:
            confidence = max(0, min(int(float(_first_payload_value(payload, "confidence", "置信度", "信心分", "评分") or 0)), 100))
        except (TypeError, ValueError):
            confidence = 0
        return {
            "action": action,
            "price_mode": price_mode,
            "limit_price": limit_price,
            "max_premium_pct": max_premium_pct,
            "confidence": confidence,
            "reason": str(_first_payload_value(payload, "reason", "理由", "原因") or "").strip(),
        }

    json_candidates = []
    fenced = re.findall(r"```(?:json)?\s*(\{.*?\})\s*```", text, flags=re.I | re.S)
    json_candidates.extend(fenced)

    decoder = json.JSONDecoder()
    for match in re.finditer(r"\{", text):
        try:
            value, end = decoder.raw_decode(text[match.start():])
            if isinstance(value, dict):
                json_candidates.append(json.dumps(value, ensure_ascii=False))
        except Exception:
            continue

    first_obj = re.search(r"\{.*?\}", text, flags=re.S)
    if first_obj:
        json_candidates.append(first_obj.group(0))
    for candidate in json_candidates:
        try:
            parsed = _normalize_payload(json.loads(candidate))
            if parsed:
                return parsed
        except Exception:
            continue

    # Extract action
    m = re.search(r"(?:action|动作|操作|决策|结论)\s*[:：=]\s*([A-Za-z_]+|[\u4e00-\u9fff]{1,12})", text, re.IGNORECASE)
    action = _normalize_action_value(m.group(1)) if m else None
    # Extract price_mode
    m = re.search(r"(?:price_mode|报价模式|价格模式)\s*[:：=]\s*([A-Za-z_]+|[\u4e00-\u9fff]{1,12})", text, re.IGNORECASE)
    price_mode = _normalize_price_mode_value(m.group(1)) if m else "NONE"
    # Extract limit_price (may be null or a number)
    m = re.search(r"(?:limit_price|限价|报价)\s*[:：=]\s*(null|none|无|[-+]?[0-9]*\.?[0-9]+)", text, re.IGNORECASE)
    limit_price = None
    if m:
        raw_limit = m.group(1).strip().lower()
        if raw_limit not in {"null", "none", "无"}:
            limit_price = float(raw_limit)
    # Extract max_premium_pct
    m = re.search(r"(?:max_premium_pct|最大溢价|最大追价)\s*[:：=]\s*([-+]?[0-9]*\.?[0-9]+)", text, re.IGNORECASE)
    max_premium_pct = float(m.group(1)) if m else 0.0
    # Extract confidence
    m = re.search(r"(?:confidence|置信度|信心分|评分)\s*[:：=]\s*([0-9]+)", text, re.IGNORECASE)
    confidence = max(0, min(int(m.group(1)), 100)) if m else 0
    # Extract reason (everything after "reason:" to end of line)
    m = re.search(r"(?:reason|理由|原因)\s*[:：=]\s*(.+?)(?:\n|$)", text, re.IGNORECASE)
    reason = m.group(1).strip() if m else ""

    # Validate action
    valid_actions = {"BUY_NOW", "WAIT", "SKIP_TODAY", "KEEP_ORDER", "CANCEL_WAIT", "CANCEL_REBUY", "CANCEL_SKIP_TODAY"}
    if action not in valid_actions:
        return None
    # Validate price_mode
    valid_pm = {"NONE", "FOLLOW", "PASSIVE", "DIP", "CUSTOM"}
    if price_mode not in valid_pm:
        price_mode = "NONE"
    return _normalize_payload({
        "action": action,
        "price_mode": price_mode,
        "limit_price": limit_price,
        "max_premium_pct": max_premium_pct,
        "confidence": confidence,
        "reason": reason,
    })


def call_llm_buy_timing_decision(signal: Dict, quote: Dict, pending_order: Dict = None, now: datetime = None, technical_snapshot: Dict = None) -> Dict:
    """LLM timing decision with thinking mode. Fail-closed to WAIT."""
    try:
        prompt = _build_buy_timing_prompt(signal, quote, pending_order, now, technical_snapshot)
        timeout = int(os.getenv("INTRADAY_BUY_TIMING_LLM_TIMEOUT", "90"))
    except Exception as e:
        logger.warning(f"LLM分时买入提示词构建失败 {signal.get('stock')}: {e}")
        return {
            "action": "WAIT",
            "confidence": 0,
            "reason": f"LLM提示词构建失败，保守等待: {e}",
            "_llm_status": "failed",
            "_llm_error_type": "prompt_build_error",
        }

    primary_model = str(INTRADAY_BUY_TIMING_LLM_MODEL or "")
    fallback_model = str(INTRADAY_BUY_TIMING_LLM_FALLBACK_MODEL or "")
    primary_is_minimax = "minimax" in primary_model.lower()
    retries = int(os.getenv("INTRADAY_BUY_TIMING_LLM_RETRIES", "1"))
    thinking_budget = int(os.getenv("INTRADAY_BUY_TIMING_THINKING_BUDGET", "16000"))
    max_tokens = int(os.getenv("INTRADAY_BUY_TIMING_MAX_TOKENS", "8192"))

    def _minimax_preview(raw_text: str, limit: int = 300) -> str:
        text = str(raw_text or "").replace("\r", "\n")
        text = re.sub(r"\s+", " ", text).strip()
        return text[:limit]

    def _try_minimax_text_decision(label: str) -> Optional[Dict]:
        if not primary_is_minimax:
            return None
        try:
            raw_text = _call_thinking_minimax(
                prompt,
                timeout=timeout,
                thinking_budget=thinking_budget,
                max_tokens=max_tokens,
            )
            parsed = _parse_buy_timing_text_response(raw_text)
            if parsed:
                parsed["_llm_model"] = primary_model
                parsed["_llm_path"] = f"minimax_thinking_text:{label}"
                parsed["_llm_status"] = "ok"
                logger.info(f"LLM分时买入MiniMax文本解析成功 {signal.get('stock')}: {parsed.get('action')} ({label})")
                return parsed
            logger.warning(
                "LLM分时买入MiniMax文本无法解析 %s: len=%s preview=%s (%s)",
                signal.get("stock"),
                len(raw_text or ""),
                _minimax_preview(raw_text),
                label,
            )
        except Exception as text_error:
            logger.warning(f"LLM分时买入MiniMax文本解析失败 {signal.get('stock')}: {text_error} ({label})")
        return None

    def _try_minimax_format_retry() -> Optional[Dict]:
        if not primary_is_minimax:
            return None
        strict_prompt = (
            f"{prompt}\n\n"
            "上一次输出未被程序解析。请只输出一个 JSON 对象，不要 markdown，不要解释文字，"
            "第一个字符必须是 {，最后一个字符必须是 }。格式示例：\n"
            '{"action":"WAIT","price_mode":"NONE","limit_price":null,'
            '"max_premium_pct":0,"confidence":50,"reason":"一句话原因"}'
        )
        try:
            raw_text = _call_thinking_minimax(
                strict_prompt,
                timeout=timeout,
                thinking_budget=thinking_budget,
                max_tokens=max_tokens,
            )
            parsed = _parse_buy_timing_text_response(raw_text)
            if parsed:
                parsed["_llm_model"] = primary_model
                parsed["_llm_path"] = "minimax_thinking_text:format_retry"
                parsed["_llm_status"] = "ok"
                logger.info(f"LLM分时买入MiniMax格式重试解析成功 {signal.get('stock')}: {parsed.get('action')}")
                return parsed
            logger.warning(
                "LLM分时买入MiniMax格式重试仍无法解析 %s: len=%s preview=%s",
                signal.get("stock"),
                len(raw_text or ""),
                _minimax_preview(raw_text),
            )
        except Exception as text_error:
            logger.warning(f"LLM分时买入MiniMax格式重试失败 {signal.get('stock')}: {text_error}")
        return None

    def _try_structured_decision(model: str, allow_fallback: bool, label: str, fallback: str = "") -> Optional[Dict]:
        from stock_selection_debate.providers import call_structured
        decision = call_structured(
            prompt,
            IntradayBuyTimingDecision,
            model=model,
            timeout=timeout,
            retries=retries,
            thinking_budget=thinking_budget,
            max_tokens=max_tokens,
            allow_fallback=allow_fallback,
            fallback_model=fallback,
        )
        if not decision:
            return None
        result = _decision_to_dict(decision)
        result["_llm_model"] = model
        result["_llm_path"] = label
        result["_llm_status"] = "ok"
        logger.info(f"LLM分时买入决策({model}, {label})成功 {signal.get('stock')}: {result.get('action')}")
        return result

    # MiniMax M3 deep-thinking 更适合先输出文本再解析；严格 JSON 仅作为非 MiniMax 或兜底模型路径。
    if primary_is_minimax:
        parsed = _try_minimax_text_decision("primary")
        if parsed:
            return parsed
        parsed = _try_minimax_format_retry()
        if parsed:
            return parsed
        if fallback_model:
            try:
                fallback_result = _try_structured_decision(
                    fallback_model,
                    allow_fallback=False,
                    label=f"fallback_after_minimax_text_failed:{fallback_model}",
                )
                if fallback_result:
                    return fallback_result
            except Exception as fallback_error:
                logger.warning(f"LLM分时买入GPT-5.6 Sol兜底失败 {signal.get('stock')}: {fallback_error}")
        logger.warning(f"LLM分时买入({primary_model})文本解析和备用模型均失败 {signal.get('stock')}")
        return {
            "action": "WAIT",
            "confidence": 0,
            "reason": "LLM无有效结构化输出，保守等待",
            "_llm_model": primary_model,
            "_llm_path": "all_paths_failed",
            "_llm_status": "failed",
            "_llm_error_type": "parse_or_provider_error",
        }

    try:
        result = _try_structured_decision(
            primary_model,
            allow_fallback=True,
            label="structured",
            fallback=fallback_model,
        )
        if result:
            return result
        logger.warning(f"LLM分时买入({primary_model})无有效结构化输出 {signal.get('stock')}")
        return {
            "action": "WAIT",
            "confidence": 0,
            "reason": "LLM无有效结构化输出，保守等待",
            "_llm_model": primary_model,
            "_llm_path": "structured_empty",
            "_llm_status": "failed",
            "_llm_error_type": "empty_output",
        }
    except Exception as e:
        logger.warning(f"LLM分时买入决策(thinking模式)失败 {signal.get('stock')}: {e}")
        return {
            "action": "WAIT",
            "confidence": 0,
            "reason": f"LLM决策失败，保守等待: {e}",
            "_llm_model": primary_model,
            "_llm_path": "structured_exception",
            "_llm_status": "failed",
            "_llm_error_type": type(e).__name__,
        }



def _sanitize_timing_action(action: str, has_pending: bool, now: datetime = None) -> str:
    now = now or datetime.now()
    action = str(action or "WAIT").upper()
    if now.time() >= _buy_timing_cutoff() and action in {"BUY_NOW", "CANCEL_REBUY"}:
        return "KEEP_ORDER" if has_pending else "WAIT"
    if now.time() < _buy_timing_skip_earliest() and action in {"SKIP_TODAY", "CANCEL_SKIP_TODAY"}:
        return "KEEP_ORDER" if has_pending else "WAIT"
    if has_pending:
        if action in {"BUY_NOW", "WAIT"}:
            return "KEEP_ORDER"
        if action not in {"KEEP_ORDER", "CANCEL_WAIT", "CANCEL_REBUY", "CANCEL_SKIP_TODAY"}:
            return "KEEP_ORDER"
        return action
    if action not in {"BUY_NOW", "WAIT", "SKIP_TODAY"}:
        return "WAIT"
    return action


def _format_buy_timing_round_message(round_summary: Dict, state: Dict, name_map: Dict[str, str], now: datetime) -> str:
    decisions = round_summary.get("decisions", [])
    lines = [
        f"🤖 分时买入运行中 {now.strftime('%H:%M')}",
        f"{_buy_timing_skip_earliest().strftime('%H:%M')}前不永久踢出观察池；本轮结论 {len(decisions)} 只",
    ]
    if not decisions:
        lines.append("本轮没有可判断标的，可能已成交、已撤或暂时无行情。")
        return "\n".join(lines)

    for item in decisions:
        stock = item.get("stock", "")
        name = name_map.get(stock, stock)
        action = item.get("action", "WAIT")
        price = item.get("quote_price")
        confidence = item.get("confidence", 0)
        reason = str(item.get("reason", "")).strip()
        trigger = item.get("technical_trigger")
        entry = _state_entry(state, stock) if stock else {}
        status = entry.get("status", "open")
        lines.append("")
        lines.append(f"{stock} {name}")
        lines.append(f"结论: {action} / 状态: {status} / 现价: {price} / 置信: {confidence}分")
        if trigger:
            lines.append(f"技术触发: {trigger}")
        if reason:
            lines.append(f"理由: {reason}")
    return "\n".join(lines)


def _buy_order_price_from_quote(quote: Dict) -> float:
    """Legacy fallback quote: latest price plus 1.5%, capped by limit-up."""
    price = float(quote.get("price", 0) or 0)
    limit_up = float(quote.get("limit_up", price * 1.10) or price * 1.10)
    return min(round(price * 1.015, 2), limit_up)


def _normalize_price_pct_points(value, default: float = 0.0) -> float:
    """Normalize pct points: 1.0 means 1%, 0.15 means 0.15%."""
    raw = _as_float(value, default)
    if raw is None:
        return default
    return raw


def _buy_limit_up_from_quote(quote: Dict, stock: str = "", name: str = "") -> float:
    price = float(quote.get("price", 0) or 0)
    explicit_limit = _as_float(quote.get("limit_up"))
    if explicit_limit and explicit_limit > 0:
        return float(explicit_limit)
    limit_pct = _stock_limit_pct(stock or quote.get("stock") or quote.get("code") or "", name or quote.get("name") or "")
    prev_close = _as_float(quote.get("prev_close") or quote.get("lastClose") or quote.get("preClose"))
    if prev_close and prev_close > 0:
        return round(prev_close * (1 + limit_pct), 2)
    return round(price * (1 + limit_pct), 2) if price > 0 else 0.0


def _buy_order_price_from_decision(quote: Dict, decision: Dict, stock: str = "", name: str = "") -> Dict:
    """Use one unified intraday buy quote: latest price * 1.015, capped by limit-up."""
    price = float(quote.get("price", 0) or 0)
    if price <= 0:
        return {"ok": False, "reason": "最新价无效"}

    limit_up = _buy_limit_up_from_quote(quote, stock=stock, name=name)
    if limit_up <= 0:
        limit_up = price
    raw_price = round(price * 1.015, 2)
    order_price = round(min(raw_price, limit_up), 2)
    mode = str(decision.get("price_mode") or "").upper()

    guard = "统一报价: 最新价×1.015"
    if order_price != raw_price:
        guard += f"，超过涨停价已截断到{order_price}"
    return {
        "ok": True,
        "order_price": order_price,
        "raw_order_price": raw_price,
        "price_mode": "UNIFIED_1_015",
        "requested_price_mode": mode or "NONE",
        "max_premium_pct": 1.5,
        "price_guard": guard,
        "price_bounds": {"upper": round(limit_up, 2)},
    }


def _buy_min_shares(stock: str) -> int:
    stock = str(stock or "")
    if stock.startswith("688"):
        return 200
    return 100


def _buy_quantity_for_amount(stock: str, amount: float, order_price: float) -> int:
    stock = str(stock or "")
    raw_quantity = int(float(amount or 0) / float(order_price or 0)) if order_price else 0
    if raw_quantity <= 0:
        return 0
    if stock.startswith("688"):
        return raw_quantity if raw_quantity >= 200 else 0
    if stock.startswith(("8", "4", "920")):
        return raw_quantity if raw_quantity >= 100 else 0
    return (raw_quantity // 100) * 100


def _buy_trade_key(api_order: Dict) -> str:
    order_id = _extract_order_id(api_order)
    if order_id not in (None, ""):
        return f"order:{order_id}"
    order_time = api_order.get("order_time") or api_order.get("time") or api_order.get("trade_time")
    stock = str(api_order.get("stock") or api_order.get("stockCode") or api_order.get("code") or "")
    price = api_order.get("trade_price") or api_order.get("price") or ""
    quantity = api_order.get("quantity") or api_order.get("tradeCount") or ""
    order_quantity = api_order.get("order_quantity") or api_order.get("order_count") or api_order.get("count") or ""
    if order_time and stock and price and quantity:
        return f"fill:{stock}:{order_time}:{price}:{quantity}:{order_quantity}"
    return ""


def _should_stop_buy_timing_loop(once: bool, signals_done: bool, today_report_loaded: bool) -> bool:
    if once:
        return True
    if signals_done and today_report_loaded:
        return True
    return False


def _calc_timing_buy_order(signal: Dict, quote: Dict, initial_cash: float, available_cash: float, decision: Dict = None) -> Dict:
    sig = _parse_signal_for_buy(signal)
    conf = int(_confidence_value(signal) or 60)
    pos_pct = _legacy_buy_position_pct(sig, conf)
    available_cash = float(available_cash or 0)
    initial_cash = float(initial_cash or 0)
    cash_base = initial_cash if initial_cash > 0 else available_cash
    max_per_stock = min(cash_base * pos_pct, available_cash)
    if max_per_stock <= 0:
        return {"ok": False, "reason": "可用资金不足"}
    stock = str(signal.get("stock", "") or "")
    price_plan = _buy_order_price_from_decision(quote, decision or {}, stock=stock, name=signal.get("name", ""))
    if not price_plan.get("ok"):
        return {"ok": False, "reason": price_plan.get("reason", "LLM报价无效")}
    order_price = price_plan["order_price"]
    min_shares = _buy_min_shares(stock)
    quantity = _buy_quantity_for_amount(stock, max_per_stock, order_price)
    if quantity < min_shares:
        cost_min = order_price * min_shares
        if cost_min <= available_cash:
            quantity = min_shares
        else:
            return {"ok": False, "reason": f"不足最低买入数量({cost_min:.0f}元 > 可用{available_cash:.0f}元)"}
    return {
        "ok": True,
        "order_price": order_price,
        "quantity": quantity,
        "pos_pct": pos_pct,
        "estimated_amount": order_price * quantity,
        **{k: v for k, v in price_plan.items() if k != "ok"},
    }


def _append_confirmed_buy_trade(signal: Dict, api_order: Dict, name: str, decision: Optional[Dict] = None) -> bool:
    stock = signal.get("stock", "")
    quantity = int(api_order.get("quantity", 0) or 0)
    price = float(api_order.get("trade_price", 0) or 0)
    order_id = _extract_order_id(api_order)
    order_id = str(order_id) if order_id not in (None, "") else ""
    trade_key = _buy_trade_key(api_order)
    if not stock or quantity <= 0 or price <= 0:
        return False
    trades = _load_trades()
    today = date.today().isoformat()
    for rec in trades.get("records", []):
        if rec.get("stock") != stock or rec.get("buy_date") != today or rec.get("source") != "intraday_buy_timing":
            continue
        record_order_id = str(rec.get("order_id") or "")
        record_trade_key = str(rec.get("trade_key") or "")
        lot_order_ids = {
            str(lot.get("order_id") or "")
            for lot in (rec.get("buy_records") or [])
            if isinstance(lot, dict) and lot.get("order_id") not in (None, "")
        }
        lot_trade_keys = {
            str(lot.get("trade_key") or "")
            for lot in (rec.get("buy_records") or [])
            if isinstance(lot, dict) and lot.get("trade_key") not in (None, "")
        }
        same_known_order = bool(order_id and (record_order_id == order_id or order_id in lot_order_ids))
        same_trade_key = bool(trade_key and (record_trade_key == trade_key or trade_key in lot_trade_keys))
        same_unknown_order = not order_id and not trade_key and not record_order_id and not record_trade_key and not lot_order_ids and not lot_trade_keys
        if not (same_known_order or same_trade_key or same_unknown_order):
            continue
        changed = (
            rec.get("quantity") != quantity
            or abs(float(rec.get("buy_price", 0) or 0) - price) > 1e-6
            or (order_id and record_order_id != order_id)
            or (trade_key and record_trade_key != trade_key)
        )
        if changed:
            rec["buy_price"] = price
            rec["quantity"] = quantity
            rec["remaining_quantity"] = quantity
            if order_id:
                rec["order_id"] = order_id
            if trade_key:
                rec["trade_key"] = trade_key
            rec["buy_records"] = [{
                "date": today,
                "price": price,
                "quantity": quantity,
                "remaining": quantity,
                "source": "intraday_buy_timing",
                **({"order_id": order_id} if order_id else {}),
                **({"trade_key": trade_key} if trade_key else {}),
            }]
            _save_trades(trades)
        return changed
    record = {
        "stock": stock,
        "name": name,
        "buy_date": today,
        "buy_price": price,
        "quantity": quantity,
        "remaining_quantity": quantity,
        # 已成交一律 BUY；confidence / reason / trigger 用 buy-timing 实际决策（fallback 到 phase2 signal）
        "action": "BUY",
        "confidence": int((decision or {}).get("confidence") or signal.get("confidence") or 60),
        "reason": ((decision or {}).get("reason") or signal.get("reason") or "").strip(),
        "buy_trigger": (decision or {}).get("technical_trigger", ""),
        "pool": signal.get("pool", ""),
        "source_pools": signal.get("source_pools", []),
        "strategy_type": signal.get("strategy_type", ""),
        "entry_bias": signal.get("entry_bias", ""),
        "source": "intraday_buy_timing",
        **({"order_id": order_id} if order_id else {}),
        **({"trade_key": trade_key} if trade_key else {}),
        "buy_records": [{
            "date": today,
            "price": price,
            "quantity": quantity,
            "remaining": quantity,
            "source": "intraday_buy_timing",
            **({"order_id": order_id} if order_id else {}),
            **({"trade_key": trade_key} if trade_key else {}),
        }],
        "sells": [],
    }
    trades.setdefault("records", []).append(record)
    _save_trades(trades)
    return True


def _mark_buy_timing_entry_filled(entry: Dict, api_order: Dict) -> None:
    """Persist the filled order identity in timing state for restart/audit clarity."""
    entry["status"] = "filled"
    entry.pop("pending_order", None)
    entry["filled_quantity"] = int(api_order.get("quantity", 0) or 0)
    entry["filled_price"] = float(api_order.get("trade_price", 0) or 0)
    entry["filled_at"] = api_order.get("order_time")
    order_id = _extract_order_id(api_order)
    if order_id not in (None, ""):
        entry["order_id"] = str(order_id)
    trade_key = _buy_trade_key(api_order)
    if trade_key:
        entry["trade_key"] = trade_key
    compact = _compact_order(api_order)
    if compact:
        entry["last_order"] = compact
    fill_time = _parse_state_datetime(api_order.get("order_time")) or datetime.now()
    _append_buy_timing_event(
        "ORDER_FILL",
        stock=str(entry.get("stock") or api_order.get("stock") or ""),
        now=fill_time,
        order={
            "order_id": order_id,
            "filled_price": entry.get("filled_price"),
            "filled_quantity": entry.get("filled_quantity"),
            "status": _order_status_text(api_order),
        },
    )


def _repair_filled_timing_entry_identity(entry: Dict) -> None:
    """Backfill order identity for filled entries created before state schema v2."""
    if not isinstance(entry, dict) or entry.get("status") != "filled":
        return
    order = entry.get("last_order") if isinstance(entry.get("last_order"), dict) else {}
    order_id = _extract_order_id(order)
    if order_id not in (None, "") and not entry.get("order_id"):
        entry["order_id"] = str(order_id)
    if entry.get("order_id") and not entry.get("trade_key"):
        entry["trade_key"] = f"order:{entry['order_id']}"


def _has_buy_timing_trackable_orders(state: Dict, signals_by_stock: Dict[str, Dict]) -> bool:
    for stock in signals_by_stock:
        entry = _state_entry(state, stock)
        status = entry.get("status")
        if status in {"filled", "skip_today", "cancelled"}:
            continue
        if (
            status == "pending"
            or entry.get("pending_order")
            or entry.get("submitted_order_count")
            or entry.get("submitted_orders")
            or entry.get("last_order")
        ):
            return True
    return False


def _refresh_buy_timing_fills(
    state: Dict,
    signals_by_stock: Dict[str, Dict],
    name_map: Dict[str, str],
    today_orders: Dict = None,
    now: datetime = None,
) -> List[Dict]:
    if not _has_buy_timing_trackable_orders(state, signals_by_stock):
        return []
    today_orders = today_orders if today_orders is not None else get_today_orders()
    orders_snapshot_ok = today_orders.get("_ok") is not False
    buy_orders = today_orders.get("buys", [])
    order_map = _build_buy_order_map(buy_orders)
    order_id_map = _build_order_id_map(buy_orders)
    confirmed = []
    for stock, signal in signals_by_stock.items():
        entry = _state_entry(state, stock)
        if entry.get("status") in {"filled", "skip_today", "cancelled"}:
            continue
        if not (
            entry.get("submitted_order_count")
            or entry.get("submitted_orders")
            or entry.get("last_order")
            or entry.get("pending_order")
            or entry.get("status") == "pending"
        ):
            continue
        local_order_id = _entry_order_id(entry)
        api_order = order_id_map.get(local_order_id) if local_order_id else None
        if not api_order:
            api_order = _latest_order_for_stock(buy_orders, stock) or order_map.get(stock)
        if not api_order:
            if orders_snapshot_ok and (entry.get("status") == "pending" or entry.get("pending_order")):
                if _should_keep_missing_pending_order(entry, now):
                    entry["status"] = "pending"
                    entry["last_order_status"] = "missing_in_order_snapshot_grace"
                    continue
                entry["status"] = "open"
                entry.pop("pending_order", None)
                entry["last_order_status"] = "not_found_in_order_snapshot"
            continue
        if _is_pending_order(api_order):
            entry["status"] = "pending"
            entry["partial_filled_quantity"] = int(api_order.get("quantity", 0) or 0)
            entry["partial_filled_price"] = float(api_order.get("trade_price", 0) or 0)
            entry["pending_order"] = _compact_order(api_order)
            continue
        # 非pending：可能是已成交、已撤单、废单 → 清理pending_order
        entry.pop("pending_order", None)
        filled_qty = int(api_order.get("quantity", 0) or 0)
        if filled_qty > 0:
            _mark_buy_timing_entry_filled(entry, api_order)
            if not entry.get("trade_recorded") or entry.get("recorded_quantity") != entry["filled_quantity"]:
                last_decision = entry.get("last_decision") or {}
                entry["trade_recorded"] = _append_confirmed_buy_trade(signal, api_order, name_map.get(stock, stock), decision=last_decision)
                entry["recorded_quantity"] = entry["filled_quantity"]
            confirmed.append({"stock": stock, **api_order})
        else:
            # 已撤/废单：重置为 open，允许重新评估（用户方案 2）
            entry["status"] = "open"
            entry["last_order_status"] = _order_status_text(api_order)
    if confirmed:
        try:
            report = reconcile_trades_file_with_account(source="intraday_buy_timing")
            logger.info(
                f"买入成交后持仓同步: fixed={len(report.get('fixed', []))}, "
                f"consistent={report.get('is_consistent')}"
            )
        except Exception as e:
            logger.error(f"买入成交后持仓同步失败: {e}")
    return confirmed


def _cancel_timing_pending_orders(state: Dict, reason: str = "截止时间未成交", stocks: set = None) -> List[Dict]:
    cancelled = []
    today_orders = get_today_orders(force=True)
    pending = [
        o for o in today_orders.get("buys", [])
        if _is_pending_order(o) and (stocks is None or o.get("stock", "") in stocks)
    ]
    if today_orders.get("_ok") is False:
        logger.warning("截止撤单时今日委托查询失败，改用本地pending_order兜底撤单")
        known = {
            str(_extract_order_id(o) or "")
            for o in pending
            if _extract_order_id(o) not in (None, "")
        }
        for stock in (stocks or set((state.get("stocks") or {}).keys())):
            entry = _state_entry(state, stock)
            local_order = entry.get("pending_order") if isinstance(entry.get("pending_order"), dict) else None
            if not local_order and entry.get("status") == "pending":
                local_order = entry.get("last_order") if isinstance(entry.get("last_order"), dict) else None
            if not local_order:
                continue
            order_id = str(_extract_order_id(local_order) or "")
            if not order_id or order_id in known:
                continue
            fallback_order = dict(local_order)
            fallback_order.setdefault("stock", stock)
            pending.append(fallback_order)
            known.add(order_id)
    for order in pending:
        stock = order.get("stock", "")
        entry = _state_entry(state, stock)
        result = cancel_buy_order(_extract_order_id(order), stock, reason)
        _append_buy_timing_event(
            "ORDER_CANCEL",
            stock=stock,
            order={
                "order_id": _extract_order_id(order),
                "status": result.get("status"),
                "error": result.get("error"),
                "reason": reason,
            },
        )
        entry["last_cancellation"] = {
            "time": datetime.now().isoformat(),
            "reason": reason,
            "order": _compact_order(order),
            "status": result.get("status"),
            "error": result.get("error"),
        }
        entry["cancellation_count"] = int(entry.get("cancellation_count", 0) or 0) + 1
        if result.get("status") in {"submitted", "dry_run"}:
            # 主动撤单：重置为 open，允许重新评估（用户方案 2）
            entry["status"] = "open"
            entry.pop("pending_order", None)
        cancelled.append({"stock": stock, "result": result})
    return cancelled


def _run_buy_timing_round(
    signals: List[Dict],
    name_map: Dict[str, str],
    state: Dict,
    initial_cash: float,
    available_cash: float,
    now: datetime = None,
) -> float:
    now = now or datetime.now()
    signals_by_stock = {s.get("stock"): s for s in signals if s.get("stock")}
    _append_buy_timing_event(
        "ROUND_HEARTBEAT",
        now=now,
        details={"active_stocks": sorted(stock for stock in signals_by_stock if stock)},
    )
    tracked_orders = _has_buy_timing_trackable_orders(state, signals_by_stock)
    # 没有本地可追踪挂单时，不在每分钟技术检查前查 /orders。
    # 真正准备 BUY_NOW 前会再强制查询一次委托，做重复下单保护。
    today_orders = {"buys": [], "sells": [], "_skipped": True}
    orders_snapshot_ok = True
    if tracked_orders:
        today_orders = get_today_orders(force=True)
        orders_snapshot_ok = today_orders.get("_ok") is not False
        _refresh_buy_timing_fills(state, signals_by_stock, name_map, today_orders=today_orders, now=now)
    pending_by_stock = {}
    # ── 大盘指数默认关闭：分时买入只依赖Top5个股当天技术面，减少盘中QMT压力。
    state["_round_index_data"] = {}
    if _buy_timing_index_data_enabled():
        try:
            xt = _get_xq_client()
            if xt is not None:
                ticks = xt.xtdata.get_full_tick(["000300.SH", "399006.SZ", "000001.SH", "000688.SH"])
                index_data = {}
                for idx_code in ("000300.SH", "399006.SZ", "000001.SH", "000688.SH"):
                    if idx_code in ticks:
                        t = ticks[idx_code]
                        prev = float(t.get("lastClose") or 0)
                        cur = float(t.get("lastPrice") or 0)
                        chg_pct = round((cur - prev) / prev * 100, 2) if prev > 0 else 0.0
                        index_data[idx_code] = {
                            "lastPrice": cur,
                            "change_pct": chg_pct,
                            "prevClose": prev,
                        }
                state["_round_index_data"] = index_data
        except Exception as e:
            logger.debug(f"大盘指数拉取失败: {e}")

    # ── 板块实时涨跌默认关闭：盘中买入以Top5个股当天技术面为主，避免akshare慢接口拖住主轮询。
    board_data = {}
    if os.getenv("INTRADAY_BUY_INCLUDE_BOARD_DATA", "0") == "1":
        try:
            import akshare as ak
            board_df = ak.stock_board_industry_name_em()
            if not board_df.empty:
                board_data = {
                    str(row.get("板块名称", "")): float(row.get("涨跌幅", 0) or 0)
                    for _, row in board_df.iterrows()
                    if row.get("板块名称")
                }
            # 概念板块
            concept_df = ak.stock_board_concept_name_em()
            if not concept_df.empty:
                for _, row in concept_df.iterrows():
                    name = str(row.get("板块名称", ""))
                    pct = float(row.get("涨跌幅", 0) or 0)
                    if name:
                        board_data[name] = pct
        except Exception as e:
            logger.debug(f"akshare板块涨跌拉取失败: {e}")
    state["_round_board_data"] = board_data

    for order in today_orders.get("buys", []):
        order_stock = order.get("stock")
        if order_stock not in signals_by_stock:
            continue
        entry = _state_entry(state, order_stock)
        if _is_terminal_filled_order(order) and int(order.get("quantity", 0) or 0) > 0:
            if entry.get("status") not in {"skip_today", "cancelled"}:
                _mark_buy_timing_entry_filled(entry, order)
                if not entry.get("trade_recorded") or entry.get("recorded_quantity") != entry["filled_quantity"]:
                    last_decision = entry.get("last_decision") or {}
                    entry["trade_recorded"] = _append_confirmed_buy_trade(
                        signals_by_stock[order_stock],
                        order,
                        name_map.get(order_stock, order_stock),
                        decision=last_decision,
                    )
                    entry["recorded_quantity"] = entry["filled_quantity"]
            continue
        if _is_pending_order(order):
            pending_by_stock.setdefault(order_stock, order)
            if entry.get("status") not in {"filled", "skip_today", "cancelled"}:
                entry["status"] = "pending"
                entry["pending_order"] = _compact_order(order)

    if tracked_orders:
        # 如果订单接口有返回但某只本地 pending 不再出现在未完成委托里，允许重新评估。
        # 注意：接口限速/失败时 today_orders 为空，保守不清本地 pending，避免重复挂单。
        if orders_snapshot_ok:
            for stock in signals_by_stock:
                entry = _state_entry(state, stock)
                if entry.get("status") == "pending" and stock not in pending_by_stock:
                    if _should_keep_missing_pending_order(entry, now):
                        entry["status"] = "pending"
                        entry["last_order_status"] = "missing_in_order_snapshot_grace"
                        continue
                    entry["status"] = "open"
                    entry.pop("pending_order", None)
                    entry["last_order_status"] = "not_found_in_order_snapshot"

    round_summary = {"time": now.isoformat(), "decisions": []}
    max_reprices = int(os.getenv("INTRADAY_BUY_MAX_REPRICES", "3"))

    try:
        for signal in signals:
            stock = signal.get("stock", "")
            if not stock:
                continue
            entry = _state_entry(state, stock)
            pending_order = pending_by_stock.get(stock)
            # 当日去重：本轮只使用开头那一次 /orders 快照，不再每只股票重复查。
            if pending_order:
                logger.info(f"[未成交挂单] {stock} 有未完成委托，进入本轮重评")
            elif entry.get("pending_order") or entry.get("status") == "pending":
                logger.info(f"[当日去重] {stock} 本地仍有未确认挂单，跳过本轮避免重复下单")
                continue
            if entry.get("status") in {"filled", "skip_today", "cancelled"}:
                logger.info(f"[当日去重] {stock} status={entry.get('status')}，跳过本轮")
                continue

            if not orders_snapshot_ok:
                decision = {
                    "action": "WAIT",
                    "price_mode": "NONE",
                    "limit_price": None,
                    "max_premium_pct": 0.0,
                    "confidence": 0,
                    "reason": "当日委托查询失败，无法确认是否已有未成交买单，本轮跳过以避免重复挂单",
                    "llm_skipped": True,
                }
                _record_buy_timing_decision(entry, decision, now)
                round_summary["decisions"].append({"stock": stock, **decision})
                continue

            if not _claim_buy_timing_stock(state, stock, "round", ttl_seconds=90):
                logger.info(f"[技术检查互斥] {stock} 正在技术触发检查，跳过本轮")
                continue

            name = name_map.get(stock, signal.get("name", stock))
            quote = get_intraday_buy_quote(stock)
            if not quote:
                decision = {"action": "WAIT", "confidence": 0, "reason": "实时行情获取失败，保守等待"}
                _append_buy_timing_event("MARKET_DATA_ERROR", stock=stock, now=now, error="实时行情获取失败")
                _record_buy_timing_decision(entry, decision, now)
                round_summary["decisions"].append({"stock": stock, **decision})
                continue

            has_pending = bool(pending_order)
            bars = get_intraday_1m_bars(stock, count=500)
            technical_snapshot = _intraday_technical_snapshot(stock, quote, bars)
            _cache_buy_timing_market(stock, bars, quote, now)
            # 注入大盘指数和板块数据
            technical_snapshot["_index_data"] = state.get("_round_index_data", {})
            technical_snapshot["_board_data"] = state.get("_round_board_data", {})
            decision = _technical_buy_timing_decision(signal, quote, bars, entry, now, pending_order)
            if decision and decision.get("force_llm_review"):
                technical_trigger = decision.get("technical_trigger")
                trigger_detail = decision.get("trigger_detail")
                if _is_technical_llm_due(entry, technical_trigger, now):
                    llm_started = datetime.now()
                    _append_buy_timing_event(
                        "TECHNICAL_TRIGGER",
                        stock=stock,
                        now=llm_started,
                        decision=decision,
                        market=_compact_audit_market(quote, technical_snapshot),
                    )
                    decision = call_llm_buy_timing_decision(signal, quote, pending_order, now, technical_snapshot)
                    llm_finished = datetime.now()
                    decision["_llm_started_at"] = llm_started.isoformat()
                    decision["_llm_finished_at"] = llm_finished.isoformat()
                    decision["_llm_latency_seconds"] = round((llm_finished - llm_started).total_seconds(), 3)
                    decision["technical_trigger"] = decision.get("technical_trigger") or technical_trigger
                    decision["trigger_detail"] = decision.get("trigger_detail") or trigger_detail
                    entry["last_llm_check_at"] = now.isoformat()
                    entry["last_technical_llm_check_at"] = now.isoformat()
                    entry["last_technical_llm_trigger"] = technical_trigger
                    entry["next_priority_check_at"] = (now + timedelta(seconds=int(os.getenv("INTRADAY_BUY_TECH_TRIGGER_REVIEW_SECONDS", "120")))).isoformat()
                else:
                    decision = {
                        "action": "WAIT",
                        "price_mode": "NONE",
                        "limit_price": None,
                        "max_premium_pct": 0.0,
                        "confidence": 0,
                        "reason": f"{technical_trigger}已触发，未到短周期复判间隔，继续观察",
                        "technical_trigger": technical_trigger,
                        "trigger_detail": trigger_detail,
                        "llm_skipped": True,
                        "skip_reason": "technical_review_interval",
                    }
            elif decision is None:
                if _is_buy_timing_llm_due(entry, now):
                    llm_started = datetime.now()
                    decision = call_llm_buy_timing_decision(signal, quote, pending_order, now, technical_snapshot)
                    llm_finished = datetime.now()
                    decision["_llm_started_at"] = llm_started.isoformat()
                    decision["_llm_finished_at"] = llm_finished.isoformat()
                    decision["_llm_latency_seconds"] = round((llm_finished - llm_started).total_seconds(), 3)
                    entry["last_llm_check_at"] = now.isoformat()
                else:
                    decision = {
                        "action": "WAIT",
                        "price_mode": "NONE",
                        "limit_price": None,
                        "max_premium_pct": 0.0,
                        "confidence": 0,
                        "reason": f"技术触发未命中，LLM未到{_buy_timing_llm_interval_minutes(now)}分钟检查间隔，继续观察",
                        "llm_skipped": True,
                    }
            action = _sanitize_timing_action(decision.get("action"), has_pending, now)
            decision = {
                "action": action,
                "price_mode": str(decision.get("price_mode") or "NONE").upper(),
                "limit_price": _as_float(decision.get("limit_price")),
                "max_premium_pct": _normalize_price_pct_points(decision.get("max_premium_pct"), 0.0),
                "confidence": int(decision.get("confidence", 0) or 0),
                "reason": str(decision.get("reason", "")).strip(),
                "quote_price": quote.get("price"),
                "quote_source": quote.get("source"),
                "technical_trigger": decision.get("technical_trigger"),
                "trigger_detail": decision.get("trigger_detail"),
                "skip_reason": decision.get("skip_reason"),
                "anti_chase_reason": decision.get("anti_chase_reason"),
                "llm_skipped": bool(decision.get("llm_skipped")),
                "_llm_model": decision.get("_llm_model") or decision.get("llm_model"),
                "_llm_path": decision.get("_llm_path") or decision.get("llm_path"),
                "_llm_status": decision.get("_llm_status") or decision.get("llm_status"),
                "_llm_error_type": decision.get("_llm_error_type") or decision.get("llm_error_type"),
                "_llm_started_at": decision.get("_llm_started_at") or decision.get("llm_started_at"),
                "_llm_finished_at": decision.get("_llm_finished_at") or decision.get("llm_finished_at"),
                "_llm_latency_seconds": decision.get("_llm_latency_seconds") or decision.get("llm_latency_seconds"),
                "market_snapshot": _compact_audit_market(quote, technical_snapshot),
            }
            _record_buy_timing_decision(entry, decision, now)
            round_summary["decisions"].append({"stock": stock, **decision})

            cancelled_pending_this_round = False
            if has_pending:
                if action == "KEEP_ORDER":
                    entry["status"] = "pending"
                    entry["pending_order"] = _compact_order(pending_order)
                    continue

                if action == "CANCEL_REBUY" and int(entry.get("reprice_count", 0) or 0) >= max_reprices:
                    action = "KEEP_ORDER"
                    _record_buy_timing_decision(entry, {
                        "action": action,
                        "confidence": 0,
                        "reason": f"撤改单次数已达上限{max_reprices}，保留原挂单",
                    }, now)
                    entry["status"] = "pending"
                    entry["pending_order"] = _compact_order(pending_order)
                    continue

                cancel_result = cancel_buy_order(_extract_order_id(pending_order), stock, decision.get("reason", "LLM建议撤单"))
                _append_buy_timing_event(
                    "ORDER_CANCEL",
                    stock=stock,
                    now=now,
                    order={
                        "order_id": _extract_order_id(pending_order),
                        "status": cancel_result.get("status"),
                        "error": cancel_result.get("error"),
                        "reason": decision.get("reason", "LLM建议撤单"),
                    },
                )
                entry["last_cancellation"] = {
                    "time": now.isoformat(),
                    "reason": decision.get("reason", "LLM建议撤单"),
                    "order": _compact_order(pending_order),
                    "status": cancel_result.get("status"),
                    "error": cancel_result.get("error"),
                }
                entry["cancellation_count"] = int(entry.get("cancellation_count", 0) or 0) + 1
                if cancel_result.get("status") not in {"submitted", "dry_run"}:
                    entry["status"] = "pending"
                    entry["pending_order"] = _compact_order(pending_order)
                    continue
                entry["status"] = "open"
                entry.pop("pending_order", None)
                if action == "CANCEL_WAIT":
                    continue
                if action == "CANCEL_SKIP_TODAY":
                    entry["status"] = "skip_today"
                    continue
                if action == "CANCEL_REBUY":
                    entry["reprice_count"] = int(entry.get("reprice_count", 0) or 0) + 1
                    cancelled_pending_this_round = True
                    action = "BUY_NOW"

            if action == "SKIP_TODAY":
                entry["status"] = "skip_today"
                continue
            if action != "BUY_NOW":
                entry["status"] = "open"
                continue

            if today_orders.get("_skipped") and not cancelled_pending_this_round:
                today_orders = get_today_orders(force=True)
                orders_snapshot_ok = today_orders.get("_ok") is not False
            if not orders_snapshot_ok and not cancelled_pending_this_round:
                _append_buy_timing_event(
                    "ORDER_BLOCKED",
                    stock=stock,
                    now=now,
                    error="买入前当日委托查询失败",
                    details={"category": "order_snapshot"},
                )
                _record_buy_timing_decision(entry, {
                    "action": "WAIT",
                    "price_mode": "NONE",
                    "confidence": 0,
                    "reason": "买入前当日委托查询失败，跳过以避免重复挂单",
                    "quote_price": quote.get("price"),
                }, now)
                entry["status"] = "open"
                continue
            existing_order = None
            if not cancelled_pending_this_round:
                existing_order = _best_existing_buy_order_for_stock(today_orders.get("buys", []), stock)
            if existing_order and int(existing_order.get("quantity", 0) or 0) > 0 and not _is_pending_order(existing_order):
                _mark_buy_timing_entry_filled(entry, existing_order)
                if not entry.get("trade_recorded") or entry.get("recorded_quantity") != entry["filled_quantity"]:
                    last_decision = entry.get("last_decision") or decision
                    entry["trade_recorded"] = _append_confirmed_buy_trade(signal, existing_order, name, decision=last_decision)
                    entry["recorded_quantity"] = entry["filled_quantity"]
                continue
            if existing_order and _is_pending_order(existing_order):
                entry["status"] = "pending"
                entry["pending_order"] = _compact_order(existing_order)
                continue

            # 跌停硬截断：禁止在任何情况下对已跌停股票下单
            price = float(quote.get("price", 0) or 0)
            limit_down = float(quote.get("limit_down", 0) or 0)
            change_pct = float(quote.get("change_pct", 0) or 0)
            if _is_buy_quote_limit_down(quote):
                logger.warning(f"{stock} 跌停价截断买入: price={price} limit_down={limit_down} change_pct={change_pct}")
                _record_buy_timing_decision(entry, {
                    "action": "WAIT",
                    "price_mode": "NONE",
                    "confidence": 0,
                    "reason": _limit_down_block_reason(quote),
                    "quote_price": quote.get("price"),
                }, now)
                entry["status"] = "open"
                continue

            # 只有决定 BUY_NOW 后才查询真实可用资金；首次买入时记录当天初始资金作为仓位分档基准。
            available_cash = _get_available_cash()
            if available_cash is None:
                logger.warning(f"{stock} 获取可用资金失败，跳过本轮")
                _append_buy_timing_event(
                    "ORDER_BLOCKED",
                    stock=stock,
                    now=now,
                    error="获取可用资金失败",
                    details={"category": "cash_snapshot"},
                )
                entry["status"] = "open"
                entry["last_skip_reason"] = "获取可用资金失败"
                continue
            if not state.get("initial_cash"):
                state["initial_cash"] = available_cash
            initial_cash = state.get("initial_cash")

            order_plan = _calc_timing_buy_order(signal, quote, initial_cash, available_cash, decision)
            if not order_plan.get("ok"):
                _append_buy_timing_event(
                    "ORDER_BLOCKED",
                    stock=stock,
                    now=now,
                    details={"category": "order_plan", "reason": order_plan.get("reason")},
                )
                entry["status"] = "open"
                entry["last_skip_reason"] = order_plan.get("reason")
                continue

            # 涨停保护：已涨停时若LLM仍返回BUY_NOW，强制以涨停价为限价
            limit_up = float(quote.get("limit_up", 0) or 0)
            if limit_up > 0 and price >= limit_up and order_plan.get("order_price", 0) > limit_up:
                logger.warning(f"{stock} 已涨停(现价={price} >= 涨停价={limit_up}), 强制以涨停价挂单")
                order_plan["order_price"] = limit_up
                order_plan["price_mode"] = "LIMIT"
                order_plan["price_bounds"] = {"limit_up": limit_up}

            # 资金只在 BUY_NOW 前查一次，避免同一只票重复打 /positions。
            reason_prefix = "技术触发买入" if decision.get("technical_trigger") else "LLM分时买入"
            reason = f"{reason_prefix}: {decision.get('reason', '')}"
            result = buy_stock(stock, name, order_plan["order_price"], order_plan["quantity"], reason)
            _append_buy_timing_event(
                "ORDER_SUBMIT",
                stock=stock,
                now=now,
                order={
                    "order_id": result.get("order_id"),
                    "order_price": order_plan.get("order_price"),
                    "raw_order_price": order_plan.get("raw_order_price"),
                    "quote_price": quote.get("price"),
                    "quantity": order_plan.get("quantity"),
                    "status": result.get("status"),
                    "error": result.get("error"),
                    "reason": reason,
                },
                market=_compact_audit_market(quote, technical_snapshot),
            )
            if result.get("status") in {"submitted", "dry_run"}:
                pending_record = {
                    "time": now.isoformat(),
                    "order_id": result.get("order_id"),
                    "order_price": order_plan["order_price"],
                    "raw_order_price": order_plan.get("raw_order_price"),
                    "price_mode": order_plan.get("price_mode"),
                    "price_guard": order_plan.get("price_guard"),
                    "price_bounds": order_plan.get("price_bounds"),
                    "quantity": order_plan["quantity"],
                    "quote_price": quote.get("price"),
                    "result": result,
                }
                entry["status"] = "pending"
                entry["pending_order"] = _compact_order(pending_record)
                entry["last_order"] = _compact_order(pending_record)
                entry["submitted_order_count"] = int(entry.get("submitted_order_count", 0) or 0) + 1
                # 不再手动扣减available_cash，每次买入前重新查询API获取真实余额
            else:
                entry["status"] = "open"
                entry["last_error"] = result.get("error", result.get("status"))
            time.sleep(1)
    finally:
        _release_buy_timing_owner_claims(state, "round")

    # 技术触发或定时LLM判断完成后都推送，减少每分钟噪音
    has_trigger = any(d.get("technical_trigger") for d in round_summary.get("decisions", []))
    has_llm_run = any(not d.get("llm_skipped") for d in round_summary.get("decisions", []))
    if has_trigger or has_llm_run:
        feishu_push(_format_buy_timing_round_message(round_summary, state, name_map, now))
    return available_cash


def _buy_timing_finished_today(now: datetime = None) -> bool:
    now = now or datetime.now()
    try:
        state = _load_buy_timing_state()
        finished_at = _parse_state_datetime(state.get("finished_at"))
        return bool(finished_at and finished_at.date() == now.date())
    except Exception as e:
        logger.debug(f"分时买入完成状态检查失败: {e}")
        return False


def run_buy_timing_mode():
    setup_logging()
    now = datetime.now()
    if _should_skip_non_trading_day("分时买入", now):
        return 0
    if (
        now.time() < _buy_timing_launch_earliest()
        and os.getenv("ALLOW_BUY_OUTSIDE_WINDOW") != "1"
    ):
        logger.info(
            f"当前早于分时买入允许启动时间 {_buy_timing_launch_earliest().strftime('%H:%M')}，退出避免提前占用锁"
        )
        return 0
    if (
        now.time() >= _buy_timing_cutoff()
        and os.getenv("ALLOW_BUY_OUTSIDE_WINDOW") != "1"
        and _buy_timing_finished_today(now)
    ):
        return 0
    lock = _acquire_buy_timing_process_lock()
    if lock is None:
        feishu_push("⚠️ 盘中买入已有进程在运行，本次启动已跳过，避免重复下单")
        return 75
    try:
        _run_buy_timing_mode_unlocked()
        return 0
    except Exception as exc:
        _append_buy_timing_event("TASK_ERROR", error=exc, details={"fatal": True})
        _flush_buy_timing_market(force=True)
        raise
    finally:
        _release_buy_timing_process_lock(lock)


def _run_buy_timing_mode_unlocked():
    """
    技术优先分时买入模式:
    - 股票池固定为早报 phase2.top_picks
    - 09:31 首轮开盘强势直接追入
    - 09:31 后每分钟检查 1分钟 MA120 上穿，触发LLM判断，午休暂停
    - 其余情况由 LLM 仅按当天技术面中性判断，10点前每3分钟、10点后每10分钟
    - 最晚14:57停止新买入并撤掉未成交买入挂单
    """
    now = datetime.now()
    if now.time() >= _buy_timing_cutoff() and os.getenv("ALLOW_BUY_OUTSIDE_WINDOW") != "1":
        state = _load_buy_timing_state()
        finished_at = _parse_state_datetime(state.get("finished_at"))
        if finished_at and finished_at.date() == now.date():
            return
        logger.info(f"当前已超过{_buy_timing_cutoff().strftime('%H:%M')},跳过分时买入")
        return

    logger.info("=" * 50)
    logger.info("Phase 4: 技术优先分时买入模式")
    logger.info("=" * 50)
    _append_buy_timing_event("TASK_START", now=now, details={"stage": "initializing"})

    if not API_KEY:
        logger.error("MX_APIKEY 未设置")
        _append_buy_timing_event("TASK_ERROR", now=now, error="MX_APIKEY 未设置", details={"fatal": True})
        feishu_push("⚠️ MX_APIKEY 未配置,无法执行分时买入")
        return

    if _should_skip_non_trading_day("分时买入", now):
        _append_buy_timing_event(
            "TASK_END",
            now=now,
            details={"status": "skipped", "reason": "non_trading_day"},
        )
        return

    today_str = date.today().strftime("%Y%m%d")

    report_file = OUTPUT_DIR / f"daily_report_{today_str}.json"
    report = _load_daily_report_with_top_picks(report_file)

    phase2 = report.get("phase2", {}) if report else {}
    ranked = phase2.get("ranked_candidates", [])
    top_picks = phase2.get("top_picks", [])
    name_map = {}
    for s in list(ranked) + list(top_picks):
        stock_code = s.get("stock")
        if stock_code:
            name_map[stock_code] = s.get("name", stock_code)

    today_signals = _select_intraday_timing_pool(report, 5) if report else []
    carryover_signals = _carryover_intraday_timing_signals(today_signals, date.today())
    if not today_signals and not carryover_signals:
        report = _load_ready_daily_report_for_timing(report_file)
        if not report:
            _append_buy_timing_event("TASK_END", details={"status": "skipped", "reason": "daily_report_unavailable"})
            return
        phase2 = report.get("phase2", {})
        ranked = phase2.get("ranked_candidates", [])
        top_picks = phase2.get("top_picks", [])
        for s in list(ranked) + list(top_picks):
            stock_code = s.get("stock")
            if stock_code:
                name_map[stock_code] = s.get("name", stock_code)
        today_signals = _select_intraday_timing_pool(report, 5)
        carryover_signals = _carryover_intraday_timing_signals(today_signals, date.today())
    signals = _merge_intraday_timing_pool(today_signals, carryover_signals)
    if not signals:
        _append_buy_timing_event("TASK_END", details={"status": "skipped", "reason": "empty_watch_pool"})
        feishu_push(f"📋 {date.today()} 分时买入\n今日早报Top5和昨日未成交顺延池均为空，跳过")
        return
    for s in carryover_signals:
        stock_code = s.get("stock")
        if stock_code:
            name_map.setdefault(stock_code, s.get("name", stock_code))

    state = _load_buy_timing_state()
    state["selected_stocks"] = [s.get("stock") for s in signals]
    state["selected_signals"] = [_normalize_buy_signal(s) for s in signals if s.get("stock")]
    state["carryover_stocks"] = [s.get("stock") for s in carryover_signals if s.get("stock")]
    state["started_at"] = datetime.now().isoformat()
    state.pop("finished_at", None)
    if not state.get("initial_cash"):
        state.pop("initial_cash", None)
    _save_buy_timing_state(state)
    _append_buy_timing_event(
        "WATCH_POOL_READY",
        details={
            "selected_stocks": state.get("selected_stocks") or [],
            "daily_top5_count": len(today_signals),
            "carryover_count": len(carryover_signals),
        },
    )
    for signal in signals:
        _append_buy_timing_event(
            "WATCH_POOL_ADD",
            stock=str(signal.get("stock") or ""),
            details={
                "name": signal.get("name"),
                "source": "carryover" if signal.get("carryover_from") else "daily_top5",
                "carryover_from": signal.get("carryover_from"),
                "signal": signal.get("signal") or signal.get("action"),
                "confidence": signal.get("confidence"),
                "buy_score": signal.get("buy_score"),
            },
        )

    once = os.getenv("INTRADAY_BUY_TIMING_ONCE") == "1"
    feishu_push(
        f"🤖 技术分时买入启动 {date.today()}\n"
        f"股票池: {', '.join(state['selected_stocks'])}\n"
        f"今日Top5: {len(today_signals)}只 | 昨日未成交顺延: {len(carryover_signals)}只\n"
        f"规则: 09:31开盘强势直接追入；之后MA120上穿/早盘强势延续/回踩再上攻触发LLM判断；其余纯技术LLM按10点前{_buy_timing_llm_interval_minutes(datetime.combine(date.today(), dt_time(9, 59)))}分钟/10点后{_buy_timing_llm_interval_minutes(datetime.combine(date.today(), dt_time(10, 0)))}分钟中性判断\n"
        f"截止: {_buy_timing_cutoff().strftime('%H:%M')} | 报价规则: 最新价×1.015，且不超过当天涨停价"
    )

    # 主循环每分钟检查技术触发，并按3/10分钟节奏做非触发LLM判断。
    # 旧实时兼容线程会重复拉行情/K线并共享状态，已彻底停用，避免QMT/mx接口压力和误判崩溃。
    _buy_timing_realtime_thread_enabled()
    logger.info("[实时硬触发] 兼容线程已停用；由主轮询每分钟检查技术触发")

    today_report_loaded = bool(today_signals)
    while True:
        now = datetime.now()
        if now.time() >= _buy_timing_cutoff() and os.getenv("ALLOW_BUY_OUTSIDE_WINDOW") != "1":
            logger.info(f"已到{_buy_timing_cutoff().strftime('%H:%M')}截止时间，停止新买入判断")
            break
        next_check = _next_buy_timing_check(now)
        if not _is_in_buy_timing_session(now):
            if next_check is None:
                break
            sleep_seconds = max(1, int((next_check - now).total_seconds()))
            logger.info(f"当前不在分时买入判断窗口，等待到 {next_check.strftime('%H:%M')}")
            if once:
                break
            time.sleep(sleep_seconds)
            continue

        if not today_report_loaded:
            fresh_report = _load_daily_report_with_top_picks(report_file)
            if fresh_report:
                fresh_today_signals = _select_intraday_timing_pool(fresh_report, 5)
                if fresh_today_signals:
                    fresh_phase2 = fresh_report.get("phase2", {})
                    for s in list(fresh_phase2.get("ranked_candidates", [])) + list(fresh_phase2.get("top_picks", [])):
                        stock_code = s.get("stock")
                        if stock_code:
                            name_map.setdefault(stock_code, s.get("name", stock_code))
                    today_signals = fresh_today_signals
                    signals[:] = _merge_intraday_timing_pool(today_signals, carryover_signals)
                    state["selected_stocks"] = [s.get("stock") for s in signals]
                    state["selected_signals"] = [_normalize_buy_signal(s) for s in signals if s.get("stock")]
                    state["carryover_stocks"] = [s.get("stock") for s in carryover_signals if s.get("stock")]
                    today_report_loaded = True
                    _save_buy_timing_state(state)
                    feishu_push(f"📋 早报Top5已生成并加入盘中买入观察池: {', '.join(s.get('stock', '') for s in today_signals)}")

        try:
            _run_buy_timing_round(signals, name_map, state, state.get("initial_cash"), None, now)
            state["consecutive_round_errors"] = 0
        except Exception as e:
            logger.exception(f"分时买入本轮异常，已跳过本轮并继续下一轮: {e}")
            _append_buy_timing_event("ROUND_ERROR", now=datetime.now(), error=e)
            errors = state.setdefault("round_errors", [])
            errors.append({
                "time": datetime.now().isoformat(),
                "error": str(e),
            })
            state["round_errors"] = errors[-20:]
            state["consecutive_round_errors"] = int(state.get("consecutive_round_errors", 0) or 0) + 1
            feishu_push(f"⚠️ 分时买入本轮异常，任务未退出，下轮继续: {e}")
        _save_buy_timing_state(state)

        signals_done = all(
            _state_entry(state, s.get("stock")).get("status") in {"filled", "skip_today", "cancelled"}
            for s in signals
            if s.get("stock")
        )
        if _should_stop_buy_timing_loop(once, signals_done, today_report_loaded):
            break

        next_check = _next_buy_timing_check(datetime.now())
        if next_check is None:
            break
        sleep_seconds = max(1, int((next_check - datetime.now()).total_seconds()))
        logger.info(f"下一轮分时买入判断: {next_check.strftime('%H:%M')}")
        time.sleep(sleep_seconds)

    signals_by_stock = {s.get("stock"): s for s in signals if s.get("stock")}
    _refresh_buy_timing_fills(state, signals_by_stock, name_map)
    if not once and datetime.now().time() >= _buy_timing_cutoff():
        _cancel_timing_pending_orders(state, f"{_buy_timing_cutoff().strftime('%H:%M')}截止仍未成交", set(signals_by_stock.keys()))
    state["finished_at"] = datetime.now().isoformat()
    _save_buy_timing_state(state)
    _flush_buy_timing_market(force=True)

    status_counts = {}
    for stock in state.get("selected_stocks", []):
        status = _state_entry(state, stock).get("status", "open")
        status_counts[status] = status_counts.get(status, 0) + 1
    lines = [
        f"📋 分时买入结束 {date.today()}",
        f"状态: {status_counts}",
    ]
    for stock in state.get("selected_stocks", []):
        entry = _state_entry(state, stock)
        name = name_map.get(stock, stock)
        filled = f" 成交{entry.get('filled_quantity')}股@{entry.get('filled_price')}" if entry.get("status") == "filled" else ""
        lines.append(f"{stock} {name}: {entry.get('status')}{filled}")
    _append_buy_timing_event(
        "TASK_END",
        details={"status_counts": status_counts, "selected_stocks": state.get("selected_stocks") or []},
    )
    feishu_push("\n".join(lines))


# ── 止盈卖出数量计算 ────────────────────────────────────

def _tracked_remaining_quantity(record: Dict) -> int:
    buy_records = record.get("buy_records", [])
    if buy_records:
        return int(sum(max(0, int(br.get("remaining", 0) or 0)) for br in buy_records))
    return int(record.get("remaining_quantity", 0) or 0)


def _select_trade_record_for_stock(trades: Dict, stock: str) -> Optional[Dict]:
    records = [
        rec for rec in (trades or {}).get("records", [])
        if str(rec.get("stock") or "") == str(stock)
    ]
    if not records:
        return None
    active = [rec for rec in records if _tracked_remaining_quantity(rec) > 0]
    return sorted(active or records, key=lambda rec: str(rec.get("buy_date") or ""))[-1]


def _scaled_position_price(pos: Dict, value_key: str, dec_key: str, default_dec: int) -> float:
    if value_key == "costPrice" and "cost_price" in pos:
        return float(pos.get("cost_price") or 0)
    if value_key == "price" and "current_price" in pos:
        return float(pos.get("current_price") or 0)
    value = pos.get(value_key)
    if value in (None, ""):
        return 0.0
    try:
        return float(value or 0) / pow(10, int(pos.get(dec_key, default_dec) or default_dec))
    except Exception:
        return 0.0


def _positions_snapshot_for_trade_sync(positions: List[Dict]) -> List[Dict]:
    snapshot = []
    for pos in positions or []:
        stock = str(pos.get("stock") or pos.get("stockCode") or pos.get("secCode") or "")
        quantity = int(pos.get("quantity", pos.get("totalQuantity", pos.get("count", 0))) or 0)
        if not stock or quantity <= 0:
            continue
        snapshot.append({
            "stock": stock,
            "name": pos.get("name") or pos.get("stockName") or pos.get("secName") or stock,
            "quantity": quantity,
            "avail_quantity": int(pos.get("avail_quantity", pos.get("availQuantity", pos.get("availCount", quantity))) or 0),
            "cost_price": _scaled_position_price(pos, "costPrice", "costPriceDec", 3),
            "current_price": _scaled_position_price(pos, "price", "priceDec", 2),
        })
    return snapshot


def _reconcile_trades_to_positions_snapshot(trades: Dict, positions: List[Dict], source: str) -> tuple:
    snapshot = _positions_snapshot_for_trade_sync(positions)
    if not snapshot:
        return trades, {"fixed": [], "is_consistent": True}
    return reconcile_trades_with_positions(trades, snapshot, source=source)


def _reconcile_trade_record_to_position(record: Dict, actual_quantity: int, reference_price: float) -> bool:
    """Make local trade lots match broker-reported sellable quantity before writing sells."""
    if not record or actual_quantity < 0:
        return False
    tracked = _tracked_remaining_quantity(record)
    if tracked == actual_quantity:
        return False

    gap = actual_quantity - tracked
    buy_records = record.get("buy_records", [])
    reference_price = float(reference_price or record.get("buy_price") or 0)
    record["remaining_quantity"] = actual_quantity
    sold_quantity = sum(int(s.get("quantity", 0) or 0) for s in record.get("sells", []))
    record["quantity"] = max(int(record.get("quantity", 0) or 0), sold_quantity + actual_quantity)
    record["sync_warning"] = (
        f"持仓同步修正: 本地剩余{tracked}股, 券商可卖{actual_quantity}股, "
        f"差额{gap:+d}股"
    )

    if buy_records:
        if gap > 0:
            inherited_tiers = sorted(_executed_take_profit_tiers(record))
            buy_records.append({
                "date": date.today().isoformat(),
                "price": reference_price,
                "quantity": gap,
                "remaining": gap,
                "source": "position_reconcile",
                "executed_tp_tiers": inherited_tiers,
            })
        else:
            to_reduce = -gap
            reduce_order = (
                [br for br in reversed(buy_records) if str(br.get("source") or "") == "position_reconcile"]
                + [br for br in reversed(buy_records) if str(br.get("source") or "") != "position_reconcile"]
            )
            for br in reduce_order:
                if to_reduce <= 0:
                    break
                br_remaining = int(br.get("remaining", 0) or 0)
                if br_remaining <= 0:
                    continue
                reduce_qty = min(br_remaining, to_reduce)
                br["remaining"] = br_remaining - reduce_qty
                to_reduce -= reduce_qty
    return True


def _is_star_market(stock: str) -> bool:
    return str(stock or "").startswith("688")


def _min_sell_quantity(stock: str) -> int:
    # 科创板卖出 200 股起，超过 200 股部分可 1 股递增；其它 A 股按整百卖。
    return 200 if _is_star_market(stock) else 100


def _normalize_full_exit_sell_quantity(stock: str, current_quantity: int) -> int:
    """止损/三档止盈卖出数量：低于最低交易单位时一次性卖完。"""
    current_quantity = int(current_quantity or 0)
    if current_quantity <= 0:
        return 0
    min_qty = _min_sell_quantity(stock)
    if current_quantity < min_qty:
        return current_quantity
    if _is_star_market(stock):
        return current_quantity
    return (current_quantity // 100) * 100


def _normalize_partial_take_profit_sell_quantity(stock: str, current_quantity: int, raw_quantity: int) -> int:
    """一档/二档止盈卖出数量：不足按比例分档卖出时不触发。"""
    current_quantity = int(current_quantity or 0)
    raw_quantity = int(raw_quantity or 0)
    if current_quantity <= 0 or raw_quantity <= 0:
        return 0
    min_qty = _min_sell_quantity(stock)
    if current_quantity < min_qty or raw_quantity > current_quantity:
        return 0
    if _is_star_market(stock):
        return raw_quantity if raw_quantity >= min_qty else 0
    normalized = (raw_quantity // 100) * 100
    return normalized if normalized >= min_qty else 0


def _normalize_target_sell_quantity(stock: str, current_quantity: int, raw_quantity: int) -> int:
    """Normalize arbitrary target sell quantity under board lot constraints."""
    current_quantity = int(current_quantity or 0)
    raw_quantity = int(raw_quantity or 0)
    if current_quantity <= 0 or raw_quantity <= 0:
        return 0
    min_qty = _min_sell_quantity(stock)
    target = min(current_quantity, raw_quantity)
    if current_quantity < min_qty:
        return current_quantity
    if _is_star_market(stock):
        return target if target >= min_qty else 0
    normalized = (target // 100) * 100
    return normalized if normalized >= min_qty else 0


def _is_valid_sell_quantity(stock: str, current_quantity: int, sell_quantity: int) -> bool:
    current_quantity = int(current_quantity or 0)
    sell_quantity = int(sell_quantity or 0)
    if sell_quantity <= 0 or sell_quantity > current_quantity:
        return False
    min_qty = _min_sell_quantity(stock)
    if current_quantity < min_qty:
        return sell_quantity == current_quantity
    if _is_star_market(stock):
        return sell_quantity >= min_qty
    return sell_quantity >= min_qty and sell_quantity % 100 == 0


def _calculate_sell_quantity_by_reason(trades: Dict, stock: str, current_quantity: int, reason: str) -> int:
    """
    根据卖出原因和交易历史计算正确的卖出数量

    参数:
        trades: 交易记录数据
        stock: 股票代码
        current_quantity: 当前可用数量(availCount)
        reason: 卖出原因

    返回:
        符合交易单位的应卖出数量；分档止盈不足交易单位时返回0
    """
    current_quantity = int(current_quantity or 0)

    # 如果不是止盈卖出,直接使用当前可用数量做全量退出规范化
    if "止盈" not in reason:
        return _normalize_full_exit_sell_quantity(stock, current_quantity)

    # 查找该股票在交易记录中的买入记录
    for record in trades.get("records", []):
        if record.get("stock") == stock:
            initial_quantity = record.get("quantity", 0)  # 初始买入总数量
            remaining_quantity = record.get("remaining_quantity", initial_quantity)  # 当前剩余数量

            # 计算已卖出累计数量
            sold_quantity = 0
            for sell_record in record.get("sells", []):
                sold_quantity += sell_record.get("quantity", 0)

            # 验证数据一致性:已卖出 + 剩余应该等于初始
            expected_initial = sold_quantity + remaining_quantity
            if abs(expected_initial - initial_quantity) > 10:  # 允许少量误差
                logger.warning(f"数据不一致: {stock} 初始={initial_quantity}, 已卖={sold_quantity}, 剩余={remaining_quantity}")
                # 使用更可信的当前剩余数量推算初始
                initial_quantity = sold_quantity + remaining_quantity

            # 止盈分档按原始仓位计算：一档卖原始仓位30%，二档卖原始仓位20%。
            if "止盈第1档" in reason:
                return _normalize_partial_take_profit_sell_quantity(
                    stock, current_quantity, int(initial_quantity * 0.3)
                )
            elif "止盈第2档" in reason:
                return _normalize_partial_take_profit_sell_quantity(
                    stock, current_quantity, int(initial_quantity * 0.2)
                )
            elif "止盈第3档" in reason:
                return _normalize_full_exit_sell_quantity(stock, current_quantity)
            else:
                return _normalize_full_exit_sell_quantity(stock, current_quantity)

    # 如果找不到交易记录,使用简单比例计算
    logger.warning(f"未找到 {stock} 的交易记录,使用简单比例计算")
    if "止盈第3档" in reason:
        return _normalize_full_exit_sell_quantity(stock, current_quantity)
    if "止盈第1档" in reason:
        qty = int(current_quantity * 0.3)
        return _normalize_partial_take_profit_sell_quantity(stock, current_quantity, qty)
    if "止盈第2档" in reason:
        qty = int(current_quantity * 0.2)
        return _normalize_partial_take_profit_sell_quantity(stock, current_quantity, qty)
    if "止盈" in reason:
        qty = int(current_quantity / 3)
        return _normalize_partial_take_profit_sell_quantity(stock, current_quantity, qty)
    return _normalize_full_exit_sell_quantity(stock, current_quantity)


def _compute_position_bp(stock_record: Optional[Dict], cost: float) -> float:
    """计算持仓成本价 bp。

    优先级：
    1. buy_records 里 rem > 0 的 lot 加权平均
    2. broker 实时 cost（防 zombie record 误判）

    修 (2026-06-08): 不能 fallback 到 stock_record.get("buy_price", 0)。
    5月29日 _zero_record 后 buy_price 字段保留为历史值（如 000725 的 5.23），
    6月8日 14:50 用 5.23 兜底导致误判浮盈 20.5% 触发"止盈第2档"。
    详见 test_zombie_record_bp_fallback.py。
    """
    buy_records = (stock_record or {}).get("buy_records", [])
    if buy_records:
        total_cost = sum(
            br["price"] * br["remaining"]
            for br in buy_records
            if br.get("remaining", 0) > 0
        )
        total_qty = sum(
            br.get("remaining", 0)
            for br in buy_records
            if br.get("remaining", 0) > 0
        )
        return total_cost / total_qty if total_qty > 0 else cost
    return cost


def _executed_take_profit_tiers(record: Dict) -> set:
    tiers = set()
    for sell_record in (record or {}).get("sells", []):
        reason = str(sell_record.get("reason", ""))
        if "止盈第1档" in reason:
            tiers.add(1)
        if "止盈第2档" in reason:
            tiers.add(2)
        if "止盈第3档" in reason:
            tiers.add(3)
    return tiers


def _take_profit_tier_from_reason(reason: str) -> int:
    reason = str(reason or "")
    if "止盈第1档" in reason:
        return 1
    if "止盈第2档" in reason:
        return 2
    if "止盈第3档" in reason:
        return 3
    return 0


def _build_lot_states(record: Dict, fallback_price: float, fallback_quantity: int) -> List[Dict]:
    lots_raw = (record or {}).get("buy_records") or []
    lots: List[Dict] = []
    if not lots_raw:
        rem = int((record or {}).get("remaining_quantity", 0) or fallback_quantity or 0)
        if rem > 0:
            lots_raw = [{
                "date": (record or {}).get("buy_date"),
                "price": (record or {}).get("buy_price", fallback_price),
                "quantity": rem,
                "remaining": rem,
            }]
    for lot in lots_raw:
        rem = max(0, int(lot.get("remaining", 0) or 0))
        qty = max(int(lot.get("quantity", 0) or rem or 0), rem)
        if qty <= 0 and rem <= 0:
            continue
        lot_source = str(lot.get("source") or (record or {}).get("source") or "")
        lot_tiers = set(lot.get("executed_tp_tiers") or [])
        if lot_source == "position_reconcile":
            lot_tiers |= _executed_take_profit_tiers(record or {})
        lots.append({
            "date": lot.get("date") or (record or {}).get("buy_date") or "",
            "buy_price": float(lot.get("price", 0) or (record or {}).get("buy_price", 0) or fallback_price or 0),
            "original_quantity": qty,
            "remaining": rem,
            "source": lot_source,
            "executed_tp_tiers": lot_tiers,
        })
    fifo = [{"left": int(l["original_quantity"]), "ref": l} for l in lots]
    for sell in (record or {}).get("sells", []):
        reason = str(sell.get("reason", ""))
        tier = 0
        tier = _take_profit_tier_from_reason(reason)
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
    return lots


# ── Mode: monitor ────────────────────────────────────────

def run_monitor_mode():
    setup_logging()
    """
    持仓监控模式(每天 14:50 触发)
    1. 获取当前持仓
    2. 获取持仓股票实时价格
    3. 检查止盈止损条件
    4. 触发卖出
    """
    logger.info("=" * 50)
    logger.info("Phase 4: 持仓监控模式")
    logger.info("=" * 50)

    if not API_KEY:
        logger.error("MX_APIKEY 未设置")
        return

    now_dt = datetime.now()
    final_deadline = datetime.combine(date.today(), SELL_MONITOR_FINAL_TIME) + timedelta(seconds=SELL_MONITOR_FINAL_GRACE_SECONDS)
    force_run = os.getenv("INTRADAY_SELL_FORCE_RUN") == "1"
    if now_dt.time() < SELL_MONITOR_START_TIME and not force_run and os.getenv("INTRADAY_SELL_ALLOW_BEFORE_START") != "1":
        logger.info("未到10:00盘中卖出监控窗口,跳过持仓监控")
        return
    if now_dt > final_deadline and not force_run and os.getenv("INTRADAY_SELL_ALLOW_AFTER_CUTOFF") != "1":
        logger.warning("已超过14:50尾盘兜底窗口,跳过下单型持仓监控")
        feishu_push("📋 持仓监控已超过14:50尾盘兜底窗口，跳过下单型卖出检查")
        return

    # 交易日检查:复用 execute_debate_result 的 is_trading_day
    today_str = date.today().strftime("%Y%m%d")
    logger.info(f"交易日检查: today={today_str}")

    try:
        sys.path.insert(0, str(BASE_DIR))
        from execute_debate_result import is_trading_day
        if not is_trading_day():
            logger.info(f"今日({today_str})为非交易日,跳过持仓监控")
            return
        logger.info(f"今日为交易日,继续监控")
    except Exception as e:
        logger.warning(f"交易日判断失败,默认继续: {e}")

    positions = get_current_positions()
    if not positions:
        logger.info("无持仓,跳过监控")
        if INTRADAY_MONITOR_RECONCILE_ENABLED:
            try:
                report = reconcile_trades_file_with_account(source="intraday_monitor_empty")
                logger.info(
                    f"空持仓同步: fixed={len(report.get('fixed', []))}, "
                    f"consistent={report.get('is_consistent')}"
                )
            except Exception as e:
                logger.error(f"空持仓同步失败: {e}")
        return

    logger.info(f"当前持仓: {len(positions)} 只")

    sell_results = []
    sell_skipped = []  # 触发后条件消失而跳过的
    sell_reason_map = {}  # order_id -> reason；无委托号时保留 stock 兜底
    peak_updates = {}  # stock -> highest price metadata
    trades_dirty = False
    sell_orders_snapshot = None

    # 加载交易记录,用于计算正确的卖出数量。盘中默认不做全量账户同步，避免高频监控触发112限速。
    if INTRADAY_MONITOR_RECONCILE_ENABLED:
        try:
            report = reconcile_trades_file_with_account(source="intraday_monitor_start")
            if report.get("fixed"):
                logger.warning(
                    f"监控前已按模拟账户同步 trades.json: fixed={len(report.get('fixed', []))}, "
                    f"consistent={report.get('is_consistent')}"
                )
        except Exception as e:
            logger.error(f"监控前持仓同步失败: {e}")
            return
    trades = _load_trades()
    try:
        trades, snapshot_report = _reconcile_trades_to_positions_snapshot(
            trades,
            positions,
            source="intraday_monitor_snapshot",
        )
        if snapshot_report.get("fixed"):
            _save_trades(trades)
            logger.warning(
                f"监控前已按本次持仓快照同步 trades.json: fixed={len(snapshot_report.get('fixed', []))}, "
                f"consistent={snapshot_report.get('is_consistent')}"
            )
    except Exception as e:
        logger.error(f"监控前本地持仓快照同步失败: {e}")
        return
    for pos in positions:
        stock = pos.get("stockCode", "")
        name = pos.get("stockName", "")
        quantity = int(pos.get("availQuantity", 0))
        total_quantity = int(pos.get("totalQuantity", quantity) or 0)
        price_dec = pow(10, pos.get("priceDec", 2))
        cost_dec = pow(10, pos.get("costPriceDec", 3))
        current_price = float(pos.get("price", 0)) / price_dec
        cost = float(pos.get("costPrice", 0)) / cost_dec

        if not stock or total_quantity <= 0 or cost <= 0 or current_price <= 0:
            continue

        pnl_pct = (current_price - cost) / cost
        pnl_amt = (current_price - cost) * total_quantity

        # 获取ATR和MA20(用于动态止损和趋势判断)
        atr, ma20 = _get_atr_and_ma20(stock)
        if atr and ma20:
            logger.info(f"  ATR={atr:.3f}({atr/cost*100:.1f}%), MA20={ma20:.2f}")
        elif atr:
            logger.info(f"  ATR={atr:.3f}({atr/cost*100:.1f}%), MA20=N/A")
        elif ma20:
            logger.info(f"  ATR=N/A, MA20={ma20:.2f}")

        logger.info(f"{stock}({name}): 现价={current_price}, 成本={cost}, 涨跌={pnl_pct*100:.1f}%, 盈亏={pnl_amt:.2f}元")

        # T+1 检查:availQuantity=0 说明今日买入部分不可卖
        if quantity <= 0:
            if total_quantity > 0:
                logger.info(f"  → {stock} 持仓{total_quantity}股但可卖0股(T+1不可卖),跳过监控")
                sell_skipped.append({
                    "stock": stock,
                    "name": name,
                    "reason": "T+1不可卖/无可用持仓",
                    "current_pnl": f"{pnl_pct*100:+.1f}%",
                    "current_price": current_price,
                })
            else:
                logger.info(f"  → {stock} 无可用持仓,跳过")
            continue

        # ── 从trades.json读取分批买入记录（按批次独立判断） ──
        bp = 0.0
        stock_record = _select_trade_record_for_stock(trades, stock)
        if not stock_record:
            stock_record = {
                "stock": stock,
                "name": name,
                "buy_price": cost,
                "quantity": total_quantity,
                "remaining_quantity": quantity,
                "buy_records": [{
                    "date": date.today().isoformat(),
                    "price": cost,
                    "quantity": total_quantity,
                    "remaining": quantity,
                    "source": "position_fallback",
                }],
                "sells": [],
            }
        if stock_record:
            bp = _compute_position_bp(stock_record, cost)
            if _reconcile_trade_record_to_position(stock_record, total_quantity, bp or cost):
                logger.warning(stock_record.get("sync_warning", f"{stock} 持仓记录已同步"))
                trades_dirty = True

        lot_states = _build_lot_states(stock_record, bp or cost, quantity)
        rem_cost = 0.0
        rem_qty = 0
        for lot in lot_states:
            lot_rem = max(0, int(lot.get("remaining", 0) or 0))
            lot_bp = float(lot.get("buy_price", 0) or 0)
            if lot_rem > 0 and lot_bp > 0:
                rem_qty += lot_rem
                rem_cost += lot_bp * lot_rem
        bp = rem_cost / rem_qty if rem_qty > 0 else (bp if bp > 0 else cost)

        peak_price = max(current_price, _get_intraday_peak_price(stock, current_price))
        record_peak = _get_post_buy_peak_price(stock, stock_record, current_price)
        peak_price = max(peak_price, record_peak)
        for lot in lot_states:
            lot_date = str(lot.get("date", "") or "")
            lot_record = {"buy_records": [lot]} if lot_date else stock_record
            lot_peak = _get_post_buy_peak_price(stock, lot_record, current_price)
            if lot_date:
                lot_peak = max(lot_peak, float(_get_peak_price_since_buy(stock, lot_date) or 0))
            lot["peak_price"] = max(current_price, float(lot_peak or current_price))
            peak_price = max(peak_price, float(lot.get("peak_price", current_price) or current_price))
        logger.info(f"  持仓期最高价(买入至今): {peak_price:.2f}")

        unrealized_pnl = (current_price - bp) * quantity
        unrealized_pnl_pct = unrealized_pnl / (bp * quantity) if bp > 0 and quantity > 0 else pnl_pct
        # 已实现盈亏仅用于日志展示,不影响止盈止损判断
        realized_pnl = 0.0

        logger.info(f"  → 原始买入价={bp:.2f}, 当前持仓收益率={unrealized_pnl_pct*100:.1f}%", extra={"unrealized_pnl_pct": unrealized_pnl_pct})

        action = None
        reason = None
        trigger_kind = None
        sell_quantity = 0
        # ── 卖出条件检查（分批次独立判断后汇总） ──
        if ma20 and current_price < ma20:
            action = "SELL"
            reason = f"MA20止损(现价{current_price} < MA20 {ma20:.2f})"
            trigger_kind = "ma20"
            sell_quantity = _normalize_full_exit_sell_quantity(stock, quantity)
        elif unrealized_pnl_pct <= STOP_LOSS_PCT:
            action = "SELL"
            reason = f"固定止损({unrealized_pnl_pct*100:.1f}% <= {STOP_LOSS_PCT*100:.1f}%)"
            trigger_kind = "stop_loss"
            sell_quantity = _normalize_full_exit_sell_quantity(stock, quantity)

        atr_raw = 0
        tp1_raw = 0
        tp2_raw = 0
        tp3_raw = 0
        atr_notes = []
        for lot in lot_states:
            lot_rem = max(0, int(lot.get("remaining", 0) or 0))
            lot_bp = float(lot.get("buy_price", 0) or 0)
            if lot_rem <= 0 or lot_bp <= 0:
                continue
            lot_pnl = (current_price - lot_bp) / lot_bp
            lot_peak = max(float(lot.get("peak_price", 0) or 0), current_price)
            lot_peak_pnl = (lot_peak - lot_bp) / lot_bp if lot_peak > lot_bp else 0.0
            lot_tiers = set(lot.get("executed_tp_tiers") or [])
            lot_date = lot.get("date") or "-"
            atr_hit = False
            for threshold, stop_line in sorted(ATR_TIERS, key=lambda item: item[0], reverse=True):
                if lot_peak_pnl < threshold:
                    continue
                if stop_line is None:
                    if atr and atr > 0:
                        stop_price = max(lot_bp - 2 * atr, lot_bp * 0.97)
                        effective_stop = (stop_price - lot_bp) / lot_bp
                    else:
                        effective_stop = -0.03
                else:
                    effective_stop = stop_line
                if lot_pnl <= effective_stop:
                    atr_raw += lot_rem
                    atr_notes.append(f"{lot_pnl*100:.1f}% <= {effective_stop*100:.1f}%")
                    atr_hit = True
                break
            if atr_hit:
                continue
            lot_orig = max(int(lot.get("original_quantity", lot_rem) or lot_rem), lot_rem)
            if 1 not in lot_tiers and lot_pnl >= TAKE_PROFIT_1:
                tp1_raw += int(lot_orig * 0.3)
            if 2 not in lot_tiers and lot_pnl >= TAKE_PROFIT_2:
                tp2_raw += int(lot_orig * 0.2)
            if 3 not in lot_tiers and lot_pnl >= TAKE_PROFIT_3:
                tp3_raw += lot_rem

        if not action and atr_raw > 0:
            qty = _normalize_target_sell_quantity(stock, quantity, atr_raw)
            if qty > 0:
                action = "SELL"
                trigger_kind = "atr"
                sell_quantity = qty
                reason = "移动止损(分批触发)"
                if atr_notes:
                    reason += f"({atr_notes[0]})"
                    if len(atr_notes) > 1:
                        reason += " " + "；".join(atr_notes[1:3])

        if not action and tp3_raw > 0:
            qty = _normalize_target_sell_quantity(stock, quantity, tp3_raw)
            if qty > 0:
                action = "SELL"
                trigger_kind = "tp3"
                sell_quantity = qty
                reason = f"止盈第3档(分批触发)卖对应批次剩余"

        if not action and tp2_raw > 0:
            qty = _normalize_partial_take_profit_sell_quantity(stock, quantity, tp2_raw)
            if qty > 0:
                action = "SELL"
                trigger_kind = "tp2"
                sell_quantity = qty
                reason = f"止盈第2档(分批触发)卖20%仓位"

        if not action and tp1_raw > 0:
            qty = _normalize_partial_take_profit_sell_quantity(stock, quantity, tp1_raw)
            if qty > 0:
                action = "SELL"
                trigger_kind = "tp1"
                sell_quantity = qty
                reason = f"止盈第1档(分批触发)卖30%仓位"

        logger.info(f"  当前持仓盈亏={unrealized_pnl_pct*100:.1f}%(已实现+{realized_pnl:.0f}元),触发判断: {reason or '持有'}...")

        if action:
            logger.info(f"  → 当前持仓盈亏={unrealized_pnl_pct*100:.1f}%，触发卖出: {reason}")
            # 直接用持仓中的实时价格(来自mx-moni),不再调外部API
            confirmed_price = current_price
            # 使用当前持仓未实现盈亏(不受历史已实现亏损影响)
            confirmed_pnl = unrealized_pnl_pct
            ma20_ok = (ma20 and confirmed_price < ma20) if trigger_kind == "ma20" else False
            stop_ok = trigger_kind in {"atr", "stop_loss"}
            is_take_profit = trigger_kind in {"tp1", "tp2", "tp3"}
            if ma20_ok or stop_ok or is_take_profit:
                logger.info(f"  → 当前持仓盈亏={unrealized_pnl_pct*100:.1f}%，触发卖出: {reason}")

                if _is_valid_sell_quantity(stock, quantity, sell_quantity):
                    if sell_orders_snapshot is None:
                        sell_orders_snapshot = get_today_orders(force=True)
                    if sell_orders_snapshot.get("_ok") is False:
                        logger.warning(f"  → 今日委托查询失败，跳过{stock}卖出以避免重复下单")
                        sell_skipped.append({
                            "stock": stock,
                            "name": name,
                            "reason": f"{reason}，今日委托查询失败，避免重复卖出",
                            "current_pnl": f"{pnl_pct*100:+.1f}%",
                            "current_price": current_price,
                        })
                        continue
                    pending_sells = query_pending_sell_orders(stock, today_orders=sell_orders_snapshot)
                    if pending_sells:
                        logger.info(f"  → {stock} 已有未成交卖出委托，跳过重复卖出")
                        sell_skipped.append({
                            "stock": stock,
                            "name": name,
                            "reason": f"已有未成交卖出委托，跳过重复卖出",
                            "current_pnl": f"{pnl_pct*100:+.1f}%",
                            "current_price": current_price,
                        })
                        continue
                    result = sell_stock(stock, name, confirmed_price, sell_quantity, reason + " [确认执行]")
                    sell_results.append({
                        **pos,
                        "current_price": confirmed_price,
                        "pnl_pct": confirmed_pnl,
                        "sell_quantity": sell_quantity,
                        "sell_result": result,  # 捕获实际成交结果
                        "order_id": result.get("order_id"),
                        "sell_reason": reason + " [确认执行]",
                    })
                    order_id = result.get("order_id")
                    if order_id not in (None, ""):
                        sell_reason_map[f"order:{order_id}"] = reason + " [确认执行]"
                    sell_reason_map[stock] = reason + " [确认执行]"
                else:
                    logger.info(f"  → 卖出数量不满足交易单位({sell_quantity}股，可卖{quantity}股),跳过")
                    sell_skipped.append({
                        "stock": stock,
                        "name": name,
                        "reason": f"{reason}，卖出数量{sell_quantity}股不满足交易单位",
                        "current_pnl": f"{pnl_pct*100:+.1f}%",
                        "current_price": current_price,
                    })
            else:
                logger.info("  → 条件消失,跳过执行")
                sell_skipped.append({
                    "stock": stock,
                    "name": name,
                    "reason": reason,
                    "current_pnl": f"{pnl_pct*100:+.1f}%",
                    "current_price": current_price,
                })

        time.sleep(3)  # 串行执行,避免请求过快触发112限速

    # 从API获取真实卖出成交数据。只有确认成交的卖出才允许写入 trades.json。
    confirmed_sell_results = []
    unconfirmed_sell_results = []
    if sell_results:
        today_api = get_today_orders(force=True)
        sell_exec_map = _build_buy_order_map(today_api.get("sells", []))
        sell_order_id_map = _build_order_id_map(today_api.get("sells", []))
        duplicate_sells = [stock for stock, order in sell_exec_map.items() if order.get("duplicate_order_count", 1) > 1]
        if duplicate_sells:
            logger.warning(f"今日成交回查发现同股多笔卖出委托，将优先按委托号逐笔确认: {duplicate_sells}")
        for sell in sell_results:
            stock = sell.get("stockCode", "") or sell.get("stock", "")
            res = sell.get("sell_result", {})
            status = res.get("status")
            order_id = sell.get("order_id") or _extract_order_id(res)
            api_exec = sell_order_id_map.get(str(order_id)) if order_id not in (None, "") else None
            if not api_exec:
                api_exec = sell_exec_map.get(stock)
            if status not in ("submitted", "success"):
                unconfirmed_sell_results.append({**sell, "unconfirmed_reason": f"委托失败/未提交: {res.get('error', status)}"})
                continue
            if not api_exec or int(api_exec.get("quantity", 0) or 0) <= 0:
                unconfirmed_sell_results.append({**sell, "unconfirmed_reason": "未在今日成交回报中确认"})
                continue
            actual_price = api_exec["trade_price"]
            actual_qty = int(api_exec["quantity"])
            logger.info(f"  真实成交: {stock} @ {actual_price} x {actual_qty}")
            confirmed_sell_results.append({
                **sell,
                "current_price": actual_price,
                "sell_quantity": actual_qty,
                "order_id": order_id,
            })

    # 汇总推送
    lines = [
        f"📋 持仓监控 {date.today()} | 检查{len(positions)}只",
        f"  触发卖出: {len(sell_results)}只 | 已确认成交: {len(confirmed_sell_results)}只 | 未成交/失败: {len(unconfirmed_sell_results)}只 | 跳过: {len(sell_skipped)}只",
    ]
    if confirmed_sell_results:
        lines.append("  已成交:")
        for r in confirmed_sell_results:
            pnl = r.get("pnl_pct", 0) * 100
            lines.append(f"  ✅ 卖出 {r.get('stockCode')} {r.get('stockName','')} {r.get('sell_quantity', 0)}股 {pnl:+.1f}%")
    if unconfirmed_sell_results:
        lines.append("  未写入交易记录:")
        for r in unconfirmed_sell_results:
            pnl = r.get("pnl_pct", 0) * 100
            lines.append(f"  ❌ {r.get('stockCode')} {r.get('stockName','')} {pnl:+.1f}% - {r.get('unconfirmed_reason')}")
    if sell_skipped:
        lines.append("  跳过:")
        for s in sell_skipped:
            lines.append(f"  ⚠️ {s['stock']} {s['name']} {s['reason']} ({s['current_pnl']})")
    if not sell_results and not sell_skipped:
        lines.append("  ✅ 无触发,继续持有")
    feishu_push("\n".join(lines))

    logger.info(
        f"✅ 监控完成: 检查{len(positions)}只, 触发{len(sell_results)}只, "
        f"成交{len(confirmed_sell_results)}只, 未确认{len(unconfirmed_sell_results)}只, 跳过{len(sell_skipped)}只"
    )

    # 更新 trades.json 中的卖出记录；未确认成交的委托绝不写入卖出记录。
    if confirmed_sell_results or trades_dirty:
        # 预先建立 stock → record 映射，避免循环内找不到 record
        stock_record_map = {}
        for rec in trades.get("records", []):
            s = rec.get("stock", "")
            if s:
                stock_record_map[s] = _select_trade_record_for_stock(trades, s)

        for stock, peak in peak_updates.items():
            rec = stock_record_map.get(stock)
            if rec:
                rec.update(peak)

        for sell in confirmed_sell_results:
            stock = sell.get("stockCode", "") or sell.get("stock", "")
            sell_qty = sell.get("sell_quantity", 0)
            sell_price = sell.get("current_price", 0)
            remaining_to_sell = sell_qty
            order_id = sell.get("order_id")
            sell_reason = (
                sell_reason_map.get(f"order:{order_id}") if order_id not in (None, "") else None
            ) or sell.get("sell_reason") or sell_reason_map.get(stock, "持仓监控卖出")
            sell_tier = _take_profit_tier_from_reason(sell_reason)
            record = stock_record_map.get(stock)
            if not record:
                logger.error(f"trades.json 中找不到 {stock} 的记录，已成交但无法自动写入，请人工同步")
                continue
            tracked_before = _tracked_remaining_quantity(record)
            if tracked_before < sell_qty:
                logger.warning(f"{stock} 本地记录剩余{tracked_before}股 < 实际成交{sell_qty}股，先同步持仓记录再扣减")
                _reconcile_trade_record_to_position(record, sell_qty, record.get("buy_price") or sell_price)
            buy_records = record.get("buy_records", [])
            if buy_records:
                # 用FIFO从buy_records扣减
                for br in buy_records:
                    if remaining_to_sell <= 0:
                        break
                    br_avail = br.get("remaining", 0)
                    if br_avail <= 0:
                        continue
                    actual_qty = min(br_avail, remaining_to_sell)
                    if actual_qty <= 0:
                        continue
                    bp_used = br["price"]
                    pnl_pct = (sell_price - bp_used) / bp_used if bp_used else 0
                    if sell_tier:
                        tiers = set(br.get("executed_tp_tiers") or [])
                        tiers.add(sell_tier)
                        br["executed_tp_tiers"] = sorted(tiers)
                    record["sells"].append({
                        "date": date.today().isoformat(),
                        "price": sell_price,
                        "quantity": actual_qty,
                        "pnl_pct": round(pnl_pct * 100, 2),
                        "reason": sell_reason,
                        "buy_price_used": bp_used,
                    })
                    br["remaining"] = br_avail - actual_qty
                    record["remaining_quantity"] = max(0, int(record.get("remaining_quantity", 0) or 0) - actual_qty)
                    remaining_to_sell -= actual_qty
                    logger.info(f"卖出记录已更新: {stock} FIFO卖出{actual_qty}股@{sell_price}（参考买入{bp_used}），剩余{record['remaining_quantity']}股")
                if remaining_to_sell > 0:
                    logger.error(f"{stock} 成交{sell_qty}股仍有{remaining_to_sell}股未能匹配买入批次，请人工同步")
            else:
                # 兼容旧记录
                buy_price = record.get("buy_price", 0)
                rec_remaining = record.get("remaining_quantity", 0)
                actual_qty = min(rec_remaining, remaining_to_sell)
                pnl_pct = (sell_price - buy_price) / buy_price if buy_price else 0
                record["sells"].append({
                    "date": date.today().isoformat(),
                    "price": sell_price,
                    "quantity": actual_qty,
                    "pnl_pct": round(pnl_pct * 100, 2),
                    "reason": sell_reason,
                })
                record["remaining_quantity"] = max(0, int(record.get("remaining_quantity", 0) or 0) - actual_qty)
                remaining_to_sell -= actual_qty
                logger.info(f"卖出记录已更新: {stock} 记录剩余{record['remaining_quantity']}股(本次卖出{actual_qty})")
        try:
            _save_trades(trades)
            logger.info(f"✅ trades.json 已更新: {len(confirmed_sell_results)} 只卖出, peak更新={len(peak_updates)}")
            if INTRADAY_MONITOR_RECONCILE_ENABLED:
                report = reconcile_trades_file_with_account(source="intraday_monitor")
                logger.info(
                    f"卖出后持仓同步: fixed={len(report.get('fixed', []))}, "
                    f"consistent={report.get('is_consistent')}"
                )
        except Exception as e:
            logger.error(f"❌ trades.json 写入失败: {e}")
            logger.error(f"⚠️ 请立即手动同步! 待写入数据: {confirmed_sell_results}")


# ── Mode: status ─────────────────────────────────────────

def run_status_mode():
    setup_logging()
    """查看当前持仓状态"""
    logger.info("=" * 50)
    logger.info("Phase 4: 持仓状态查询")
    logger.info("=" * 50)

    positions = get_current_positions()
    if not positions:
        logger.info("无持仓")
        print("当前无持仓")
        return

    lines = ["📊 当前持仓状态\n"]
    total_value = 0
    total_cost = 0

    for pos in positions:
        stock = pos.get("stockCode", "?")
        name = pos.get("stockName", "?")
        qty = int(pos.get("totalQuantity", 0))
        price_dec = pow(10, pos.get("priceDec", 2))
        cost_dec = pow(10, pos.get("costPriceDec", 3))
        cur_price = float(pos.get("price", 0)) / price_dec
        cost = float(pos.get("costPrice", 0)) / cost_dec
        cost_total = cost * qty
        total_cost += cost_total

        if cur_price > 0 and cost > 0:
            cur_total = cur_price * qty
            pnl = (cur_price - cost) / cost * 100
            total_value += cur_total
            lines.append(
                f"  {stock} {name}\n"
                f"    数量: {qty}股 | 成本: {cost:.2f} | 现价: {cur_price:.2f}\n"
                f"    盈亏: {pnl:+.1f}% ({(cur_total-cost_total):+.2f}元)\n"
            )
        else:
            lines.append(f"  {stock} {name}: 无法获取实时价格\n")
        time.sleep(0.5)

    if total_value > 0:
        total_pnl = (total_value - total_cost) / total_cost * 100
        lines.append(f"\n总市值: {total_value:.2f}元 | 总成本: {total_cost:.2f}元 | 整体盈亏: {total_pnl:+.1f}%\n")

    status_text = "\n".join(lines)
    print(status_text)
    feishu_push(status_text)


# ── 入口 ───────────────────────────────────────────────

# ── Mode: check (盘中实时查询) ────────────────────────────

def run_check_mode():
    setup_logging()
    """
    盘中实时查询模式
    你可以随时问我:「帮我看看持仓」「要不要卖某股」
    输出:持仓现价 + 与成本价对比 + 离止盈止损的距离 + 建议
    """
    logger.info("=" * 50)
    logger.info("盘中实时查询模式")
    logger.info("=" * 50)

    positions = get_current_positions()
    if not positions:
        msg = "📊 当前无持仓"
        print(msg)
        feishu_push(msg)
        return

    lines = ["📊 持仓实时查询\n"]
    for pos in positions:
        stock = pos.get("stockCode", "?")
        name = pos.get("stockName", "?")
        qty = int(pos.get("totalQuantity", 0))
        price_dec = pow(10, pos.get("priceDec", 2))
        cost_dec = pow(10, pos.get("costPriceDec", 3))
        current_price = float(pos.get("price", 0)) / price_dec
        cost = float(pos.get("costPrice", 0)) / cost_dec

        if not stock or qty <= 0 or cost <= 0 or current_price <= 0:
            continue

        # status模式用百分比显示(注意:monitor模式pnl_pct用小数)
        pnl_pct = (current_price - cost) / cost * 100  # 百分比,如 5.0 表示5%
        pnl_amt = (current_price - cost) * qty
        dist_sl = (STOP_LOSS_PCT * 100 - pnl_pct)  # 距止损的距离
        dist_tp1 = (TAKE_PROFIT_1 * 100 - pnl_pct)  # 距一档止盈
        dist_tp2 = (TAKE_PROFIT_2 * 100 - pnl_pct)  # 距二档止盈
        dist_tp3 = (TAKE_PROFIT_3 * 100 - pnl_pct)  # 距三档止盈

        emoji = "🟢" if pnl_pct > 0 else "🔴"
        lines.append(
            f"{emoji} {stock} {name}\n"
            f"   现价:{current_price:.2f} 成本:{cost:.2f} 数量:{qty}\n"
            f"   盈亏:{pnl_pct:+.1f}% ({pnl_amt:+,.0f}元)\n"
            f"   → 距止损:{dist_sl:+.1f}% | 距止盈1:{dist_tp1:+.1f}% 止盈2:{dist_tp2:+.1f}% 止盈3:{dist_tp3:+.1f}%\n"
        )

        # 给出建议
        if pnl_pct <= STOP_LOSS_PCT * 100:
            lines.append("   ⚠️ 建议:触发止损,建议卖出控制风险\n")
        elif pnl_pct >= TAKE_PROFIT_3 * 100:
            lines.append("   ✅ 建议:已达止盈3档,建议分批止盈\n")
        elif pnl_pct >= TAKE_PROFIT_2 * 100:
            lines.append("   ✅ 建议:已达止盈2档,可以考虑卖出一半\n")
        elif pnl_pct >= TAKE_PROFIT_1 * 100:
            lines.append("   ➡️ 建议:已过一档止盈,可继续持有等二档\n")
        elif dist_sl <= 1.0:
            lines.append("   ⚠️ 建议:接近止损线,密切关注\n")
        else:
            lines.append("   ➡️ 建议:继续持有,等待机会\n")
        lines.append("\n")
        time.sleep(0.5)

    status_text = "\n".join(lines).strip()
    print(status_text)
    feishu_push(status_text)

if __name__ == "__main__":
    setup_logging()
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", choices=["buy", "buy-timing", "buy-legacy", "monitor", "check", "status"], default="buy-timing")
    args = parser.parse_args()

    if args.mode in {"buy", "buy-timing"}:
        if args.mode == "buy":
            logger.warning("--mode=buy 已映射到新的技术分时买入；旧一次性买入请用 --mode=buy-legacy")
        try:
            sys.exit(run_buy_timing_mode() or 0)
        except KeyboardInterrupt:
            logger.info("盘中买入收到停止信号，已准备退出")
            sys.exit(130)
    elif args.mode == "buy-legacy":
        run_buy_mode()
    elif args.mode == "monitor":
        run_monitor_mode()
    elif args.mode == "check":
        run_check_mode()
    elif args.mode == "status":
        run_status_mode()
