#!/usr/bin/env python3
"""
执行辩论结果 — 下个交易日 09:31
=================================
1. 读取 position_debate_result.json（含辩论决策 + 执行状态）
2. 判断今天是否为交易日
3. 用持仓 API 的现价计算买卖
4. 执行 REDUCE/CLEAR/ADD（HOLD 跳过，已成功跳过）
5. 每只股票的执行状态写回 position_debate_result.json（同一文件）
6. 交易记录追加到 trades.json（与盘中买入共用同一文件）

trades.json 结构（与盘中买入共用）：
{
  "records": [
    {"stock": "000703", "name": "恒逸石化", "buy_date": "...", "buy_price": 15.9,
     "quantity": 3000, "remaining_quantity": 3000, "action": "ADD", ...,
     "sells": [{"date": "...", "price": 16.1, "quantity": 3000, "pnl_pct": 1.26}]}
  ]
}

position_debate_result.json 每只股票的 execution 字段：
{"status": "pending/success/failed/partial", "action": "...", "price": ...,
 "quantity": ..., "reason": "...", "executed_at": "...", "last_attempt": "..."}

用法：
  python3 execute_debate_result.py [--dry-run]
"""

import os
import sys
import json
import time
import logging
import requests
import subprocess
from datetime import datetime, date
from pathlib import Path

# ── 路径配置 ──────────────────────────────────────────────
BASE_DIR = Path(__file__).parent
OUTPUT_DIR = BASE_DIR / "output"
LOG_DIR = BASE_DIR / "logs"
DEBATE_RESULT_FILE = BASE_DIR / "position_debate_result.json"
LOG_DIR.mkdir(exist_ok=True)
sys.path.insert(0, str(BASE_DIR))

from trade_position_sync import load_local_env, reconcile_trades_file_with_account

load_local_env()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[
        logging.FileHandler(LOG_DIR / f"execute_debate_{date.today().strftime('%Y%m%d')}.log"),
        logging.StreamHandler(),
    ],
)
logger = logging.getLogger("execute_debate")

# ── mx-moni API ──────────────────────────────────────────
API_URL = os.getenv("MX_API_URL", "https://mkapi2.dfcfs.com/finskillshub")
API_KEY = os.getenv("MX_APIKEY")
WEBHOOK_URL = os.getenv("FEISHU_WEBHOOK_URL")
PORTFOLIO_VALUE = float(os.getenv("PORTFOLIO_VALUE", "1000000"))

BUY_SLIPPAGE = 1.015    # 买入价 = 现价 x 1.015（不超过涨停价）
SELL_SLIPPAGE = 0.985   # 卖出价 = 现价 x 0.985（不低于跌停价）

# ── 行情获取（腾讯主方案，mx-data备用） ──────────────────
def _get_quote(code: str) -> dict:
    """
    获取股票实时报价（含最新价、涨跌幅、涨跌停价）
    主：腾讯行情API（不限次数）
    备：mx-data
    返回 {"price": float, "limit_up": float, "limit_down": float} 或 None
    """
    import subprocess
    import re
    import urllib.request

    def _tencent(code: str):
        try:
            prefix = "sh" if code.startswith(('6', '5', '9')) else "sz"
            url = f"https://qt.gtimg.cn/q={prefix}{code}"
            req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0", "Referer": "https://gu.qq.com"})
            with urllib.request.urlopen(req, timeout=10) as r:
                data = r.read().decode("gbk")
            if "none" in data.lower() or not data:
                return None
            parts = data.split("~")
            if len(parts) < 10:
                return None
            price = float(parts[3])
            y_close = float(parts[4])
            is_st = "ST" in parts[1] or "*ST" in parts[1] or "S*" in parts[1]
            limit_pct = 0.05 if is_st else 0.10
            limit_up = round(y_close * (1 + limit_pct), 2)
            limit_down = round(y_close * (1 - limit_pct), 2)
            return {"price": price, "limit_up": limit_up, "limit_down": limit_down}
        except Exception:
            return None

    def _mx(code: str):
        try:
            cmd = [sys.executable, str(BASE_DIR.parent / "skills/mx-data/mx_data.py"), f"{code} 当前最新价"]
            r = subprocess.run(cmd, capture_output=True, text=True, timeout=20)
            out = r.stdout
            if "上限" in out or "frequency" in out.lower() or "调用次数" in out:
                return None
            pm = re.findall(r'(\d+\.\d+)\s*元', out)
            if not pm:
                pm = re.findall(r'\|\s*([\d.]+)\s*\|', out)
            cm = re.findall(r'涨跌幅[：:]\s*([+-]?\d+\.?\d*)%?', out)
            price = float(pm[0]) if pm else None
            change_pct = float(cm[0]) if cm else 0.0
            if price is None:
                return None
            y_close = price / (1 + change_pct / 100)
            return {"price": price, "limit_up": round(y_close * 1.10, 2), "limit_down": round(y_close * 0.90, 2)}
        except Exception:
            return None

    import time as _time
    result = _tencent(code)
    if result:
        return result
    for attempt in range(2):
        _time.sleep(2)
        result = _mx(code)
        if result:
            return result
    return None

# 不可重试的失败原因
NON_RETRYABLE_REASONS = {
    "停牌", "涨跌停", "不在持仓", "llm解析失败",
    "资金不足（不足1手）", "持仓不足",
}

# ── trades.json 读写 ──────────────────────────────────────
TRADES_FILE = OUTPUT_DIR / "trades.json"

def _load_trades():
    if TRADES_FILE.exists():
        try:
            with open(TRADES_FILE) as f:
                return json.load(f)
        except Exception:
            pass
    return {"records": []}

def _save_trades(data):
    with open(TRADES_FILE, "w") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

# ── 交易日判断（改用共享模块） ──────────────────────────
# 共享模块路径：~/.openclaw/agents/shared/trading_calendar.py
import os as _os
_SHARED_PATH = _os.path.expanduser("~/.openclaw/agents/shared")
if _SHARED_PATH not in sys.path:
    sys.path.insert(0, _SHARED_PATH)
from trading_calendar import is_a_share_trading_day  # noqa: E402

def is_trading_day(today=None) -> bool:
    """判断是否为A股交易日（委托给共享模块 trading_calendar）"""
    if today is None:
        return is_a_share_trading_day()
    if isinstance(today, date):
        return is_a_share_trading_day(today.isoformat())
    return is_a_share_trading_day(str(today))

# ── mx-moni API ──────────────────────────────────────────
def _mx_api_post(endpoint: str, payload: dict) -> dict:
    headers = {"Content-Type": "application/json", "apikey": API_KEY}
    try:
        r = requests.post(f"{API_URL}{endpoint}", json=payload, headers=headers, timeout=30)
        if r.status_code == 200:
            data = r.json() or {}
            code = str(data.get("code") or data.get("status") or "")
            if data.get("success") is False or (code and code != "200"):
                raise RuntimeError(f"API业务失败 {endpoint}: {data.get('message') or data}")
            return data
    except Exception as e:
        logger.warning(f"API 调用失败 {endpoint}: {e}")
        raise
    return {}

def get_current_positions() -> list:
    data = _mx_api_post("/api/claw/mockTrading/positions", {})
    pos_data = data.get("data") or {}
    return [p for p in (pos_data.get("posList", []) or []) if int(p.get("count", 0) or 0) > 0]

def get_available_cash() -> float:
    data = _mx_api_post("/api/claw/mockTrading/positions", {})
    pos_data = data.get("data") or {}
    if not pos_data:
        return 0.0
    unit = pos_data.get("currencyUnit", 1) or 1
    avail = pos_data.get("availBalance", 0)
    if avail is None:
        return 0.0
    return float(avail) / unit

# ── mx-moni 买卖 ─────────────────────────────────────────
def buy_stock(code: str, price: float, quantity: int) -> dict:
    payload = {
        "type": "buy",
        "stockCode": code,
        "price": round(price, 2),
        "quantity": quantity,
        "useMarketPrice": False,
    }
    result = _mx_api_post("/api/claw/mockTrading/trade", payload)
    if result and result.get("code") == "200":
        logger.info(f"买入 {code} {quantity}股@{price:.2f}: {str(result)[:200]}")
        return {"status": "success", "result": result}
    return {"status": "error", "message": str(result.get("message", "API失败")) if result else "API调用失败"}

def sell_stock(code: str, price: float, quantity: int, retry: int = 3) -> dict:
    payload = {
        "type": "sell",
        "stockCode": code,
        "price": round(price, 2),
        "quantity": quantity,
        "useMarketPrice": False,
    }
    result = _mx_api_post("/api/claw/mockTrading/trade", payload)
    if result and result.get("code") == "200":
        logger.info(f"卖出 {code} {quantity}股@{price:.2f}: {str(result)[:200]}")
        return {"status": "success", "result": result}
    # 限速时等 5 秒重试
    msg = (result or {}).get("message", "") or ""
    if "请求频率" in msg or "112" in msg:
        for attempt in range(1, retry + 1):
            logger.warning(f"API 限速(attempt {attempt}/{retry}), 等待 5s 后重试...")
            time.sleep(5)
            result = _mx_api_post("/api/claw/mockTrading/trade", payload)
            if result and result.get("code") == "200":
                logger.info(f"[重试{attempt}] 卖出 {code} {quantity}股@{price:.2f} 成功")
                return {"status": "success", "result": result}
    return {"status": "error", "message": str(result.get("message", "API失败")) if result else "API调用失败"}

# ── 成交记录同步 ────────────────────────────────────────
def _sync_orders_from_api(today: date) -> dict:
    import datetime
    orders_data = _mx_api_post("/api/claw/mockTrading/orders", {})
    data = orders_data.get("data") if orders_data else None
    if not data:
        return {}
    all_orders = (
        (data.get("orderList") or [])
        or (data.get("orders") or [])
    )
    today_ts_start = int(datetime.datetime.combine(today, datetime.time(0, 0)).timestamp())
    today_ts_end = int(datetime.datetime.combine(today, datetime.time(23, 59, 59)).timestamp())
    result = {}
    for o in all_orders:
        t = o.get("time", 0)
        if not (today_ts_start <= t <= today_ts_end):
            continue
        code = o.get("secCode", "")
        price_dec = pow(10, o.get("priceDec", 2))
        order_count = o.get("count", 0)
        trade_count = o.get("tradeCount", 0)
        remaining = max(0, order_count - trade_count)
        result[code] = {
            "trade_price": o.get("tradePrice", 0) / price_dec,
            "trade_count": trade_count,
            "order_count": order_count,
            "remaining": remaining,
            "sec_name": o.get("secName", ""),
            "type": o.get("type"),
            "status": o.get("status"),
            "drt": o.get("drt"),  # drt=1买入 drt=2卖出
            "order_time": t,
        }
    logger.info(f"orders API 返回今日委托: {len(result)} 条")
    return result

def _merge_execution_record(today: date, execution_records: list, orders_map: dict):
    import datetime
    for rec in execution_records:
        code = rec.get("code", "")
        action = rec.get("planned_action", "")
        # drt: 1=买入, 2=卖出（type字段不等于买卖方向）
        expected_drt = 1 if action == "ADD" else 2 if action in ("REDUCE", "CLEAR") else None
        okey = next(
            (k for k, v in orders_map.items()
             if k == code and v.get("drt") == expected_drt),
            None
        )
        if okey:
            o = orders_map[okey]
            rec["actual_price"] = o["trade_price"]
            rec["actual_quantity"] = o["trade_count"]
            rec["executed_at"] = datetime.datetime.fromtimestamp(
                o["order_time"]
            ).strftime("%Y-%m-%d %H:%M:%S")
            if o["remaining"] == 0 and o["trade_count"] > 0:
                rec["execution_status"] = "SUCCESS"
                rec["note"] = f"全成: {o['trade_count']}股@{o['trade_price']:.2f}"
            elif o["trade_count"] > 0:
                rec["execution_status"] = "PARTIAL"
                rec["remaining"] = o["remaining"]
                rec["note"] = (rec.get("note", "") or "") + f" | 部分成交: {o['trade_count']}股，剩{o['remaining']}股"
            else:
                # 订单提交了但零成交：废单（可能是价格超涨跌停被拒）
                rec["execution_status"] = "FAILED"
                rec["note"] = (rec.get("note", "") or "") + " | 废单：报价超出涨跌停"
        else:
            # 订单未出现在 orders_map：被拒或未提交
            rec["execution_status"] = "FAILED"
            rec["note"] = (rec.get("note", "") or "") + " | 废单：委托被拒（不在成交记录）"
    return execution_records

def _fix_add_remaining_quantity(execution_records: list, orders_map: dict):
    """
    ADD 执行后，用 orders API 的实际成交数量修正 trades.json 的 remaining_quantity。
    计划买入量 ≠ 实际成交量的场景（部分成交/废单）必须修正。
    """
    trades = _load_trades()
    for rec in execution_records:
        code = rec.get("code", "")
        action = rec.get("planned_action", "")
        if action != "ADD":
            continue
        o = orders_map.get(code)
        if not o:
            # 没成交，删除刚才写入的 ADD 记录（或标记为废单）
            for r in trades["records"]:
                if r.get("stock") == code and r.get("buy_date") == date.today().isoformat():
                    # 找到今天写入的记录，检查是否部分成交
                    if rec.get("execution_status") in ("FAILED", "SKIPPED"):
                        r["remaining_quantity"] = 0
                        r["action"] = "ADD_FAILED"
                        logger.info(f"[ADD废单修正] {code} remaining_quantity=0")
            continue
        actual_qty = o.get("trade_count", 0)
        actual_price = o.get("trade_price", 0)
        if actual_qty <= 0:
            # 零成交，修正为0
            for r in trades["records"]:
                if r.get("stock") == code and r.get("buy_date") == date.today().isoformat():
                    r["remaining_quantity"] = 0
                    r["action"] = "ADD_FAILED"
                    logger.info(f"[ADD零成交修正] {code} remaining_quantity=0")
            continue
        # 找到今天写入的 ADD 记录，用 mx-moni 当前实际持仓修正 remaining_quantity
        from execute_debate_result import get_current_positions
        positions = get_current_positions()
        pos_dict = {p.get("secCode", ""): p.get("count", 0) for p in positions}
        actual_remaining = pos_dict.get(code, 0)
        for r in trades["records"]:
            if r.get("stock") == code and r.get("buy_date") == date.today().isoformat():
                r["remaining_quantity"] = actual_remaining
                r["quantity"] = actual_qty
                r["buy_price"] = actual_price
                logger.info(f"[ADD成交修正] {code} 实际成交{actual_qty}股，mx-moni持仓={actual_remaining}")
    _save_trades(trades)



def _stock_by_code(debate_result: dict, code: str):
    stocks = debate_result.get("stocks", debate_result.get("results", []))
    return next((s for s in stocks if s.get("code") == code), None)

def _save_debate_result(debate_result: dict):
    with open(DEBATE_RESULT_FILE, "w") as f:
        json.dump(debate_result, f, ensure_ascii=False, indent=2)

def _append_trade_record(code: str, name: str, action: str, price: float,
                          quantity: int, reason: str = ""):
    trades = _load_trades()
    if action == "ADD":
        # ADD：按FIFO追加到buy_records，保持buy_price为首笔买入价
        trades["records"].append({
            "stock": code, "name": name,
            "buy_date": date.today().isoformat(),
            "buy_price": price,  # 首笔买入价（不变）
            "quantity": quantity,
            "remaining_quantity": quantity,
            "action": "ADD",
            "total_score": 0,
            "reason": reason,
            "pool": "",
            "sells": [],
            "buy_records": [{
                "price": price,
                "quantity": quantity,
                "date": date.today().isoformat(),
            }],
        })
        logger.info(f"[加仓] {code} {quantity} 股@{price}（buy_records追加）")
    elif action in ("REDUCE", "CLEAR"):
        # REDUCE/CLEAR：按FIFO顺序递减buy_records中的剩余数量
        remaining = quantity
        for r in trades["records"]:
            if r.get("stock") != code:
                continue
            buy_records = r.get("buy_records", [])
            if not buy_records:
                # 兼容旧记录：没有buy_records，用buy_price算
                buy_price = r.get("buy_price", 0)
                avail = r.get("remaining_quantity", 0)
                reduce_qty = min(avail, remaining)
                pnl_pct = ((price - buy_price) / buy_price * 100) if buy_price else 0
                sell_reason = "CLEAR持仓超限" if action == "CLEAR" else "REDUCE辩论减仓"
                r["sells"].append({
                    "date": date.today().isoformat(),
                    "price": price,
                    "quantity": reduce_qty,
                    "pnl_pct": round(pnl_pct, 2),
                    "reason": sell_reason,
                })
                r["remaining_quantity"] -= reduce_qty
                remaining -= reduce_qty
                logger.info(f"[{'清仓' if action=='CLEAR' else '减仓'}] {code} 记录 {r.get('buy_date')} 卖出 {reduce_qty} 股（无buy_records），剩余 {r['remaining_quantity']} 股")
                continue
            # 按FIFO从buy_records扣减
            for br in buy_records:
                if remaining <= 0:
                    break
                br_avail = br.get("remaining", br["quantity"])
                if br_avail <= 0:
                    continue
                reduce_qty = min(br_avail, remaining)
                br["remaining"] = br_avail - reduce_qty
                buy_price = br["price"]
                pnl_pct = ((price - buy_price) / buy_price * 100) if buy_price else 0
                sell_reason = "CLEAR持仓超限" if action == "CLEAR" else "REDUCE辩论减仓"
                r["sells"].append({
                    "date": date.today().isoformat(),
                    "price": price,
                    "quantity": reduce_qty,
                    "pnl_pct": round(pnl_pct, 2),
                    "reason": sell_reason,
                    "buy_price_used": buy_price,  # 记录这笔卖出的参考买入价
                })
                r["remaining_quantity"] -= reduce_qty
                remaining -= reduce_qty
                logger.info(f"[{'清仓' if action=='CLEAR' else '减仓'}] {code} FIFO卖出 {reduce_qty} 股@{price}（参考买入价{buy_price}），剩余 {r['remaining_quantity']} 股")
            if remaining > 0:
                logger.warning(f"[{'清仓' if action=='CLEAR' else '减仓'}] {code} buy_records耗尽但还有 {remaining} 股未处理")
    _save_trades(trades)

def main():
    logger.info("=" * 50)
    logger.info("辩论执行流程启动")
    logger.info("=" * 50)

    dry_run = "--dry-run" in sys.argv

    if not DEBATE_RESULT_FILE.exists():
        logger.error(f"辩论结果文件不存在: {DEBATE_RESULT_FILE}")
        print("错误: 辩论结果文件不存在，请先运行 weekly_debate.py")
        return

    with open(DEBATE_RESULT_FILE) as f:
        debate_result = json.load(f)

    if debate_result.get("status") == "empty":
        logger.info("辩论结果为空，跳过执行")
        return

    stocks = debate_result.get("stocks", [])
    if not stocks and debate_result.get("results"):
        # 兼容旧格式：position_debate_result.json 用 results 而非 stocks
        _results = debate_result["results"]
        for r in _results:
            stock = {
                "code": r.get("stock_code", ""),
                "name": r.get("stock_name", ""),
                "decision": {
                    "action": r.get("rating", "Hold").replace("Sell", "REDUCE").replace("Buy", "ADD").replace("Clear", "CLEAR"),
                    "target_ratio": 0.1,
                },
                "execution": r.get("execution", {}),
            }
            stocks.append(stock)
    logger.info(f"辩论股票数: {len(stocks)}")

    today = date.today()
    if not is_trading_day(today):
        logger.info(f"今日({today})非交易日，跳过")
        print("今日非交易日")
        return

    logger.info(f"今日({today})为交易日，开始执行")

    # 获取持仓和资金
    positions = get_current_positions()
    available_cash = get_available_cash()
    logger.info(f"当前持仓: {len(positions)} 只，可用资金: {available_cash:.2f}")
    try:
        sync_report = reconcile_trades_file_with_account(source="weekly_execute_start")
        if sync_report.get("fixed"):
            logger.warning(
                f"执行前已按模拟账户同步 trades.json: fixed={len(sync_report.get('fixed', []))}, "
                f"consistent={sync_report.get('is_consistent')}"
            )
    except Exception as e:
        logger.error(f"执行前持仓同步失败，停止执行: {e}")
        return

    pos_dict = {}
    for p in positions:
        code = p.get("secCode", "")
        if not code:
            continue
        price_dec = pow(10, p.get("priceDec", 2))
        cost_dec = pow(10, p.get("costPriceDec", 3))
        pos_dict[code] = {
            "quantity": p.get("count", 0),
            "cost": p.get("costPrice", 0) / cost_dec,
            "current_price": p.get("price", 0) / price_dec,
        }

    initial_pos_dict = dict(pos_dict)

    # ── 辅助函数 ────────────────────────────────────────
    def get_status(stock):
        return stock.get("execution", {}).get("status", "pending")

    def set_execution(stock, **kwargs):
        if "execution" not in stock:
            stock["execution"] = {}
        stock["execution"].update(kwargs)

    def set_success(stock, action, price, quantity):
        set_execution(stock,
            status="success",
            action=action,
            price=price,
            quantity=quantity,
            executed_at=datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        )
        _save_debate_result(debate_result)

    def set_failed(stock, action, reason):
        set_execution(stock,
            status="failed",
            action=action,
            reason=reason,
            last_attempt=today.strftime("%Y-%m-%d"),
        )
        _save_debate_result(debate_result)

    # ── 第一批：SELL ──────────────────────────────────
    sell_stocks = [s for s in stocks if s.get("decision", {}).get("action") in ("REDUCE", "CLEAR")]
    add_stocks = [s for s in stocks if s.get("decision", {}).get("action") == "ADD"]
    add_excluded = {s["code"] for s in add_stocks if get_status(s) == "success"}
    add_stocks = [s for s in add_stocks if s["code"] not in add_excluded]

    logger.info(f"=== 第一批：SELL ({len(sell_stocks)} 只) ===")
    execution_records = []

    for stock in sell_stocks:
        code = stock["code"]
        name = stock["name"]
        action = stock.get("decision", {}).get("action", "HOLD")
        current_price = pos_dict.get(code, {}).get("current_price", 0)
        prev = stock.get("execution", {})
        prev_status = prev.get("status", "pending")

        if action == "HOLD":
            set_execution(stock, status="done", action="HOLD")
            logger.info(f"[HOLD] {code} {name}")
            continue

        if prev_status == "success":
            logger.info(f"[已执行] {code} {name} 上次已成功，跳过")
            continue

        if prev_status == "failed" and prev.get("reason") in NON_RETRYABLE_REASONS:
            logger.info(f"[不重试] {code} {name} 原因不可重试，跳过")
            continue

        if not current_price or current_price <= 0:
            logger.warning(f"{code} 价格无效，跳过")
            set_failed(stock, action, "价格获取失败")
            execution_records.append({
                "code": code, "name": name,
                "planned_action": action,
                "execution_status": "SKIPPED",
                "note": f"价格获取失败: {current_price}",
            })
            continue

        current_pos = pos_dict.get(code, {})
        current_qty = current_pos.get("quantity", 0)
        remaining_from_prev = prev.get("remaining_quantity", 0)
        trade_record = {
            "code": code, "name": name,
            "planned_action": action,
            "execution_status": "PENDING",
        }

        if action == "REDUCE":
            if remaining_from_prev > 0:
                sell_qty = int(remaining_from_prev / 100) * 100
                sell_qty = max(0, sell_qty)
                trade_record["note"] = f"补卖剩余{sell_qty}股"
            else:
                target_ratio = stock.get("decision", {}).get("target_ratio", 0)
                target_market_value = current_qty * current_price * target_ratio
                target_qty = int(target_market_value / current_price / 100) * 100
                sell_qty = max(0, current_qty - target_qty)
            # 获取跌停价，卖出价不能低于跌停价
            quote = _get_quote(code)
            limit_down = quote["limit_down"] if quote else 0
            sell_price = max(round(current_price * SELL_SLIPPAGE, 2), limit_down)
            trade_record["planned_price"] = sell_price
            trade_record["planned_quantity"] = sell_qty

            if current_qty < 100:
                set_failed(stock, action, "不在持仓")
                trade_record["execution_status"] = "SKIPPED"
                trade_record["note"] = "不在持仓"
            elif sell_qty < 100:
                set_failed(stock, action, "持仓不足")
                trade_record["execution_status"] = "SKIPPED"
                trade_record["note"] = "持仓不足"
            else:
                if dry_run:
                    logger.info(f"[干跑] 卖出 {code} {sell_qty}股@{sell_price}")
                    trade_record["execution_status"] = "DONE"
                    set_success(stock, action, sell_price, sell_qty)
                else:
                    res = sell_stock(code, sell_price, sell_qty)
                    if res["status"] == "success":
                        trade_record["execution_status"] = "SUCCESS"
                        trade_record["result"] = str(res.get("result", ""))[:200]
                        set_success(stock, action, sell_price, sell_qty)
                        _append_trade_record(code, name, action, sell_price, sell_qty)
                    else:
                        trade_record["execution_status"] = "FAILED"
                        trade_record["note"] = res.get("message", "")
                        set_failed(stock, action, res.get("message", "卖出失败"))

        elif action == "CLEAR":
            if remaining_from_prev > 0:
                sell_qty = int(remaining_from_prev / 100) * 100
                sell_qty = max(0, sell_qty)
                trade_record["note"] = f"补卖剩余{sell_qty}股"
            else:
                sell_qty = current_qty
            # 获取跌停价，卖出价不能低于跌停价
            quote = _get_quote(code)
            limit_down = quote["limit_down"] if quote else 0
            sell_price = max(round(current_price * SELL_SLIPPAGE, 2), limit_down)
            trade_record["planned_price"] = sell_price
            trade_record["planned_quantity"] = sell_qty

            if current_qty < 100:
                set_failed(stock, action, "不在持仓")
                trade_record["execution_status"] = "SKIPPED"
                trade_record["note"] = "不在持仓"
            else:
                if dry_run:
                    logger.info(f"[干跑] 清仓 {code} {current_qty}股@{sell_price}")
                    trade_record["execution_status"] = "DONE"
                    set_success(stock, action, sell_price, current_qty)
                else:
                    res = sell_stock(code, sell_price, current_qty)
                    if res["status"] == "success":
                        trade_record["execution_status"] = "SUCCESS"
                        trade_record["result"] = str(res.get("result", ""))[:200]
                        set_success(stock, action, sell_price, current_qty)
                        _append_trade_record(code, name, action, sell_price, current_qty)
                    else:
                        trade_record["execution_status"] = "FAILED"
                        trade_record["note"] = res.get("message", "")
                        set_failed(stock, action, res.get("message", "清仓失败"))

        if trade_record["execution_status"] != "PENDING":
            execution_records.append(trade_record)

        # ① 每笔间隔 1.5 秒，防止 API 限速
        time.sleep(1.5)

    # ── 第二批：ADD（等卖出资金回笼） ─────────────────
    if sell_stocks:
        logger.info("=== 卖出完成，重新获取可用资金 ===")
        available_cash = get_available_cash()
        logger.info(f"卖出后可用资金: {available_cash:.2f}")
        positions = get_current_positions()
        pos_dict = {}
        for p in positions:
            code = p.get("secCode", "")
            if not code:
                continue
            price_dec = pow(10, p.get("priceDec", 2))
            cost_dec = pow(10, p.get("costPriceDec", 3))
            pos_dict[code] = {
                "quantity": p.get("count", 0),
                "cost": p.get("costPrice", 0) / cost_dec,
                "current_price": p.get("price", 0) / price_dec,
            }
    # 计算总资产（用于ADD目标比例计算）
    total_portfolio = available_cash
    for p_code, pinfo in pos_dict.items():
        total_portfolio += pinfo.get("current_price", 0) * pinfo.get("quantity", 0)
    logger.info(f"ADD 总资产: {total_portfolio:.2f}（可用{available_cash:.2f} + 持仓{total_portfolio - available_cash:.2f}）")

    for stock in add_stocks:
        code = stock["code"]
        name = stock["name"]
        prev = stock.get("execution", {})
        prev_status = prev.get("status", "pending")

        if prev_status == "success":
            logger.info(f"[已执行] {code} {name} 上次已成功，跳过")
            continue
        if prev_status == "failed" and prev.get("reason") in NON_RETRYABLE_REASONS:
            logger.info(f"[不重试] {code} {name} 原因不可重试，跳过")
            continue

        # ── 目标比例计算买入数量 ─────────────────────────
        target_ratio = stock.get("decision", {}).get("target_ratio", 0.1)
        # 按可用资金的比例计算买入金额（不是总资产）
        buy_value = available_cash * target_ratio
        logger.info(f"ADD {code}: 可用{available_cash:.0f}×{target_ratio*100:.0f}%=买入{buy_value:.0f}元")

        if buy_value <= 0:
            current_qty = pos_dict.get(code, {}).get("quantity", 0)
            logger.info(f"[已满仓] {code} {name} 已有持仓{current_qty}股，无需ADD")
            continue

        # 获取持仓现价，如果没有则用行情
        current_price_pos = pos_dict.get(code, {}).get("current_price", 0)
        buy_price = current_price_pos if current_price_pos > 0 else 0
        if not buy_price or buy_price <= 0:
            quote = _get_quote(code)
            buy_price = quote.get("price", 0) if quote else 0
        if not buy_price or buy_price <= 0:
            logger.warning(f"{code} 价格无效，跳过")
            set_failed(stock, "ADD", "价格获取失败")
            execution_records.append({
                "code": code, "name": name,
                "planned_action": "ADD",
                "execution_status": "SKIPPED",
                "note": f"价格获取失败",
            })
            continue

        quote = _get_quote(code)
        limit_up = quote["limit_up"] if quote else 0
        buy_price = min(round(buy_price * BUY_SLIPPAGE, 2), limit_up)
        quantity = int(buy_value / buy_price / 100) * 100

        trade_record = {
            "code": code, "name": name,
            "planned_action": "ADD",
            "planned_price": buy_price,
            "planned_target_ratio": target_ratio,
            "execution_status": "PENDING",
        }


        if quantity < 100:
            set_failed(stock, "ADD", "资金不足（不足1手）")
            trade_record["execution_status"] = "SKIPPED"
            trade_record["note"] = "资金不足（不足1手）"
        else:
            trade_record["planned_quantity"] = quantity
            if dry_run:
                logger.info(f"[干跑] 买入 {code} {quantity}股@{buy_price}")
                trade_record["execution_status"] = "DONE"
                set_success(stock, "ADD", buy_price, quantity)
            else:
                res = buy_stock(code, buy_price, quantity)
                if res["status"] == "success":
                    trade_record["execution_status"] = "SUCCESS"
                    trade_record["result"] = str(res.get("result", ""))[:200]
                    set_success(stock, "ADD", buy_price, quantity)
                    _append_trade_record(code, name, "ADD", buy_price, quantity)
                else:
                    trade_record["execution_status"] = "FAILED"
                    trade_record["note"] = res.get("message", "")
                    set_failed(stock, "ADD", res.get("message", "买入失败"))

        if trade_record["execution_status"] != "PENDING":
            execution_records.append(trade_record)
        time.sleep(2)

    # ── 同步 orders API ───────────────────────────────
    if not dry_run:
        logger.info("[Step 7] 等待 10 秒后从 orders API 同步真实成交（防异步延迟）...")
        time.sleep(10)
        orders_map = _sync_orders_from_api(today)
        if orders_map:
            logger.info(f"同步到 {len(orders_map)} 条真实成交")
        execution_records = _merge_execution_record(today, execution_records, orders_map)
        
        # 修正 ADD 记录的 remaining_quantity（用实际成交数量，不是计划数量）
        _fix_add_remaining_quantity(execution_records, orders_map)
        try:
            sync_report = reconcile_trades_file_with_account(source="weekly_execute_end")
            logger.info(
                f"执行后持仓同步: fixed={len(sync_report.get('fixed', []))}, "
                f"consistent={sync_report.get('is_consistent')}"
            )
        except Exception as e:
            logger.error(f"执行后持仓同步失败: {e}")
        
        for rec in execution_records:
            code = rec.get("code", "")
            st = _stock_by_code(debate_result, code)
            if not st:
                continue
            status = rec.get("execution_status", "")
            if status == "SUCCESS":
                remaining = rec.get("remaining", 0)
                st["execution"] = {
                    "status": "partial" if remaining > 0 else "success",
                    "action": rec.get("planned_action", ""),
                    "price": rec.get("actual_price", 0),
                    "quantity": rec.get("actual_quantity", 0),
                    "remaining_quantity": remaining,
                    "executed_at": rec.get("executed_at", ""),
                }
            elif status in ("FAILED", "SKIPPED"):
                st["execution"] = {
                    "status": "failed",
                    "action": rec.get("planned_action", ""),
                    "reason": rec.get("note", ""),
                    "last_attempt": today.strftime("%Y-%m-%d"),
                }
        _save_debate_result(debate_result)

    # ── 输出结果 ─────────────────────────────────────────
    print(json.dumps({
        "execution_date": today.strftime("%Y-%m-%d"),
        "dry_run": dry_run,
        "available_cash": available_cash,
        "trades": execution_records,
    }, ensure_ascii=False, indent=2))

    # ── 推送 ────────────────────────────────────────────
    if not dry_run and WEBHOOK_URL:
        # 从 execution_records（orders API 真实成交同步）统计，而非辩论结果的 pending 状态
        success = sum(1 for t in execution_records if t.get("execution_status") == "SUCCESS")
        failed = sum(1 for t in execution_records if t.get("execution_status") in ("FAILED", "PARTIAL"))
        pending = sum(1 for t in execution_records if t.get("execution_status") in ("pending", "PENDING"))
        lines = [
            f"📋 **{today} 辩论执行结果**",
            f"可用资金: {available_cash:.2f}",
            f"执行进度: ✅{success} ❌{failed} ⏳{pending}",
        ]
        for t in execution_records:
            emoji = {"SUCCESS": "✅", "FAILED": "❌", "DONE": "✅", "SKIPPED": "⏭️"}.get(t.get("execution_status"), "⏳")
            lines.append(
                f"{emoji} {t['code']} {t['name']} | "
                f"{t.get('planned_action', '?')} | "
                f"{t.get('planned_quantity', 0)}股@{t.get('planned_price', 0):.2f} | "
                f"{t.get('execution_status', '?')} | {t.get('note', '')}"
            )
        payload = {"msg_type": "text", "content": {"text": "\n".join(lines)}}
        try:
            requests.post(WEBHOOK_URL, json=payload, timeout=10)
        except Exception as e:
            logger.error(f"推送失败: {e}")

if __name__ == "__main__":
    main()
