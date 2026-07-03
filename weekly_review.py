#!/usr/bin/env python3
"""
周复盘迭代系统
================
周五 21:00 自动运行：
1. 收集本周所有执行记录
2. 计算每只股票的实战表现
3. 添加大盘环境对比（沪深300）
4. 板块轮动分析
5. 滑点分析（信号价 vs 成交价）
6. 假信号特征分析
7. 基准对比（跑赢大盘多少）
8. 自适应参数调整

数据来源：
- output/trades.json（所有交易记录，单文件）
- output/daily_report_YYYYMMDD.json（选股报告）
- mx-data（大盘/板块数据）
"""

import os
import sys
import json
import logging
import subprocess
import time
import requests
from datetime import datetime, date, timedelta
from pathlib import Path
from typing import Dict, List, Any, Optional
from collections import defaultdict

# ── 路径配置 ──────────────────────────────────────────────
BASE_DIR = Path(__file__).parent
OUTPUT_DIR = BASE_DIR / "output"
LOG_DIR = BASE_DIR / "logs"
LOG_DIR.mkdir(exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[
        logging.FileHandler(LOG_DIR / f"weekly_review_{date.today().strftime('%Y%m%d')}.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("weekly_review")

# ── 自适应参数 ─────────────────────────────────────────────
# 这些参数会根据每周表现自动调整
PARAM_FILE = BASE_DIR / "params.json"

DEFAULT_PARAMS = {
    # ── 仓位参数 ──────────────────────────────────────────────
    "position_size_pct": 0.20,      # 单只仓位 20%
    "max_positions": 5,              # 最大持仓 5只
    "stop_loss_pct": -0.03,        # 止损 -3%
    "take_profit_1": 0.05,         # 止盈1档 +5%
    "take_profit_2": 0.10,         # 止盈2档 +10%
    "take_profit_3": 0.30,         # 止盈3档 +30%
    # ── 打分阈值 ──────────────────────────────────────────────
    "scoring_threshold": 50,         # LLM打分阈值（50分以上才BUY）
    # ── 自适应触发条件 ─────────────────────────────────────────
    "hit_rate_threshold": 0.70,      # 触发激进策略的命中率阈值
    "loss_rate_threshold": 0.40,    # 触发收紧策略的亏损率阈值
    "momentum_weeks": 3,            # 连续N周同方向才调整参数
    "bayesian_prior": 0.50,        # 贝叶斯先验命中率（避免小样本过激调整）
    "bayesian_strength": 4,         # 先验强度（越大越保守）
    # ── 分池命中率追踪（按池独立） ──────────────────────────────
    "pool_hit_rates": {             # 各池累计命中率
        "成长型": {"wins": 0, "total": 0},
        "趋势型": {"wins": 0, "total": 0},
        "逆向型": {"wins": 0, "total": 0},
        "强势型": {"wins": 0, "total": 0},
    },
    # ── 大盘环境（最近4周判断） ─────────────────────────────────
    "market_regime": "震荡",         # 牛/熊/震荡
    # ── 历史周记录 ──────────────────────────────────────────────
    "week_history": [],             # 历史周表现 [{week, hit_rate, pnl_pct, market, pools, ...}]
}


def _normalize_pct_param(value, default: float) -> float:
    try:
        normalized = float(value)
    except (TypeError, ValueError):
        return default
    if normalized == 0:
        return 0.0
    sign = -1.0 if normalized < 0 else 1.0
    normalized = abs(normalized)
    while normalized > 1:
        normalized = normalized / 100.0
    return sign * normalized


def normalize_params(params: Dict) -> Dict:
    normalized = DEFAULT_PARAMS.copy()
    normalized.update(params or {})
    for key in ("position_size_pct", "stop_loss_pct", "take_profit_1", "take_profit_2", "take_profit_3"):
        normalized[key] = _normalize_pct_param(normalized.get(key), DEFAULT_PARAMS[key])
    return normalized


def load_params() -> Dict:
    """从 params.json 加载参数，文件不存在时才用默认值。"""
    if PARAM_FILE.exists():
        with open(PARAM_FILE) as f:
            return normalize_params(json.load(f))
    return DEFAULT_PARAMS.copy()


def save_params(params: Dict):
    params = normalize_params(params)
    with open(PARAM_FILE, "w") as f:
        json.dump(params, f, ensure_ascii=False, indent=2)
    logger.info(f"参数已更新: {PARAM_FILE}")


# ── 收集本周数据 ─────────────────────────────────────────────

TRADES_FILE = OUTPUT_DIR / "trades.json"

# ── mx-moni API 配置 ─────────────────────────────────────
MX_APIKEY = os.getenv("MX_APIKEY")
MX_API_URL = os.getenv("MX_API_URL", "https://mkapi2.dfcfs.com/finskillshub")


def mx_api_post(endpoint: str, payload: Dict = {}, retries: int = 3) -> Dict:
    """调用 mx-moni API"""
    if not MX_APIKEY:
        logger.warning("MX_APIKEY 未设置，跳过 mx-moni 调用")
        return {}
    url = f"{MX_API_URL}{endpoint}"
    headers = {"Content-Type": "application/json", "apikey": MX_APIKEY}
    for attempt in range(retries):
        try:
            resp = requests.post(url, headers=headers, json=payload, timeout=20)
            resp.raise_for_status()
            result = resp.json() or {}
            if result.get("code") == 112:
                wait = (attempt + 1) * 5
                logger.warning(f"API 限速(112)，等待 {wait}s")
                time.sleep(wait)
                continue
            return result
        except Exception as e:
            logger.warning(f"API 请求异常 [{endpoint}]: {e}")
            if attempt < retries - 1:
                time.sleep(2)
                continue
            return {}
    return {}

def _normalize_price(raw: float, price_dec: int = 2) -> float:
    """将 mx-moni 的原始价格转换为元。
    
    priceDec 是小数位数：
    - 0 → 整数（÷1）
    - 1 → 1位小数（÷10）
    - 2 → 2位小数（÷100）
    - 3 → 3位小数（÷1000）
    """
    raw = float(raw)
    divisor = 10 ** max(0, price_dec)
    return raw / divisor


def _fix_sell_price(sell_price: float, current_price: float) -> float:
    """修正卖出价单位错误。

    mx-moni 的 tradePrice 有时会返回错误单位的卖出价。
    判断：若卖出价低于当前价的 20%，说明单位错了（被÷100 了），改为 ×10；
    若修正后仍低于当前价的 20%，则直接用当前价（可能是当日市价委托）。
    """
    if sell_price > 0 and current_price > 0 and sell_price < current_price * 0.2:
        corrected = sell_price * 10
        if corrected >= current_price * 0.2:
            return corrected
        # 修正后仍然过低，用当前价替代（可能是市价单，实际按当时市价成交）
        return current_price
    return sell_price


def get_mx_orders_between(start_dt: date, end_dt: date) -> List[Dict]:
    """获取指定日期范围内的所有成交记录（来自 mx-moni）"""
    orders_resp = mx_api_post("/api/claw/mockTrading/orders", {})
    all_orders = orders_resp.get("data", {}).get("orders", [])
    result = []
    for o in all_orders:
        order_time = datetime.fromtimestamp(o.get("time", 0))
        if not (start_dt <= order_time.date() <= end_dt):
            continue
        price = _normalize_price(o.get("tradePrice", 0), int(o.get("priceDec", 2)))
        qty = int(o.get("tradeCount", 0))
        drt = o.get("drt", 0)
        result.append({
            "stock": o.get("secCode", ""),
            "name": o.get("secName", ""),
            "trade_price": price,
            "trade_qty": qty,
            "trade_date": order_time.date().isoformat(),
            "trade_time": order_time.isoformat(),
            "is_sell": drt == 2,
            "order_id": o.get("id", ""),
        })
    logger.info(f"mx-moni 成交记录: {len(result)} 条 ({start_dt} ~ {end_dt})")
    return result


def get_mx_positions() -> Dict[str, Dict]:
    """获取当前持仓（含成本价、已实现/未实现盈亏）

    返回值包含：
    - qty, avg_cost, current_price, name, market_value
    - unrealized_pnl: (现价 - 成本价) × 数量
    - realized_pnl: 持仓期间已结算的盈亏（来自 API）
    - total_pnl: 累计盈亏 = unrealized + realized
    """
    resp = mx_api_post("/api/claw/mockTrading/positions", {})
    positions = resp.get("data", {}).get("posList", [])
    result = {}
    for p in positions:
        code = p.get("secCode", "")
        qty = int(p.get("count", 0))
        price_dec = int(p.get("priceDec", 2))  # 当前价小数位（分→元用÷100）

        # 成本价：必须用 costPriceDec，不是 priceDec（否则差10倍）
        cost_price_raw = float(p.get("costPrice", 0))
        cost_price_dec = int(p.get("costPriceDec", 2))  # 成本价小数位（厘/分）
        avg_cost = cost_price_raw / (10 ** max(0, cost_price_dec))

        # 当前价（分→元，priceDec=2 → ÷100）
        current_price = float(p.get("price", 0)) / 100

        # 市值（微→元）
        market_value_yuan = float(p.get("value", 0)) / 1000

        # 未实现盈亏
        unrealized_pnl = (current_price - avg_cost) * qty if qty > 0 else 0.0

        # API 自带的盈亏（可能是已实现，也可能是累计）
        api_profit = float(p.get("profit", 0)) / 100  # API 原始单位是分

        result[code] = {
            "name": p.get("secName", ""),
            "qty": qty,
            "avg_cost": avg_cost,
            "current_price": current_price,
            "market_value": market_value_yuan,
            "unrealized_pnl": unrealized_pnl,
            "api_profit": api_profit,
        }
    logger.info(f"mx-moni 当前持仓: {len(result)} 只")
    return result


def get_week_report_files() -> List[Path]:
    """获取本周所有交易日的选股报告"""
    today = date.today()
    monday = today - timedelta(days=today.weekday())
    files = []
    for i in range(5):
        day = monday + timedelta(days=i)
        f = OUTPUT_DIR / f"daily_report_{day.strftime('%Y%m%d')}.json"
        if f.exists():
            files.append(f)
    return files



def load_executions() -> List[Dict]:
    """加载本周需复盘的持仓记录（来自 mx-moni API）

    数据源：mx-moni /orders（成交记录）+ /positions（当前持仓）

    关键规则：
    - 只纳入已成交的订单（tradeCount > 0）
    - 价格单位自动检测（厘 < 100 < 分）
    - 已卖完股票 → 已实现收益 = 卖出收益 - 买入成本
    - 剩余持仓 → 未实现收益 = (现价 - 成本价) × 剩余数量
    - 成本价从 mx-moni 的持仓数据获取
    """
    today_dt = date.today()
    monday = today_dt - timedelta(days=today_dt.weekday())

    # 1. 获取本周 + 上周的成交记录（用于聚合）
    week_orders = get_mx_orders_between(monday, today_dt)
    prev_monday = monday - timedelta(days=7)
    all_orders = get_mx_orders_between(prev_monday, today_dt)

    # 2. 获取当前持仓（含成本价）
    current_positions = get_mx_positions()

    # 3. 按股票聚合已成交的买卖
    stock_trades: Dict[str, Dict] = {}
    for o in all_orders:
        if o["trade_qty"] == 0:   # 跳过未成交订单
            continue
        code = o["stock"]
        if code not in stock_trades:
            stock_trades[code] = {"name": o["name"], "buys": [], "sells": []}
        if o["is_sell"]:
            stock_trades[code]["sells"].append(o)
        else:
            stock_trades[code]["buys"].append(o)

    # 4. 构建每只股票的执行记录
    records = []
    for code, info in stock_trades.items():
        buys = sorted(info["buys"], key=lambda x: x["trade_time"])
        sells = sorted(info["sells"], key=lambda x: x["trade_time"])

        if not buys:
            continue

        # 合并买入：加权平均成本（仅已成交）
        total_buy_amt = sum(b["trade_price"] * b["trade_qty"] for b in buys)
        total_buy_qty = sum(b["trade_qty"] for b in buys)
        avg_buy_price = total_buy_amt / total_buy_qty if total_buy_qty else 0
        first_buy_date = buys[0]["trade_date"]

        # 当前持仓情况
        cur = current_positions.get(code, {})
        remaining = cur.get("qty", 0)
        current_price = cur.get("current_price", buys[-1]["trade_price"] if buys else 0)

        # 已卖出数量
        total_sell_qty = sum(s["trade_qty"] for s in sells)

        # 构建卖出记录（使用修正后的卖出价）
        sell_records = []
        for s in sells:
            fixed_price = _fix_sell_price(s["trade_price"], current_price)
            sell_records.append({
                "date": s["trade_date"],
                "price": fixed_price,
                "quantity": s["trade_qty"],
                "reason": s.get("reason", ""),
            })

        # 计算已实现盈亏（使用修正后的卖出价）
        realized_pnl = 0.0
        if total_sell_qty > 0:
            total_sell_amt = sum(
                _fix_sell_price(s["trade_price"], current_price) * s["trade_qty"]
                for s in sells
            )
            realized_pnl = total_sell_amt - avg_buy_price * total_sell_qty

        # 未实现盈亏：使用实际成交记录计算（positions API 的 costPrice 有时不准确）
        unrealized_pnl = 0.0
        if remaining > 0 and current_price:
            # 用原始成交记录重新计算持仓成本
            unrealized_pnl = (current_price - avg_buy_price) * remaining

        records.append({
            "stock": code,
            "name": info["name"] or cur.get("name", code),
            "buy_date": first_buy_date,
            "buy_price": avg_buy_price,
            "quantity": total_buy_qty,
            "remaining_quantity": remaining,
            "sold_quantity": total_sell_qty,
            "current_price": current_price,
            "realized_pnl": realized_pnl,
            "unrealized_pnl": unrealized_pnl,
            "sells": sell_records,
        })

    # 5. 加入当前持仓中不在记录里的股票
    held_codes = {r["stock"] for r in records}
    for code, cur in current_positions.items():
        if cur.get("qty", 0) > 0 and code not in held_codes:
            unrealized_pnl = cur.get("market_value", 0) - cur.get("avg_cost", 0) * cur.get("qty", 0)
            records.append({
                "stock": code,
                "name": cur["name"],
                "buy_date": "",
                "buy_price": cur.get("avg_cost", 0),
                "quantity": cur["qty"],
                "remaining_quantity": cur["qty"],
                "sold_quantity": 0,
                "current_price": cur["current_price"],
                "realized_pnl": 0.0,
                "unrealized_pnl": unrealized_pnl,
                "sells": [],
            })

    logger.info(f"load_executions: 共 {len(records)} 只股票（mx-moni 数据）")
    return records


    # 6. 加入当前持仓中不在记录里的股票（上周买入仍在仓的）
    held_codes = {r["stock"] for r in records}
    for code, cur in current_positions.items():
        if cur.get("qty", 0) > 0 and code not in held_codes:
            unrealized_pnl = cur.get("market_value", 0) - cur.get("avg_cost", 0) * cur.get("qty", 0) if cur.get("qty", 0) else 0.0
            records.append({
                "stock": code,
                "name": cur.get("name", code),
                "buy_date": "",
                "buy_price": cur.get("avg_cost", 0),
                "quantity": cur.get("qty", 0),
                "remaining_quantity": cur.get("qty", 0),
                "sold_quantity": 0,
                "current_price": cur.get("current_price", 0),
                "realized_pnl": 0.0,
                "unrealized_pnl": unrealized_pnl,
                "sells": [],
            })

    logger.info(f"load_executions: 共 {len(records)} 只股票（mx-moni 数据）")
    return records


def load_reports() -> Dict[str, Dict]:
    """加载本周所有选股报告，按日期索引"""
    reports = {}
    for f in get_week_report_files():
        try:
            day = f.stem.replace("daily_report_", "")
            with open(f) as fp:
                reports[day] = json.load(fp)
        except Exception as e:
            logger.warning(f"读取报告失败 {f}: {e}")
    return reports


# ── 价格数据获取（统一）──────────────────────────────────────

def get_realtime_price(stock_code: str) -> Optional[float]:
    """获取股票实时价格（mx-data优先，腾讯备用）"""
    import re
    import urllib.request
    # 方案1：mx-data
    try:
        cmd = [
            sys.executable,
            str(BASE_DIR.parent / "skills/mx-data/mx_data.py"),
            f"{stock_code} 当前最新价",
        ]
        r = subprocess.run(cmd, capture_output=True, text=True, timeout=20)
        output = r.stdout
        if "上限" in output or "frequency" in output.lower() or "调用次数" in output:
            raise RuntimeError("限额")
        matches = re.findall(r'\|\s*([\d.]+)\s*\|', output)
        if not matches:
            matches = re.findall(r'(\d+\.\d+)\s*元', output)
        if matches:
            return float(matches[0])
    except Exception:
        pass
    # 方案2：腾讯行情API
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


def get_price_on_date(stock_code: str, target_date: date) -> Optional[float]:
    """获取指定日期的收盘价（若无当日数据，取最近交易日）"""
    from datetime import timedelta
    for offset in range(10):  # 最多往前找10个交易日
        check_date = target_date - timedelta(days=offset)
        hist = get_historical_prices(stock_code, days=offset + 5)
        for h in reversed(hist):
            if h.get("date", "") == check_date.isoformat():
                return h.get("close")
    # fallback: 取最近一个收盘价
    hist = get_historical_prices(stock_code, days=5)
    return hist[-1]["close"] if hist else None


def get_post_sell_returns(stock_code: str, sell_date: date, sell_price: float, days_list: list) -> dict:
    """计算卖出后N个交易日的收益率。

    Returns:
        {3: (price, pct), 5: (price, pct), ...}
        若N个交易日未到，用最新可用价格替代。
    """
    # 获取足够的历史数据（卖出日 + 往后20个交易日）
    hist = get_historical_prices(stock_code, days=25)
    date_to_close = {}
    for h in hist:
        date_to_close[h["date"]] = h["close"]
    sorted_dates = sorted(date_to_close.keys())

    # 找到卖出日在历史数据中的位置
    try:
        sell_idx = sorted_dates.index(sell_date.isoformat())
    except ValueError:
        # 卖出日不在数据里，取最近的可用的前一天作基准
        sell_idx = max(0, len(sorted_dates) - 1)

    result = {}
    for n in days_list:
        future_idx = sell_idx + n
        if future_idx < len(sorted_dates):
            future_price = date_to_close[sorted_dates[future_idx]]
        else:
            # 未到N个交易日，用最新可用价
            future_price = date_to_close[sorted_dates[-1]]
        pct = (future_price - sell_price) / sell_price if sell_price else None
        result[n] = (future_price, pct)

    return result


def get_historical_prices(stock_code: str, days: int = 5) -> List[Dict]:
    """获取近N日收盘价（mx-data优先，腾讯备用）"""
    import re
    import json
    import urllib.request
    # 方案1：mx-data
    try:
        cmd = [
            sys.executable,
            str(BASE_DIR.parent / "skills/mx-data/mx_data.py"),
            f"{stock_code} 最近{days}个交易日的收盘价",
        ]
        r = subprocess.run(cmd, capture_output=True, text=True, timeout=20)
        if r.returncode == 0 and "上限" not in r.stdout and "错误" not in r.stdout:
            rows = re.findall(r'\|\s*(\d{4}-\d{2}-\d{2})\s*\|\s*([\d.]+)\s*\|', r.stdout)
            if rows:
                return [{"date": d, "close": float(p)} for d, p in rows]
    except Exception:
        pass
    # 方案2：腾讯行情API
    try:
        prefix = "sh" if stock_code.startswith(('6', '5', '9')) else "sz"
        url = (f"https://web.ifzq.gtimg.cn/appstock/app/fqkline/get"
               f"?_var=kline_dayhfq&param={prefix}{stock_code},day,,,{days},qfq")
        req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0", "Referer": "https://gu.qq.com"})
        with urllib.request.urlopen(req, timeout=10) as resp:
            raw = resp.read().decode("utf-8")
        raw = raw[raw.index("=") + 1:]
        obj = json.loads(raw)
        data = obj.get("data", {}).get(f"{prefix}{stock_code}", {}).get("qfqday", [])
        result = [{"date": row[0], "close": float(row[2])} for row in data if row[2]]
        if result:
            return result
    except Exception:
        pass
    # 方案3：akshare（支持沪深指数）
    try:
        import akshare as ak
        # 指数前缀映射（不能用股票规则，指数000/399/688都是不同的）
        sh_indices = {"000001", "000002", "000003", "000008", "000009", "000010", "000011",
                       "000012", "000015", "000016", "000688", "000300", "000016"}
        sz_indices = {"399001", "399002", "399005", "399006", "399100", "399101", "399102",
                       "399103", "399104", "399106", "399107", "399108", "399333"}
        if stock_code in sh_indices:
            sym = f"sh{stock_code}"
        elif stock_code in sz_indices:
            sym = f"sz{stock_code}"
        elif stock_code.startswith(("6", "5", "9")):
            sym = f"sh{stock_code}"
        elif stock_code.startswith(("0", "3")):
            sym = f"sz{stock_code}"
        else:
            sym = stock_code
        df = ak.stock_zh_index_daily(symbol=sym)
        if df is not None and len(df) > 0:
            df = df.tail(days)
            return [{"date": str(r["date"])[:10], "close": float(r["close"])} for _, r in df.iterrows()]
    except Exception as e:
        pass
    return []


def get_index_data(index_code: str = "000001", days: int = 5) -> List[Dict]:
    """获取大盘指数数据（用于对比基准）"""
    return get_historical_prices(index_code, days)


def get_sector_for_stock(stock_code: str) -> Optional[str]:
    """获取股票所属板块（简化：查 mx-data 的股票基本信息）"""
    try:
        cmd = [
            sys.executable,
            str(BASE_DIR.parent / "skills/mx-data/mx_data.py"),
            f"{stock_code} 所属板块 行业",
        ]
        r = subprocess.run(cmd, capture_output=True, text=True, timeout=15)
        # 提取 GICS 行业（格式：金融-银行-商业银行-综合性银行）
        # 取第一级的行业分类
        parts = r.stdout.split('-')
        if parts:
            return parts[0]  # 返回第一级（如"金融"）
    except Exception:
        pass
    return "未知板块"


# ── 市场环境分类 ─────────────────────────────────────────────

def classify_market_regime(indices: List[str] = None) -> Dict[str, Any]:
    """
    用20日数据判断大盘环境：牛市/熊市/震荡
    避免5日窗口噪声过大，改用20日
    """
    if indices is None:
        indices = ["000001", "399001", "399006", "000300"]

    index_names = {
        "000001": "上证指数",
        "399001": "深证成指",
        "399006": "创业板指",
        "000300": "沪深300",
    }

    regimes = {}
    for code in indices:
        hist = get_historical_prices(code, days=20)
        if len(hist) < 10:
            continue
        first = hist[0]["close"]
        last = hist[-1]["close"]
        change_pct = (last - first) / first * 100 if first else 0

        if change_pct > 5:
            regime = "牛市"
        elif change_pct < -5:
            regime = "熊市"
        else:
            regime = "震荡"

        regimes[code] = {
            "name": index_names.get(code, code),
            "change_20d": round(change_pct, 2),
            "regime": regime,
        }

    # 综合判断
    regime_votes = [r["regime"] for r in regimes.values()]
    if not regime_votes:
        return {"overall": "震荡", "indices": {}}
    # 多数裁定
    overall = max(set(regime_votes), key=regime_votes.count)
    return {"overall": overall, "indices": regimes}


# ── 信号质量分类 ─────────────────────────────────────────────

def classify_signal_quality(
    stock_pnl: float,
    market_change: float,
    stock_change: float,
    threshold: float = 0.01,
) -> str:
    """
    区分 alpha 收益 / beta 收益 / 假信号

    Args:
        stock_pnl: 个股盈亏比例（正=赚，负=亏）
        market_change: 大盘20日涨跌幅
        stock_change: 个股20日涨跌幅
        threshold: 判断阈值（默认1%）

    Returns:
        "alpha_win": 个股涨且跑赢大盘
        "beta_win":  个股涨但跟随大盘（没超额）
        "beta_lose": 个股亏但跑赢大盘（防御好）
        "false_signal": 个股亏且跑输大盘（选股失败）
    """
    if stock_pnl > threshold:
        if stock_change - market_change > threshold:
            return "alpha_win"       # 真正有alpha
        else:
            return "beta_win"        # 只是大盘带上来的
    else:
        if market_change < -threshold and stock_change > market_change + threshold:
            return "beta_lose"       # 亏了但防御性强
        else:
            return "false_signal"    # 真假信号（亏+跑输）


# ── 核心分析函数 ─────────────────────────────────────────────

def analyze_performance(executions: List[Dict], reports: Dict[str, Dict], week_start: date, week_end: date) -> Dict[str, Any]:
    """
    分析本周实战表现（增强版：含池来源 + 信号质量分类）
    """
    if not executions:
        return {"status": "no_executions", "summary": "本周无买入记录"}

    # 20日大盘涨跌幅（用于信号质量分类）
    # 20日大盘涨跌幅（用于信号质量分类）
    # 沪深300优先，若无数据则用上证指数
    hs300_hist = get_historical_prices("000300", 20)
    if len(hs300_hist) >= 2:
        market_change = (hs300_hist[-1]["close"] - hs300_hist[0]["close"]) / hs300_hist[0]["close"] * 100
    else:
        # fallback：上证指数
        sh_hist = get_historical_prices("000001", 20)
        if len(sh_hist) >= 2:
            market_change = (sh_hist[-1]["close"] - sh_hist[0]["close"]) / sh_hist[0]["close"] * 100
        else:
            market_change = 0.0

    analyzed = []
    for record in executions:
        stock = record.get("stock", "")
        buy_date = record.get("buy_date", "")
        buy_price = record.get("buy_price", 0)
        quantity = record.get("quantity", 0)
        remaining = record.get("remaining_quantity", 0)
        sells = record.get("sells", [])
        pool = record.get("pool", "选股池")

        # 计算已实现盈亏（卖出部分）
        realized_pnl_pct = 0.0
        realized_pnl_amt = 0.0
        sold_qty = 0
        sell_reasons = []  # 收集所有卖出原因
        for s in sells:
            sold_qty += s.get("quantity", 0)
            realized_pnl_amt += s.get("quantity", 0) * (s.get("price", 0) - buy_price)
            r = s.get("reason", "")
            if r:
                sell_reasons.append(r)
        if sold_qty > 0 and buy_price:
            realized_pnl_pct = realized_pnl_amt / (sold_qty * buy_price)

        # 获取最新/收盘价（用于计算未实现盈亏）
        current_price = get_realtime_price(stock)
        if not current_price:
            hist5 = get_historical_prices(stock, 5)
            if hist5:
                current_price = hist5[-1]["close"]

        if not current_price or not buy_price:
            logger.warning(f"无法获取 {stock} 价格数据，跳过")
            continue

        # 未实现盈亏（剩余持仓）
        unrealized_pnl_amt = remaining * (current_price - buy_price) if remaining else 0
        unrealized_pnl_pct = (current_price - buy_price) / buy_price if buy_price else 0

        # 总盈亏 = 已实现 + 未实现
        total_pnl_amt = realized_pnl_amt + unrealized_pnl_amt
        total_pnl_pct = total_pnl_amt / (quantity * buy_price) if quantity and buy_price else 0

        # 20日个股涨幅（用于信号质量分类）
        hist20 = get_historical_prices(stock, 20)
        stock_change_20d = 0.0
        if len(hist20) >= 2:
            stock_change_20d = (hist20[-1]["close"] - hist20[0]["close"]) / hist20[0]["close"] * 100

        # 信号质量分类（用总盈亏）
        signal_quality = classify_signal_quality(
            total_pnl_pct, market_change, stock_change_20d
        )

        # 滑点（无signal_price，跳过）
        slippage = 0.0

        # 板块
        sector = record.get("sector") or get_sector_for_stock(stock)

        # ── 本周 vs 老仓标签 ──────────────────────────────
        try:
            bd = date.fromisoformat(buy_date) if buy_date else None
            is_new_this_week = bd is not None and week_start <= bd <= week_end
        except Exception:
            is_new_this_week = False

        # 本周是否有过卖出
        is_sold_this_week = False
        last_sell_price = 0.0
        last_sell_date = ""
        for s in sells:
            try:
                sd = date.fromisoformat(s.get("date", "")) if s.get("date") else None
                if sd and week_start <= sd <= week_end:
                    is_sold_this_week = True
                    last_sell_price = s.get("price", 0)
                    last_sell_date = s.get("date", "")
                    break
            except Exception:
                pass

        # 卖出后走势追踪（本周有卖出的）
        post_sell_return_pct = 0.0
        post_sell_3d_price = None
        post_sell_3d_pct = None
        post_sell_5d_price = None
        post_sell_5d_pct = None
        if is_sold_this_week and last_sell_price > 0:
            if last_sell_date:
                try:
                    sd = date.fromisoformat(last_sell_date)
                    rets = get_post_sell_returns(stock, sd, last_sell_price, [3, 5])
                    post_sell_3d_price, post_sell_3d_pct = rets[3]
                    post_sell_5d_price, post_sell_5d_pct = rets[5]
                    post_sell_return_pct = post_sell_3d_pct if post_sell_3d_pct is not None else 0.0
                except Exception:
                    pass

        analyzed.append({
            "stock": stock,
            "name": record.get("name", stock),
            "buy_date": buy_date,
            "buy_price": buy_price,
            "current_price": current_price,
            "quantity": quantity,
            "remaining_quantity": remaining,
            "sold_quantity": sold_qty,
            "realized_pnl_pct": realized_pnl_pct,
            "realized_pnl_amt": realized_pnl_amt,
            "unrealized_pnl_pct": unrealized_pnl_pct,
            "unrealized_pnl_amt": unrealized_pnl_amt,
            "total_pnl_pct": total_pnl_pct,
            "pnl_pct": total_pnl_pct,
            "total_pnl_amt": total_pnl_amt,
            "signal_price": buy_price,
            "slippage": slippage,
            "llm_score": record.get("total_score", 0) or 0,
            "pool": pool,
            "signal_quality": signal_quality,
            "market_change": market_change,
            "sector": sector,
            "action": record.get("action", "BUY"),
            "reason": record.get("reason", ""),
            "sell_reasons": sell_reasons,
            "sells": sells,
            "result": record.get("result", {}),
            # 新增字段
            "is_new_this_week": is_new_this_week,
            "is_sold_this_week": is_sold_this_week,
            "last_sell_price": last_sell_price,
            "last_sell_date": last_sell_date,
            "post_sell_return_pct": post_sell_return_pct,
            "post_sell_3d_price": post_sell_3d_price,
            "post_sell_3d_pct": post_sell_3d_pct,
            "post_sell_5d_price": post_sell_5d_price,
            "post_sell_5d_pct": post_sell_5d_pct,
        })

    return {"status": "analyzed", "stocks": analyzed, "market_change": market_change}


def analyze_market_context(days: int = 5) -> Dict[str, Any]:
    """
    分析大盘环境：获取沪深300/上证指数近5日走势
    """
    indices = {
        "000001": "上证指数",
        "399001": "深证成指",
        "399006": "创业板指",
        "000300": "沪深300",
    }

    context = {}
    for code, name in indices.items():
        hist = get_historical_prices(code, days)
        if len(hist) >= 2:
            first = hist[0]["close"]
            last = hist[-1]["close"]
            change_pct = (last - first) / first * 100
            trend = "多头" if last > first else "空头"
            context[code] = {
                "name": name,
                "start": first,
                "end": last,
                "change_pct": change_pct,
                "trend": trend,
            }

    return context


def analyze_sector_rotation(analyzed_stocks: List[Dict]) -> Dict[str, Any]:
    """
    板块轮动分析：哪些板块的股票表现好/差
    """
    sector_perf = defaultdict(list)
    for s in analyzed_stocks:
        sector = s.get("sector", "未知板块")
        sector_perf[sector].append(s.get("pnl_pct", 0))

    sector_summary = []
    for sector, pnls in sector_perf.items():
        avg_pnl = sum(pnls) / len(pnls) if pnls else 0
        sector_summary.append({
            "sector": sector,
            "count": len(pnls),
            "avg_pnl_pct": avg_pnl * 100,
            "pnls": pnls,
        })

    sector_summary.sort(key=lambda x: x["avg_pnl_pct"], reverse=True)
    return {
        "strongest_sectors": sector_summary[:2],
        "weakest_sectors": sector_summary[-2:] if len(sector_summary) >= 2 else [],
        "all_sectors": sector_summary,
    }


def analyze_false_signals(analyzed_stocks: List[Dict]) -> Dict[str, Any]:
    """
    假信号分析：亏损或涨幅很小的信号有什么共同特征
    """
    # 定义假信号：涨幅 < 1% 或 亏损
    threshold = 0.01
    losers = [s for s in analyzed_stocks if s.get("pnl_pct", 0) < threshold]

    if not losers:
        return {"false_signal_count": 0, "characteristics": "无假信号"}

    # 分析共同特征
    scores = [s.get("llm_score", 0) for s in losers]
    avg_score = sum(scores) / len(scores) if scores else 0
    sectors = [s.get("sector", "未知") for s in losers]
    slippages = [s.get("slippage", 0) for s in losers]

    characteristics = []
    if avg_score < 55:
        characteristics.append(f"假信号平均打分({avg_score:.0f})偏低，说明低分信号风险高")
    if slippages:
        avg_slip = sum(slippages) / len(slippages)
        if avg_slip > 0.5:
            characteristics.append(f"平均滑点({avg_slip:.2f}%)偏高，成交价高于信号价")

    # 分析卖出原因
    all_sell_reasons = [r for s in analyzed_stocks for r in s.get("sell_reasons", [])]
    if all_sell_reasons:
        reason_count = {}
        for r in all_sell_reasons:
            reason_count[r] = reason_count.get(r, 0) + 1
        top_reasons = sorted(reason_count.items(), key=lambda x: -x[1])[:3]
        characteristics.append(f"卖出原因: {'; '.join(f'{r}x{n}' for r,n in top_reasons)}")

    return {
        "false_signal_count": len(losers),
        "false_signal_rate": len(losers) / len(analyzed_stocks) if analyzed_stocks else 0,
        "avg_score": avg_score,
        "common_sectors": list(set(sectors)),
        "characteristics": characteristics,
        "losers_detail": [
            {"stock": s["stock"], "pnl_pct": s["total_pnl_pct"] * 100, "score": s.get("llm_score", 0)}
            for s in losers
        ],
    }


def analyze_benchmark(analyzed_stocks: List[Dict], market_context: Dict) -> Dict[str, Any]:
    """
    基准对比：跑赢/跑输大盘多少
    """
    hs300 = market_context.get("000300", {})
    sz001 = market_context.get("000001", {})

    if not hs300 and not sz001:
        return {"status": "insufficient_data"}

    # 计算组合平均表现
    if not analyzed_stocks:
        return {"status": "no_stocks"}

    avg_pnl = sum(s.get("pnl_pct", 0) for s in analyzed_stocks) / len(analyzed_stocks)

    benchmark_pct = (hs300.get("change_pct", 0) + sz001.get("change_pct", 0)) / 2 / 100

    excess_return = avg_pnl - benchmark_pct

    return {
        "portfolio_avg_pnl": avg_pnl * 100,
        "hs300_change": hs300.get("change_pct", 0),
        "sz001_change": sz001.get("change_pct", 0),
        "excess_return": excess_return * 100,
        "verdict": "跑赢大盘" if excess_return > 0 else "跑输大盘",
    }


def calculate_adaptive_params(
    analyzed_stocks: List[Dict],
    current_params: Dict,
    week_history: List[Dict],
    market_regime: str = "震荡",
) -> Dict[str, Any]:
    """
    自适应参数调整（增强版）
    - 贝叶斯平滑：避免小样本过激调整
    - 动量法：连续N周同一方向才调整
    - 分池追踪：各池独立累计命中率
    - 大盘环境：熊市降低仓位基准
    """
    if not analyzed_stocks:
        return {
            "new_params": current_params.copy(),
            "reason": "本周无买入，无法判断，参数不变",
            "pool_rates": current_params.get("pool_hit_rates", {}),
        }

    threshold = 0.01  # 赚钱标准：涨幅 > 1%
    winners = [s for s in analyzed_stocks if s.get("pnl_pct", 0) > threshold]
    raw_hit_rate = len(winners) / len(analyzed_stocks)
    avg_pnl = sum(s.get("pnl_pct", 0) for s in analyzed_stocks) / len(analyzed_stocks)

    # ── 贝叶斯平滑 ──────────────────────────────────────────
    # posterior = (wins + prior * strength) / (total + strength)
    prior = current_params.get("bayesian_prior", 0.50)
    strength = current_params.get("bayesian_strength", 4)
    total = len(analyzed_stocks)
    wins = len(winners)
    bayesian_hit_rate = (wins + prior * strength) / (total + strength)

    # ── 分池统计 ──────────────────────────────────────────────
    pool_rates = {k: dict(v) for k, v in current_params.get("pool_hit_rates", {}).items()}
    for stock in analyzed_stocks:
        pool = stock.get("pool", "选股池")
        if pool not in pool_rates:
            pool_rates[pool] = {"wins": 0, "total": 0}
        pool_rates[pool]["total"] += 1
        if stock.get("pnl_pct", 0) > threshold:
            pool_rates[pool]["wins"] += 1

    # 计算各池贝叶斯命中率
    pool_bayesian = {}
    for pool, data in pool_rates.items():
        r = (data["wins"] + prior * strength) / (data["total"] + strength)
        pool_bayesian[pool] = round(r * 100, 1)

    # ── 动量法：看最近N周方向是否一致 ─────────────────────────
    momentum_weeks = current_params.get("momentum_weeks", 3)
    recent_weeks = week_history[-momentum_weeks:] if week_history else []
    recent_directions = []
    for w in recent_weeks:
        hr = w.get("hit_rate", 0.5)
        recent_directions.append("up" if hr >= prior else "down")

    # 本周方向（已计算但暂未使用）
    # this_direction = "up" if raw_hit_rate >= prior else "down"

    # ── 大盘环境调节系数 ─────────────────────────────────────
    regime_multiplier = {
        "牛市": 1.2,
        "熊市": 0.6,
        "震荡": 1.0,
    }.get(market_regime, 1.0)

    new_params = current_params.copy()
    reasons = []

    # ── 动量判断：方向要连续才动 ──────────────────────────────
    def should_adjust(direction: str, target: float, current: float, 
                      adjust_fn) -> tuple:
        """返回 (should_act, new_value, reason)"""
        all_same = all(d == direction for d in recent_directions + [direction])
        if not all_same or len(recent_directions) < momentum_weeks - 1:
            return False, current, "动量不足，等数据"
        new_val = adjust_fn(current)
        return True, new_val, f"连续{momentum_weeks}周{direction}方向确认"

    # ── 基于贝叶斯命中率判断（而非原始命中率）───────────────────
    if bayesian_hit_rate >= current_params["hit_rate_threshold"]:
        def adj_up(x): return min(x * 1.1 * regime_multiplier, 0.30)
        def thresh_down(x): return max(x - 3, 40)
        should, new_pos, r1 = should_adjust(
            "up", bayesian_hit_rate, new_params["position_size_pct"], adj_up)
        if should:
            new_params["position_size_pct"] = new_pos
            new_params["scoring_threshold"] = thresh_down(new_params["scoring_threshold"])
            reasons.append(f"{r1}，贝叶斯命中率{bayesian_hit_rate*100:.0f}%>70%，加仓至{new_pos*100:.0f}%")

    elif bayesian_hit_rate <= current_params["loss_rate_threshold"]:
        def adj_down(x): return max(x * 0.8 * regime_multiplier, 0.05)
        def thresh_up(x): return min(x + 5, 70)
        should, new_pos, r2 = should_adjust(
            "down", bayesian_hit_rate, new_params["position_size_pct"], adj_down)
        if should:
            new_params["position_size_pct"] = new_pos
            new_params["scoring_threshold"] = thresh_up(new_params["scoring_threshold"])
            reasons.append(f"{r2}，贝叶斯命中率{bayesian_hit_rate*100:.0f}%<40%，降仓至{new_pos*100:.0f}%")

    if not reasons:
        reasons.append(f"贝叶斯命中率{bayesian_hit_rate*100:.0f}%在正常区间，参数不变")

    # ── 分池分析：找出表现最好/差的池 ─────────────────────────
    sorted_pools = sorted(pool_bayesian.items(), key=lambda x: x[1], reverse=True)
    if sorted_pools:
        best_pool, best_rate = sorted_pools[0]
        worst_pool, worst_rate = sorted_pools[-1]
        reasons.append(f"最强池: {best_pool}({best_rate}%) | 最弱池: {worst_pool}({worst_rate}%)")

    return {
        "new_params": new_params,
        "reasons": reasons,
        "raw_hit_rate": round(raw_hit_rate * 100, 1),
        "bayesian_hit_rate": round(bayesian_hit_rate * 100, 1),
        "avg_pnl_pct": round(avg_pnl * 100, 2),
        "pool_rates": pool_rates,
        "pool_bayesian": pool_bayesian,
        "momentum_weeks": momentum_weeks,
        "recent_directions": recent_directions,
        "market_regime": market_regime,
    }


# ── 报告生成 ─────────────────────────────────────────────

def generate_weekly_report(
    week_start: date,
    week_end: date,
    perf: Dict,
    market: Dict,
    sector: Dict,
    false_signals: Dict,
    benchmark: Dict,
    adaptive: Dict,
) -> str:
    """生成周复盘报告（飞书推送格式）"""

    lines = [
        f"📊 周复盘 {week_start} ~ {week_end}",
        "=" * 40,
        "",
    ]

    # 1. 大盘环境
    lines.append("【大盘环境】")
    for code, info in market.items():
        emoji = "🟢" if info["trend"] == "多头" else "🔴"
        lines.append(
            f"  {emoji} {info['name']}: {info['change_pct']:+.2f}% ({info['trend']})"
        )
    lines.append("")

    # 2. 本周新股（本周买入的股票）
    stocks = perf.get("stocks", [])
    new_this_week = [s for s in stocks if s.get("is_new_this_week")]
    sold_this_week = [s for s in stocks if s.get("is_sold_this_week")]
    held_from_before = [s for s in stocks if not s.get("is_new_this_week") and not s.get("is_sold_this_week")]

    if new_this_week:
        wins = sum(1 for s in new_this_week if s["total_pnl_amt"] > 0)
        total_pnl = sum(s["total_pnl_amt"] for s in new_this_week)
        total_cost = sum(s["quantity"] * s["buy_price"] for s in new_this_week)
        total_pnl_pct = total_pnl / total_cost * 100 if total_cost else 0
        lines.append(f"【本周新股】买入 {len(new_this_week)} 只 | 胜率 {wins}/{len(new_this_week)} ({wins/len(new_this_week)*100:.0f}%) | 总盈亏 {total_pnl_pct:+.1f}% ({total_pnl:+.0f}元)")
        for s in sorted(new_this_week, key=lambda x: x["total_pnl_amt"], reverse=True):
            emoji = "🟢" if s["total_pnl_amt"] > 0 else "🔴"
            # 按股票代码选择对应指数：上证(60/68)→上证指数，沪深(00/30)→深证成指
            code = s["stock"]
            idx_code = "000001" if code.startswith(("6", "68")) else "399001"
            mkt_20d = market.get(idx_code, {}).get("change_pct", market.get("000001", {}).get("change_pct", 0))
            vs_market = s["pnl_pct"] * 100 - mkt_20d
            vs_market = s["pnl_pct"] * 100 - mkt_20d
            mkt_str = f"(vs大盘{vs_market:+.1f}%)" if vs_market != 0 else ""
            lines.append(
                f"  {emoji} {s['stock']} {s.get('name', '')} "
                f"买:{s['buy_price']:.3f} → 现:{s['current_price']:.2f} "
                f"{s['pnl_pct']*100:+.1f}% {mkt_str}"
            )
        lines.append("")
    else:
        lines.append("【本周新股】本周无新股买入")
        lines.append("")

    # 3. 卖出追踪（本周卖出的股票）
    if sold_this_week:
        wins = sum(1 for s in sold_this_week if s["realized_pnl_amt"] > 0)
        kept_winning = sum(1 for s in sold_this_week if s.get("post_sell_return_pct", 0) > 0)
        # 统计卖后3日/5日继续上涨
        rising3 = sum(1 for s in sold_this_week if s.get("post_sell_3d_pct") and s["post_sell_3d_pct"] > 0)
        rising5 = sum(1 for s in sold_this_week if s.get("post_sell_5d_pct") and s["post_sell_5d_pct"] > 0)
        count3 = sum(1 for s in sold_this_week if s.get("post_sell_3d_pct") is not None)
        count5 = sum(1 for s in sold_this_week if s.get("post_sell_5d_pct") is not None)

        lines.append(f"【卖出追踪】卖出 {len(sold_this_week)} 只 | 止盈 {wins} 只 | 3日续涨 {rising3}/{count3} | 5日续涨 {rising5}/{count5}")
        for s in sorted(sold_this_week, key=lambda x: x["realized_pnl_amt"], reverse=True):
            emoji = "🟢" if s["realized_pnl_amt"] > 0 else "🔴"
            p3p = s.get("post_sell_3d_pct")
            p5p = s.get("post_sell_5d_pct")
            p3_str = f"{p3p*100:+.1f}%" if p3p is not None else "-"
            p5_str = f"{p5p*100:+.1f}%" if p5p is not None else "-"
            m3 = "↑" if p3p and p3p > 0 else ("↓" if p3p and p3p < 0 else " ")
            m5 = "↑" if p5p and p5p > 0 else ("↓" if p5p and p5p < 0 else " ")
            lines.append(
                f"  {emoji} {s['stock']} {s.get('name', '')} "
                f"已卖{s['sold_quantity']}股 卖出:{s.get('last_sell_price', 0):.2f} "
                f"3日:{m3}{p3_str} 5日:{m5}{p5_str} "
                f"({s['realized_pnl_amt']:+.0f}元)"
            )
        lines.append("")
    else:
        lines.append("【卖出追踪】本周无卖出")
        lines.append("")

    # 4. 持仓追踪（老仓，仅显示盈亏）
    if held_from_before:
        total_unreal = sum(s["unrealized_pnl_amt"] for s in held_from_before)
        wins = sum(1 for s in held_from_before if s["unrealized_pnl_amt"] > 0)
        lines.append(f"【持仓追踪】老仓 {len(held_from_before)} 只 | 浮盈 {wins} 只 | 浮亏 {len(held_from_before)-wins} 只 | 浮盈亏 {total_unreal:+.0f}元")
        lines.append("")

    # 3. 基准对比
    if benchmark.get("status") != "insufficient_data":
        lines.append("【基准对比】")
        lines.append(
            f"  组合平均: {benchmark.get('portfolio_avg_pnl', 0):+.1f}% | "
            f"沪深300: {benchmark.get('hs300_change', 0):+.1f}%"
        )
        lines.append(f"  {benchmark.get('verdict', '')} {abs(benchmark.get('excess_return', 0)):.1f}%")
        lines.append("")

    # 4. 板块轮动
    if sector.get("all_sectors"):
        lines.append("【板块轮动】")
        for s in sector.get("strongest_sectors", []):
            lines.append(f"  📈 {s['sector']} 强势 (+{s['avg_pnl_pct']:.1f}%)")
        for s in sector.get("weakest_sectors", []):
            lines.append(f"  📉 {s['sector']} 弱势 ({s['avg_pnl_pct']:.1f}%)")
        lines.append("")

    # 5. 假信号
    if false_signals.get("false_signal_count", 0) > 0:
        lines.append("【假信号分析】")
        lines.append(
            f"  假信号 {false_signals['false_signal_count']} 只 "
            f"({false_signals['false_signal_rate']*100:.0f}%)"
        )
        for c in false_signals.get("characteristics", []):
            lines.append(f"  ⚠️ {c}")
        lines.append("")

    # 6. 自适应调整
    lines.append("【参数调整】")
    adaptive_reasons = adaptive.get("reasons", ["参数维持不变"])
    for r in adaptive_reasons:
        lines.append(f"  → {r}")
    lines.append(f"  初步建议仓位: {adaptive['new_params']['position_size_pct']*100:.0f}%（辩论确认后生效）")
    lines.append(f"  初步建议阈值: {adaptive['new_params']['scoring_threshold']}（辩论确认后生效）")
    lines.append("")

    # 7. 下周操作建议
    lines.append("【下周操作建议】")
    if adaptive['new_params']['position_size_pct'] < current_params()["position_size_pct"]:
        lines.append("  ⚠️ 仓位降低，建议谨慎操作")
    elif adaptive['new_params']['position_size_pct'] > current_params()["position_size_pct"]:
        lines.append("  ✅ 仓位提升，可以适度积极")
    else:
        lines.append("  ➡️ 策略稳定，维持现有节奏")
    lines.append(f"  重点关注: {[s['sector'] for s in sector.get('strongest_sectors', [])]}")

    return "\n".join(lines)


def current_params() -> Dict:
    """获取当前生效的参数（从 intraday_executor 读取）"""
    return load_params()


# ── 主函数 ─────────────────────────────────────────────

def run_weekly_review():
    """周复盘主入口"""
    today = date.today()
    week_end = today
    week_start = today - timedelta(days=today.weekday())

    logger.info("=" * 50)
    logger.info(f"周复盘系统启动 | {week_start} ~ {week_end}")
    logger.info("=" * 50)

    # 1. 收集数据
    logger.info("Step 1: 收集本周数据...")
    executions = load_executions()
    reports = load_reports()
    logger.info(f"  本周执行: {len(executions)} 笔, 报告: {len(reports)} 份")

    # 2. 实战表现分析
    logger.info("Step 2: 分析实战表现...")
    perf = analyze_performance(executions, reports, week_start, week_end)
    logger.info(f"  分析结果: {perf.get('status')}")

    # 3. 大盘环境（新：20日分类）
    logger.info("Step 3: 分析大盘环境（20日）...")
    market_regime_result = classify_market_regime()
    market_regime = market_regime_result.get("overall", "震荡")
    logger.info(f"  大盘环境: {market_regime} | {market_regime_result.get('indices', {})}")
    market = analyze_market_context(20)  # 20日数据

    # 4. 板块轮动
    logger.info("Step 4: 板块轮动分析...")
    sector = analyze_sector_rotation(perf.get("stocks", []))
    logger.info(f"  涉及板块: {len(sector.get('all_sectors', []))}")

    # 5. 假信号分析
    logger.info("Step 5: 假信号分析...")
    false_signals = analyze_false_signals(perf.get("stocks", []))

    # 6. 基准对比
    logger.info("Step 6: 基准对比...")
    benchmark = analyze_benchmark(perf.get("stocks", []), market)

    # 7. 自适应参数
    logger.info("Step 7: 计算自适应参数...")
    current_p = load_params()
    week_history = current_p.get("week_history", [])
    adaptive = calculate_adaptive_params(
        perf.get("stocks", []),
        current_p,
        week_history,
        market_regime=market_regime,
    )

    # 8. 保存本周记录（辩论后再确定参数，暂不保存 params.json）
    # week_record = {  # 暂存但未使用，待辩论确认后保存
    # 9. 生成报告
    report_text = generate_weekly_report(
        week_start, week_end, perf, market, sector,
        false_signals, benchmark, adaptive
    )

    # 10. 飞书推送
    webhook = os.getenv("FEISHU_WEBHOOK_URL")
    if webhook:
        try:
            import requests
            requests.post(
                webhook,
                json={"msg_type": "text", "content": {"text": report_text}},
                timeout=10
            )
            logger.info("飞书推送成功")
        except Exception as e:
            logger.error(f"飞书推送失败: {e}")

    # 辩论由 weekly_strategy/run_weekly_debate.py 在周六 09:00 独立执行（见 crontab）

    # 11. 保存报告
    report_file = OUTPUT_DIR / "weekly_review_latest.json"
    full_report = {
        "week_start": week_start.isoformat(),
        "week_end": week_end.isoformat(),
        "performance": perf,
        "market_context": market,
        "market_regime": market_regime,
        "sector_rotation": sector,
        "false_signals": false_signals,
        "benchmark": benchmark,
        "adaptive": adaptive,
        "debate": {"status": "pending"},  # 辩论在周六 09:00 由 weekly_strategy/run_weekly_debate.py 执行
        "report_text": report_text,
    }
    with open(report_file, "w") as f:
        json.dump(full_report, f, ensure_ascii=False, indent=2)
    logger.info(f"报告已保存: {report_file}")

    # ── 同步到知识库 ───────────────────────────────────
    try:
        KB_DIR = Path(os.environ.get("OPENCLAW_WORKSPACE", "./workspace")) / "knowledge-base" / "weekly-debates"
        KB_DIR.mkdir(parents=True, exist_ok=True)

        # 归档精简版周报（不含长字段）
        week_key = week_end.strftime("%Y%m%d")  # YYYYMMDD
        archive_path = KB_DIR / f"review_{week_key}.json"
        archive_data = {
            "week_start": week_start.isoformat(),
            "week_end": week_end.isoformat(),
            "performance": perf,
            "market_regime": market_regime,
            "adaptive": adaptive,
            "saved_at": datetime.now().isoformat(),
        }
        with open(archive_path, "w", encoding="utf-8") as f:
            json.dump(archive_data, f, ensure_ascii=False, indent=2)
        logger.info(f"周报归档已保存: {archive_path}")
    except Exception as e:
        logger.warning(f"周报归档失败: {e}")

    # 打印报告
    print("\n" + "=" * 50)
    print(report_text)

    logger.info("✅ 周复盘完成")

    # 清理trades.json中已结清的记录（remaining_quantity == 0）
    _cleanup_settled_records()

    # ── 自动触发辩论 ───────────────────────────────────
    logger.info("触发辩论流程...")
    try:
        result = subprocess.run(
            [sys.executable, "weekly_strategy/run_weekly_debate.py"],
            cwd=BASE_DIR,
            timeout=1800,
            capture_output=False,
        )
        if result.returncode == 0:
            logger.info("辩论流程完成")
        else:
            logger.warning(f"辩论流程异常退出: {result.returncode}")
    except subprocess.TimeoutExpired:
        logger.warning("辩论流程超时（30分钟）")
    except Exception as e:
        logger.warning(f"辩论流程启动失败: {e}")

    return full_report


def _cleanup_settled_records():
    """删除trades.json中已全部卖出的记录（复盘后执行）"""
    if not TRADES_FILE.exists():
        return
    try:
        with open(TRADES_FILE) as f:
            data = json.load(f)
        original = len(data.get("records", []))
        data["records"] = [
            r for r in data.get("records", [])
            if r.get("remaining_quantity", 0) > 0
        ]
        remaining = len(data["records"])
        if remaining < original:
            with open(TRADES_FILE, "w") as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
            logger.info(f"🧹 清理trades.json: 删除{original - remaining}条已结清记录，剩余{remaining}条")
        else:
            logger.info("🧹 trades.json无需清理")
    except Exception as e:
        logger.warning(f"清理trades.json失败: {e}")


if __name__ == "__main__":
    run_weekly_review()
