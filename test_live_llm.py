#!/usr/bin/env python3
"""
方案2测试：5只股票 LLM实时在线判断
- 每只股票保持一个持久化 message history
- 每分钟更新行情，追加进context
- LLM持续输出决策（BUY_NOW/WAIT/SKIP_TODAY等）
"""
import os, sys, json, time, logging, re, urllib.request
from datetime import datetime, date
from pathlib import Path

if os.environ.get("RUN_LIVE_LLM_TESTS") != "1":
    print("skipped: set RUN_LIVE_LLM_TESTS=1 to run live intraday LLM probe")
    raise SystemExit(0)

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("live_llm_test")

BASE_DIR = Path(__file__).parent
sys.path.insert(0, str(BASE_DIR.parent / "knowledge-base" / "xqshare"))
API_KEY = os.getenv("MX_APIKEY", "")
MODEL = "minimax-portal/MiniMax-M3"
STOCKS = ["002888", "000411", "000100"]

MINIMAX_API_KEY = os.getenv("MINIMAX_API_KEY", "")
if not MINIMAX_API_KEY:
    print("skipped: MINIMAX_API_KEY is not set")
    raise SystemExit(0)

# ── XQShare rpyc 客户端 ────────────────────────────────
_XQ = None

def get_xq_client():
    global _XQ
    if _XQ is not None:
        return _XQ
    from client import XtQuantRemote
    _XQ = XtQuantRemote(host="127.0.0.1", port=18812, log_level="WARNING")
    try:
        _XQ._ensure_connected()
    except:
        pass
    return _XQ

def get_1m_bars(stock: str, count: int = 120) -> list:
    """通过XQShare rpyc获取1分钟K线（有download_history_data workaround）"""
    xt = get_xq_client()
    sym = f"{stock}.SZ" if not stock.startswith("6") else f"{stock}.SH"
    try:
        # download_history_data workaround for same-day 1m data
        xt.xtdata.download_history_data(stock_code=sym, period="1m", start_time="", end_time="")
        data = xt.xtdata.get_market_data(
            field_list=["time", "open", "high", "low", "close", "volume"],
            stock_list=[sym], period="1m", count=count, end_time="", start_time="",
        )
    except Exception as e:
        logger.warning(f"XQShare 1m获取失败 {stock}: {e}")
        return []
    bars = []
    try:
        if hasattr(data, "iterrows"):
            for idx, row in data.iterrows():
                close = float(row.get("close", 0))
                if close <= 0:
                    continue
                bars.append({
                    "time": str(idx), "open": float(row.get("open", close)),
                    "high": float(row.get("high", close)), "low": float(row.get("low", close)),
                    "close": close, "volume": float(row.get("volume", 0)),
                })
        elif isinstance(data, dict):
            from collections import OrderedDict
            n = len(list(data.get("close", {}).values())) if data.get("close") else 0
            for i in range(n):
                bars.append({})
            for fld in ["open","high","low","close","volume"]:
                fld_data = data.get(fld, {})
                if not hasattr(fld_data, "items"):
                    continue
                for raw_t, val in fld_data.items():
                    import re
                    m = re.match(r"(\d{4})-(\d{2})-(\d{2}) (\d{2}):(\d{2})", str(raw_t))
                    bar_idx = -1
                    if m:
                        t_key = f"{m.group(1)}{m.group(2)}{m.group(3)}{m.group(4)}{m.group(5)}"
                        bar_idx = len(bars) - 1
                    else:
                        t_key = str(raw_t)
                        bar_idx = len(bars) - 1
                    val_f = float(val) if not isinstance(val, dict) else float(val.get(sym, 0))
                    if val_f <= 0:
                        continue
                    if bar_idx >= 0 and bar_idx < len(bars):
                        bars[bar_idx][fld] = val_f
                        if fld == "close" and "time" not in bars[bar_idx]:
                            bars[bar_idx]["time"] = str(raw_t)
    except Exception as e:
        logger.warning(f"1m解析失败 {stock}: {e}")
    today = date.today()
    today_str = today.strftime("%Y%m%d")
    result = [b for b in bars if b.get("time") and today_str in str(b.get("time",""))]
    if not result:
        result = bars
    return result
    # 过滤，只留今日数据
    today_str = date.today().strftime("%Y%m%d")
    result = []
    for bar in bars:
        t = str(bar.get("time", ""))
        if t.startswith(today_str) or t.startswith(date.today().strftime("%Y-%m-%d")):
            result.append(bar)
    if not result and bars:
        result = bars[-count:]  # 兜底返回最近的数据
    return result

def get_quote(stock: str) -> dict:
    """用XQShare HTTP API获取实时报价"""
    import urllib.request
    XQ_HTTP = "http://127.0.0.1:8080"
    sym = f"{stock}.SZ" if not stock.startswith("6") else f"{stock}.SH"
    try:
        url = f"{XQ_HTTP}/full_tick?stocks={sym}"
        with urllib.request.urlopen(url, timeout=6) as r:
            d = json.loads(r.read().decode())
        if d.get("success"):
            data = d.get("data", {})
            tick = data.get(sym, data.get(stock, next(iter(data.values()), {})))
            if tick:
                price = float(tick.get("lastPrice", tick.get("close", 0)))
                pct = float(tick.get("pctChg", 0))
                return {"price": price, "change_pct": pct}
    except:
        pass
    return {"price": 0, "change_pct": 0}

# ── 技术指标 ─────────────────────────────────────────────
def sma(data: list, n: int) -> float:
    if len(data) < n:
        return None
    return sum(data[-n:]) / n

def technical_snapshot(stock: str, quote: dict, bars: list) -> dict:
    if not bars:
        return {}
    closes = [b.get("close", 0) for b in bars if b.get("close")]
    latest = closes[-1] if closes else 0
    prev = closes[-2] if len(closes) >= 2 else latest
    change_pct = quote.get("change_pct", 0)
    ma = {}
    above = []
    crossed = []
    for w in [5, 20, 60, 120]:
        mv = sma(closes, w)
        if mv:
            ma[f"ma{w}"] = round(mv, 4)
            if latest > mv:
                above.append(f"ma{w}")
            if len(closes) >= w + 1:
                prev_mv = sma(closes[:-1], w)
                if prev_mv and prev <= prev_mv and latest > mv:
                    crossed.append(f"ma{w}")
    return {
        "stock": stock, "latest": latest, "bar_count": len(bars),
        "change_pct": change_pct, "ma": ma,
        "above_ma": above, "crossed_up_ma": crossed,
    }

# ── LLM调用 ─────────────────────────────────────────────
def call_live_llm(messages: list, timeout: int = 60) -> dict:
    url = "https://api.minimaxi.com/anthropic/v1/messages"
    headers = {
        "Authorization": f"Bearer {MINIMAX_API_KEY}",
        "Content-Type": "application/json",
        "anthropic-version": "2023-06-01",
    }
    body = {
        "model": "MiniMax-M3",
        "messages": messages,
        "temperature": 0.3,
        "max_tokens": 800,
    }
    req = urllib.request.Request(
        url, data=json.dumps(body, ensure_ascii=False).encode("utf-8"),
        headers=headers, method="POST",
    )
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        raw = resp.read().decode("utf-8")
    result = json.loads(raw)
    for block in result.get("content", []):
        if block.get("type") == "text":
            text = block["text"].strip()
            for pat in [r"```json\s*(\{.*?\})\s*```", r"```\s*(\{.*?})\s*```", r"(\{.*?\})"]:
                m = re.search(pat, text, re.DOTALL)
                if m:
                    try:
                        obj = json.loads(m.group(1))
                        if isinstance(obj, dict) and "action" in obj:
                            return obj
                    except:
                        pass
    return {"action": "WAIT", "confidence": 0, "reason": "解析失败"}

def build_system_prompt() -> str:
    return """你是盘中分时买入实时判断专家。每分钟接收最新行情和技术指标，输出决策。

决策规则：
- BUY_NOW：有支撑、走势未坏、位置尚可且报价可执行 → 立即买入
- WAIT：技术面偏空、横盘无方向、反弹力度一般 → 继续观察
- SKIP_TODAY：14:30后且趋势明确破坏 → 当天永久放弃

只关注日内1分钟K线、MA5/MA20/MA60/MA120、量价配合、日内高低位。

输出格式（纯JSON，不要任何其他文字）：
{"action": "BUY_NOW|WAIT|SKIP_TODAY", "price_mode": "NONE|FOLLOW|PASSIVE|DIP|CUSTOM", "limit_price": null, "max_premium_pct": 0.0, "confidence": 0-100, "reason": "一句话理由"}"""

def build_user_prompt(stock: str, quote: dict, snap: dict) -> str:
    ma = snap.get("ma", {})
    return f"""{stock} | {quote.get('price')}元 | {quote.get('change_pct'):+.2f}% | {datetime.now().strftime('%H:%M')}
MA: ma5={ma.get('ma5')} ma20={ma.get('ma20')} ma60={ma.get('ma60')} ma120={ma.get('ma120')}
K线: {snap.get('bar_count')}根 | 上穿:{snap.get('crossed_up_ma')} | 均线之上:{snap.get('above_ma')}
输出JSON决策："""

# ── 核心测试 ──────────────────────────────────────────────
def run_test():
    logger.info(f"方案2测试启动 | 模型={MODEL} | 股票={STOCKS}")

    # 先测试1分钟K线数据是否正常
    test_stock = STOCKS[0]
    bars = get_1m_bars(test_stock, 10)
    logger.info(f"1分钟K线测试({test_stock}): {len(bars)}根 | {bars[-1] if bars else '空'}")

    conversations = {}
    for stock in STOCKS:
        conversations[stock] = [{"role": "system", "content": build_system_prompt()}]

    # 初始化
    for stock in STOCKS:
        quote = get_quote(stock)
        bars = get_1m_bars(stock, 120)
        snap = technical_snapshot(stock, quote, bars)
        msg = {"role": "user", "content": build_user_prompt(stock, quote, snap)}
        conversations[stock].append(msg)
        resp = call_live_llm(conversations[stock])
        conversations[stock].append({"role": "assistant", "content": json.dumps(resp, ensure_ascii=False)})
        logger.info(f"{stock} 初始化: {resp.get('action')} @{quote.get('price')} | 置信{resp.get('confidence')} | {resp.get('reason')}")
        time.sleep(2)

    logger.info("=" * 50)
    logger.info("初始化完成，开始实时循环")
    logger.info("=" * 50)

    round_num = 0
    while True:
        now = datetime.now()
        if now.hour >= 15:
            logger.info("已过15:00，停止")
            break
        round_num += 1
        logger.info(f"\n--- 第{round_num}轮 {now.strftime('%H:%M:%S')} ---")
        for stock in STOCKS:
            quote = get_quote(stock)
            bars = get_1m_bars(stock, 10)
            snap = technical_snapshot(stock, quote, bars)
            msg = {"role": "user", "content": build_user_prompt(stock, quote, snap)}
            conversations[stock].append(msg)
            try:
                resp = call_live_llm(conversations[stock])
            except Exception as e:
                logger.error(f"{stock} LLM失败: {e}")
                resp = {"action": "WAIT", "confidence": 0, "reason": str(e)}
            conversations[stock].append({"role": "assistant", "content": json.dumps(resp, ensure_ascii=False)})
            logger.info(f"  {stock}: {resp.get('action')} @{quote.get('price')} | 置信{resp.get('confidence')} | {resp.get('reason')}")
            if len(conversations[stock]) > 22:
                conversations[stock] = [conversations[stock][0]] + conversations[stock][-20:]
            time.sleep(2)

        # 等下一分钟
        next_min = now.replace(second=0, microsecond=0)
        if now.second >= 5:
            next_min = next_min.replace(minute=next_min.minute + 1)
        wait = (next_min - now).total_seconds()
        logger.info(f"  等待{wait:.0f}秒")
        time.sleep(max(10, min(wait, 60)))

if __name__ == "__main__":
    run_test()
