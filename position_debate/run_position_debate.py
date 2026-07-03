#!/usr/bin/env python3
"""
持仓辩论主入口（周日）
=====================
对齐 TradingAgents v0.2.5 单股票分析流程：

持仓来源：mx-moni 妙想模拟账户实时持仓
辩论流程：Bull/Bear → Research Manager → Trader → 三风控 → Portfolio Manager
输出：每只股票的 BUY/HOLD/SELL 决策

用法：
  python3 run_stock_debate.py
  python3 run_stock_debate.py --stocks 5    # 最多辩论5只（按浮盈亏排序）
  python3 run_stock_debate.py --input positions.json  # 从文件读取持仓
"""

import os, sys, json, time, logging, argparse
from pathlib import Path
from datetime import datetime, date
from typing import Dict, Any, List, Optional

BASE_DIR = Path(__file__).parent.parent
OUTPUT_DIR = BASE_DIR / "output"
LOG_DIR = BASE_DIR / "logs"
QMT_HTTP_HOST = os.environ.get("QMT_HTTP_HOST", "127.0.0.1")
QMT_HTTP_PORT = int(os.environ.get("QMT_HTTP_PORT", "8080"))
MX_APIKEY = os.environ.get("MX_APIKEY", "")
MX_API_URL = os.environ.get("MX_API_URL", "https://mkapi2.dfcfs.com/finskillshub")


def fetch_stock_news(stock_code: str, stock_name: str, max_results: int = 5) -> List[Dict]:
    """
    通过 mx-search 获取个股近3天资讯，返回去重后的新闻列表。
    失败时返回空列表，不阻断辩论流程。
    """
    if not MX_APIKEY:
        return []
    import requests as _req
    url = "https://mkapi2.dfcfs.com/finskillshub/api/claw/news-search"
    headers = {"Content-Type": "application/json", "apikey": MX_APIKEY}
    query = f"{stock_name} 2026年5月"
    try:
        resp = _req.post(url, headers=headers, json={"query": query}, timeout=20)
        if resp.status_code != 200:
            return []
        data = resp.json()
        # 解析 mx-search 返回格式：data.data.data.llmSearchResponse.data
        raw = data if isinstance(data, dict) else {}
        items = raw.get("data", {}).get("data", {}).get("llmSearchResponse", {}).get("data", [])
        if not items:
            return []
        news_items = []
        for item in items[:max_results]:
            title = str(item.get("title", "")[:80])
            if not title or len(title) < 5:
                continue
            content = str(item.get("content", "")[:200])
            date = str(item.get("date", "")[:16])
            source = str(item.get("source", ""))
            news_items.append({
                "title": title,
                "content": content,
                "date": date,
                "source": source,
            })
        return news_items
    except Exception as e:
        logger.warning(f"获取{stock_code}新闻失败: {e}")
        return []


def _safe_float(val, default=0.0):
    try:
        return float(val) if val is not None else default
    except (ValueError, TypeError):
        return default


def _ema(closes: List[float], n: int) -> float:
    if len(closes) < n:
        return sum(closes) / len(closes) if closes else 0.0
    k = 2.0 / (n + 1)
    ema = sum(closes[:n]) / n
    for price in closes[n:]:
        ema = price * k + ema * (1 - k)
    return ema


def _rsi_position(closes: List[float], period: int = 20) -> float:
    if len(closes) < period:
        return 50.0
    window = closes[-period:]
    mn, mx = min(window), max(window)
    return ((closes[-1] - mn) / (mx - mn) * 100) if mx != mn else 100.0


def get_tech_indicators(stock_code: str, days: int = 60) -> Dict[str, Any]:
    """从 QMT HTTP API 获取近 N 日 K 线，计算 RSI/MACD/MA 技术指标"""
    suffix = ".SZ" if stock_code.startswith(("000", "001", "002", "003", "300", "301")) else ".SH"
    url = f"http://{QMT_HTTP_HOST}:{QMT_HTTP_PORT}/market_data3?stock={stock_code}{suffix}&period=1d&count={days}"
    try:
        import urllib.request
        req = urllib.request.Request(url)
        with urllib.request.urlopen(req, timeout=10) as resp:
            result = json.loads(resp.read().decode())
        if not result or not result.get("success"):
            return {}
        close_data = result.get("data", {}).get("close", {})
        dates = sorted(close_data.keys(), reverse=True)
        if len(dates) < 20:
            return {}
        closes = [float(close_data[d].get(f"{stock_code}{suffix}", 0))
                  for d in dates if close_data[d].get(f"{stock_code}{suffix}")]
        if len(closes) < 20:
            return {}
        closes = closes[::-1]
        current = closes[-1]
        prev_close = closes[-2] if len(closes) > 1 else current

        # RSI(14)
        deltas = [closes[i] - closes[i - 1] for i in range(1, len(closes))]
        gains = [d if d > 0 else 0 for d in deltas[-14:]]
        losses = [-d if d < 0 else 0 for d in deltas[-14:]]
        avg_gain = sum(gains) / 14 if gains else 0
        avg_loss = sum(losses) / 14 if losses else 0
        rs = avg_gain / avg_loss if avg_loss > 0 else 999
        rsi_14 = round(100 - 100 / (1 + rs), 1) if avg_loss > 0 else 100

        # RSI 20日位置
        rsi_pos = round(_rsi_position(closes, 20), 1)

        # MACD (12/26/9)
        ema12 = _ema(closes, 12)
        ema26 = _ema(closes, 26)
        macd_val = round(ema12 - ema26, 3)
        signal = _ema([ema12 - ema26] * min(9, len(closes)), 9)
        macd_hist = round(macd_val - signal, 3) if signal else 0

        # 均线
        ma5 = round(sum(closes[-5:]) / 5, 2) if len(closes) >= 5 else current
        ma10 = round(sum(closes[-10:]) / 10, 2) if len(closes) >= 10 else current
        ma20 = round(sum(closes[-20:]) / 20, 2) if len(closes) >= 20 else current
        ma60 = round(sum(closes[-60:]) / 60, 2) if len(closes) >= 60 else current

        ma_order = "多头" if ma5 > ma10 > ma20 else ("空头" if ma5 < ma10 < ma20 else "混乱")
        trend = "上升" if current > ma20 else ("下降" if current < ma20 else "震荡")

        high20, low20 = max(closes[-20:]), min(closes[-20:]) if len(closes) >= 20 else (current, current)
        price_pos_20d = round((current - low20) / (high20 - low20) * 100, 1) if high20 != low20 else 50
        gain_5d = round((current / closes[-6] - 1) * 100, 2) if len(closes) > 5 else 0

        return {
            "close": current,
            "pct_change": round((current / prev_close - 1) * 100, 2) if prev_close else 0,
            "rsi_14": rsi_14,
            "rsi_position_20d": rsi_pos,
            "macd": macd_val,
            "macd_signal": "金叉" if macd_val > signal else "死叉",
            "macd_hist": macd_hist,
            "ma5": ma5, "ma10": ma10, "ma20": ma20, "ma60": ma60,
            "ma_order": ma_order,
            "trend": trend,
            "price_position_20d": price_pos_20d,
            "gain_5d": gain_5d,
            "days": len(closes),
        }
    except Exception as e:
        logger.warning(f"获取{stock_code}技术指标失败: {e}")
        return {}


def build_market_report(stock_code: str, stock_name: str) -> str:
    """构建技术分析报告，注入知识库蜡烛图+均线标准"""
    tech = get_tech_indicators(stock_code)
    news = fetch_stock_news(stock_code, stock_name)
    ref_file = BASE_DIR / "technical_analysis_reference.md"
    kb_text = ""
    if ref_file.exists():
        kb = ref_file.read_text(encoding="utf-8")
        kb_text = "\n\n=== 【技术分析参考标准】===\n" + kb[:3500]

    if not tech:
        lines = [f"（{stock_name}技术数据获取失败）"]
    else:
        lines = [
            f"【{stock_name}({stock_code})技术分析】",
            f"最新价: {tech['close']}  涨跌: {tech['pct_change']:+.2f}%",
            f"均线(5/10/20/60日): {tech['ma5']} / {tech['ma10']} / {tech['ma20']} / {tech['ma60']}",
            f"均线排列: {tech['ma_order']}  趋势: {tech['trend']}",
            f"RSI(14): {tech['rsi_14']}  RSI位置(20日): {tech['rsi_position_20d']}/100",
            f"MACD: {tech['macd']}  信号: {tech['macd_signal']}  柱值: {tech['macd_hist']}",
            f"价格位置(20日): {tech['price_position_20d']}/100  5日涨幅: {tech['gain_5d']:+.2f}%",
        ]
        rsi = tech['rsi_14']
        if rsi > 75:
            lines.append(f"⚠️ RSI超买(>{75})，注意回调风险")
        elif rsi < 35:
            lines.append(f"⚠️ RSI超卖(<{35})，注意反弹机会")
        if tech['trend'] == '下降':
            lines.append(f"⚠️ 价格跌破20日均线({tech['ma20']})，短期趋势偏空")
        elif tech['trend'] == '上升':
            lines.append(f"✅ 价格站稳20日均线上方，多头延续")

    # 注入新闻
    if news:
        lines.append("\n【近3日个股资讯】")
        for n in news[:5]:
            lines.append(f"  • {n['title']}: {n['content'][:100]}")
    else:
        lines.append("\n【近3日个股资讯】（暂无）")

    return "\n".join(lines) + kb_text


def get_tech_indicators(stock_code: str, days: int = 60) -> Dict[str, Any]:
    """从 QMT HTTP API 获取近 N 日 K 线，计算 RSI/MACD/MA 技术指标"""
    suffix = ".SZ" if stock_code.startswith(("000", "001", "002", "003", "300", "301")) else ".SH"
    url = f"http://{QMT_HTTP_HOST}:{QMT_HTTP_PORT}/market_data3?stock={stock_code}{suffix}&period=1d&count={days}"
    try:
        import urllib.request
        req = urllib.request.Request(url)
        with urllib.request.urlopen(req, timeout=10) as resp:
            result = json.loads(resp.read().decode())
        if not result or not result.get("success"):
            return {}
        close_data = result.get("data", {}).get("close", {})
        dates = sorted(close_data.keys(), reverse=True)
        if len(dates) < 20:
            return {}
        closes = [float(close_data[d].get(f"{stock_code}{suffix}", 0))
                  for d in dates if close_data[d].get(f"{stock_code}{suffix}")]
        if len(closes) < 20:
            return {}
        closes = closes[::-1]
        current = closes[-1]
        prev_close = closes[-2] if len(closes) > 1 else current

        # RSI(14)
        deltas = [closes[i] - closes[i - 1] for i in range(1, len(closes))]
        gains = [d if d > 0 else 0 for d in deltas[-14:]]
        losses = [-d if d < 0 else 0 for d in deltas[-14:]]
        avg_gain = sum(gains) / 14 if gains else 0
        avg_loss = sum(losses) / 14 if losses else 0
        rs = avg_gain / avg_loss if avg_loss > 0 else 999
        rsi_14 = round(100 - 100 / (1 + rs), 1) if avg_loss > 0 else 100

        # RSI 20日位置
        rsi_pos = round(_rsi_position(closes, 20), 1)

        # MACD (12/26/9)
        ema12 = _ema(closes, 12)
        ema26 = _ema(closes, 26)
        macd_val = round(ema12 - ema26, 3)
        signal = _ema([ema12 - ema26] * min(9, len(closes)), 9)
        macd_hist = round(macd_val - signal, 3) if signal else 0

        # 均线
        ma5 = round(sum(closes[-5:]) / 5, 2) if len(closes) >= 5 else current
        ma10 = round(sum(closes[-10:]) / 10, 2) if len(closes) >= 10 else current
        ma20 = round(sum(closes[-20:]) / 20, 2) if len(closes) >= 20 else current
        ma60 = round(sum(closes[-60:]) / 60, 2) if len(closes) >= 60 else current

        ma_order = "多头" if ma5 > ma10 > ma20 else ("空头" if ma5 < ma10 < ma20 else "混乱")
        trend = "上升" if current > ma20 else ("下降" if current < ma20 else "震荡")

        high20, low20 = max(closes[-20:]), min(closes[-20:]) if len(closes) >= 20 else (current, current)
        price_pos_20d = round((current - low20) / (high20 - low20) * 100, 1) if high20 != low20 else 50
        gain_5d = round((current / closes[-6] - 1) * 100, 2) if len(closes) > 5 else 0

        return {
            "close": current,
            "pct_change": round((current / prev_close - 1) * 100, 2) if prev_close else 0,
            "rsi_14": rsi_14,
            "rsi_position_20d": rsi_pos,
            "macd": macd_val,
            "macd_signal": "金叉" if macd_val > signal else "死叉",
            "macd_hist": macd_hist,
            "ma5": ma5, "ma10": ma10, "ma20": ma20, "ma60": ma60,
            "ma_order": ma_order,
            "trend": trend,
            "price_position_20d": price_pos_20d,
            "gain_5d": gain_5d,
            "days": len(closes),
        }
    except Exception as e:
        logger.warning(f"获取{stock_code}技术指标失败: {e}")
        return {}


def build_market_report(stock_code: str, stock_name: str) -> str:
    """构建技术分析报告，注入知识库蜡烛图+均线标准+个股资讯"""
    tech = get_tech_indicators(stock_code)
    news = fetch_stock_news(stock_code, stock_name)
    ref_file = BASE_DIR / "technical_analysis_reference.md"
    kb_text = ""
    if ref_file.exists():
        kb = ref_file.read_text(encoding="utf-8")
        kb_text = "\n\n=== 【技术分析参考标准】===\n" + kb[:3500]

    if not tech:
        lines = [f"（{stock_name}技术数据获取失败）"]
    else:
        lines = [
            f"【{stock_name}({stock_code})技术分析】",
            f"最新价: {tech['close']}  涨跌: {tech['pct_change']:+.2f}%",
            f"均线(5/10/20/60日): {tech['ma5']} / {tech['ma10']} / {tech['ma20']} / {tech['ma60']}",
            f"均线排列: {tech['ma_order']}  趋势: {tech['trend']}",
            f"RSI(14): {tech['rsi_14']}  RSI位置(20日): {tech['rsi_position_20d']}/100",
            f"MACD: {tech['macd']}  信号: {tech['macd_signal']}  柱值: {tech['macd_hist']}",
            f"价格位置(20日): {tech['price_position_20d']}/100  5日涨幅: {tech['gain_5d']:+.2f}%",
        ]
        rsi = tech['rsi_14']
        if rsi > 75:
            lines.append(f"⚠️ RSI超买(>{75})，注意回调风险")
        elif rsi < 35:
            lines.append(f"⚠️ RSI超卖(<{35})，注意反弹机会")
        if tech['trend'] == '下降':
            lines.append(f"⚠️ 价格跌破20日均线({tech['ma20']})，短期趋势偏空")
        elif tech['trend'] == '上升':
            lines.append(f"✅ 价格站稳20日均线上方，多头延续")

    # 注入新闻
    if news:
        lines.append("\n【近3日个股资讯】")
        for n in news[:5]:
            lines.append(f"  • {n['title']}: {n['content'][:100]}")
    else:
        lines.append("\n【近3日个股资讯】（暂无）")

    return "\n".join(lines) + kb_text

for d in [OUTPUT_DIR, LOG_DIR]:
    d.mkdir(exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[
        logging.FileHandler(LOG_DIR / f"stock_debate_{date.today().strftime('%Y%m%d')}.log"),
        logging.StreamHandler(),
    ],
)
logger = logging.getLogger("run_stock_debate")


# ── mx-moni 持仓获取 ─────────────────────────────────────

def get_positions_from_mx() -> List[Dict]:
    """从 mx-moni 获取当前持仓列表"""
    import requests

    if not MX_APIKEY:
        logger.error("MX_APIKEY 未配置，请先设置环境变量")
        return []

    url = f"{MX_API_URL}/api/claw/mockTrading/positions"
    headers = {"apikey": MX_APIKEY, "Content-Type": "application/json"}

    try:
        resp = requests.post(url, headers=headers, json={"moneyUnit": 1}, timeout=30)
        resp.raise_for_status()
        data = resp.json()

        if data.get("code") not in (0, "0", 200, "200"):
            logger.error(f"mx-moni 返回错误: {data}")
            return []

        # 字段映射：API 返回 posList，字段名是 secCode/secName/costPrice/price/profitPct
        pos_list = data.get("data", {}).get("posList", []) or data.get("data", {}).get("positions", []) or []
        logger.info(f"获取到 {len(pos_list)} 只持仓")

        return [
            {
                "stock_code": p.get("secCode", ""),
                "stock_name": p.get("secName", ""),
                "buy_price": float(p.get("costPrice", 0) or 0) / 100,
                "current_price": float(p.get("price", 0) or 0) / 100,
                "pnl_pct": float(p.get("profitPct", 0) or 0),
                "total_pnl_pct": float(p.get("profitPct", 0) or 0),
                "volume": int(p.get("count", 0) or 0),
                "market": "sh" if p.get("secMkt") == 1 else "sz",
            }
            for p in pos_list
            if p.get("count", 0) > 0
        ]
    except Exception as e:
        logger.error(f"获取持仓失败: {e}")
        return []


def load_positions_from_file(path: Path) -> List[Dict]:
    """从 JSON 文件加载持仓列表"""
    with open(path) as f:
        data = json.load(f)

    if isinstance(data, list):
        return data
    if isinstance(data, dict) and "data" in data:
        return data["data"]
    return []


def get_pe_for_stock(stock_code: str) -> Optional[float]:
    """获取股票 PE（TTM），腾讯行情优先，akshare 兜底"""
    # 方式1：腾讯行情（最快）
    try:
        import requests as _req
        suffix = "sz" if stock_code.startswith(("000","001","002","300","301")) else "sh"
        url = f"https://qt.gtimg.cn/q={suffix}{stock_code}"
        resp = _req.get(url, timeout=5)
        if resp.status_code == 200:
            fields = resp.text.split("~")
            if len(fields) > 39 and fields[39] not in ("", "-", "NA", "None", "0"):
                return float(fields[39])
    except Exception:
        pass

    # 方式2：akshare（东方财富）
    try:
        import akshare as _ak
        # 获取实时行情，再从中取 PE
        df = _ak.stock_zh_a_spot_em()
        row = df[df["代码"] == stock_code]
        if not row.empty:
            pe = row.iloc[0].get("市盈率")
            if pe is not None and pe > 0:
                return float(pe)
    except Exception:
        pass

    return None


def enrich_with_pe_and_technicals(positions: List[Dict]) -> List[Dict]:
    """给持仓列表补充 PE 和 technicals（含 RSI）"""
    for pos in positions:
        code = pos.get("stock_code", "")
        pos["pe"] = get_pe_for_stock(code)
        tech = get_tech_indicators(code)
        pos["technicals"] = {
            "rsi": tech.get("rsi_14"),
            "rsi_position_20d": tech.get("rsi_position_20d"),
            "macd": tech.get("macd"),
            "macd_signal": tech.get("macd_signal"),
            "ma5": tech.get("ma5"),
            "ma10": tech.get("ma10"),
            "ma20": tech.get("ma20"),
            "trend": tech.get("trend"),
            "ma_order": tech.get("ma_order"),
        }
    return positions


# ── 主辩论流程 ─────────────────────────────────────────────

def run_single_stock_debate(
    stock: Dict,
    graph,
    max_debate_rounds: int = 1,
    max_risk_rounds: int = 1,
) -> Dict:
    """对单只股票运行完整辩论流程"""
    stock_code = stock.get("stock_code", "")
    stock_name = stock.get("stock_name", "")

    init_state = {
        "stock_code": stock_code,
        "stock_name": stock_name,
        "trade_date": date.today().isoformat(),
        "market_report": build_market_report(stock_code, stock_name),
        "sentiment_report": "",
        "news_report": "",
        "fundamentals_report": "",
        "investment_debate_state": {
            "bull_history": "",
            "bear_history": "",
            "history": "",
            "current_response": "",
            "judge_decision": "",
            "count": 0,
        },
        "investment_plan": "",
        "trader_investment_plan": "",
        "risk_debate_state": {
            "aggressive_history": "",
            "conservative_history": "",
            "neutral_history": "",
            "history": "",
            "latest_speaker": "",
            "current_aggressive_response": "",
            "current_conservative_response": "",
            "current_neutral_response": "",
            "judge_decision": "",
            "count": 0,
        },
        "final_trade_decision": "",
        "past_context": "",
        "buy_price": stock.get("buy_price", 0),
        "current_price": stock.get("current_price", 0),
        "pnl_pct": stock.get("pnl_pct", 0) / 100,
        "action": "HOLD",
    }

    try:
        result = graph.invoke(init_state)

        # 解析最终决策
        final_decision = result.get("final_trade_decision", "")
        investment_plan = result.get("investment_plan", "")
        trader_plan = result.get("trader_investment_plan", "")

        # 从决策文本中提取 Rating
        rating = "Hold"
        investment_plan_lower = investment_plan.lower()
        trader_plan_lower = trader_plan.lower()
        if "sell" in investment_plan_lower or "清仓" in investment_plan:
            rating = "Sell"
        elif "buy" in investment_plan_lower and "sell" not in investment_plan_lower:
            rating = "Buy"
        elif "sell" in trader_plan_lower or "清仓" in trader_plan:
            rating = "Sell"
        elif "buy" in trader_plan_lower and "sell" not in trader_plan_lower:
            rating = "Buy"
        
        # 最终决策优先从 investment_plan 或 trader_proposal 提取
        final_decision = investment_plan if investment_plan else (trader_plan if trader_plan else "")

        return {
            "stock_code": stock_code,
            "stock_name": stock_name,
            "rating": rating,
            "final_decision": final_decision,
            "investment_plan": investment_plan,
            "trader_proposal": trader_plan,
            "pnl_pct": stock.get("pnl_pct", 0),
            "buy_price": stock.get("buy_price", 0),
            "current_price": stock.get("current_price", 0),
            "volume": stock.get("volume", 0),
            "pe": stock.get("pe"),
            "technicals": stock.get("technicals", {}),
        }
    except Exception as e:
        logger.error(f"[{stock_code}] 辩论异常: {e}")
        return {
            "stock_code": stock_code,
            "stock_name": stock_name,
            "rating": "Hold",
            "final_decision": f"辩论失败: {e}",
            "investment_plan": "",
            "trader_proposal": "",
            "pnl_pct": stock.get("pnl_pct", 0),
            "buy_price": stock.get("buy_price", 0),
            "current_price": stock.get("current_price", 0),
            "volume": stock.get("volume", 0),
            "pe": stock.get("pe"),
            "technicals": stock.get("technicals", {}),
            "error": str(e),
        }


def run_stock_debates(
    stocks: List[Dict],
    max_debate_rounds: int = 1,
    max_risk_rounds: int = 1,
) -> List[Dict]:
    """对每持仓股票运行完整辩论"""
    import sys as _sys
    _parent = Path(__file__).parent.parent  # daily-stock-workflow/
    if str(_parent) not in _sys.path:
        _sys.path.insert(0, str(_parent))

    from .graph import get_stock_debate_graph

    graph = get_stock_debate_graph(max_debate_rounds, max_risk_rounds)

    results = []
    total = len(stocks)

    for i, stock in enumerate(stocks):
        logger.info(f"[{i+1}/{total}] 辩论: {stock.get('stock_code')} {stock.get('stock_name')}")
        start = time.time()
        result = run_single_stock_debate(stock, graph, max_debate_rounds, max_risk_rounds)
        elapsed = time.time() - start
        result["elapsed_seconds"] = round(elapsed, 1)
        results.append(result)
        logger.info(f"[{i+1}/{total}] 完成: {result.get('rating')}, 耗时 {elapsed:.1f}s")

    return results


def apply_decisions_and_archive(results: List[Dict]) -> None:
    """将辩论决策归档到知识库"""
    KB_DIR = BASE_DIR / "knowledge-base"
    KB_DIR.mkdir(exist_ok=True)

    try:
        week_key = date.today().strftime("%Y%m%d")
        archive_path = KB_DIR / f"stock_debate_{week_key}.json"

        archive_data = {
            "generated_at": datetime.now().isoformat(),
            "method": "tradingagents_stock_debate",
            "results": results,
            "summary": {
                "total": len(results),
                "buy": sum(1 for r in results if r.get("rating") == "Buy"),
                "sell": sum(1 for r in results if r.get("rating") == "Sell"),
                "hold": sum(1 for r in results if r.get("rating") == "Hold"),
            },
        }

        with open(archive_path, "w", encoding="utf-8") as f:
            json.dump(archive_data, f, ensure_ascii=False, indent=2)
        logger.info(f"持仓辩论归档已保存: {archive_path}")

        # 更新索引
        index_path = KB_DIR / "_stock_debate_index.json"
        if index_path.exists():
            with open(index_path, encoding="utf-8") as f:
                index = json.load(f)
        else:
            index = {"debates": [], "summary": {"total": 0}}

        index["debates"].insert(0, {
            "date": date.today().isoformat(),
            "file": f"stock_debate_{week_key}.json",
            "total": len(results),
            "buy": sum(1 for r in results if r.get("rating") == "Buy"),
            "sell": sum(1 for r in results if r.get("rating") == "Sell"),
            "hold": sum(1 for r in results if r.get("rating") == "Hold"),
        })
        index["debates"] = index["debates"][:12]
        index["summary"]["total"] = len(index["debates"])

        with open(index_path, "w", encoding="utf-8") as f:
            json.dump(index, f, ensure_ascii=False, indent=2)
        logger.info(f"索引已更新: {index_path}")
    except Exception as e:
        logger.warning(f"知识库归档失败: {e}")


def main():
    parser = argparse.ArgumentParser(description="持仓辩论（对齐 TradingAgents v0.2.5 单股票流程）")
    parser.add_argument("--input", help="持仓JSON文件路径（默认从mx-moni获取）")
    parser.add_argument("--output", help="输出文件路径")
    parser.add_argument("--stocks", type=int, default=0, help="最多辩论N只股票（0=全部，按浮盈亏排序）")
    parser.add_argument("--max-debate-rounds", type=int, default=1, help="投资辩论最大轮数（默认1）")
    parser.add_argument("--max-risk-rounds", type=int, default=1, help="风险辩论最大轮数（默认1）")
    args = parser.parse_args()

    # 获取持仓列表
    if args.input:
        positions = load_positions_from_file(Path(args.input))
        logger.info(f"从文件加载持仓: {len(positions)} 只")
        positions = enrich_with_pe_and_technicals(positions)
    else:
        positions = get_positions_from_mx()
        if not positions:
            logger.error("无法获取持仓，退出")
            sys.exit(1)
        # 补充 PE 和技术指标
        positions = enrich_with_pe_and_technicals(positions)

    if not positions:
        logger.error("持仓列表为空，退出")
        sys.exit(1)

    # 按浮盈亏排序（最差的先辩论，或最好的先辩论）
    positions = sorted(positions, key=lambda x: x.get("pnl_pct", 0), reverse=True)

    # 限制数量
    if args.stocks > 0:
        positions = positions[:args.stocks]
        logger.info(f"限制辩论数量: {args.stocks} 只")

    logger.info(f"开始持仓辩论: {len(positions)} 只")
    logger.info(f"持仓列表: {[p['stock_code'] for p in positions]}")

    # 运行辩论
    results = run_stock_debates(
        positions,
        max_debate_rounds=args.max_debate_rounds,
        max_risk_rounds=args.max_risk_rounds,
    )

    # 归档
    apply_decisions_and_archive(results)

    # 输出
    if args.output:
        output_path = Path(args.output)
    else:
        output_path = OUTPUT_DIR / "position_debate_result.json"

    with open(output_path, "w", encoding="utf-8") as f:
        json.dump({"generated_at": datetime.now().isoformat(), "results": results}, f, ensure_ascii=False, indent=2)

    logger.info(f"结果已保存: {output_path}")

    # 打印摘要
    buy_count = sum(1 for r in results if r.get("rating") == "Buy")
    sell_count = sum(1 for r in results if r.get("rating") == "Sell")
    hold_count = sum(1 for r in results if r.get("rating") == "Hold")

    print(f"\n{'='*60}")
    print(f"持仓辩论完成 | 共 {len(results)} 只 | 耗时 {sum(r.get('elapsed_seconds', 0) for r in results):.0f}s")
    print(f"{'='*60}")
    print(f"Buy(加仓):  {buy_count} 只")
    print(f"Sell(清仓): {sell_count} 只")
    print(f"Hold(维持): {hold_count} 只")
    print(f"{'='*60}")

    for r in results:
        pnl = r.get("pnl_pct", 0)
        rating = r.get("rating", "?")
        code = r.get("stock_code", "")
        name = r.get("stock_name", "")
        decision = r.get("final_decision", "")[:100]
        print(f"  [{rating}] {code} {name} ({pnl:+.2f}%)")
        print(f"         {decision}")

    print(f"{'='*60}")


if __name__ == "__main__":
    main()