"""
QMT HTTP API 客户端
用于从 Windows QMT HTTP 服务获取行情和技术指标数据
"""

import json
import logging
import os
import urllib.request
from typing import Dict, Optional, Any

import akshare as ak  # noqa: E402 (for fetch_sector_strength)
import pandas as pd  # noqa: E402 (for fetch_prev_close_and_pe)

logger = logging.getLogger("qmt_client")


QMT_HTTP_URL = os.getenv("QMT_HTTP_URL", "http://127.0.0.1:8080").rstrip("/")


def _get(url: str, timeout: int = 10) -> Optional[Dict]:
    """HTTP GET 请求"""
    try:
        req = urllib.request.Request(url)
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return json.loads(resp.read().decode())
    except Exception:
        return None


def is_qmt_available() -> bool:
    """检查 QMT HTTP 服务是否在线"""
    result = _get(f"{QMT_HTTP_URL}/health", timeout=3)
    return result is not None


def fetch_tech_data(stock_code: str) -> Optional[Dict]:
    """
    获取单只股票技术数据 via QMT HTTP API
    返回: {"rsi": float, "ma_trend": str, "vol_ratio": float} 或 None
    """
    if not is_qmt_available():
        return None

    try:
        # 并行请求：K线数据 + 技术指标
        import urllib.request
        import json
        import concurrent.futures

        market_url = f"{QMT_HTTP_URL}/market_data?stock={stock_code}&fields=close,volume&period=1d&count=20"
        tech_url = f"{QMT_HTTP_URL}/technical_indicators?stock={stock_code}&period=1d&count=5"

        results = {}
        with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
            f1 = executor.submit(_get, market_url, 8)
            f2 = executor.submit(_get, tech_url, 8)
            try:
                market_data = f1.result(timeout=8)
                results['market'] = market_data
            except Exception:
                results['market'] = None
            try:
                tech_data = f2.result(timeout=8)
                results['tech'] = tech_data
            except Exception:
                results['tech'] = None

        market = results.get('market')
        tech = results.get('tech')

        if not market or not tech:
            return None

        # ── RSI ──────────────────────────────────────────
        rsi_data = tech.get('data', {}).get('rsi', {})
        rsi_list = rsi_data.get('rsi', [])
        rsi = float(rsi_list[-1]) if rsi_list and rsi_list[-1] == rsi_list[-1] else None  # 过滤 NaN

        # ── MA5/MA20 趋势 ────────────────────────────────
        close_data = market.get('data', {}).get('close', {})
        # close_data: {"20260429": {"000001.SZ": 11.52}, ...}
        date_prices = []
        for date, stock_dict in sorted(close_data.items()):
            price = list(stock_dict.values())[0] if stock_dict else None
            if price:
                date_prices.append(float(price))

        if len(date_prices) < 20:
            return None

        ma5 = sum(date_prices[-5:]) / 5
        ma20 = sum(date_prices[-20:]) / 20
        ma_trend = "多头" if ma5 > ma20 else "空头"

        # ── 量比 ────────────────────────────────────────
        vol_data = market.get('data', {}).get('volume', {})
        vols = []
        for date, stock_dict in sorted(vol_data.items()):
            vol = list(stock_dict.values())[0] if stock_dict else 0
            vols.append(float(vol))

        if len(vols) < 5:
            return None

        vol_ma5 = sum(vols[-5:]) / 5
        vol_ratio = vols[-1] / vol_ma5 if vol_ma5 > 0 else 1.0

        return {
            "rsi": float(rsi) if rsi is not None else None,
            "ma_trend": ma_trend,
            "vol_ratio": round(float(vol_ratio), 2),
        }

    except Exception:
        return None


def fetch_financial_data(stock_code: str) -> Optional[Dict]:
    """
    获取单只股票财务数据 via QMT HTTP API
    注意：QMT 财务 API 需要预先下载数据，否则返回空
    目前仅用于尝试，失败则继续用 mx-data
    """
    if not is_qmt_available():
        return None

    # QMT 财务数据目前仍返回空，暂不使用
    return None


def fetch_prev_close_and_pe(stock_code: str) -> Optional[Dict]:
    """
    获取昨日收盘价、昨日涨幅、PE
    返回: {"prev_close": float, "yesterday_chg": float|None, "pe": float|None} 或 None

    数据源优先级:
    1. QMT HTTP K线 → 昨收 + 算昨日涨幅
    2. akshare → PE
    3. 腾讯财经 fallback → PE
    """
    prev_close = None
    day_before_close = None
    yesterday_chg = None
    pe = None

    # ── Step 1: QMT HTTP K线 → 昨收 + 昨日涨幅 ──────────────
    if is_qmt_available():
        try:
            market_url = f"{QMT_HTTP_URL}/market_data?stock={stock_code}&fields=close&period=1d&count=5"
            data = _get(market_url, timeout=8)
            if data:
                close_data = data.get("data", {}).get("close", {})
                # close_data: {"20260515": {"000001.SZ": 11.52}, "20260514": {...}, ...}
                sorted_dates = sorted(close_data.keys(), reverse=True)
                # sorted_dates[0]=今天, [1]=昨天, [2]=前天
                if len(sorted_dates) >= 2:
                    prev_val = _extract_close(close_data.get(sorted_dates[1], {}))
                    bef_val = _extract_close(close_data.get(sorted_dates[2], {}))
                    if prev_val and prev_val > 0:
                        prev_close = float(prev_val)
                    if bef_val and bef_val > 0:
                        day_before_close = float(bef_val)
                    if prev_close and day_before_close:
                        yesterday_chg = round((prev_close / day_before_close - 1) * 100, 2)
                        logger.info(f"QMT HTTP {stock_code}: 昨收={prev_close}, 昨日涨幅={yesterday_chg}%")
        except Exception as e:
            logger.debug(f"QMT HTTP K线失败 {stock_code}: {e}")

    # ── akshare 兜底：K线获取昨收 + 昨日涨幅 ─────────────────
    if prev_close is None:
        try:
            df = ak.stock_zh_a_hist(symbol=stock_code, period="daily", adjust="qfq")
            if df is not None and len(df) >= 2:
                df = df.tail(5).reset_index(drop=True)
                prev_close = float(df.iloc[-2]["收盘"])      # 倒数第2行=昨天
                day_before_close = float(df.iloc[-3]["收盘"])  # 倒数第3行=前天
                if prev_close and day_before_close:
                    yesterday_chg = round((prev_close / day_before_close - 1) * 100, 2)
                logger.info(f"akshare {stock_code}: 昨收={prev_close}, 昨日涨幅={yesterday_chg}%")
        except Exception as e:
            logger.debug(f"akshare K线失败 {stock_code}: {e}")

    if prev_close is None:
        return None

    # ── Step 2: akshare → PE ────────────────────────────────
    try:
        df_pe = ak.stock_a_indicator_lg(symbol=stock_code)
        if df_pe is not None and not df_pe.empty:
            row = df_pe.tail(1).iloc[0]
            pe_val = row.get("市盈率(PE)") or row.get("市盈率") or row.get("pe_ttm")
            if pe_val and float(pe_val) > 0:
                pe = float(pe_val)
                logger.info(f"akshare PE {stock_code}: {pe}")
    except Exception as e:
        logger.debug(f"akshare PE失败 {stock_code}: {e}")

    # ── Step 3: 腾讯财经 fallback → PE ─────────────────────
    if pe is None:
        try:
            if stock_code.startswith("6"):
                tx_code = f"sh{stock_code}"
            else:
                tx_code = f"sz{stock_code}"
            url = f"https://qt.gtimg.cn/q={tx_code}"
            req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
            with urllib.request.urlopen(req, timeout=8) as resp:
                content = resp.read().decode("gbk", errors="replace")
            import re
            m = re.search(r'="([^"]+)"', content)
            if m:
                fields = m.group(1).split("~")
                # 字段39=市盈率, 字段36=昨收
                if len(fields) > 39 and fields[39] and fields[39] not in ["", "-", "NA"]:
                    pe = float(fields[39])
                    logger.info(f"腾讯财经 PE {stock_code}: {pe}")
        except Exception as e:
            logger.debug(f"腾讯财经 PE失败 {stock_code}: {e}")

    return {
        "prev_close": prev_close,
        "yesterday_chg": yesterday_chg,
        "pe": pe,
    }


def _extract_close(val) -> Optional[float]:
    """从 QMT HTTP close 数据块中提取价格"""
    if val is None:
        return None
    if isinstance(val, (int, float)):
        return float(val) if float(val) > 0 else None
    if isinstance(val, dict):
        v = list(val.values())[0] if val else None
        return float(v) if v and float(v) > 0 else None
    return None


def fetch_sector_strength() -> Optional[Dict[str, Any]]:
    """
    获取今日强势板块 vs 弱势板块
    返回: {
        "hot_sectors": [{"name": str, "chg_pct": float}, ...],
        "cold_sectors": [{"name": str, "chg_pct": float}, ...],
        "market_time": str,
    } 或 None
    """
    try:
        # akshare 行业板块涨跌排行（新浪行业）
        df = ak.stock_sector_spot(indicator="行业")
        if df is None or df.empty:
            return None

        # 按涨跌幅排序
        df = df.sort_values("涨跌幅", ascending=False)

        hot = []
        cold = []

        for _, row in df.iterrows():
            name = str(row.get("板块", ""))
            chg = float(row.get("涨跌幅", 0))
            if len(hot) < 5 and chg > 0:
                hot.append({"name": name, "chg_pct": chg})
            elif len(cold) < 5 and chg < 0:
                cold.append({"name": name, "chg_pct": chg})
        hot = sorted(hot, key=lambda x: -x["chg_pct"])[:5]
        cold = sorted(cold, key=lambda x: x["chg_pct"])[:5]

        from datetime import datetime
        return {
            "hot_sectors": hot[:5],
            "cold_sectors": cold[:5],
            "market_time": datetime.now().strftime("%Y-%m-%d %H:%M"),
        }
    except Exception as e:
        logger.debug(f"板块强弱获取失败: {e}")
        return None
