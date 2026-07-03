import os
import json
import logging
from pathlib import Path
from typing import List, Dict

logger = logging.getLogger("llm_scorer")


def _calculate_tech_from_df(df) -> Dict:
    """从 DataFrame 计算技术指标"""
    import pandas as pd
    if df is None or df.empty or "close" not in df.columns:
        return None
    try:
        close = df["close"].dropna()
        volume = df.get("volume", pd.Series(dtype=float)).dropna()
        if len(close) < 5:
            return None
        close = close.sort_index()
        ma5 = close.rolling(5).mean().iloc[-1]
        ma10 = close.rolling(10).mean().iloc[-1]
        ma20 = close.rolling(20).mean().iloc[-1]
        ma60 = close.rolling(60).mean().iloc[-1] if len(close) >= 60 else None
        current_price = close.iloc[-1]
        avg_volume = volume.iloc[-20:].mean() if len(volume) >= 20 else volume.mean()
        volume_ratio = float(volume.iloc[-1] / avg_volume) if avg_volume > 0 else 0
        delta = close.diff()
        gain = delta.clip(lower=0).rolling(14).mean()
        loss = (-delta.clip(upper=0)).rolling(14).mean()
        rs = gain / (loss + 1e-10)
        rsi = float(100 - 100 / (1 + rs).iloc[-1]) if len(rs) > 0 else 50
        return {
            "close": float(current_price),
            "ma5": round(float(ma5), 2),
            "ma10": round(float(ma10), 2),
            "ma20": round(float(ma20), 2),
            "ma60": round(float(ma60), 2) if ma60 else None,
            "volume_ratio": round(volume_ratio, 2),
            "rsi": round(rsi, 1),
        }
    except Exception:
        return None


def _batch_fetch_tech_via_xqshare(stock_codes: List[str]) -> Dict[str, Dict]:
    """批量获取多只股票技术数据 via XQShare（一次连接全量获取）
    返回: {stock_code: tech_data_dict}
    """
    if not stock_codes:
        return {}

    import pandas as pd

    try:
        import sys as _sys
        _sys.path.insert(0, str(Path(__file__).parent))
        import xqshare

        host = os.environ.get("XQSHARE_HOST", "127.0.0.1")
        port = int(os.environ.get("XQSHARE_PORT", "18812"))
        client = xqshare.connect(host, port, auto_reconnect=True, max_retries=2)
        xtdata = client.xtdata

        def _ensure_suffix(code: str) -> str:
            code = code.strip()
            if code.startswith(("6", "000", "001", "002", "003", "8", "4", "9")):
                return code + ".SH" if not code.endswith(".SH") and not code.endswith(".SZ") else code
            return code + ".SZ" if not code.endswith(".SH") and not code.endswith(".SZ") else code

        stock_list = [_ensure_suffix(s) for s in stock_codes]

        # 批量获取全部历史K线
        data = xtdata.get_market_data(
            field_list=["close", "volume"],
            stock_list=stock_list,
            period="1d",
            count=60,  # 只取最近60个交易日（足够算MA5/10/20/RSI）
            dividend_type="none",
        )

        results = {}
        if isinstance(data, dict):
            close_df = data.get("close")
            vol_df = data.get("volume")
            if close_df is not None and vol_df is not None:
                for stock_raw in stock_codes:
                    stock_suffix = _ensure_suffix(stock_raw)
                    row_idx = None
                    for idx in close_df.index:
                        if idx.replace(".SH", "").replace(".SZ", "") == stock_raw.replace(".SH", "").replace(".SZ", ""):
                            row_idx = idx
                            break
                    if row_idx is None:
                        continue

                    try:
                        close_series = close_df.loc[row_idx]
                        vol_series = vol_df.loc[row_idx]
                        combined = pd.DataFrame({"close": close_series, "volume": vol_series})
                        combined.index = pd.to_datetime(combined.index, format="%Y%m%d")
                        combined = combined.sort_index()
                        td = _calculate_tech_from_df(combined)
                        if td:
                            results[stock_raw] = td
                    except Exception:
                        pass

        xqshare.disconnect()
        return results

    except Exception as e:
        logger.warning(f"XQShare批量获取技术数据失败: {e}")
        return {}


def _batch_fetch_financial_via_xtquant(stock_codes: List[str]) -> Dict[str, Dict]:
    """批量获取多只股票财务数据 via XQShare（一次连接全量获取）
    返回: {stock_code: financial_data_dict}
    """
    if not stock_codes:
        return {}

    import pandas as pd

    try:
        import sys as _sys
        _sys.path.insert(0, str(Path(__file__).parent))
        import xqshare

        host = os.environ.get("XQSHARE_HOST", "127.0.0.1")
        port = int(os.environ.get("XQSHARE_PORT", "18812"))
        client = xqshare.connect(host, port, auto_reconnect=True, max_retries=2)
        xtdata = client.xtdata

        def _ensure_suffix(code: str) -> str:
            code = code.strip()
            if code.startswith(("6", "000", "001", "002", "003", "8", "4", "9")):
                return code + ".SH" if not code.endswith(".SH") and not code.endswith(".SZ") else code
            return code + ".SZ" if not code.endswith(".SH") and not code.endswith(".SZ") else code

        stock_list = [_ensure_suffix(s) for s in stock_codes]

        d = xtdata.get_financial_data(stock_list, ['PERSHAREINDEX'], '', '', 'report_time')

        results = {}
        if not isinstance(d, dict):
            xqshare.disconnect()
            return results

        def get_val(row, col):
            if row is None:
                return None
            v = row.get(col) if hasattr(row, 'get') else None
            if v is None or (isinstance(v, float) and pd.isna(v)):
                return None
            return round(float(v), 2)

        for stock_raw in stock_codes:
            suffix = _ensure_suffix(stock_raw)
            stock_data = d.get(suffix)
            if not stock_data:
                for k in d.keys():
                    if k.replace(".SH", "").replace(".SZ", "") == stock_raw.replace(".SH", "").replace(".SZ", ""):
                        stock_data = d[k]
                        break

            if not stock_data:
                continue

            df = stock_data.get('PERSHAREINDEX')
            if df is None or df.empty:
                continue

            df = df.sort_values('m_timetag', ascending=False)

            annual_df = df[df['m_timetag'].astype(str).str.endswith('1231')]
            quarter_df = df[~df['m_timetag'].astype(str).str.endswith('1231')]

            annual_row = annual_df.iloc[0] if not annual_df.empty else None
            quarter_row = quarter_df.iloc[0] if not quarter_df.empty else None

            roe_annual = get_val(annual_row, 'equity_roe')
            if roe_annual is None:
                continue

            results[stock_raw] = {
                "roe_annual_latest": roe_annual,
                "roe_quarter_latest": get_val(quarter_row, 'equity_roe'),
                "营收增速": get_val(quarter_row, 'inc_revenue_rate'),
                "净利润增长率": get_val(quarter_row, 'inc_net_profit_rate'),
                "负债率": get_val(annual_row, 'gear_ratio'),
            }

        xqshare.disconnect()
        return results

    except Exception as e:
        logger.warning(f"XQShare批量获取财务数据失败: {e}")
        return {}
