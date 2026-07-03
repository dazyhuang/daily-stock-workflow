#!/usr/bin/env python3
"""
止盈三档阈值优化回测
- 读取每日选股报告，提取推荐股票（兼容新旧格式）
- 以选股日次日开盘价买入，模拟三档止盈（每档卖1/3）
- 遍历不同阈值组合，找出最优
- 同时用 AkShare 和 Backtrader 两种方式回测
"""

import json
import os
import sys
import logging
from pathlib import Path
from datetime import datetime, timedelta
from itertools import product
from typing import Dict, List, Tuple, Optional

import pandas as pd
import numpy as np

# ── 配置 ────────────────────────────────────────────────
BASE_DIR = Path(__file__).parent
REPORTS_DIR = BASE_DIR / "output"
QMT_HTTP_HOST = "127.0.0.1"
QMT_HTTP_PORT = 8080  # QMT HTTP 服务端口

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
logger = logging.getLogger(__name__)

# ── 读取历史报告（兼容新旧格式）────────────────────────
def load_recommended_trades():
    """从所有 daily_report_*.json 中提取推荐股票，兼容新旧格式"""
    files = sorted(REPORTS_DIR.glob("daily_report_*.json"))
    logger.info(f"找到 {len(files)} 份选股报告")

    seen = {}  # (date, stock) -> record（去重）

    for f in files:
        try:
            with open(f) as fp:
                r = json.load(fp)
        except Exception as e:
            logger.warning(f"读取 {f.name} 失败: {e}")
            continue

        date = r.get("date") or f.stem.split("_")[-1]
        phase2 = r.get("phase2", {})
        ranked = phase2.get("ranked_candidates", [])

        # 判断格式：新格式有 confidence 数字，旧格式 top_picks 用 action
        if ranked and len(ranked) > 0 and isinstance(ranked[0], dict) and "confidence" in ranked[0]:
            # 新格式（20260507起）
            for s in ranked:
                sig = s.get("signal", "WATCH")
                conf = s.get("confidence", 0)
                key = (date, s.get("stock", "").zfill(6))
                if sig in ("WATCH", "BUY") and conf >= 40 and key not in seen:
                    seen[key] = {
                        "date": date,
                        "stock": s.get("stock", "").zfill(6),
                        "name": s.get("name", ""),
                        "confidence": conf,
                        "signal": sig,
                    }
        else:
            # 旧格式（20260405~20260505，top_picks + action）
            top = phase2.get("top_picks", [])
            for s in top:
                if not isinstance(s, dict):
                    continue
                action = s.get("action", "WATCH")
                if action in ("WATCH", "BUY"):
                    key = (date, s.get("stock", "").zfill(6))
                    if key not in seen:
                        seen[key] = {
                            "date": date,
                            "stock": s.get("stock", "").zfill(6),
                            "name": s.get("name", ""),
                            "confidence": s.get("total_score", 50) or 50,
                            "signal": action,
                        }

    result = list(seen.values())
    logger.info(f"共提取 {len(result)} 条推荐记录（去重后），日期 {min(t['date'] for t in result)} ~ {max(t['date'] for t in result)}")
    return result


# ── 获取历史价格（QMT优先，AkShare兜底）─────────────────
def _get_qmt_price(stock: str, start: str, end: str) -> Optional[pd.DataFrame]:
    """通过 HTTP API 从 QMT 获取日线数据"""
    try:
        import urllib.request, json as _json

        # 转换代码: 000001 -> 000001.SZ, 600498 -> 600498.SH
        s = stock.zfill(6)
        if s.startswith(("0", "3", "002", "001", "300")):
            qmt_code = f"{s}.SZ"
        else:
            qmt_code = f"{s}.SH"

        url = f"http://{QMT_HTTP_HOST}:{QMT_HTTP_PORT}/market_data?stock={qmt_code}&period=1d&start={start}&end={end}&count=300"
        req = urllib.request.Request(url)
        with urllib.request.urlopen(req, timeout=10) as resp:
            data = _json.loads(resp.read())

        if not data.get("success") or not data.get("data"):
            return None

        close_data = data["data"].get("close", {})
        open_data = data["data"].get("open", {})
        high_data = data["data"].get("high", {})
        low_data = data["data"].get("low", {})
        volume_data = data["data"].get("volume", {})
        if not close_data:
            return None

        dates = sorted(close_data.keys())
        rows = []
        for dt in dates:
            cv = close_data[dt]
            ov = open_data.get(dt, {})
            hv = high_data.get(dt, {})
            lv = low_data.get(dt, {})
            vv = volume_data.get(dt, {})
            if qmt_code in cv:
                rows.append({"date": pd.to_datetime(dt), "close": cv[qmt_code],
                            "open": ov.get(qmt_code), "high": hv.get(qmt_code),
                            "low": lv.get(qmt_code), "volume": vv.get(qmt_code)})

        if not rows:
            return None
        df = pd.DataFrame(rows).set_index("date").sort_index()
        df = df[["open", "close", "high", "low", "volume"]].dropna()
        return df if len(df) >= 3 else None
    except Exception as e:
        logger.debug(f"QMT获取{stock}失败: {e}")
        return None


def _get_akshare_price(stock: str, start: str, end: str) -> Optional[pd.DataFrame]:
    """用 AkShare 获取股票历史日线（兜底）"""
    try:
        import akshare as ak

        # 转换代码格式: 600498 -> sh600498, 000001 -> sz000001
        s = stock.zfill(6)
        if s.startswith(("0", "3", "002", "001", "300")):
            code = f"sz{s}"
        else:
            code = f"sh{s}"

        df = ak.stock_zh_a_daily(
            symbol=code,
            start_date=start.replace("-", ""),
            end_date=end.replace("-", ""),
            adjust="qfq"
        )
        if df is None or df.empty:
            return None

        df = df.rename(columns={"date": "date", "open": "open", "close": "close",
                                "high": "high", "low": "low"})
        df["date"] = pd.to_datetime(df["date"])
        df = df[["date", "open", "close", "high", "low"]].dropna()
        df = df.set_index("date").sort_index()
        return df if len(df) >= 3 else None
    except Exception as e:
        logger.debug(f"akshare获取{stock}失败: {e}")
        return None


def get_price(stock: str, buy_date: str) -> Tuple[Optional[float], Optional[pd.DataFrame]]:
    """获取股票在 buy_date 的开盘价（前推10天~后推60天数据）"""
    dt = datetime.strptime(buy_date, "%Y-%m-%d") if "-" in buy_date else datetime.strptime(buy_date, "%Y%m%d")
    start = (dt - timedelta(days=10)).strftime("%Y-%m-%d")
    end = (dt + timedelta(days=60)).strftime("%Y-%m-%d")

    df = _get_qmt_price(stock, start, end)
    if df is not None and len(df) >= 3:
        return df.at[df.index[0], "open"], df

    df = _get_akshare_price(stock, start, end)
    if df is not None and len(df) >= 3:
        return df.at[df.index[0], "open"], df
    return None, None


# ── 模拟单笔三档止盈 ────────────────────────────────────
def simulate_tp3(df: pd.DataFrame, buy_price: float,
                 tp1: float, tp2: float, tp3: float,
                 stop_loss: float = -0.03,
                 hold_days_max: int = 20) -> Optional[Dict]:
    """模拟一笔三档止盈交易，返回 sell_prices 和收益"""

    if df is None or len(df) < 2:
        return None

    tier1_qty = 100 // 3   # 33
    tier2_qty = 100 // 3   # 33
    tier3_qty = 100 - tier1_qty - tier2_qty  # 34

    sell_prices = []
    sold_tier1 = sold_tier2 = False
    sell_reason = ""

    for i, (dt, row) in enumerate(df.iterrows()):
        if i == 0:
            continue  # 跳过买入日

        close = row["close"]
        high = row.get("high", close)
        low = row.get("low", close)
        pnl = (close - buy_price) / buy_price

        # 止损
        if pnl <= stop_loss:
            sell_reason = f"止损({pnl*100:.1f}%)"
            sell_prices.append(close)
            break

        # 止盈第1档（当天最高价触碰即卖）
        if not sold_tier1 and (high - buy_price) / buy_price >= tp1:
            sell_reason = f"止盈1档({(high/buy_price-1)*100:.1f}%)"
            sell_prices.append(close)
            sold_tier1 = True

        # 止盈第2档
        if not sold_tier2 and sold_tier1 and (high - buy_price) / buy_price >= tp2:
            sell_reason = f"止盈2档({(high/buy_price-1)*100:.1f}%)"
            sell_prices.append(close)
            sold_tier2 = True

        # 止盈第3档（到期或数据结束）
        if (i - 1) >= hold_days_max or i == len(df) - 1:
            sell_reason = f"{sell_reason}到期({pnl*100:.1f}%)" if sell_reason else f"到期({pnl*100:.1f}%)"
            if len(sell_prices) == 0:
                sell_prices.append(close)
            elif len(sell_prices) == 1 and not sold_tier1:
                sell_prices.append(close)
            elif len(sell_prices) == 2 and not sold_tier2:
                sell_prices.append(close)
            break

    # 补齐 sell_prices 到3个（未触发档位用最后收盘价）
    while len(sell_prices) < 3:
        sell_prices.append(df.iloc[-1]["close"])

    # 计算总收益率（已实现 + 剩余持仓市值变化）
    realized = (sell_prices[0] - buy_price) * tier1_qty + (sell_prices[1] - buy_price) * tier2_qty
    remaining_value = sell_prices[2] * tier3_qty
    remaining_cost = buy_price * tier3_qty
    total_pnl = realized + (remaining_value - remaining_cost)
    total_cost = buy_price * 100
    total_return = total_pnl / total_cost if total_cost > 0 else 0.0

    return {
        "sell_prices": sell_prices,
        "buy_price": buy_price,
        "total_return": total_return,
        "sell_reason": sell_reason,
    }


# ── 回测所有阈值组合 ────────────────────────────────────
def backtest_all(recommended: List[Dict],
                 tp1_range: List[float],
                 tp2_range: List[float],
                 tp3_range: List[float],
                 stop_loss: float = -0.03,
                 hold_days: int = 20) -> pd.DataFrame:

    logger.info("预获取历史价格...")
    stock_data = {}  # (date, stock) -> (open_price, df)

    for rec in recommended:
        key = (rec["date"], rec["stock"])
        if key in stock_data:
            continue
        open_price, df = get_price(rec["stock"], rec["date"])
        if open_price is not None and open_price > 0:
            stock_data[key] = (open_price, df)

    logger.info(f"成功获取 {len(stock_data)}/{len(recommended)} 只股票价格")
    logger.info(f"开始回测 {len(tp1_range)*len(tp2_range)*len(tp3_range)} 种阈值组合...")

    results = []
    total_combos = len(tp1_range) * len(tp2_range) * len(tp3_range)
    done = 0

    for tp1, tp2, tp3 in product(tp1_range, tp2_range, tp3_range):
        done += 1
        if tp1 >= tp2 or tp2 >= tp3:
            continue

        total_pnl = 0.0
        num_trades = 0
        wins = 0
        losses = 0

        for (date, stock), (buy_price, df) in stock_data.items():
            res = simulate_tp3(df, buy_price, tp1, tp2, tp3, stop_loss, hold_days)
            if res is not None:
                ret = res["total_return"]
                if abs(ret) > 0.0001:  # 过滤无效
                    total_pnl += ret
                    num_trades += 1
                    if ret > 0:
                        wins += 1
                    else:
                        losses += 1

        avg_pnl = total_pnl / num_trades if num_trades > 0 else 0.0
        win_rate = wins / (wins + losses) if (wins + losses) > 0 else 0.0

        results.append({
            "tp1": f"+{tp1*100:.0f}%",
            "tp2": f"+{tp2*100:.0f}%",
            "tp3": f"+{tp3*100:.0f}%",
            "tp1_raw": tp1,
            "tp2_raw": tp2,
            "tp3_raw": tp3,
            "total_pnl": total_pnl,
            "num_trades": num_trades,
            "avg_pnl": avg_pnl,
            "win_rate": win_rate,
            "final_equity": (1 + total_pnl / num_trades) ** num_trades if num_trades > 0 else 1.0,
        })

        if done % 30 == 0:
            logger.info(f"  进度 {done}/{total_combos}")

    df = pd.DataFrame(results)
    df = df.sort_values("total_pnl", ascending=False).reset_index(drop=True)
    return df


# ── Backtrader 方式（简化逐笔模拟）──────────────────────
def backtest_backtrader_style(recommended: List[Dict],
                               tp1_range: List[float],
                               tp2_range: List[float],
                               tp3_range: List[float]) -> pd.DataFrame:
    """用逐笔方式模拟 Backtrader 的事件驱动逻辑"""

    logger.info("Backtrader 模式回测（逐笔事件驱动）...")

    # 预加载数据
    stock_data = {}
    for rec in recommended:
        key = (rec["date"], rec["stock"])
        if key not in stock_data:
            open_price, df = get_price(rec["stock"], rec["date"])
            if open_price is not None and open_price > 0:
                stock_data[key] = (open_price, df)

    results = []
    for tp1, tp2, tp3 in product(tp1_range, tp2_range, tp3_range):
        if tp1 >= tp2 or tp2 >= tp3:
            continue

        total_pnl = 0.0
        num_trades = 0

        for (date, stock), (buy_price, df) in stock_data.items():
            # Backtrader 风格：持仓期间按事件触发，不预知未来
            # 每天以 close 价结算（实际成交用当日 close 模拟）
            tier1_qty = 33
            tier2_qty = 33
            tier3_qty = 34

            sell_prices = []
            sold_tier1 = sold_tier2 = False

            for i, (dt, row) in enumerate(df.iterrows()):
                if i == 0:
                    continue

                close = row["close"]
                high = row.get("high", close)

                # 每天以 close 价作为"可成交价"判断是否触发
                if not sold_tier1 and (high - buy_price) / buy_price >= tp1:
                    sell_prices.append(close)
                    sold_tier1 = True
                if not sold_tier2 and sold_tier1 and (high - buy_price) / buy_price >= tp2:
                    sell_prices.append(close)
                    sold_tier2 = True

                if i >= 19:  # 20天到期
                    if not sell_prices:
                        sell_prices.append(close)
                    elif len(sell_prices) == 1 and not sold_tier1:
                        sell_prices.append(close)
                    elif len(sell_prices) == 2 and not sold_tier2:
                        sell_prices.append(close)
                    break

            # 补齐
            while len(sell_prices) < 3:
                sell_prices.append(df.iloc[-1]["close"])

            realized = (sell_prices[0] - buy_price) * tier1_qty + (sell_prices[1] - buy_price) * tier2_qty
            remaining = (sell_prices[2] - buy_price) * tier3_qty
            ret = (realized + remaining) / (buy_price * 100)
            if abs(ret) > 0.0001:
                total_pnl += ret
                num_trades += 1

        avg_pnl = total_pnl / num_trades if num_trades > 0 else 0.0

        results.append({
            "tp1": f"+{tp1*100:.0f}%",
            "tp2": f"+{tp2*100:.0f}%",
            "tp3": f"+{tp3*100:.0f}%",
            "tp1_raw": tp1,
            "tp2_raw": tp2,
            "tp3_raw": tp3,
            "total_pnl": total_pnl,
            "num_trades": num_trades,
            "avg_pnl": avg_pnl,
            "win_rate": 0.0,
            "final_equity": (1 + total_pnl / num_trades) ** num_trades if num_trades > 0 else 1.0,
        })

    df = pd.DataFrame(results)
    df = df.sort_values("total_pnl", ascending=False).reset_index(drop=True)
    return df


# ── 主程序 ─────────────────────────────────────────────
def main():
    logger.info("=" * 60)
    logger.info("止盈三档阈值优化回测")
    logger.info("=" * 60)

    recommended = load_recommended_trades()
    if not recommended:
        logger.error("没有历史推荐记录，退出")
        return

    # 阈值搜索空间（可调）
    tp1_range = [0.03, 0.04, 0.05, 0.06, 0.07, 0.08]
    tp2_range = [0.08, 0.10, 0.12, 0.15, 0.18]
    tp3_range = [0.15, 0.20, 0.25, 0.30]

    logger.info(f"第1档候选: {[f'+{x*100:.0f}%' for x in tp1_range]}")
    logger.info(f"第2档候选: {[f'+{x*100:.0f}%' for x in tp2_range]}")
    logger.info(f"第3档候选: {[f'+{x*100:.0f}%' for x in tp3_range]}")

    # ── 方式1：含已实现盈亏的总收益率方式 ───────────────
    logger.info("\n=== 方式1：含已实现盈亏的总收益率法 ===")
    df1 = backtest_all(recommended, tp1_range, tp2_range, tp3_range, stop_loss=-0.03, hold_days=20)

    if not df1.empty:
        print("\n========== 总收益率法 TOP 10 ==========")
        print(df1.head(10).to_string(index=False))

        best = df1.iloc[0]
        print(f"\n🏆 最优: tp1={best['tp1']} tp2={best['tp2']} tp3={best['tp3']}")
        print(f"   总收益率: {best['total_pnl']*100:.2f}%")
        print(f"   均笔收益: {best['avg_pnl']*100:.2f}%")
        print(f"   交易笔数: {best['num_trades']}")

        out = BASE_DIR / "output" / "tp_optimization_total_return.csv"
        df1.to_csv(out, index=False)
        logger.info(f"结果已保存: {out}")
    else:
        logger.error("方式1无结果")

    # ── 方式2：Backtrader 风格（逐日事件驱动）───────────
    logger.info("\n=== 方式2：Backtrader 逐日事件驱动法 ===")
    df2 = backtest_backtrader_style(recommended, tp1_range, tp2_range, tp3_range)

    if not df2.empty:
        print("\n========== Backtrader 风格 TOP 10 ==========")
        print(df2.head(10).to_string(index=False))

        best2 = df2.iloc[0]
        print(f"\n🏆 最优: tp1={best2['tp1']} tp2={best2['tp2']} tp3={best2['tp3']}")
        print(f"   总收益率: {best2['total_pnl']*100:.2f}%")
        print(f"   均笔收益: {best2['avg_pnl']*100:.2f}%")
        print(f"   交易笔数: {best2['num_trades']}")

        out2 = BASE_DIR / "output" / "tp_optimization_backtrader.csv"
        df2.to_csv(out2, index=False)
        logger.info(f"结果已保存: {out2}")
    else:
        logger.error("方式2无结果")

    logger.info("\n✅ 回测完成")


if __name__ == "__main__":
    main()